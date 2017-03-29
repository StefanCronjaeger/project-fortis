#
# Deploy_FortisServices.ps1

Param(
	[string] [Parameter(Mandatory=$true)]  $SkuName,
	[int][Parameter(Mandatory=$true)] $SkuCapacity,
	[string] [Parameter(Mandatory=$true)]  $GeoTwitSkuName,
	[int][Parameter(Mandatory=$true)] $GeoTwitSkuCapacity,
    [string] [Parameter(Mandatory=$true)][ValidateLength(1,6)] $DeploymentPostFix,
    [string] [Parameter(Mandatory=$true)] $Location,
    [string] [Parameter(Mandatory=$true)] [ValidateLength(1,14)] $ResourceGroupName,
    [string] [Parameter(Mandatory=$true)] $SubscriptionId,
    [string] [Parameter(Mandatory=$true)] $GeoTwitLanguageFilter,
    [string] [Parameter(Mandatory=$true)] $GeoTwitFilterKeywords,
	[string] [Parameter(Mandatory=$true)] $GeoTwitBoundingBox,
    [string] [Parameter(Mandatory=$true)] $GeoTwitConsumerKey, 
    [string] [Parameter(Mandatory=$true)] $GeoTwitConsumerSecret, 
    [string] [Parameter(Mandatory=$true)] $GeoTwitAccessTokenKey, 
    [string] [Parameter(Mandatory=$true)] $GeoTwitAccessTokenSecret, 
	[string] [Parameter(Mandatory=$true)] $FacebookToken,
	[string] [Parameter(Mandatory=$true)] $TranslationServiceClientId,
    [string] [Parameter(Mandatory=$true)] $TranslationServiceClientSecret,
    [string] [Parameter(Mandatory=$true)] $SiteName, 
    [string] [Parameter(Mandatory=$true)] $HdiPassword,
    [string] [Parameter(Mandatory=$true)] $PostgresPassword,
    [string] [Parameter(Mandatory=$true)] $PostgresUser,
    [string] [Parameter(Mandatory=$true)] $AzureGitAccount,
    [Boolean] [Parameter(Mandatory = $true)] $DeployHdi,
	[Boolean] [Parameter(Mandatory = $true)] $DeployServices,
    [Boolean] [Parameter(Mandatory = $true)] $DeploySites,
	[Boolean] [Parameter(Mandatory = $true)] $CreateSite)
    

function GenerateKey() {
    $bytes = New-Object Byte[] 32
    $rand = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rand.GetBytes($bytes)
    $rand.Dispose()
    $key = [System.Convert]::ToBase64String($bytes)
    Write-Host $key

    return $key
}
function Convert-HashToString
{
    param
    (
        [Parameter(Mandatory = $true)]
        [System.Collections.Hashtable]
        $Hash
    )
    $hashstr = ""
    $keys = $Hash.keys
    foreach ($key in $keys)
    {
        $v = $Hash[$key]
        $hashstr += $key + "=" + $v + "`n"
    }
    return $hashstr
}

if (Get-Module -ListAvailable -Name Azure) {
    #configure powershell with Azure modules
	Import-Module Azure -ErrorAction SilentlyContinue
} else {
    Write-Host "Azure Module does not exist - see https://docs.microsoft.com/en-us/powershell/azureps-cmdlets-docs/ for installation guidance"
	exit
}


Write-Host "This script needs to run in an elevated shell (as Administrator)"
Write-Host "Before you start, you need to do the following things:"
Write-Host "1.) Login-AzureRmAccount"
Write-Host "---Press a key when ready---"
$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")


Set-AzureSubscription -SubscriptionId $SubscriptionId
Select-AzureRmSubscription -SubscriptionId $SubscriptionId
Select-AzureSubscription -SubscriptionId $SubscriptionId

function Create-StorageAccountIfNotExist {
    [CmdletBinding()] 
    param ( 
        [string] [Parameter(Mandatory = $true)] $StorageRGName,
        [string] [Parameter(Mandatory = $true)] $StorageAccountName
    )

     $StorageAccount = Get-AzureRmStorageAccount | Where-Object {$_.StorageAccountName -eq $StorageAccountName }  
     if ($StorageAccount -eq $null) { 
         Write-Host "create storage account $StorageAccountName in $Location" 
         New-AzureRmResourceGroup -Name $StorageRGName -Location $Location
         New-AzureRmStorageAccount -ResourceGroup $StorageRGName -AccountName $StorageAccountName -Location $Location -Type "Standard_GRS"
     } 
     else { 
         Write-Host "storage $StorageAccountName already exists" 
     }   
}

if (($DeployServices -eq $true) -Or ($DeployHdi -eq $true) -Or ($DeploySites -eq $true)) {
	$DeploymentResourceGroupName = $ResourceGroupName+"-Deployment"
	$ResourceGroupNameStoragePrefix = $ResourceGroupName
	$ResourceGroupNameStoragePrefix = $ResourceGroupNameStoragePrefix -Replace "[^a-zA-Z0-9]", ''
	$DeploymentStorageAccountName = ($ResourceGroupNameStoragePrefix + "deployment").ToLower()
	Create-StorageAccountIfNotExist $DeploymentResourceGroupName $DeploymentStorageAccountName
	$DeploymentStorageAccountContext = (Get-AzureRmStorageAccount | Where-Object{$_.StorageAccountName -eq $DeploymentStorageAccountName}).Context    
	$DeploymentStorageAccountKey = (Get-AzureRmStorageAccountKey -ResourceGroupName $DeploymentResourceGroupName -AccountName $DeploymentStorageAccountName).Value[0]

	Write-Host "Copy the deployment templates to Templates"
	$ArtifactStagingDirectoryName = "Templates"
	$ArtifactStagingDirectory = "..\$ArtifactStagingDirectoryName"
	Push-Location
	cd ..
	new-item $ArtifactStagingDirectoryName -itemtype directory -ErrorAction "Ignore"
	copy ..\*\deploy\*.json .\$ArtifactStagingDirectoryName\
	ls .\$ArtifactStagingDirectoryName\.
	Pop-Location
}
$HdiControllerName = ("fortishdictrl"+$DeploymentPostFix).toLower()
$HdiClusterName = ("fortishdi"+$DeploymentPostFix).toLower()

$PostgresDnsName = ("fortisspatial"+$DeploymentPostFix).toLower()
$LocationDnsString = $Location.Replace(" ", "").ToLower()

$PostgresUser = $PostgresUser.ToLower()
$PostgresUri = $PostgresDnsName+"."+$LocationDnsString+".cloudapp.azure.com"
$PostgressConnectionString = "formatpostgres://"+$PostgresUser+":"+ $PostgresPassword +"@"+$PostgresUri+"/fortis"
Write-Host $PostgressConnectionString

$FortisRG = Get-AzureRmResourceGroup $ResourceGroupName -ErrorAction SilentlyContinue
$SpatialResourceGroupName = $ResourceGroupName+"-Spatial"
$FortisSpatialRG = Get-AzureRmResourceGroup $SpatialResourceGroupName -ErrorAction SilentlyContinue

if ($DeployServices -eq $true) {
	if ($FortisRG -eq $null) {

		# Copy bash script to storage account container
		$StorageContainerName = "postgres-setup"
		$SetupScript = "install_and_setup_postgres.sh"

		copy $SetupScript "$SetupScript.bck"

		(Get-Content "$SetupScript") | 
		Foreach-Object {$_ -replace '___POSTGRESS_PW___',$PostgresPassword} |
		Foreach-Object {$_ -replace '___POSTGRESS_USER___',$PostgresUser} |

    
		Out-File $SetupScript -Encoding utf8

		New-AzureStorageContainer -Name $StorageContainerName -Context $DeploymentStorageAccountContext -Permission Container -ErrorAction SilentlyContinue *>&1
		Set-AzureStorageBlobContent -File $SetupScript -Blob $SetupScript -Container $StorageContainerName -Context $DeploymentStorageAccountContext -Force

		Remove-Item "$SetupScript" 
		copy "$SetupScript.bck" $SetupScript

		$ScriptUris = "https://$DeploymentStorageAccountName.blob.core.windows.net/$StorageContainerName/$SetupScript"

		Write-Host "Deploying ResourceGroup $ResourceGroupName"
		$OptionalParameters = New-Object -TypeName Hashtable
		$OptionalParameters.Add("skuName", $SkuName)
		$OptionalParameters.Add("skuCapacity", $SkuCapacity)
		$OptionalParameters.Add("geoTwitSkuName", $GeoTwitSkuName)
		$OptionalParameters.Add("geoTwitSkuCapacity", $GeoTwitSkuCapacity)
		$OptionalParameters.Add("deploymentPostFix", $DeploymentPostFix)
		$OptionalParameters.Add("siteName", $SiteName)
		$OptionalParameters.Add("translationServiceClientId", $TranslationServiceClientId)
		$OptionalParameters.Add("translationServiceClientSecret", $TranslationServiceClientSecret)
		$OptionalParameters.Add("geoTwitLanguageFilter", $GeoTwitLanguageFilter)
		$OptionalParameters.Add("geoTwitBoundingBox", $GeoTwitBoundingBox) 
		$OptionalParameters.Add("geoTwitFilterKeywords", $GeoTwitFilterKeywords)
		$OptionalParameters.Add("geoTwitConsumerKey", $GeoTwitConsumerKey) 
		$OptionalParameters.Add("geoTwitConsumerSecret", $GeoTwitConsumerSecret) 
		$OptionalParameters.Add("geoTwitAccessTokenKey", $GeoTwitAccessTokenKey) 
		$OptionalParameters.Add("geoTwitAccessTokenSecret", $GeoTwitAccessTokenSecret) 
		$OptionalParameters.Add("postgresAdminUsername", $PostgresUser)
		$OptionalParameters.Add("postgresAdminPassword", $PostgresPassword)
		$OptionalParameters.Add("postgresDnsNamePrefix", $PostgresDnsName)
		$OptionalParameters.Add("postgresVmSize", "Standard_DS3_v2")
		$OptionalParameters.Add("postgresUbuntuOSVersion", "16.04.0-LTS")
	    $OptionalParameters.Add("deploymentAccountName", $DeploymentStorageAccountName)
		$OptionalParameters.Add("deploymentAccountKey", $DeploymentStorageAccountKey)
	    $OptionalParameters.Add("postgresDeploymentScriptUris", $ScriptUris)
		$OptionalParameters.Add("hdiControllerName", $HdiControllerName)
		$OptionalParameters.Add("hdiClusterName", $HdiClusterName)
		$key = GenerateKey
		$OptionalParameters.Add("eventHubSendPrimaryKeyKeywords", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubSendSecondaryKeyKeywords", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubListenPrimaryKeyKeywords", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubListenSecondaryKeyKeywords", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubSendPrimaryKeyFacts", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubSendSecondaryKeyFacts", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubListenPrimaryKeyFacts", $key )
		$key = GenerateKey
		$OptionalParameters.Add("eventHubListenSecondaryKeyFacts", $key )

		$FortisRG = .\Deploy-AzureResourceGroup -ResourceGroupLocation $Location `
			-ResourceGroupName $ResourceGroupName `
			-OptionalParameters $OptionalParameters `
			-TemplateFile '..\Templates\Fortis.json' `
			-TemplateParametersFile '..\Templates\Fortis.parameters.json' `
			-UploadArtifacts -StorageAccountName $DeploymentStorageAccountName `
			-ArtifactStagingDirectory $ArtifactStagingDirectory
	}
	else {
		Write-Host "ResourceGroup $ResourceGroupName already exists"
	}
}
$Deployment = Get-AzureRmResourceGroupDeployment $ResourceGroupName
$DataStorageAccountName = $Deployment.Outputs.dataStorageAccountName.Value
$DataStorageAccountKey = $Deployment.Outputs.dataStorageAccountKey.Value
$DataStorageAccountConnectionString = $Deployment.Outputs.dataStorageAccountConnectionString.Value
$ClusterStorageAccountName = $Deployment.Outputs.clusterStorageAccountName.Value
$ClusterStorageAccountKey = $Deployment.Outputs.clusterStorageAccountKey.Value
$WebJobWebSiteName = $Deployment.Outputs.webJobWebSiteName.Value
$KeywordsSAJobName = $Deployment.Outputs.keywordsSAJobName.Value
$WebjobWebSiteName = $Deployment.Outputs.webJobWebSiteName.Value
$FunctionsWebSiteName = $Deployment.Outputs.functionsWebSiteName.Value
$DashboardWebSiteName = $Deployment.Outputs.dashboardWebSiteName.Value
$GeoTwitWebSiteName = $Deployment.Outputs.geoTwitWebSiteName.Value
$ServicesWebSiteName = $Deployment.Outputs.servicesWebSiteName.Value

if ($DeployHdi -eq $true) {
	$HdiResourceGroupName = $ResourceGroupName+"-hdi"    
	
	$ServicePrincipal = .\New-AzureServicePrincipal `
	-SubscriptionId $SubscriptionId `
	-CertPlainPassword "Test2@Test!"

	Write-Host ($ServicePrincipal | Out-String)

	Write-Host "Deploying ResourceGroup $HdiResourceGroupName"
	.\Deploy-FortisHdiOrchestration `
		-DeploymentStorageAccountName $DeploymentStorageAccountName `
		-ArtifactStagingDirectory $ArtifactStagingDirectory `
		-ResourceGroupName $HdiResourceGroupName `
		-ResourceGroupLocation $Location `
		-AppName $HdiControllerName `
		-ClusterName $HdiClusterName `
		-ClusterStorageAccountName $ClusterStorageAccountName `
		-ClusterStorageAccountKey $ClusterStorageAccountKey `
		-DataStorageAccountName $DataStorageAccountName `
		-DataStorageAccountKey $DataStorageAccountKey `
		-HdiUserName "admin" `
		-HdiPassword $HdiPassword `
		-SshUserName "sshadmin" `
		-SshPassword $HdiPassword `
		-ServicePrincipalClientId $ServicePrincipal.ServicePrincipalClientId `
		-ServicePrincipalSecret $ServicePrincipal.ServicePrincipalSecret `
		-ServicePrincipalDomain $ServicePrincipal.ServicePrincipalDomain `
		-ServicePrincipalSubscriptionId $ServicePrincipal.ServicePrincipalSubscriptionId `
		-DeploySites $DeploySites `
		-PyPath "..\..\fortis-aggregation-spark\jobs" `
		-PyFile "bytileAggregator.py"
}
if ($DeploySites -eq $true) {
    Write-Host "Starting $KeywordsSAJobName stream analytics job"
    Start-AzureRMStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $KeywordsSAJobName
    
    Push-Location
    cd..
    cd..
	
	git submodule foreach git pull origin master

	cd fortis-services
    git remote remove azure
    git remote add azure https://$AzureGitAccount@$ServicesWebSiteName.scm.azurewebsites.net:443/$ServicesWebSiteName.git
    git push azure master 
    cd..

    cd fortis-webjobs
    git remote remove azure
    git remote add azure https://$AzureGitAccount@$WebjobWebSiteName.scm.azurewebsites.net:443/$WebjobWebSiteName.git
    git push azure master
    cd..

    cd fortis-geotwit
    git remote remove azure
    git remote add azure https://$AzureGitAccount@$GeoTwitWebSiteName.scm.azurewebsites.net:443/$GeoTwitWebSiteName.git
    git push azure master
    cd..

    cd fortis-functions
    git remote remove azure
    git remote add azure https://$AzureGitAccount@$FunctionsWebSiteName.scm.azurewebsites.net:443/$FunctionsWebSiteName.git
    git push azure master 
    cd..

    cd fortis-interfaces
    git remote remove azure
    git remote add azure https://$AzureGitAccount@$DashboardWebSiteName.scm.azurewebsites.net:443/$DashboardWebSiteName.git
    git push azure master
    cd..

    Pop-Location

}
if ($CreateSite -eq $true) {
Write-Host "Create site $SiteName"
$Body = @'
{
"variables": {
  "input": {
    "defaultZoomLevel": 6,
    "name":  "{site_id}",
	"fbToken": "{fb_token}",
    "storageConnectionString": "{azure_storage_connection_string}",
    "featuresConnectionString": "{postgres_connection_string}",
    "supportedLanguages": ["en"]
  }
},
"query": "mutation CreateOrReplaceSite($input: SiteDefinition!) {createOrReplaceSite(input: $input) {name}}"  
}
'@
	$Body = $Body.Replace("{site_id}", $SiteName)
	$Body = $Body.Replace("{fb_token}", $FacebookToken)
	$Body = $Body.Replace("{azure_storage_connection_string}", $DataStorageAccountConnectionString)
	$Body = $Body.Replace("{postgres_connection_string}", $PostgressConnectionString)

	Write-Host $Body
	$Response = Invoke-RestMethod -Uri https://$ServicesWebSiteName.azurewebsites.net/api/settings -ContentType "application/json" -Method POST -Body $Body

	Write-Host $Response
}

"https://$DashboardWebSiteName.azurewebsites.net/#/site/$SiteName/admin"