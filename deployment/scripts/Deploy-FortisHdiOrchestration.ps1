#
# Deploy_FortisServices.ps1

Param(
    [string] [Parameter(Mandatory=$true)] $DeploymentStorageAccountName,
    [string] [Parameter(Mandatory=$true)] $ArtifactStagingDirectory,
    [string] [Parameter(Mandatory=$true)] $ResourceGroupName,
    [string] [Parameter(Mandatory=$true)] $ResourceGroupLocation,
    [string] [Parameter(Mandatory=$true)] $AppName,
    [string] [Parameter(Mandatory=$true)] $ClusterName,
	[string] [Parameter(Mandatory=$true)] $ClusterStorageAccountName,
    [string] [Parameter(Mandatory=$true)] $ClusterStorageAccountKey,
    [string] [Parameter(Mandatory=$true)] $DataStorageAccountName,
    [string] [Parameter(Mandatory=$true)] $DataStorageAccountKey,
    [string] [Parameter(Mandatory=$true)] $HdiUserName,
    [string] [Parameter(Mandatory=$true)] $HdiPassword,
    [string] [Parameter(Mandatory=$true)] $SshUserName,
    [string] [Parameter(Mandatory=$true)] $SshPassword,
    [string] [Parameter(Mandatory=$true)] $ServicePrincipalClientId,
    [string] [Parameter(Mandatory=$true)] $ServicePrincipalSecret,
    [string] [Parameter(Mandatory=$true)] $ServicePrincipalDomain,
    [string] [Parameter(Mandatory=$true)] $ServicePrincipalSubscriptionId,
    [string] [Parameter(Mandatory=$true)] $PyPath,
    [string] [Parameter(Mandatory=$true)] $PyFile,
    [Boolean] [Parameter(Mandatory = $false)] $DeploySites = $true,
    [string[]] [Parameter(Mandatory=$false)] $PipDependencyPackages = @("nltk", "langid"),
    [string] [Parameter(Mandatory=$false)] $ZipCmd = "C:\Program Files\7-Zip\7z.exe"
)

# Create or update the resource group using the specified template file and template parameters file
$FortisHdiRG = Get-AzureRmResourceGroup $ResourceGroupName -ErrorAction SilentlyContinue

if ($FortisHdiRG -eq $null) {
    Write-Host "Deploying ResourceGroup $ResourceGroupName"
    $OptionalParameters = New-Object -TypeName Hashtable
    $OptionalParameters.Add("appName", $AppName)
    $OptionalParameters.Add("clusterName", $ClusterName)
    $OptionalParameters.Add("servicePrincipalClientId", $ServicePrincipalClientId)
    $OptionalParameters.Add("servicePrincipalDomain", $ServicePrincipalDomain)
    $OptionalParameters.Add("servicePrincipalSecret", $ServicePrincipalSecret)
    $OptionalParameters.Add("servicePrincipalSubscriptionId", $ServicePrincipalSubscriptionId)
    $OptionalParameters.Add("clusterStorageAccountName", $ClusterStorageAccountName)
    $OptionalParameters.Add("clusterStorageAccountKey", $ClusterStorageAccountKey)
	$OptionalParameters.Add("clusterContainer", "hdi")
    $OptionalParameters.Add("dataStorageAccountName", $DataStorageAccountName)
    $OptionalParameters.Add("dataStorageAccountKey", $DataStorageAccountKey)
    $OptionalParameters.Add("clusterLoginUserName", $HdiUserName)
    $OptionalParameters.Add("clusterLoginPassword", $HdiPassword)
    $OptionalParameters.Add("sshUserName", $SshUserName)
    $OptionalParameters.Add("sshPassword", $SshPassword)

    $FortisHdiRG = .\Deploy-AzureResourceGroup -ResourceGroupLocation $ResourceGroupLocation `
        -ResourceGroupName $ResourceGroupName `
        -OptionalParameters $OptionalParameters `
        -TemplateFile '..\Templates\fortis-hdinsight-controller.json' `
        -TemplateParametersFile '..\Templates\fortis-hdinsight-controller.parameters.json' `
        -UploadArtifacts -StorageAccountName $DeploymentStorageAccountName `
		-ArtifactStagingDirectory $ArtifactStagingDirectory
}
else {
    Write-Host "ResourceGroup $ResourceGroupName already exists"
}

if ($FortisHdiRg -ne $null) {
    $Deployment = Get-AzureRmResourceGroupDeployment $ResourceGroupName

    # Zip pip depenendencies

    $httpsFolderUri = "https://$clusterStorageAccountName.blob.core.windows.net/$ClusterName/fortis"

    Write-Host "Create zip file"
    $zipFile = "artifacts.zip"
    $a = "a"

    $pipCmd = "pip"
    $pipShowCmd = "show"
    $pipInstallCmd = "install"

    foreach ($package in $PipDependencyPackages)
    {
        $packageOutput = &$pipCmd $pipShowCmd $package
        if ($packageOutput.Length -eq 0)
        {
            Write-Host "Installing pip package" $package 
            &$pipCmd $pipInstallCmd $package
            $packageOutput = &$pipCmd $pipShowCmd $package
        }

        # match location output from pip command
        $packagePath = $packageOutput -match "^Location: (.*)\\site-packages$"
        if ($packagePath.Length -eq 0) 
        {
            Write-Host "Failed to locate package" $package        
        }
        else
        {
            # Remove "Location:" prefix and "site-packages" suffix 
            $packagePath = $packagePath[0]
            $packagePath = $packagePath.Substring(10, $packagePath.Length - 24) 

            # Use "*" instead of "site-packages" to keep folder structure intact
            $packagePath = ($packagePath + "\*\" + $package)
            Write-Host "Zipping pip package" $packagePath "with" $ZipCmd
            &$ZipCmd $a $zipFile $packagePath
        }
    }

    $rootPath = (Get-Item -Path ".\" -Verbose).FullName
    $azCopyPath = [System.IO.Path]::Combine($rootPath, "..\Tools\AzCopy.exe")
    $pyRootPath = (Get-Item -Path $PyPath -Verbose).FullName
    
    # Push Python script and zipped pip dependencies

    $RootPath = (Get-Item -Path ".\" -Verbose).FullName
    # $AzCopyPath = [System.IO.Path]::Combine($PSScriptRoot, "..\Tools\AzCopy.exe")

    &$azCopyPath $pyRootPath $httpsFolderUri $PyFile /DestKey:$clusterStorageAccountKey /Y
    &$azCopyPath $rootPath $httpsFolderUri $zipFile /DestKey:$clusterStorageAccountKey /Y
}
else {
    Write-Host "Failed to create resource group"
}

if ($DeploySites -eq $true) {
	Push-Location
	cd..
	cd..

	cd hdinsight-controller
	git remote remove azure
	git remote add azure https://$AzureGitAccount@$AppName.scm.azurewebsites.net:443/$AppName.git
	git push azure master
	cd..

	Pop-Location
}