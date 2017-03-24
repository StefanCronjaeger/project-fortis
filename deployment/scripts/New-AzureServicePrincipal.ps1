# Requires -RunAsAdministrator
# for details on adding the secret:
# https://www.sabin.io/blog/adding-an-azure-active-directory-application-and-key-using-powershell/
Param (

[Parameter(Mandatory=$true)]
[String] $SubscriptionId,

[Parameter(Mandatory=$true)]
[String] $CertPlainPassword,

[Parameter(Mandatory=$false)]
[String] $ApplicationDisplayName = "FortisHdiController",

[Parameter(Mandatory=$false)]
[int] $NoOfMonthsUntilExpired = 120
)

Import-Module Azure -ErrorAction SilentlyContinue

#Login-AzureRmAccount
Import-Module AzureRM.Resources
Select-AzureRmSubscription -SubscriptionId $SubscriptionId

Write-Host "Check if we already have a Certificate"
# Find the cert Required
$CertToExport = dir cert:\LocalMachine\My | where {$_.Subject -eq "CN=$ApplicationDisplayName"}
if ($CertToExport -ne $null){
	Write-Host "We found an existing certificate  $CertPath"
	# Export The Targeted Cert In Bytes For The CER format
	$Cert = $CertToExport.export("Cert")
	$KeyValue = [System.Convert]::ToBase64String($Cert)
}
else
{
	$CertPath = Join-Path $env:TEMP ($ApplicationDisplayName + ".pfx")
	Write-Host "Create the certificate  $CertPath"
	$Cert = New-SelfSignedCertificate -DnsName $ApplicationDisplayName -CertStoreLocation cert:\LocalMachine\My -KeyExportPolicy Exportable -Provider "Microsoft Enhanced RSA and AES Cryptographic Provider"
	$KeyValue = [System.Convert]::ToBase64String($Cert.GetRawCertData())
}
Write-Host "Cert Key is $KeyValue"

# We use the last 44 characters of the KeyValue. (We basically use the certificate for storing the secret). This allows us to retrieve it
# in the case we already have created a AD application
$Secret = $KeyValue.Substring($KeyValue.Length - 44, 44)
Write-Host "Secret is $Secret"

$PsadCredential = New-Object Microsoft.Azure.Commands.Resources.Models.ActiveDirectory.PSADPasswordCredential  
$StartDate = Get-Date  
$PsadCredential.StartDate = $StartDate  
$PsadCredential.EndDate = $StartDate.AddMonths($NoOfMonthsUntilExpired)  
$PsadCredential.KeyId = [guid]::NewGuid()  
$PsadCredential.Password = $Secret 

Write-Host "Check if we already have a ADApplication for $ApplicationDisplayName"
$Application = Get-AzureRmADApplication -DisplayNameStartWith $ApplicationDisplayName
if ($Application -eq $null){
	Write-Host "Create ADApplication $ApplicationDisplayName"
	$CurrentDate = Get-Date
	$EndDate = $CurrentDate.AddMonths($NoOfMonthsUntilExpired)
	$KeyId = (New-Guid).Guid
	$Application = New-AzureRmADApplication -DisplayName $ApplicationDisplayName `
											-HomePage ("http://" + $ApplicationDisplayName) `
											-IdentifierUris ("http://" + $KeyId) `
											-PasswordCredentials $PsadCredential 

	Write-Host "create new service principal"
	$ServicePrincipal = New-AzureRMADServicePrincipal -ApplicationId $Application.ApplicationId -ErrorAction SilentlyContinue 
	Write-Host "ServicePrincipal Details:"
	Write-Host ($ServicePrincipal | Out-String)

	$NewRole = $null
	$Retries = 0;
	While ($NewRole -eq $null -and $Retries -le 2)
	{
	  Write-Host "Sleep here for a few seconds to allow the service principal application to become active (should only take a couple of seconds normally)"
	  Sleep 5
	  New-AzureRMRoleAssignment -RoleDefinitionName Contributor -ServicePrincipalName $Application.ApplicationId -ErrorAction SilentlyContinue  
	  Sleep 5
	  $NewRole = Get-AzureRMRoleAssignment -ServicePrincipalName $Application.ApplicationId -ErrorAction SilentlyContinue
	  $Retries++;
	}
	Write-Host "Role Details:"
	Write-Host ($NewRole | Out-String)

}
Write-Host "ADApplication Details:"
Write-Host ($Application | Out-String)

Write-Host "Get the tenant id for this subscription"
$SubscriptionInfo = Get-AzureRmSubscription -SubscriptionId $SubscriptionId
$TenantID = ($SubscriptionInfo | Select TenantId -First 1).TenantId

return @{ ServicePrincipalClientId = $Application.ApplicationId;  
	 ServicePrincipalSecret = $Secret;
	 ServicePrincipalDomain = $TenantID;
     ServicePrincipalSubscriptionId = $SubscriptionId;}
