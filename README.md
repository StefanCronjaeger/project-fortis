# Fortis
Fortis implements a data gathering and intelligence pipeline that collects, analyzes and aggregates information from social media outlets and other public / private web data sources 

## Configure and deploy the pipeline

### Pre-requisites to deploy

* Windows 8.x or 10
* Powershell Version 5. or higher - you can check the version by running `$PSVersionTable.PSVersion` - also ensure you allow the execution of remote signed scripts by running `Set-ExecutionPolicy RemoteSigned`
* [Azure Powershell cmdlets](https://docs.microsoft.com/en-us/powershell/azureps-cmdlets-docs/) - you can install them by running `Install-Module AzureRM` 
* [Git](https://git-scm.com/download/win) - ensure the path is known to Powershell - it is recommended to enable the [credentials cache](https://help.github.com/articles/caching-your-github-password-in-git/) as well as to install [posh-git](https://github.com/dahlbyk/posh-git): 
```
git config --global credential.helper wincred

PowerShellGet\Install-Module posh-git -Scope CurrentUser
Update-Module posh-git 
```

* [7-Zip](http://www.7-zip.org/) - it is expected to be installed to `C:\Program Files\7-Zip\7z.exe`. If you prefer another location, you can adjust the deployment script with your path [here](https://github.com/CatalystCode/project-fortis/blob/master/deployment/scripts/Deploy-FortisHdiOrchestration.ps1#L27)
* [python 2.7.9](https://www.python.org/downloads/windows/) or higher

### Cloning the repo
in addition to just cloning the repo, you also need to fetch the submodules:
```
git clone
git submodule init 
git submodule update --init --remote
git submodule foreach git checkout master
git submodule foreach git pull origin
```

### Obtaining tokens and keys

* Get your azure subscription id by running `Get-AzureSubscription` in PowerShell

* Ensure you set 
* Get the Twitter tokens for the location inference webjobs by creating a new app [here](https://apps.twitter.com/)
* Get the Facebook token (TODO: describe how to do this!!!)
* Get the tokens for the translation service (TODO: describe how to do this!!!)
* Create a comma separated list of the most common words for your language of interest (e.g from [here](http://www.101languages.net/common-words/))
* Create the bounding box for the geo-inference service. The format is `west_long, south_lat, east_long, north_lat`. You can obtain them through your preferred mapping tool. As an example, here the rough bounding box for Indonesia and Sri Lanka `19.938716, 8.948158, 32.938335,25.129859`


### Running the deployment script

* Navigate to  [deployment/scripts](./deployment/scripts) and open your PowerShell in elevated mode (Admin mode) 
* Login to your Azure account by running `Login-AzureRmAccount`
* Execute  `Deploy-FortisServices` : 

```
.\Deploy-FortisServices `
    -SubscriptionId <YOUR_AZURE_SUBSCRIPTION_ID> `
	-SkuName "S2" `
	-SkuCapacity 1 `
	-GeoTwitSkuName "S2" `
	-GeoTwitSkuCapacity 1 `
    -DeploymentPostFix <YOUR_UNIQUE_INSTANCE_ID> `
	-ResourceGroupName <YOUR_RESOURCE_GROUP_NAME> `
	-Location "West Europe" `
	-GeoTwitConsumerKey <YOUR_TOKEN> `
    -GeoTwitConsumerSecret <YOUR_TOKEN>  `
    -GeoTwitAccessTokenKey <YOUR_TOKEN>  `
    -GeoTwitAccessTokenSecret <YOUR_TOKEN>  `
	-GeoTwitLanguageFilter <YOUR_LANGUAGE_FILTER> ` # a comma separated list of twitter language ids e.g. en,in 
	-GeoTwitFilterKeywords <YOUR_GEOTWIT_FILTER_KEYWORDS> ` # a comma separated list of words: "the, a, I,me"
	-GeoTwitBoundingBox <YOUR_BOUNDING_BOX> `# e.g. "23.503229, 60.884577, 36.896913, 75.540339" `
	-FacebookToken <YOUR_TOKEN> `
	-PostgresUser <YOUR_POSTGRES_USER> `
	-PostgresPassword <YOUR_STRONG_POSTGRES_PASSWORD> `
	-SiteName <YOUR_SITE_NAME> `
	-TranslationServiceClientId <YOUR_TRANSLATION_ID> `
	-TranslationServiceClientSecret <YOUR_TRANSLATION_TOKEN> `
	-HdiPassword <YOUR_STRONG_HDI_CLUSTER_PASSWORD> `
	-AzureGitAccount <YOUR_AZURE_GIT_USER> `
    -DeployHdi $true `
	-DeployServices $true `
	-DeploySites $true  `
	-CreateSite $true
```
The above script creates all required Azure resources and deploys all services. 



