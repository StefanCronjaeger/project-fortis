# Fortis
Fortis implements a pipeline to observe domain related Twitter streams across time and location. 
It leverages the Twitter streaming api by filtering on a set of keywords, language and a geographical bounding box.
As part of the pipeline, we infere groups (the occurence of a keyword combination) as well as the location for tweets
which haven't been geo-tagged. The final output of the pipeline is an aggregation of keywords and groups across time 
(or other dimensions such as location). 

This pipeline is a useful tool for gathering intelligence. For instance, we used it for better planning the need for humatarian aid or for fighting epidemic diseases 
(such as an outbreak of dengue fever). Infact, the keyword configuration of this repo was used to filter on dengue fever 
related tweets within Indonesia and Sri Lanka. We then aggregated the data across time and location and visualized it 
as part of a research board.

## Clone the repo
in addition to just cloning the repo, you also need to fetch the submodules:
```
git clone
git submodule init 
git submodule update --init --remote
git submodule foreach git checkout master
git submodule foreach git pull origin
```

## Configure and deploy the pipeline
TBD

You should obtain the Twitter tokens by creating a new app [here](https://apps.twitter.com/)
As the final preparation, you need to obtain the bounding box for the geo-filtering of tweets:
e.g. here the rough bounding box for Indonesia and Sri Lanka ``"19.938716 , 8.948158, 32.938335,25.129859"``

Finally, navigate to  [deployment/scripts](./deployment/scripts) and open your PowerShell in elevated mode and execute  ``Deploy-FortisServices`` : 

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



