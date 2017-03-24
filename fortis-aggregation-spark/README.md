# fortis-aggregation-spark

A set of Spark scripts for aggregating social media data stats by keyword, time, location, etc.

## Configure the incremental behavior of the 'byTile' job**

The 'byTile' job can run either with an assumption that it has run before (i.e., incremental) or not. In general, we will always run the script incrementally, however, changing this assumption is useful for bootstrapping the process (the first time you run the script there is no incremental data to 'join' against).

To switch the script between incremental or not, change the value [here](https://github.com/CatalystCode/project-fortis/blob/master/fortis-aggregation-spark/jobs/bytileAggregator.py#L550).

## Configure the data of the 'byTile' job**

The streams of social media messages are aggregated by a StreamAnalytics job into the `fortis-messages` container in Azure Blob Storage. Each day, the job creates a new folder, at which point the previous days' data is static for processing.

The 'byTile' job is parameterized on a date so it knows which file to process from `fortis-messages`.  You can configure that date. If you want to run the script for all dates in `fortis-messages` you can just replace the parameter with "*", but this is not recommended.


## Output of the Spark scripts

After the 'byTile' job is complete, the script will have written all messages plus a list of keywords and a sentiment score for each message in the `processed-messages` container.  It will also have written the data that needs to be added / updated in Postgres in the `processed-tiles` container.  Lastly, it will have replaced the contents of the `processed-tiles-prev` container with the latest aggregation results for all time.

After the 'timeSeries' job is complete, the script will have written the aggregated keyword data to `processed-timeseries`.

Obviously each of these output containers is configurable, and it is advisable to try things out in different containers from the ones listed here before trying to run the scripts against the containers used for production.
