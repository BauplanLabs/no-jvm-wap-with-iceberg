# Write-Audit-Publish on a data lake (no JVM!)

## Overview

We aim to provide a no-nonsense, reference implementation for the Write-Audit-Publish (WAP) pattern on a data lake, using Iceberg as the open table format, Nessie as a branch-aware data catalog and PyIceberg (plus some custom Python code) as code abstraction over our tables: all the logic is expressed in Python (_no JVM!_) and all infrastructure is provisioned and run for us by AWS.

In the spirit of interoperability, we provide an example downstream application (a quality dashboard) - that also avoids the JVM and can run entirely within a Python interpreter -, and a bonus section on how to query the final table with Snowflake: fully leveraging the lakehouse pattern, we can move (at least some) data quality checks _outside the warehouse_ and still take advantage of Snowflake for querying certified artifacts.

Note that the project is not intended to be a production-ready solution, but rather a reference implementation that can be used as a starting point for more complex scenarios: all the code is verbose and heavily commented, making it easy to modify and extend the basic concepts to better suit your use cases.

## Setup

### Prerequisites

The intent of this project is mostly pedagogical, so dependencies and frameworks have been
kept to a minimum:

* AWS credentials with appropriate permissions when the local scripts run;
* the [serverless framework](https://www.serverless.com/framework/) to deploy the WAP lambda with one command;
* Docker installed locally to prepare the lambda container.
* BONUS: Slack, if you wish to receive failure notifications from the lambda on Slack; 
* BONUS: a Snowflake account if you wish to query the post-ETL table with Snowflake.

### Installation

#### Setup S3 buckets and a Nessie catalog

In `src/serverless`, copy `local.env` to `.env` and fill the values for two buckets: `SOURCE_BUCKET` is the name of the bucket simulating ingestion of raw data, `LAKE_BUCKET` is the bucket that we connect to the data catalog, containing the Iceberg version of the data. 

The buckets will get created and a Nessie server will be deployed as a Lightsail service through a simple `boto3` script:

```bash
cd src
python setup_nessie.py -v=create -n=nessieservice
```

After the script runs, you should get in the terminal the URL of the Nessie service, which we will use to interact with the data catalog. _Before continuing, check with the AWS Lighsail console if the service is finished deployed and the endpoint is up._

#### Local environment

Prepare a virtual environment and install the dependencies for the local scripts:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Fill the rest of the `.env` file with the following variables: `SLACK_TOKEN` and `SLACK_CHANNEL` are needed if you wish to send failure notifications to Slack - you can get a Bot Token by creating a new Slack App in your [Slack workspace](https://api.slack.com/tutorials/tracks/getting-a-token).`NESSIE_ENDPOINT` is the URL of the Nessie service you just deployed with the above setup script.

#### AWS Lambda

The lambda is deployed with the serverless framework, so make sure it's installed. You can deploy the lambda with:

```bash
cd src/serverless
serverless deploy
```

Note: if you don't want to use Slack as a notification channel, leave the relevant variables in the `.env` file empty and the lambda will not attempt to send notifications.

## Run the project

Make sure the Python environment is activated and the `.env` file is properly configured.

### Start the loader

Then cd into `src` and run the following command:

```bash
python loader.py --no-null -n 1000 --verbose
```

The loader scrit will write 1000 records to a table and upload it to `SOURCE_BUCKET`, triggering the lambda.

### Start the webapp

#TODO: my idea is that the web app is a simple streamlit app that for example my count how many rows there are in main, so everytime your refresh it it will show the updated count.

Alternatively, it could also be a query over a branch that you can specify as an input field, such that it shows how many NULLs there are in case of failures.

### Scenarios

#### No errors

Running

```bash
python loader.py --no-null -n 1000 --verbose
```

will produce "properly formatted data", that will survive the quality checks (checking for nulls), and therefore result in the main table over in `main` to be updated with the new data.

#### Errors

Running

```bash
python loader.py -n 1000 --verbose
```

will produce data with NULLs, so the quality check in the lambda will fail, the upload branch won't be merged and the Slack channel should notify you of the failure.

## Bonus: querying the final table with Snowflake

A consequence of using Iceberg tables for ingestions and ETL is that we can move out of the warehouse costly data quality checks and still leverage the warehouse for querying the final, certified data. In this bonus section, we show how to query the final table in Snowflake.

Settinp up Snowflake to query the table we created in the data lake is quite cumbersome but not hard: unfortunately, it is a manual process going back and forth between AWS IAM roles and Snowflake instructions, as detailed in these three steps (you need all three):

* add your S3 bucket with the data lake file as an [external volume](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume);
* configure an [external catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration) for Iceberg tables;
* finally, [create a Snowflake table from Iceberge metadata](https://docs.snowflake.com/en/user-guide/tables-iceberg-create#label-tables-iceberg-create-catalog-int) (it's a no-op, only involving metadata): to finalize it, check the current metadata file associated with the table in the main branch in [Nessie][images/nessie.png] and use the `METADATA_FILE_PATH` to create the table in Snowflake -> e.g.

```sql
CREATE ICEBERG TABLE customer_cleaned_data
  EXTERNAL_VOLUME='icebergwapvolume'
  CATALOG='myWapInt'
  METADATA_FILE_PATH='metadata/0000xxx-xxxx-xxxxx.metadata.json';
```

In this case, we are creating a Snowflake external table called `CUSTOMER_CLEANED_DATA` that represents the rows in our ingestion table after they have been certified by our WAP process. Once you have the table, you can run any query [you like in the Snwoflake UI](images/snowflake.png): e.g. we compute the sum of a column, the average of another and the total number of rows in the table (note that the total number of rows is the same as what is displayed in our webapp above!):

```sql
SELECT SUM(MY_COL_0), AVG(MY_COL_2), COUNT(*) AS TOT_ROWS FROM CUSTOMER_CLEANED_DATA;
```

Of course, now that the volume and the external catalog are set up, we could leverage the Snowflake Python connector to automatically create (in the lambda) the table from the metadata file after every merge (by reading the metadata from PyIceberg after WAP is completed).

We leave this as an exercise to the reader!

## FAQs

#TODO

## License

We provide the code as a reference, pedagogical implementation of the Write-Audit-Publish pattern on a data lake, using entirely Python in the business logic and AWS services for the infrastructure. The code is provided as-is, with no warranties or guarantees of any kind and it's licensed under the MIT License. Feel free to use it, modify it, extend it, but please provide attribution to the original authors.

