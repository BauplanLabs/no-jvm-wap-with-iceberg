# Write-Audit-Publish on a data lake (no JVM!)

## Overview

We aim to provide a no-nonsense, reference implementation for the Write-Audit-Publish (WAP) pattern on a data lake, using Iceberg as the open table format, Nessie as a branch-aware data catalog and PyIceberg (plus some
custome Python code) as code abstraction over our tables: all the logic is expressed in Python (_no JVM!_) and all
infrastructure is provisioned and run for us by AWS.

In the spirit of interoperability, we provide an example downstream application (a quality dashboard) - that
also avoids the JVM and can run entirely within a Python interpreter -, and a bonus section on how to query the final
table with Snowflake: fully leveraging the lakehouse pattern, we can move (at least some) data quality checks _outside the warehouse_ and still take advantage of Snowflake for querying certified artifacts.

Note that the project is not intended to be a production-ready solution, but rather a reference implementation that can be used as a starting point for more complex scenarios: all the code is verbose and heavily commented, making it easy to modify and extend the basic concepts to better suit your use cases.

## Setup

### Prerequisites

The intent of this project is mostly pedagogical, so dependencies and frameworks have been
kept to a minimum:

* AWS credentials with appropriate permissions when the local scripts run;
* the serverless framework to deploy the WAP lambda with one command;
* Docker installed locally to prepare the lambda container.
* BONUS: a Snowflake account if you wish to query the post-ETL table with Snowflake!

### Installation

#### Setup Nessie

The Nessie server is deployed as a Lightsail service, and the installation is available as a simple `boto3` script:

```bash
cd src
python setup_nessie.py -v=create -n=nessieservice
```

After the script runs, you should get in the terminal the URL of the Nessie service, which we will use to interact with the data catalog. Check with the AWS Lighsail console if the service is finished deployed and the endpoint is up.

#### Local environment

Prepare a virtual environment and install the dependencies for the local scripts:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

In `src/serverless`, copy `local.env` to `.env` and fill in the required values: `SOURCE_BUCKET` is the name of the bucket simulating ingestion of raw data, `LAKE_BUCKET` is the bucket connected to the data catalog and containing the Iceberg version of the data.

#### AWS Lambda

The lambda is deployed with the serverless framework, so make sure it's installed. Then, you can deploy the lambda with:

```bash
cd src/serverless
serverless deploy
```

## Run the project

Make sure the python environment is activated and the `.env` file is properly configured.

### Start the loader

Then cd into `src` and run the following command:

```bash
python loader.py --no-null -n 1000 --verbose
```

This will start the loader, which will write 1000 records to a table and upload it to the SOURCE_BUCKET in the `.env` file, triggering the lambda.

### Start the webapp


### Scenarios

#### No errors


#### Errors


## Bonus: querying the final table with Snowflake


## FAQs


## License
