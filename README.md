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

## Run the project

### Start the loader


### Start the webapp


### Scenarios

#### No errors


#### Errors


## Bonus: querying the final table with Snowflake


## FAQs


## License
