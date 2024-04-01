"""

This is the main Python file for the lambda. lambda_handler is the entry point
and it's triggered by an S3 event (every time a new file is uploaded to the source bucket, which
in this project happens when we run the loader.py from our laptop).

The function leverages the PyIceberg library to interact with the Nessie catalog (thanks to
our integration with PyNessie and monkey patching) and provide the WAP logic:

* read the new rows from the S3 event into an arrow table;
* create an uploading new branch in the Nessie catalog from main;
* append the new rows to the table in the branch;
* run a quality check on the new rows re-using PyCessie to scan the table back in the branch;
* if the quality check is successful, merge the branch into the main table.

"""


import os
from time import time
# arrow imports
import pyarrow as pa
import pyarrow.parquet as pq
# data catalog imports
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import IsNull
import monkey_patch
from pyiceberg_patch_nessie import NessieCatalog
# slack imports
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
# preload the friendlywords package
import friendlywords as fw
fw.preload()


# name of the table in the Nessie catalog
# we have only one table, to which we append the new rows
# as they come in in the source bucket
TABLE_NAME = 'customer_data_log'


# decorator
def measure_func(func):
    """
    
    This function shows the execution time of the function object passed -
    this is convenient when debugging to keep track of where the program
    spends its time.
    
    """
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


@measure_func
def send_slack_alert(
    table_name,
    branch_name
):
    """
    
    Send a slack alert to the channel specified in the environment variables.
    
    If no Slack related envs are specified, we assume that we don't want to send
    any notifications and we just return.
    
    """
    # we first check if the slack envs are set
    # otherwise we just return
    if not (os.environ['SLACK_TOKEN'] and os.environ['SLACK_CHANNEL']):
        print("Slack envs not set, skipping alert")
        return False

    slack_token = os.environ['SLACK_TOKEN']
    slack_channel = os.environ['SLACK_CHANNEL']
    
    client = WebClient(token=slack_token)
    message = "Quality check failed on table {} in branch {}".format(table_name, branch_name)

    try:
        result = client.chat_postMessage(
            channel=slack_channel,
            text=message
        )
        print(f"Message sent successfully, message ID: {result['ts']}")
        return True
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")
    
    return False


@measure_func
def read_rows_into_arrow(record) -> pa.Table:
    """
    
    Read a parquet file in S3 into an arrow table.
    
    """
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    # make sure we are trying to read a parquet file
    assert key.endswith('.parquet'), "Only parquet files are supported"
    s3_path = f"s3://{bucket}/{key}"
    cnt_table = pq.read_table(s3_path)
    print("Table has {} rows".format(cnt_table.num_rows))
    
    return cnt_table


@measure_func
def create_table_if_not_exists(catalog, table_name, branch, schema, datalake_location):
    """
    
    Create a table in the Nessie catalog if it does not exist by first checking
    the tables in the main branch, and then creating the table with the schema.
    
    Note that we return True if the table is created, False otherwise (in theory,
    just on the first run the table should get created).
    
    """
    tables = catalog.list_tables('main')
    print("Tables in the catalog in {}: {}".format(branch, tables))
    # this is the result of the list_tables call when a table is there
    # [('main@2031105876d7dbd8f57fcff4820863edebf25fdbb7b75b184a915acd8cac5484', 'customer_data_log')]
    if any([table_name == t[1] for t in tables]):
        return False
    
    rt_0 = catalog.create_table(
        identifier=('main', table_name),
        schema=schema,
        location=datalake_location,
    )
    tables = catalog.list_tables('main')
    print("Now Tables in the catalog in {}: {}".format(branch, tables))
    
    return True


@measure_func
def create_branch_from_main(catalog):
    """
    
    Create a random branch with a human readable name starting from main.
    
    """
    
    branch_name = fw.generate('ppo', separator='-')
    catalog.create_branch(branch_name, 'main')

    return branch_name


@measure_func
def append_rows_to_table_in_branch(
    catalog, 
    table_name, 
    branch_name, 
    arrow_table
    ):
    """
    
    Add the new rows in the arrow table to the table in the branch.
    
    """
    try:
        _table = catalog.load_table((branch_name, table_name))
        _table.append(arrow_table)
    except Exception as e:
        print("Error appending rows to table: {}".format(e))
        return False
    
    return True

@measure_func
def run_quality_checks(
    catalog, 
    table_name, 
    branch_name
):
    """
    
    Run quality checks on the table in the branch, which by this point should
    contain all the new rows (and the old one). 
    
    Obviously, this is a simulation, and we could have just use the arrow table representing
    the new rows to obtain the exact same result instead of reading back the rows from the
    object storage!
    
    However, by reading back the rows from the Iceberg table, we are simulating the possibility of
    doing a further processing step in the pipeline that could happen literally anywhere,
    for example in a Dremio / Trino job, a separate Python script within a larger orchestration platform, 
    a Snowflake query etc. 
    
    Note that here we could have also used SQL instead of Python, for example by running duckdb queries 
    on the arrow table directly in-process.
    
    Moreover, we provide a working example of leveraging the Nessie catalog with Pyiceberg
    to perform a S3 scan using Python, which we believe is a useful feature to have anyway, even
    setting aside WAP.
    
    The function returns True if the quality check is successful (no nulls!), False otherwise.
    
    """
    # what's the column we should check for nulls?
    target_column = "my_col_1"
    
    try:
        # we get a reference to the iceberg table in the branch
        _table = catalog.load_table((branch_name, table_name))
        # we pushed down the scan by specifying 
        # both a specific column and a filter
        scan = _table.scan(
            row_filter=IsNull(target_column),
            selected_fields=(target_column,) # need to pass a tuple!
        ).to_arrow()
        # we should have no results in the scan, so
        # if the number of rows is 0, the quality check is successful
        return scan.num_rows == 0
        
    except Exception as e:
        print("Quality check failed: {}".format(e))
        
    # if there is an exception, we return False
    return False
    

def lambda_handler(event, context):
    """
    
    This is the entry point for the lambda function. The function is triggered
    by an S3 event (every time a new file is uploaded to the source bucket).
    
    The function reads the parquet file from the source bucket, opens a branch in the data catalog,
    and commits the changes there.
    
    It then performs a quality check (simulating a furthere processing step in the pipeline) and, if the
    check is successful, merge the branch into the main table.
    
    """
    # print a copy of the event in cloudwatch
    # for debugging purposes
    print(event)
    # make sure the environment variables are set
    assert os.environ['SOURCE_BUCKET'], "Please set the SOURCE_BUCKET environment variable"
    assert os.environ['LAKE_BUCKET'], "Please set the LAKE_BUCKET environment variable"
    # get the records from the event
    records = event['Records']
    if not records:
        print("No records found in the event")
        return None
    
    # this is needed so that pynessie can write to the local filesystem
    # some configuration for the user...
    os.environ['NESSIEDIR'] = '/tmp'
    # initialize the Nessie catalog
    catalog = load_catalog(
        name='default',
        type='nessie',
        endpoint=os.environ['NESSIE_ENDPOINT'],
        default_branch='main'
    )    
    # if the target table does not exist in main (first run), create it
    datalake_location = 's3://{}'.format(os.environ['LAKE_BUCKET'])
    # loop over the records, even if it should be 1
    for record in records:
        # get the new rows out as an arrow table
        arrow_table = read_rows_into_arrow(record)
        # make sure the table in main is there (it won't be there at the first run)
        # we use the current schema to create the table for simplicity
        is_created = create_table_if_not_exists(
            catalog, 
            TABLE_NAME, 
            'main', 
            arrow_table.schema, 
            datalake_location
        )
        print("Table created: {}".format(is_created))
        # create a new branch in the catalog from main
        branch_name = create_branch_from_main(catalog)
        print("Branch created: {}".format(branch_name))
        # write the new rows to the branch
        _appended = append_rows_to_table_in_branch(
            catalog, 
            TABLE_NAME, 
            branch_name, 
            arrow_table
        )
        if not _appended:
            print("Error appending rows to branch")
            return None
        # quality check the rows in the branch (simulation)
        _success = run_quality_checks(catalog, TABLE_NAME, branch_name)
        # if successful, merge the branch into the main table
        if _success:
            print("Quality check passed, merging branch: {}".format(branch_name))
            catalog.merge(branch_name, 'main')
            catalog.drop_branch(branch_name)
            print("Branch merged and dropped")
        else:
            # if not, send a slack alert
            print("Quality check failed, not merging branch")
            _sent = send_slack_alert(
                table_name=TABLE_NAME,
                branch_name=branch_name
            )
            if _sent:
                print("Slack alert sent")

    return None



