import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
import monkey_patch
from pyiceberg_patch_nessie import NessieCatalog
from time import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
# preload the friendlywords package
import friendlywords as fw
fw.preload()


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
def send_slack_alert():
    """
    
    Send a slack alert to the channel specified in the environment variables.
    
    If no Slack related envs are specified, we assume that we don't want to send
    any notifications and we just return.
    
    """
    # we first check if the slack envs are set
    # otherwise we just return
    if not (os.environ['SLACK_TOKEN'] and os.environ['SLACK_CHANNEL']):
        print("Slack envs not set, skipping alert")
        return None

    slack_token = os.environ['SLACK_TOKEN']
    slack_channel = os.environ['SLACK_CHANNEL']
    
    client = WebClient(token=slack_token)
    message = "test test test"

    try:
        result = client.chat_postMessage(
            channel=slack_channel,
            text=message
        )
        print(f"Message sent successfully, message ID: {result['ts']}")
        return True
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")
    
    return None


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
    
    # initialize the Nessie catalog
    #catalog = load_catalog(
    #    name='default',
    #    type='nessie',
    #    endpoint='https://nessieservice.90cuqda0rgpdi.us-east-1.cs.amazonlightsail.com',
    #    default_branch='main'
    #)
    datalake_location = 's3://{}'.format(os.environ['LAKE_BUCKET'])
    # loop over the records, even if it should be 1
    for record in records:
        # get the new rows out as an arrow table
        arrow_table = read_rows_into_arrow(record)
        # create a new branch in the catalog
        #branch_name = fw.generate('ppo', separator='-')
        #print("Creating branch {}".format(branch_name))
        #catalog.create_branch(branch_name, 'main')
        # write the new rows to the branch
        
        # quality check the rows in the branch (simulation)
        # is_successful = quality_check(table_name, branch_name)
        # if successful, merge the branch into the main table
        
        # if not, send a slack alert
        _sent = send_slack_alert()

    return None



