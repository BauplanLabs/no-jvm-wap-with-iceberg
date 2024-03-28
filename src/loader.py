"""

Sample script to run locally that will create some tabular data (given a few parameters)
and upload the resulting table as a parquet file to S3.

The file upload will trigger the WAP process in the lambda (contained in the serverless folder),
resulting either in a successful merge of the new rows or a Slack notification of an error.

NOTE: follow the setup instructions in the README to ensure the lambda is deployed *before* you run the 
script.

Example usage: 

python loader.py -bucket mybucket --no-null -n 1000

> upload a parquet file with 1000 rows, without nulls (so any data check for no-nulls will succeed), to the
bucket named "mybucket". Make sure "mybucket" is the same one you used when deploying the lambda (check the
README for the full setup).

"""

import os
from datetime import datetime
import boto3
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
# make sure this is reproducible
from random import randint, seed, random
seed(10)
# preload the friendlywords package
import friendlywords as fw
fw.preload()


def generate_schema() -> pa.Schema:
    """
    
    Given some arrow types, generate a table with templated column names and
    the declared types.
    
    """
    interesting_types = [
        pa.int32(),
        pa.string(),
        pa.float64()
    ]
    column_names = ['my_col_{}'.format(_) for _ in range(len(interesting_types))]
    return pa.schema([
        (_, t) for _, t in zip(column_names, interesting_types)
    ])


def get_random_array_from_type(
    n: int, # n of rows
    field: pa.Field,
    no_null: bool = False
):
    """
    
    Based on the arrow type, generate a pyArrow array which will become a column
    in the final table. If you need more types, add them here.
    
    If the flag no_null is passed, we add a null value to each column: this will
    simulate a "faulty" batch of rows, and any test for no-nulls downstream will
    therefore fail.
    
    """
    a = [] if no_null else [None]
    rows = n if no_null else n - 1
    if field.type == pa.int64():
        return pa.array([randint(1, 131731739) for _ in range(rows)] + a)
    elif field.type == pa.string():
        return pa.array([fw.generate('ppo', separator=' ') for _ in range(rows)] + a)
    elif field.type == pa.float64():
        return pa.array([float(_ * random())  for _ in range(rows)] + a)
    else:
        raise ValueError('Unsupported type: {}'.format(field.type))


def generate_table_from_schema(
    n: int, 
    schema: pa.Schema,
    no_null: bool = False
) -> pa.Table:
    """
    
    Given a schema, return an Arrow table with n rows filled with random values.
    
    """
    data = {
        field.name: get_random_array_from_type(n, field, no_null=no_null)
        for field in schema
    }
    return pa.table(data, schema=schema)
    

def generate_table(
    n: int, # number of rows
    no_null: bool = False, # not include null in the table
    verbose: bool = False # verbose print
):
    """
    
    Generate an Arrow table by quickly create some random arrays as columns,
    with n as the number of rows. To generate a different data layout, feel free to
    change the schema declaration.
    
    """
    # first get the schema
    schema = generate_schema()
    if verbose:
        print("Generated schema: {}".format(schema))
    # then generate the table as in-memory arrow table
    arrow_table = generate_table_from_schema(n, schema, no_null=no_null)
    # make sure the table has the right size
    assert n == arrow_table.num_rows
    
    return arrow_table


def upload_table_to_s3(
    table: pa.Table,
    bucket: str,
    s3_client,
    verbose: bool = False
):
    # write a parquet file in a temp directory and upload to s3
    file_name = str(uuid.uuid4())
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = os.path.join(tmpdirname, file_name)
        if verbose:
            print("Writing parquet file to {}".format(file_path))
        writer = pq.ParquetWriter(file_path, table.schema)
        writer.write_table(table)
        writer.close()
        if verbose:
            print("Uploading to s3 bucket {}".format(bucket))
        # upload to s3 using the boto client
        s3_client.Bucket(bucket).upload_file(file_path, file_name)
    
    return
    

def load_raw_data_to_s3(
    bucket: str,  # bucket name
    n: int, # number of rows
    no_null: bool = False, # not null
    verbose: bool = False # verbose print
):
    """
    
    Entry point for the loader, this function takes as input the command line
    arguments and orchestrate the various steps in the loading process.
    
    """
    # start processing
    print("\nStarting loader at {}".format(datetime.now()))
    
    # verify the target bucket exists - get a boto3 client
    s3 = boto3.resource("s3")
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except Exception as e:
        print(e)
        print("Error: bucket {} does not exist".format(bucket))
        return
    # generate some data
    table = generate_table(n, no_null=no_null, verbose=verbose)
    if verbose:
        print("Generated table with {} rows".format(table.num_rows))
    # upload to s3
    upload_table_to_s3(table, bucket, s3_client=s3)
    if verbose:
        print("Data uploaded to s3 bucket {}".format(bucket))
    
    # say goodbye
    print("\nAll done at {}\nSee you, space cowboy".format(datetime.now()))

    return


if __name__ == "__main__":
    import argparse
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-bucket", help="S3 bucket to upload the parquet file to", required=True)
    parser.add_argument("-n", help="Number of rows to generate", default=10000, type=int)
    parser.add_argument("--no-null", help="If set, will not include nulls in the data", action="store_true")
    parser.add_argument("--verbose", help="If set, will print more information", action="store_true")
    # run the script
    load_raw_data_to_s3(
        bucket=parser.parse_args().bucket,
        n=parser.parse_args().n,
        no_null=parser.parse_args().no_null,
        verbose=parser.parse_args().verbose
    )

