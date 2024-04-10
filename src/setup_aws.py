"""

Amazon Lightsail is a lightweight service for running small containerized apps, such as a Nessie server
for a prototype (https://projectnessie.org/try/docker/). Since we already assume familiarity with Python and boto3
in the project, we wrote a small script that deploys a containerized Nessie catalog to Lightsail. The basic
catalog is not suited for production use cases, but it's more than enough for our prototype (plus, using boto3 help us
avoid including complex tools like Terraform or CloudFormation in the project).

The code below should be self-explanatory, and it's mostly just wrapping the AWS calls (we thank ChatGPT for 
giving us a good start with this script).

Example:

python setup_nessie.py -v=create -n=nessieservice

> creates a new container service named "NessieAppService" in Lightsail and deploys a container with the Nessie image.

Note that every time you run it, the script will try to create (if not already created) the source and lake buckets
to store the raw and transformed data (Iceberg tables), respectively. Make sure your .env file is filled
correctly as specified in the README.

"""


import boto3
import os
import json


def deploy_lightsail_application(
    service_name: str = 'nessieservice',
):
    # Create a boto3 client for the Lightsail service
    client = boto3.client('lightsail', region_name='us-east-1')
    
    # Create or identify your Lightsail container service
    # Note: This code assumes the service does not already exist and will create it.
    # If your service already exists, you would instead call describe_container_services
    try:
        print(f"Creating container service: {service_name}")
        client.create_container_service(
            serviceName=service_name,
            power='micro',  # The instance size of the container service
            scale=1  # The number of instances to run in the container service
        )
    except client.exceptions.ServiceException as e:
        # If service already exists or other service exception, print the message
        print(f"Service exception: {e}")
        return
    
    # Define the container deployment parameters
    deployment = {
        'containers': {
            'nessie-container': {
                'image': 'ghcr.io/projectnessie/nessie',
                'ports': {
                    '19120': 'HTTP'
                }
            }
        },
        'publicEndpoint': {
            'containerName': 'nessie-container',
            'containerPort': 19120,
            'healthCheck': {
                'path': '/',
                'intervalSeconds': 10,
                'timeoutSeconds': 5,
                'healthyThreshold': 2,
                'unhealthyThreshold': 2
            }
        }
    }
    
    # Create a new deployment with the specified container
    print("Creating new deployment...")
    _result = client.create_container_service_deployment(
        serviceName=service_name,
        containers=deployment['containers'],
        publicEndpoint=deployment['publicEndpoint']
    )
    
    print(f"Deployment created. Container service '{service_name}' is deploying the specified container.")
    print("\nNessie will be available soon at {}: check the Lightsail dashboard for the Status".format(
        _result['containerService']['url']
    ))
    
    return


def create_bucket_if_not_exists(
    s3,
    target_bucket: str
) -> bool:
    """
    
    Create a bucket in S3 if it does not exist and return True
    if the bucket is created, False otherwise (already exists).
    
    """
    
    try:
        s3.meta.client.head_bucket(Bucket=target_bucket)
        print(f"Data lake bucket {target_bucket} already exists.")
        return False
    except Exception as e:
        new_bucket = s3.create_bucket(Bucket=target_bucket)
        print(f"Data lake bucket {target_bucket} created.")

    return True


def create_buckets(
    source_bucket: str,
    lake_bucket: str
):
    """
    
    Create the source and lake buckets if they don't exist. The source bucket
    is where the raw data is stored / uploaded, and the lake bucket is where the transformed
    data is stored as an Iceberg table.
    
    """
    
    # get a boto3 client
    s3 = boto3.resource("s3")
    # create the source bucket
    _is_created = create_bucket_if_not_exists(s3, source_bucket)
    if _is_created:
        print(f"Source bucket {source_bucket} created.")
    # create the lake bucket
    _is_created = create_bucket_if_not_exists(s3, lake_bucket)
    if _is_created:
        print(f"Lake bucket {lake_bucket} created.")
    
    return True





if __name__ == '__main__':
    # parse the CLI arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", help="Action to execute: create or destroy", required=True)
    parser.add_argument("-n", help="Name of the container service", required=True)
    # add a verbose flag for more verbose debugging
    parser.add_argument("--verbose", help="If set, will print more information", action="store_true")
    
    from dotenv import load_dotenv
    load_dotenv('serverless/.env')
    # we need to make sure the two buckets exist before we start
    assert os.environ['SOURCE_BUCKET'], "Please set the SOURCE_BUCKET environment variable"
    assert os.environ['LAKE_BUCKET'], "Please set the LAKE_BUCKET environment variable"
    source_bucket = os.environ['SOURCE_BUCKET']
    lake_bucket = os.environ['LAKE_BUCKET']
    # we create the two buckets if they don't exist
    create_buckets(source_bucket, lake_bucket)
    # now, if the action is 'create', we deploy the container service
    if parser.parse_args().v == 'create':
        deploy_lightsail_application(
            service_name=parser.parse_args().n
        )
    else:
        raise NotImplementedError("Only the 'create' action is implemented.")
