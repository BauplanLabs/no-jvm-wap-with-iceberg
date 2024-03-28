"""

Amazon Lightsail is a lighweight service for running small containerized apps, such as a Nessie server
for a prototype (https://projectnessie.org/try/docker/). Since we already assume familiarity with Python and boto3
in the project, we wrote a small script that deploys a containerized Nessie catalog to Lightsail. The basic
catalog is not suited for production use cases, but it's more than enough for our prototype (plus, using boto3 help us
avoid including complex tools like Terraform or CloudFormation in the project).

The code below should be self-explanatory, and it's mostly just wrapping the AWS calls (we thank ChatGPT for 
giving us a good start with this script).

Example:

python setup_nessie.py -v=create -n=NessieAppService

> creates a new container service named "NessieAppService" in Lightsail and deploys a container with the Nessie image.

python setup_nessie.py -v=destroy -n=NessieAppService

> destroys the container service named "NessieAppService" in Lightsail.


"""

import boto3
import json


def deploy_lightsail_application():
    # Create a boto3 client for the Lightsail service
    client = boto3.client('lightsail')
    
    # Specify the container service name
    service_name = 'NessieAppService'
    
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
                'timeoutSeconds': 10,
                'healthyThreshold': 2,
                'unhealthyThreshold': 2
            }
        }
    }
    
    # Create a new deployment with the specified container
    print("Creating new deployment...")
    client.create_container_service_deployment(
        serviceName=service_name,
        containers=deployment['containers'],
        publicEndpoint=deployment['publicEndpoint']
    )
    
    print(f"Deployment created. Container service '{service_name}' is deploying the specified container.")
    
    return


def destroy_lightsail_application():
    
    return


if __name__ == '__main__':
    # parse the CLI arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", help="Action to execute: create or destroy", required=True)
    parser.add_argument("-n", help="Name of the container service", required=True)
    # add a verbose flag for more verbose debugging
    parser.add_argument("--verbose", help="If set, will print more information", action="store_true")
    
    if parser.parse_args().v == 'create':
        deploy_lightsail_application()
    elif parser.parse_args().v == 'destroy':
        destroy_lightsail_application()
    

