import boto3


def lambda_handler(event, context):
    # Specify the EMR cluster parameters
    emr_cluster_name = "your-emr-cluster-name"
    emr_release_label = "emr-6.4.0"  # Use the desired EMR release label
    emr_instance_type = "m5.xlarge"   # Use the desired EC2 instance type
    num_core_instances = 2

    # Create an EMR client
    emr_client = boto3.client('emr', region_name='your-region')

    # Define the EMR cluster configuration
    emr_cluster_config = {
        'Name': emr_cluster_name,
        'ReleaseLabel': emr_release_label,
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': "MasterNode",
                    'InstanceRole': "MASTER",
                    'InstanceType': emr_instance_type,
                    'InstanceCount': 1,
                },
                {
                    'Name': "CoreNodes",
                    'InstanceRole': "CORE",
                    'InstanceType': emr_instance_type,
                    'InstanceCount': num_core_instances,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'Applications': [{'Name': 'Spark'}],
    }

    # Start the EMR cluster
    response = emr_client.run_job_flow(**emr_cluster_config)

    return {
        'statusCode': 200,
        'body': response
    }
