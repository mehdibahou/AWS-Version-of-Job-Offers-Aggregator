import boto3
from selenium import webdriver


def lambda_handler(event, context):
    # Initialize AWS SDK clients
    ecs = boto3.client('ecs')

    # Define ECS task parameters
    cluster_name = 'your-ecs-cluster-name'
    task_definition = 'linkedin-data-collection-task'

    # Start the ECS task for LinkedIn data collection
    response = ecs.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition,
        count=1  # Number of tasks to run
    )

    if 'tasks' in response and len(response['tasks']) > 0:
        print('LinkedIn data collection task started successfully.')
    else:
        print('Failed to start the LinkedIn data collection task.')

    return {
        'statusCode': 200,
        'body': 'LinkedIn data collection initiated successfully.'
    }
