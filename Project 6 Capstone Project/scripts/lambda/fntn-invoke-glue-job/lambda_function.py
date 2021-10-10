import json
import boto3
import os
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
client = boto3.client('glue')
            
client_sf = boto3.client('stepfunctions')
# replace activity arn with respective activivty arn
activity = os.environ['activity_step_function']

def lambda_handler(event, context):
    task = client_sf.get_activity_task(activityArn=activity, workerName="Raw-To-Refined-ETL")
    print(task)
    
    response = client.start_job_run(JobName = os.environ['glue_job_name'], 
                Arguments = {
                    '--task_token': task['taskToken'],
                    '--output_folder': os.environ['output_folder'],
                    '--database' : os.environ['database'],
                    '--crashes_table' : os.environ['crashes_table'],
                    '--vehicles_table' : os.environ['vehicles_table'],
                    '--bucket' : os.environ['bucket'],
                    '--persons_table' : os.environ['persons_table']
                })