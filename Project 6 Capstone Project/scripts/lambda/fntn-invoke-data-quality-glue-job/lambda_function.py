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
    task = client_sf.get_activity_task(activityArn=activity, workerName="Data-Quality-Check")
    print(task)
    
    response = client.start_job_run(JobName = os.environ['glue_job_name'], 
                Arguments = {
                    '--task_token': task['taskToken'],
                    '--vehicles_target_table': os.environ['dim_vehicles'],
                    '--database' : os.environ['database'],
                    '--persons_target_table' : os.environ['dim_persons'],
                    '--persons_source_table' : os.environ['src_persons'],
                    '--crashes_target_table' : os.environ['dim_crashes'],
                    '--vehicles_source_table' : os.environ['src_vehicles'],
					'--crashes_source_table' : os.environ['src_crashes']
                })

