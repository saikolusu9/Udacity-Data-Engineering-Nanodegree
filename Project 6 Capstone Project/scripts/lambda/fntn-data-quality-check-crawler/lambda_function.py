import json
import boto3
import os

# Glue Client
client = boto3.client('glue')

# step-fn-activity
client_sf = boto3.client('stepfunctions')

# replace activity arn with respective activivty arn
activity = os.environ['arn_step_function']


def lambda_handler(event, context):
    # TODO implement
    print("Starting Glue Crawler")
    
    class CrawlerException(Exception):
        pass
    
    try:
        crawler_name = os.environ['crawler_name']
        print("The Glue Crawler name is "+ crawler_name)
        response = client.start_crawler(Name = crawler_name)
        print("Glue Crawler running")
        
    except CrawlerRunningException as c:
        raise CrawlerException('Crawler In Progress!')
        print('Crawler in progress')
    except Exception as e:
        #send activity failure token
        task = client_sf.get_activity_task(activityArn = activity, workerName = crawler_name)
        response = client_sf.send_task_failure(taskToken = task['taskToken'])
        print('Problem while invoking crawler')