import json
import boto3
import os
client = boto3.client('glue')

# step-fn-activity
client_sf = boto3.client('stepfunctions')
# replace activity arn with respective activivty arn
activity = os.environ['arn_activity_step_function']
def lambda_handler(event, context):
    print(event)
    class CrawlerException(Exception):
        pass
    crawler_name = os.environ['crawler_name']
    response = client.get_crawler_metrics(CrawlerNameList = [crawler_name])
    print(response['CrawlerMetricsList'][0]['CrawlerName']) 
    print(response['CrawlerMetricsList'][0]['TimeLeftSeconds']) 
    print(response['CrawlerMetricsList'][0]['StillEstimating']) 
    
    if (response['CrawlerMetricsList'][0]['StillEstimating']):
        raise CrawlerException('Crawler In Progress!')
    elif (response['CrawlerMetricsList'][0]['TimeLeftSeconds'] > 0):
        raise CrawlerException('Crawler In Progress!')
    else :
        #send activity success token
        task = client_sf.get_activity_task(activityArn=activity, workerName="data-quality-check-crawler-activity")
        response = client_sf.send_task_success(taskToken=task['taskToken'], output=json.dumps({'message':'Data Quality  Crawler Completed'}))