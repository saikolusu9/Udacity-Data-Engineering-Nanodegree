import json
# importing the requests library
import boto3
from sodapy import Socrata
import requests
from read_data import ReadDataAPI
from read_data2 import ReadDataAPI
import os

def lambda_handler(event, context):
    
    table_name = event['tablename']
    
    flag_full_load = event['full_load']
    
    client = ReadDataAPI()
    
    client.read_data_from_api(table_name, full_load=flag_full_load)
    