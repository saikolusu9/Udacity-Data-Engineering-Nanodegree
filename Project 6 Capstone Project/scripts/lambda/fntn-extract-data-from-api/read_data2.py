import datetime
from datetime import datetime,timedelta
import os
import requests
import boto3
import json
from sodapy import Socrata
import pandas as pd
from io import StringIO


class ReadDataAPI():
    def __init__(self):
        self.host = os.environ['host']
        self.vehicleclient = os.environ['client_vehicles']
        self.collisionclient = os.environ['client_collisions']
        self.personsclient = os.environ['client_persons']
        self.bucket = os.environ['bucket_name']
        self.folder = os.environ['folder_name']

    def get_previousdays_date(self):
        yesterday = datetime.now() - timedelta(30)
        return datetime.strftime(yesterday, '%Y-%m-%d')
        
    def read_data_from_api(self, sub_foldername, full_load='True'):
        
        client = Socrata(self.host, None)
        
        collisions_dict = {'Crashes':os.environ['client_collisions'], 'Vehicles':os.environ['client_vehicles'], 'Persons': os.environ['client_persons']}

        # First 2000 results, returned as JSON from API / converted to Python list of
        # dictionaries by sodapy.
        results = client.get(collisions_dict[sub_foldername], limit = 10000000)
        
        key =  self.folder + "/" + sub_foldername + "/" + sub_foldername
        
        df = pd.DataFrame(results)
        print(df.info())
        
        if full_load == 'False':
            date = self.get_previousdays_date()
            print(date)
            df = df[df['crash_date'].str.contains(date)]
            print(len(df))
            key = key + "_" + date
    
    
        #x = json.dumps(results)
        
        s3 = boto3.client("s3")
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        key = key +".csv"
        s3.put_object(Bucket = self.bucket, Body=csv_buf.getvalue(), Key = key)