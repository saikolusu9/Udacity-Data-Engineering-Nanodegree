from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
import sys
from awsglue.job import Job
import boto3
import json

#Initialize contexts and session
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'task_token', 'database', 'vehicles_target_table', 'persons_target_table', 'crashes_source_table', 'crashes_target_table', 'persons_source_table','vehicles_source_table'])
job.init(args['JOB_NAME'], args)

#Batch Execution parameter
full_load = True


#Parameters
crashes_source_tablename = args['crashes_source_table']
vehicles_source_tablename = args['vehicles_source_table']
persons_source_tablename = args['persons_source_table']
crashes_target_tablename = args['crashes_target_table']
vehicles_target_tablename = args['vehicles_target_table']
persons_target_tablename = args['persons_target_table']
glue_db = args['database']

####################################################### Define Functions ###############################################################

def read_source_glue_table(glue_tbl,tablename):
    """
        Reads all the source tables
    
        Parameters:
            tablename in glue catalog, module name to point to glue bookmark
    
        Returns:
            returns the glue dataframe
    """
    df_glue = glue_context.create_dynamic_frame.from_catalog(
                                database=glue_db,
                                table_name=glue_tbl)
    return df_glue

def read_target_glue_table(glue_tbl,tablename):
    """
        Reads all the targets tables
    
        Parameters:
            tablename in glue catalog, module name to point to glue bookmark
    
        Returns:
            returns the glue dataframe
    """
    if full_load:
        df_glue = glue_context.create_dynamic_frame.from_catalog(
                        database=glue_db,
                        table_name=glue_tbl)
    else:
        yesterday = datetime.now() - timedelta(30)
        year = str(yesterday.year)
        month = str(yesterday.month)
        day= str(yesterday.day)
        str_push_down_predicate = "year = %s and month = %s and day = %s" % (year, month, day)
        print(str_push_down_predicate)
        df_glue = glue_context.create_dynamic_frame.from_catalog(
                    database=glue_db,
                    table_name=glue_tbl,
                    push_down_predicate = str_push_down_predicate,
                    transformation_ctx = tablename + "_push_down")
    print(df_glue.count())
    return df_glue
    
def validate_target_table_successful_load(target_tbl, tablename):
    """
        validates if data loaded into staging s3 folder i.e dimension tables
    
        Parameters:
            tablename in glue catalog, module name to point to glue bookmark
    
        Returns:
            None
    """
    try:
        count = target_tbl.count()
        if count > 0:
            print("The %s has loaded successfully and has %d records" %(tablename, count))
        else:
            raise Exception('No records found')
    except Exception:
        print('No records found')

def validate_source_target_table_count(source_tbl, target_tbl, tablename):
    """
        Verify if the source/target table count is equal 
    
        Parameters:
            tablename in glue catalog, module name to point to glue bookmark
    
        Returns:
            None
    """
    try:
        source_table_count = source_tbl.count()
        target_table_count = target_tbl.count()
        if source_table_count == target_table_count:
            print("%s : The source and target table is equal . The record count is %s" %(tablename, str(target_table_count)))
        else:
            print("Not Equal")
            raise Exception("%s : The source and target table does not match . Source record count is %d and target record count is" %(tablename, source_table_count, target_table_count))
    except Exception as e:
        print(e)
        
        
if __name__ == '__main__':

    try:    
        # step-fn-activity
        client = boto3.client(service_name='stepfunctions', region_name = 'us-west-2')
        # replace activity arn with respective activivty arn
        activity = "arn:aws:states:us-west-2:730005311531:activity:accident_analysis"
        
        #read all source tables
        df_source_crashes = read_source_glue_table(crashes_source_tablename, "Crashes")
        df_source_vehicles = read_source_glue_table(vehicles_source_tablename, "Vehicles")
        df_source_persons = read_source_glue_table(persons_source_tablename, "Persons")
        
        #read all target tables i.e. dimension tables
        df_target_crashes = read_target_glue_table(crashes_target_tablename, "Crashes")
        df_target_vehicles = read_target_glue_table(vehicles_target_tablename, "Vehicles")
        df_target_persons = read_target_glue_table(persons_target_tablename, "Persons")
        
        #validates if data loaded into staging s3 folder i.e dimension tables
        validate_target_table_successful_load(df_target_crashes, "Crashes")
        validate_target_table_successful_load(df_target_vehicles, "Vehicles")
        validate_target_table_successful_load(df_target_persons, "Persons")
        
        #Verify if the source/target table count is equal
        validate_source_target_table_count(df_source_crashes,df_target_crashes, "Crashes")
        validate_source_target_table_count(df_source_vehicles,df_target_vehicles, "Vehicles")
        validate_source_target_table_count(df_source_persons,df_target_persons, "Persons")
        
        # send success token to step function activity
        print("Execution completed successfully")
        response = client.send_task_success(taskToken = args['task_token'], output = json.dumps({'message':'Data Quality Check Completed'}))
    except Exception as e:
        # send failure token to step function activity
        response = client.send_task_failure(taskToken = args['task_token'])
        print(e)
        raise e
    
    job.commit()
    print("Execution Completed")