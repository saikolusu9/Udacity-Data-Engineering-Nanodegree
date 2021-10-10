from datetime import datetime
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
import geopy
from geopy.geocoders import Nominatim
from functools import reduce
from operator import add
from pyspark.sql.window import Window
import sys
from awsglue.job import Job
import boto3
import json

#Initialize contexts and session
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'task_token', 'bucket', 'database', 'output_folder', 'persons_table', 'vehicles_table','crashes_table'])
job.init(args['JOB_NAME'], args)

#Declare Parameters
boroughs_list = ["BRONX", "QUEENS", "BROOKLYN", "MANHATTAN", "STATEN ISLAND"]
glue_db = args['database']
crashes_tablename = args['crashes_table']
vehicles_tablename = args['vehicles_table']
persons_tablename = args['persons_table']
crash_date_col = "CRASH_DATE"
crash_time_col = "CRASH_TIME"
coalesce_cols_crashes = ["STREET_NAME", "CONTRIBUTING_FACTOR_VEHICLE", "VEHICLE_TYPE_CODE"]
coalesce_cols_vehicles = ["VEHICLE_DAMAGE"]
kill_injure_dict = {"INJURED": "NUMBER_PEOPLE_INJURED", "KILLED":"NUMBER_PEOPLE_KILLED"}
columns_crashes = ["COLLISION_ID", "year", "month", "day","BOROUGH", "STREET_NAME", "CONTRIBUTING_FACTOR_VEHICLE","NUMBER_PEOPLE_INJURED","NUMBER_PEOPLE_KILLED"]
columns_vehicles = ["COLLISION_ID","UNIQUE_ID","STATE_REGISTRATION","year", "month", "day","VEHICLE_TYPE","VEHICLE_MAKE","VEHICLE_MODEL","VEHICLE_YEAR","DRIVER_SEX","POINT_OF_IMPACT","VEHICLE_DAMAGE"]
columns_persons = ["COLLISION_ID","UNIQUE_ID","PERSON_TYPE","year", "month", "day","PERSON_INJURY","PERSON_AGE","PERSON_SEX"]
columns_time = ["COLLISION_ID","CRASH_DATE_TIME", 'YEAR', 'MONTH', 'DAY', 'HOUR', 'WEEKOFYEAR']
output_data = '/'.join(["s3a:/",args['bucket'],args['output_folder'],""])


####################################################### Define Functions ###############################################################
 
def read_glue_table(glue_tbl, tablename):
    """
        Creates a Spark Dataframe from a Glue Dataframe
    
        Parameters:
            tablename in glue catalog, module name to point to glue bookmark
    
        Returns:
            returns the spark dataframe
    """
    df_glue = glue_context.create_dynamic_frame.\
                from_catalog(database=glue_db,table_name=glue_tbl, transformation_ctx = "df_glue" + "_" + tablename)
    return df_glue.toDF()


    
def replace_space_underscore_columns(df):
    """
        Replaces special characters like spaces and underscores in the dataframe
    
        Parameters:
            dataframe under execution
    
        Returns:
            returns the transformed dataframe
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", "_"))
    return df


   
def get_last_element(l):
    """
        Extract the datetime string from timestamp column
    
        Parameters:
            timestamp
    
        Returns:
            datetime
    """
    date = l[:10]
    split_str = l.split(' ')
    date_time = ' '.join([date, split_str[-1]])
    return date_time


        
def extract_indiv_cols(df,crash_date, crash_time) :
    """
        Create individual columns from timestamp like datetime, year, month, day and weekof year
    
        Parameters:
            dataframe under execution, crash date and crash time column
    
        Returns:
            returns the transformed dataframe with columns
    """
    concat_columns = [crash_date,crash_time]
    df = df.withColumn('concat_crash_date_time',concat_ws(" ", *[col(x) for x in concat_columns]))
    get_last_element_udf = udf(lambda x: get_last_element(x), StringType())
    df = df.withColumn('input_crash_date_time', get_last_element_udf(df.concat_crash_date_time))
    df = df.withColumn('CRASH_DATE_TIME',to_timestamp(unix_timestamp(df.input_crash_date_time, 'yyyy-MM-dd HH:mm').cast('timestamp')))
    df = df.withColumn('YEAR',year("crash_date_time")) \
            .withColumn('MONTH',month("crash_date_time")) \
            .withColumn('DAY',dayofmonth("crash_date_time")) \
            .withColumn('HOUR',hour("crash_date_time"))\
            .withColumn("WEEKOFYEAR", weekofyear("crash_date_time"))
    
    return df
    

    
def convert_struct_to_string(df, *args):
    """
        Converts the columns of struct type to string type
    
        Parameters:
            dataframe under execution
    
        Returns:
            returns the transformed dataframe
    """
    for arg in args:
        df = df.withColumn(arg, col(arg).cast(StringType()))

    return df

def subset_columns(df, cols_list):
    """
        Subset the dataframe with only required columns
    
        Parameters:
            dataframe under execution
    
        Returns:
            returns the subsetted dataframe
    """
    return df.select(cols_list)
    
    
def coalesce_df (df, new_column):
    """
        Performs coalesce function on group of columns to get the first non null value
    
        Parameters:
            dataframe under execution, coalesce column name
    
        Returns:
            returns the transformed dataframe
    """
    selected_columns = [column for column in df.columns if new_column.upper() in column.upper()]
    expr = ', '.join("df['" + arg + "'].cast(StringType())" for arg in selected_columns )
    expr = "coalesce({})".format(expr)
    df = df.withColumn(new_column, eval(expr))
    return df
    
def calculate_killed_and_injured(df,colstr,new_column):
    """
        Calculates the number of people killed and injured among motorists, pedestrians and passengers for
        each crash
    
        Parameters:
            dataframe under execution, sum number column name column name
    
        Returns:
            returns the transformed dataframe
    """
    selected_columns = [column for column in df.columns if colstr.upper() in column.upper()]
    df = df.withColumn(new_column,reduce(add, [col(x) for x in selected_columns]))
    return df

def rename_columns(df, columns):
    """
        renames the columns in the dataframe
    
        Parameters:
            dataframe under execution, column names
    
        Returns:
            returns the transformed dataframe
    """
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
    
    return df
        
        
def create_fact_table(df_crashes,df_vehicles,df_persons):
    """
        creates the fact tables based on the dimension tables
    
        Parameters:
            fact tables
    
        Returns:
            None
    """
    df_crashes.createOrReplaceTempView("df_temp_crashes")
    df_vehicles.createOrReplaceTempView("df_temp_vehicles")
    df_persons.createOrReplaceTempView("df_temp_persons")
    
    df_crashes_fact = spark.sql("Select distinct a.COLLISION_ID, b.VEHICLE_ID, c.PERSON_ID, a.CRASH_DATE_TIME, a.year, a.month, a.day \
                                    from df_temp_crashes a join df_temp_vehicles b \
                                        on a.COLLISION_ID = b.COLLISION_ID \
                                            join df_temp_persons c \
                                                on a.COLLISION_ID = c.COLLISION_ID")
                                                
    df_crashes_fact=df_crashes_fact\
                    .withColumn("crash_id",row_number()\
                            .over(Window.orderBy(monotonically_increasing_id())))
                            
    df = write_to_s3(df_crashes_fact,"tbl_fact_crashes",partition=True)
    
    
                        
def write_to_s3(df,foldername,partition=True):
    """
        writes the dataframe to s3 bucket in parquet format
    
        Parameters:
            dataframe, outputfolder, partition condition
    
        Returns:
            None
    """
    if partition:
        df2 = df.write.partitionBy("year","month","day")\
                    .mode("overwrite")\
                        .parquet(output_data + foldername) 
    else:
        df2 = df.write.mode("overwrite").parquet(output_data + foldername)


def execute_main(glue_tbl, tablename, coalesce_cols, subset_cols, rename_cols, outputfoldername):
    """
        execute all the steps in the data transformation and loading
    
        Parameters:
            dataframe, tablename,columns, output folder name
    
        Returns:
            None
    """
    
    
    #Read each table collisons, vehicles, persons from glue catalog
    df = read_glue_table(glue_tbl, tablename)
    
    #Replace special characters
    df = replace_space_underscore_columns(df)
    
    #Extract date time and year month day columns
    df = extract_indiv_cols(df,crash_date_col,crash_time_col)
    
    if glue_tbl == crashes_tablename:
        
        #Create time dimension table from date time and year month day columns
        df_time_table = subset_columns(df,columns_time)
        
        #write the time dimension table to s3
        df_time_table = write_to_s3(df_time_table,"dim_Time",partition=False) 
        
        #convert any struct columns in dimenstion tables to stric type
        df = convert_struct_to_string(df, 'zip_code')
        
        for k,v in kill_injure_dict.items():
            #calculate the number of persons killed and injured
            df = calculate_killed_and_injured(df,k,v)
    
    if len(coalesce_cols) > 0:    
        for colname in coalesce_cols:
            df = coalesce_df(df, colname)
    
    #subset columns  
    df = subset_columns(df,subset_cols)
    
    #rename columns
    df = rename_columns(df,rename_cols)
    
    #write the dataframe to s3 in parquet format
    write_to_s3(df,outputfoldername,partition=True)
    
    return df

if __name__ == '__main__':

    try:    
        # step-fn-activity
        client = boto3.client(service_name='stepfunctions', region_name = 'us-west-2')
        # replace activity arn with respective activivty arn
        activity = "arn:aws:states:us-west-2:730005311531:activity:accident_analysis"
        
        #execute the data transformation for collions dimension table
        df_crashes = execute_main(crashes_tablename, "Crashes", coalesce_cols_crashes, columns_crashes, None, "dim_Crashes")
        print("ETL of Crashes table completed")
        
        #execute the data transformation for vehicles dimension table
        df_vehicles = execute_main(vehicles_tablename, "Vehicles", coalesce_cols_vehicles, columns_vehicles, {'UNIQUE_ID':'VEHICLE_ID'}, "dim_Vehicles")
        print("ETL of Vehicles table completed")
        
        #execute the data transformation for persons dimension table
        df_persons = execute_main(persons_tablename, "Persons", [], columns_persons, {'UNIQUE_ID':'PERSON_ID'}, "dim_Persons")
        print("ETL of Persons table completed")
                
        # send success token to step function activity
        response = client.send_task_success(taskToken = args['task_token'], output = json.dumps({'message':'ETL Completed'}))
    except Exception as e:
        # send failure token to step function activity
        response = client.send_task_failure(taskToken = args['task_token'])
        raise e
    
    job.commit()
    print("Execution Completed")