import json
import etl
import psycopg2
import os

def lambda_handler(event, context):
    # TODO implement
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}" \
            .format(os.environ['db_host'], os.environ['db_name'], os.environ['db_user'], os.environ['db_password'], os.environ['db_port']))
    cur = conn.cursor()
    
    etl.drop_tables(cur, conn)
    print("Drop Tables Successfull!!")

    etl.create_tables(cur, conn)
    print("Create Tables Successfull!!")
    
    etl.load_staging_tables(cur, conn)
    print("Insert Staging Tables Successfull!!")
    
    etl.insert_tables(cur, conn)
    print("Insert Tables Successfull!!")
    
    conn.close()
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
