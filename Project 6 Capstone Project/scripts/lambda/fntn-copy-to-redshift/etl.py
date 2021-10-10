import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
        
def getNthWordinString(string, n) :
    '''
    Splits a String and returns the Nth word in it

            Parameters:
                    string: The String that needs to be split
                    n (int): position of word to be needed

            Returns:
                    returns the nth word
    '''
    return string.split(' ')[n-1] 

def load_staging_tables(cur, conn):
    '''
    Loads data into staging tables

            Parameters:
                    conn: database connection
                    cur (conn): database connection cursor

            Returns:
                    does not return anything
    '''
    for query in copy_table_queries:
        cur.execute(query)
        print("Copying " + getNthWordinString(query,2)+ " staging table is completed")
        conn.commit()


def insert_tables(cur, conn):
    '''
    inserts data into redshift tables

            Parameters:
                    conn: database connection
                    cur (conn): database connection cursor

            Returns:
                    does not return anything
    '''
    for query in insert_table_queries:
        cur.execute(query)
        print("Loading into " + getNthWordinString(query,4) + " table is completed")
        conn.commit()
    
def get_s3_folder(tablename):
    folder = '/'.join(["s3:/",os.environ['bucket'], tablename,""])
    print(folder)
    return folder