import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

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
    
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()