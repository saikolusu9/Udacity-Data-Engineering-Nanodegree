    #!/usr/bin/env python
# coding: utf-8

# In[92]:


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col,from_unixtime, monotonically_increasing_id,row_number
from pyspark.sql.types import TimestampType,IntegerType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window



def create_spark_session():
    """
        Creates a Spark Session
    
        Parameters:
            none
    
        Returns:
            returns the spark object
    """
    
    spark = SparkSession.builder\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                    .getOrCreate()
                      
    return spark


# In[95]:


def process_song_data(spark, input_data, output_data):
    """
        extracts the song data from s3, processes it and writes to S3 in parquet format
    
        Parameters:
            spark: spark session object
            input_data: path of the song data in s3
            output_data: path of s3 bucket where the output data has to be stored
    
        Returns:
            returns nothing
    """
        
    # get filepath to song data file
    song_data = input_data + "/song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    #df.show(5)
    
    # extract columns to create songs table
    songs_table = df.select("song_id","title", "artist_id","year", "duration")
    songs_table = songs_table.dropDuplicates()
    songs_table.createOrReplaceTempView("temp_songs_table")
    # write songs table to parquet files partitioned by year and artist
    songs_table = df.write.partitionBy("year","artist_id")\
                    .mode("overwrite").parquet(output_data + "/songs_table")
    print("Songs Table Successfully created")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name", \
                                  "artist_location","artist_latitude", "artist_longitude")
    artists_table = artists_table.dropDuplicates()
    artists_table.createOrReplaceTempView("temp_artists_table")
    
    # write artists table to parquet files
    artists_table = df.write.mode("overwrite").parquet(output_data + "/artists_table")
    print("Artists Table Successfully created")


def process_log_data(spark, input_data, output_data):
    '''
        extracts the log data from s3, processes it and writes to S3 in parquet format

            Parameters:
                    spark: spark session object
                    input_data: path of the log data in s3
                    output_data: path of s3 bucket where the output data has to be stored

            Returns:
                    returns nothing
    '''
    
    
    # get filepath to log data file
    log_data = input_data + "/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    #df.show(5)
    
    # filter by actions for song plays
    df = df[df["page"]== "NextSong"]

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table = df.write.mode("overwrite").parquet(output_data + "/users_table")
    print("Users Table Successfully created")
    
    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.from_unixtime(col("ts")/1000,'yyyy-MM-dd HH:mm:ss'))
    
    # extract columns to create time table
    time_table = df.withColumn("start_time", col("timestamp"))\
                    .withColumn("year", year("timestamp"))\
                        .withColumn("month", month("timestamp"))\
                            .withColumn("dayofmonth", dayofmonth("timestamp"))\
                                .withColumn("hour", hour("timestamp"))\
                                    .withColumn("weekofyear", weekofyear("timestamp"))
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year","month")\
                    .mode("overwrite")\
                        .parquet(output_data + "/timetable_table")
    time_table = time_table.dropDuplicates()
    print("Time Table Successfully created")
    
    # read in song data to use for songplays table
    df.createOrReplaceTempView("temp_df")
    
    query = "SELECT DISTINCT a.timestamp as start_time, a.userId as user_id, a.level,\
                    b.song_id, c.artist_id, a.sessionId as session_id,\
                        a.location as artist_location, a.userAgent as user_agent \
                            FROM temp_df a \
                                join temp_songs_table b \
                                    on a.song = b.title \
                                        join temp_artists_table c\
                                            on a.artist = c.artist_name"
    song_df = spark.sql(query)

    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df['start_time', 'user_id', 'level', \
                                  'song_id','artist_id', 'session_id', \
                                      'artist_location', 'user_agent']
    
    songplays_table=songplays_table\
                    .withColumn("songplay_id",row_number()\
                            .over(Window.orderBy(monotonically_increasing_id())))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table\
                        .write.mode("overwrite")\
                            .partitionBy("year","month")\
                                .parquet(output_data + "/songplays_table")
    print("Songs Play Table Successfully created")


# In[97]:


def main():
    spark = create_spark_session()
    #input_data = os.getcwd() + "/data/inputdata"
    #output_data = os.getcwd() + "/data/outputdata"
    input_data = "s3a://udacity-dend"
    output_data = "s3a://spark-project-kolusu"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


# In[98]:


if __name__ == "__main__":
    main()


# In[ ]: