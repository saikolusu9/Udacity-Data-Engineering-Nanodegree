from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2021, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

#Start Operator is a dummy operator used to indicate the start of the execution
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#This Operator is used to create tables in Redshift
create_redshift_tables = CreateTablesOperator(
    task_id='create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)


#This Operator is used to create staging events table in Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

#This Operator is used to create staging songs table in Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json="auto"
)

#This Operator is used to create fact table 'songplays' in Redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songplays",
    query=SqlQueries.songplay_table_insert,
    truncate_table = True
)

#This Operator is used to create 'users' table in Redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.users",
    query=SqlQueries.users_table_insert,
    truncate_table = True
)


#This Operator is used to create 'songs' table in Redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.songs",
    query=SqlQueries.song_table_insert,
    truncate_table=True
)

#This Operator is used to create 'songs' table in Redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.artists",
    query=SqlQueries.artist_table_insert,
    truncate_table=True
)

#This Operator is used to create 'time' table in Redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.time",
    query=SqlQueries.time_table_insert,
    truncate_table=True
)

#This Operator is used to run quality checks on the tables created in Redshift
run_data_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["artists", "songs", "songplays"]
)

#End Operator is a dummy operator used to indicate the end of the execution
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> \
create_redshift_tables >> \
[stage_songs_to_redshift, stage_events_to_redshift] >> \
load_songplays_table >> \
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,load_time_dimension_table] >> \
run_data_quality_checks >> \
end_operator