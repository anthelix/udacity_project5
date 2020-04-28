# with staging functions
# donr't use this .py
# prends et efface

#from datetime import timedelta
import datetime
#import airflow
#import logging
#from airflow.models import Variable
from airflow import DAG
#from airflow.plugins_manager import AirflowPlugin
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator


from airflow.operators.udacity_plugin import (StageToRedshiftOperator,
                                              LoadFactOperator,
                                              LoadDimensionOperator,
                                              DataQualityOperator,
                                              HasRowsOperator)

from helpers import CreateTables, SqlQueries
from subdag import get_s3_to_redshift_subdag, get_dimTables_to_Redshift_subdag

########################################################################

#########################################################################
# edit the .dags/config/variables.json

# docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
# docker-compose run --rm webserver airflow variables --get s3_bucket
# docker-compose run --rm webserver airflow variables --set var4 value4]

default_args = {
    'owner': 'Sparkify & Co',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date' : datetime.datetime(2018, 11, 30, 0, 0, 0, 0),
    'email_on_retry': False,
    'retries': 3,
    'provide_context': True, # access to ds, previous_ds
    'retry_delay': datetime.timedelta(seconds=30),                    # TODO: CHANGE BEFORE PUSH 5 minutes
   
}

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')


# DAGS  ## changer starttime, schedule interval.
dag = DAG(
    'ETL_Sparkify_v3',
    default_args=default_args,
    description='ETL from S3 to Redshift with Airflow',
    schedule_interval='@hourly', # schedule_interval='0 * * * *'
    max_active_runs=2
)

####################################################################################
# STAGING_TASKS SUBDAG
####################################################################################

# task created by instantiating operators DummmyOerator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Taks to create, insert from s3_bucket/udacity-dend/log_data and check staging songs table
staging_songs_task_id = "staging_songs_subdag"
staging_songs_task = SubDagOperator(
    subdag=get_s3_to_redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        staging_songs_task_id, #task_id
        "redshift", #redshift_conn_id
        "aws_credential", #aws_credentials_id
        CreateTables.staging_songs_table_create, # create_tbl
        "staging_songs", #target_table
        s3_bucket= "udacity-dend",
        s3_key= "song_data",
        custom= " json 'auto' compupdate off region 'us-west-2'",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=staging_songs_task_id,
    dag=dag
)
#Taks to create, insert from s3_bucket/udacity-dend/log_data and check staging events table
staging_events_task_id = "staging_events_subdag"
staging_events_task = SubDagOperator(
    subdag=get_s3_to_redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        staging_events_task_id, #task_id
        "redshift", #redshift_conn_id
        "aws_credential", #aws_credentials_id
        CreateTables.staging_events_table_create, # create_tbl
        "staging_events", #target_table
        s3_bucket= "udacity-dend",
        s3_key= "log_data",
        custom="format as json 's3://udacity-dend/log_json_path.json'",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=staging_events_task_id,
    dag=dag
)

####################################################################################
# FACTS TABLE TASKs
####################################################################################
# Task to create and insert data from staging table to fact table the check
load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songplays",    
    create_tbl=CreateTables.songplay_table_create,
    source=SqlQueries.songplay_table_insert,
)

check_songplays_quality = DataQualityOperator(
    task_id = 'songplays_quality',
    redshift_conn_id='redshift',
    target_table = "songplays",
    pk = "playid",
    dag=dag
)
####################################################################################
# DIMENSION TABLES TASKS SUBDAG
####################################################################################
# Task to create, insert data and run data quality on table artists
load_artists_dimension_table_id = "load_artists_dimension_table_subdag"
load_artists_dimension_table_task = SubDagOperator(
    subdag=get_dimTables_to_Redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        load_artists_dimension_table_id, #task_id
        "redshift", #redshift_conn_id
        CreateTables.artist_table_create, # create_tbl
        "artists", #target_table
        SqlQueries.artist_table_insert, # source_table
        append_data=False,
        pk = "artistid",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=load_artists_dimension_table_id,
    dag=dag,
)
# Task to create, insert data and run data quality on table songs
load_songs_dimension_table_id = "load_songs_dimension_table_subdag"
load_songs_dimension_table_task = SubDagOperator(
    subdag=get_dimTables_to_Redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        load_songs_dimension_table_id, #task_id
        "redshift", #redshift_conn_id
        CreateTables.song_table_create, # create_tbl
        "songs", #target_table
        SqlQueries.song_table_insert, # source_table
        append_data=False,
        pk = "songid",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=load_songs_dimension_table_id,
    dag=dag,
)
# Task to create, insert data and run data quality on table time
load_time_dimension_table_id = "load_time_dimension_table_subdag"
load_time_dimension_table_task = SubDagOperator(
    subdag=get_dimTables_to_Redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        load_time_dimension_table_id, #task_id
        "redshift", #redshift_conn_id
        CreateTables.time_table_create, # create_tbl
        "time", #target_table
        SqlQueries.time_table_insert, # source_table
        append_data=False,
        pk = "start_time",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=load_time_dimension_table_id,
    dag=dag,
)
# Task to create, insert data and run data quality on table users
load_users_dimension_table_id = "load_users_dimension_table_subdag"
load_users_dimension_table_task = SubDagOperator(
    subdag=get_dimTables_to_Redshift_subdag(
        "ETL_Sparkify_v3", #name parent dag
        load_users_dimension_table_id, #task_id
        "redshift", #redshift_conn_id
        CreateTables.user_table_create, # create_tbl
        "users", #target_table
        SqlQueries.user_table_insert, # source_table
        append_data=False,
        pk ="userid",
        start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    ),
    task_id=load_users_dimension_table_id,
    dag=dag,
)


end_operator = DummyOperator(task_id='End_execution',  dag=dag)
###############################################################################
# DEPENDENCIES
###############################################################################


start_operator >> staging_songs_task
start_operator >> staging_events_task

staging_songs_task  >> load_songplays_table
staging_events_task >> load_songplays_table

load_songplays_table    >> check_songplays_quality

check_songplays_quality >> load_users_dimension_table_task
check_songplays_quality >> load_songs_dimension_table_task
check_songplays_quality >> load_artists_dimension_table_task
check_songplays_quality >> load_time_dimension_table_task 


load_users_dimension_table_task   >> end_operator
load_songs_dimension_table_task   >> end_operator
load_artists_dimension_table_task >> end_operator
load_time_dimension_table_task    >> end_operator
