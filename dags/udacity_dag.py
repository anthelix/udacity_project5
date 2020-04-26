# with staging functions
# donr't use this .py
# prends et efface

from __future__ import print_function
from datetime import timedelta
import datetime
import airflow
import logging
import pprint
import os
import glob  
from airflow.models import Variable
from airflow import DAG
from airflow.plugins_manager import AirflowPlugin
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators.udacity_plugin import HasRowsOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from airflow.operators.udacity_plugin import LoadFactOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator
from airflow.operators.udacity_plugin import DataQualityOperator

from helpers import CreateTables, SqlQueries

########################################################################
#0_begin process
#1_verifier et imprimer les paramtres
#2_afficher la liste des fichiers dans s3
#3_charger les donnees de s3 a redshift
#4-creer les stagging table
#    * staging_events pour log_file file
#    * staging_songs pour song_data file
#5_charger les donnees dans les stagiing tables
#6_creer les dimTables et FactTables
#   *factSongplays
#   *dimSong
#   *dimUser
#   *dimArtist
#   *dimTime
#7_Polulate all the tables
#8_make check for all tables
#9_stop 




#########################################################################
# edit the .dags/config/variables.json

# docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
# docker-compose run --rm webserver airflow variables --get s3_bucket
# docker-compose run --rm webserver airflow variables --set var4 value4]

default_args = {
    'owner': 'Chatagns',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'catchup': False,
    'email_on_retry': False,
    'retries': 3,
    'provide_context': True, # access to ds, previous_ds
    'retry_delay': timedelta(seconds=30),                    # TODO: CHANGE BEFORE PUSH
    #'catchup': True # Run only the last DAG
}

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')



#########################################################################
# FUNCTIONS
#########################################################################
# DAGS  ## changer starttime, schedule interval.
dag = DAG(
    'ETL_Sparkify_v8',
    default_args=default_args,
    description='ETL with from S3 to Redshift with Airflow',
    start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 12, 1, 0, 0, 0, 0),
    schedule_interval='@daily',
    # concurrency=1,
    max_active_runs=1
)

####################################################################################
# TASKS

# task created by instantiating operators DummmyOerator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create Staging tables
#create_staging_events_table = PostgresOperator(
#    task_id="create_staging_events",
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql=CreateTables.staging_events_table_create
#)

#create_staging_songs_table = PostgresOperator(
#    task_id="create_staging_songs",
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql=CreateTables.staging_songs_table_create
#)



# Copy From s3 to Redshift
copy_songs_task = StageToRedshiftOperator(
    task_id="copy_songs_from_s3_to_staging_songs_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credential",
    create_tbl = CreateTables.staging_songs_table_create,
    target_table= "staging_songs",
    s3_bucket= "udacity-dend",
    s3_key= "song_data",
    custom= " json 'auto' compupdate off region 'us-west-2'"
)

check_staging_songs = HasRowsOperator(
    task_id= 'check_songs_rows',
    redshift_conn_id='redshift',
    table= "staging_songs",
    dag=dag
)

copy_logs_task = StageToRedshiftOperator(
    task_id="copy_events_from_s3_to_staging_events_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credential",
    create_tbl=CreateTables.staging_events_table_create,
    target_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    custom="format as json 's3://udacity-dend/log_json_path.json'",
)

check_staging_events = HasRowsOperator(
    task_id= 'check_events_rows',
    redshift_conn_id='redshift',
    table= "staging_events",
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songplays",    
    create_tbl=CreateTables.songplay_table_create,
    source=SqlQueries.songplay_table_insert,
)

check_songplays_quality = DataQualityOperator(
    task_id = 'Control_songplays_quality',
    redshift_conn_id='redshift',
    target_table = "songplays",
    pk = "playid",
    dag=dag
)

# Create and load dims table # check loaded

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    redshift_conn_id='redshift',
    create_tbl= CreateTables.artist_table_create,
    target_table= "artists",
    source_table= SqlQueries.artist_table_insert,
    append_data=False,
    dag=dag
)

check_artist_quality = DataQualityOperator(
    task_id = 'Control_artist_quality',
    redshift_conn_id='redshift',
    target_table = "artists",
    pk = "artistid",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    redshift_conn_id='redshift',
    create_tbl= CreateTables.song_table_create,
    target_table= "songs",
    source_table= SqlQueries.song_table_insert,
    append_data=False,
    dag=dag
)

check_song_quality = DataQualityOperator(
    task_id = 'Control_songs_quality',
    redshift_conn_id='redshift',
    target_table = "songs",
    pk = "songid",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    redshift_conn_id='redshift',
    create_tbl= CreateTables.time_table_create,
    target_table= "time",
    source_table= SqlQueries.time_table_insert,
    append_data=False,
    dag=dag
)

check_time_quality = DataQualityOperator(
    task_id = 'Control_start_time_quality',
    redshift_conn_id='redshift',
    target_table = "time",
    pk = "start_time",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    create_tbl= CreateTables.user_table_create,
    target_table= "users",
    source_table= SqlQueries.user_table_insert,
    append_data=False,
    dag=dag
)

check_user_quality = DataQualityOperator(
    task_id = 'Control_data_quality',
    redshift_conn_id='redshift',
    target_table = "users",
    pk = "userid",
    dag=dag
)

templated_command = """
{% for i in range(2) %}
    echo "{{ ds }}"
    echo "{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}"
    echo "{{ params.my_param }}"
    echo "log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json"
{% endfor %}
"""

t6 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'formated path log'},
    dag=dag,
)

middle_operator = DummyOperator(task_id='Middle_execution',  dag=dag)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)



start_operator >> t6
start_operator  >> copy_songs_task
start_operator >> copy_logs_task

copy_songs_task  >> check_staging_songs
copy_logs_task  >> check_staging_events

check_staging_songs >> middle_operator
check_staging_events >> middle_operator

middle_operator >> load_user_dimension_table
middle_operator >> load_song_dimension_table
middle_operator >> load_time_dimension_table
middle_operator >> load_artist_dimension_table
middle_operator >> load_songplays_table 

load_user_dimension_table   >> check_user_quality
load_song_dimension_table   >> check_song_quality
load_time_dimension_table   >> check_time_quality
load_artist_dimension_table >> check_artist_quality
load_songplays_table >> check_songplays_quality


check_songplays_quality >> end_operator
check_user_quality   >> end_operator
check_song_quality   >> end_operator
check_time_quality   >> end_operator
check_artist_quality >> end_operator
