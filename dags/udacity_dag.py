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
from airflow.operators.udacity_plugin import MyFirstOperator
from airflow.operators.udacity_plugin import MyFirstSensor
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from airflow.operators.udacity_plugin import LoadFactOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator

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
    'retry_delay': timedelta(minutes=1),                    # TODO: CHANGE BEFORE PUSH
    #'catchup': True # Run only the last DAG
}

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')



#########################################################################
# FUNCTIONS
#########################################################################
# DAGS  ## changer starttime, schedule interval.
dag = DAG(
    'ETL_Sparkify_v3',
    default_args=default_args,
    description='ETL with from S3 to Redshift with Airflow',
    start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 12, 1, 0, 0, 0, 0),
    schedule_interval='@daily',
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
)

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
    region= "us-west-2",
    json_format="auto",
)

copy_logs_task = StageToRedshiftOperator(
    task_id="copy_events_from_s3_to_staging_songs_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credential",
    create_tbl=CreateTables.staging_events_table_create,
    target_table="staging_events",
    s3_bucket="udacity-dend",
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    s3_key= "log_data",
    region= "us-west-2",
    json_format="log_json_path.json",
)

# Create and load facts table
#create_songsplays = PostgresOperator(
#    task_id='create_facts_table',
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql=CreateTables.songplay_table_create,
#)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songplays",
    create_tbl=CreateTables.songplay_table_create,
    sql=SqlQueries.songplay_table_insert,
)



# Create and load dims table # check loaded

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artists_dim_table',
    redshift_conn_id='redshift',
    create_tbl=CreateTables.artist_table_create,
    target_table= "dimArtist",
    source_table= SqlQueries.artist_table_insert,
    append_data=True,
    dag=dag
)

check_artist = HasRowsOperator(
    task_id='check_artists_rows',
    redshift_conn_id='redshift',
    table= "dimArtist",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_songs_dim_table',
    redshift_conn_id='redshift',
    create_tbl=CreateTables.song_table_create,
    target_table= "dimSong",
    source_table= SqlQueries.song_table_insert,
    append_data=True,
    dag=dag
)

check_song = HasRowsOperator(
    task_id='check_songs_rows',
    redshift_conn_id='redshift',
    table="dimSong",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    redshift_conn_id='redshift',
    create_tbl=CreateTables.song_table_create,
    target_table= "dimTime",
    source_table= SqlQueries.time_table_insert,
    append_data=True,
    dag=dag
)

check_time = HasRowsOperator(
    task_id='check_time_rows',
    redshift_conn_id='redshift',
    table= "dimTime",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    create_tbl=CreateTables.song_table_create,
    target_table= "dimUser",
    source_table= SqlQueries.user_table_insert,
    append_data=True,
    dag=dag
)

check_user = HasRowsOperator(
    task_id= 'check_users_rows',
    redshift_conn_id='redshift',
    table= "dimUser",
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

copy_songs_task >> middle_operator
copy_logs_task  >> middle_operator

middle_operator >> load_user_dimension_table
middle_operator >> load_song_dimension_table
middle_operator >> load_time_dimension_table
middle_operator >> load_artist_dimension_table
middle_operator >> load_songplays_table 

load_user_dimension_table   >> check_user
load_song_dimension_table   >> check_song
load_time_dimension_table   >> check_time
load_artist_dimension_table >> check_artist

load_songplays_table >> end_operator
check_user   >> end_operator
check_song   >> end_operator
check_time   >> end_operator
check_artist >> end_operator