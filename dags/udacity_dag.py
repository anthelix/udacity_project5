# with staging functions
# donr't use this .py
# prends et efface

from __future__ import print_function
from datetime import timedelta, datetime
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


from airflow.operators import (MyFirstOperator, MyFirstSensor,
                                StageToRedshiftOperator)
from helpers import CreateTables

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
    'start_date': airflow.utils.dates.days_ago(7),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False # Run only the last DAG
}

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')



#########################################################################
# FUNCTIONS
def print_hello():
    return 'Hello Word!'

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credential')
    bucket = Variable.get('s3_bucket')
    logging.info(f"Listing Keys from {bucket}")
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

#########################################################################
# DAGS  ## changer starttime, schedule interval.
dag = DAG(
    'ETL_Sparkify',
    default_args=default_args,
    description='ETL with from S3 to Redshift with Airflow',
    schedule_interval='0 12 * * *',
    start_date=datetime(2020, 3, 17), catchup=False,
)



####################################################################################
# TASKS

# task created by instantiating operators DummmyOerator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# task display pathfiles
list_keys = PythonOperator(
    task_id="get_path_files",
    python_callable=list_keys,
    dag=dag
)

copy_songs_task = StageToRedshiftOperator(
    task_id="copy_songs_from_s3_to_staging_songs_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credential",
    target_table= "staging_events",
    s3_bucket= "udacity-dend",
    s3_key= "song_data",
    json_format="auto"
)


# Create Staging tables
create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.staging_events_table_create
)

create_staging_songs = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTables.staging_songs_table_create
)

# stage tables



start_operator >> create_staging_songs
start_operator >> list_keys
# start_operator >> create_staging_events

create_staging_songs >> copy_songs_task
# create_staging_events >> copy_logs_task
