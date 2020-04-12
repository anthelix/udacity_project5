from datetime import timedelta
from airflow.models import Variable
import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import CreateTables

# edit the .dags/config/variables.json
dag_config = Variable.get("variables_config", deserialize_json=True)
aws_default_region = dag_config["aws_default_region"]
s3_bucket = dag_config["s3_bucket"]
s3_prefix = dag_config["s3_prefix"]

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# my varaiables
WORKFLOW_RETRY_DELAY = timedelta(minutes=5)
WORKFLOW_SCHEDULE_INTERVAL = '@daily'
WORKFLOW_START_DATE = datetime.datetime.utcnow()

WORKFLOW_DAG_ALERT = ['airflow@example.com']
WORKFLOW_DAG_CATCHUP = False
WORKFLOW_DAG_DESCRIPTION = 'Extract data from s3, Transform and Load in Redshift'
WORKFLOW_DAG_ID = 'sparkify_dag'

WORKFLOW_DEFAULT_ARGS = {
    'owner': 'dend_stephanie',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'retries': 3,
    'retry_delay': WORKFLOW_RETRY_DELAY
}


dag = DAG(
    dag_id=                 WORKFLOW_DAG_ID,
    default_args=           WORKFLOW_DEFAULT_ARGS,
    description=            WORKFLOW_DAG_DESCRIPTION,
    schedule_interval=      WORKFLOW_SCHEDULE_INTERVAL, 
    catchup=                WORKFLOW_DAG_CATCHUP
)

# Download_data > send_data_to_processing > momitor_processinf > generate_report > send_email
# Check that bucket for hour exist → if exist run tasks → [insert data to BigQuery, insert data to PostgreSQL

def print_hello():
    return 'Hello Word!'
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)




start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Create Staging tables
create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    #postgres_conn_id="postgres_default",
    sql=CreateTables.staging_events_table_create
)

create_staging_songs = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    #postgres_conn_id="postgres_default",
    sql=CreateTables.staging_songs_table_create
)

# Download_data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)


# send_data_to_processing
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## Define all task dependencies
# STAGE_task depend of start_tasks


start_operator >> hello_operator

#start_operator >> [create_staging_events, create_staging_songs]
#create_staging_events >> stage_events_to_redshift
#create_staging_songs >> stage_songs_to_redshift

