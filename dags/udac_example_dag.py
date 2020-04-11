from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# my varaiables

WORKFLOW_START_DATE = datetime(2019, 1, 12)
WORKFLOW_DAG_ID = 'sparkify_dag'
WORFLOW_DAG_DESCRIPTION = 'Extract data from s3, Transform and Load in Redshift'
WORFLOW_SCHEDULE_INTERVAL = '@daily'
WORFLOW_RETRY_DELAY = timedelta(minutes=5)
WORFLOW_DAG_ALERT = ['airflow@example.com']

WORKFLOW_DEFAULT_ARGS = {
    'owner': 'airflow',
    # = True a la fin du projet
    'depends_on_past': False,
    # a definir depuis la date des premiers fichiers
    'start_date': WORKFLOW_START_DATE,
    # ecrire une tache pour envoie de mail, faire un test et capture d'ecran
    'email': WORFLOW_DAG_ALERT,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': WORFLOW_RETRY_DELAY ,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    dag_id=                 WORKFLOW_DAG_ID,
    default_args=           WORKFLOW_DEFAULT_ARGS,
    description=            WORFLOW_DAG_DESCRIPTION,
    schedule_interval=      WORFLOW_SCHEDULE_INTERVAL 
)

# Download_data > send_data_to_processing > momitor_processinf > generate_report > send_email


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

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
