import pprint
import logging
import os
import glob
from datetime import timedelta, datetime
import airflow
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


import pandas as pd
def list_keys():
    all_files = []
    bucket = Variable.get('folder')
    logging.info(f"Linstings Keys from {bucket}")
    for root, dirs, files in os.walk(bucket):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.basename(f))
    for key in all_files:
        logging.info(f"{key}")



default_args = {
    'owner': 'dend_stephanie',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'Working_locally',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2019, 3, 20), catchup=False
)


t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t8 = PythonOperator(
    task_id="get_path_files",
    python_callable=list_keys,
    dag=dag
)



t1 >> t8