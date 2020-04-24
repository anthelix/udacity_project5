# with staging functions
# donr't use this .py
# prends et efface

from __future__ import print_function
import pprint
import logging
import os
import glob
import airflow
import datetime
from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import MyFirstOperator, MyFirstSensor

from airflow.sensors.sql_sensor import SqlSensor
from helpers import CreateTables

def print_hello():
    return 'Bonjour!'
# Dans la base de donnnes creer maunuellemt, creer 2 tables source_schema et dest_schema
# puis creer 2 connection postgres, host=postgres
def move_data():
    src = PostgresHook(postgres_conn_id='source', schema='source_schema')
    dest = PostgresHook(postgres_conn_id='dest', schema='dest_schema')
    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    cursor.execute("SELECT * FROM users;")
    dest.insert_rows(table=MY_DEST_TABLE, rows=cursor)

# docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
# docker-compose run --rm webserver airflow variables --get s3_bucket
# docker-compose run --rm webserver airflow variables --set var4 value4]


# display all the template variable contexte
def _print_exec_date(**context):
    pprint.pprint(context)




#########################################################################
# edit the .dags/config/variables.json
dag_config = Variable.get("variables_config", deserialize_json=True)

default_args = {
    'owner': 'dend_stephanie',
    'depends_on_past': False,
    'catchup': False, 
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

target_db = 'mini-warehouse-db'

dag = DAG(
    'chadag_dailyBackSlashDsS',
    start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    schedule_interval='@daily',
    default_args=default_args,
    description='A simple tutorial DAG',
    )
####################################################################################
# A simple sensor checking wether clickstream data is present 
# for the day following the execution_date of the DAG Run
#sensor_query_template = '''
#SELECT TRUE
#FROM clickstream
#WHERE click_timestamp::DATE >= '{{ execution_date + macros.timedelta(days=1) }}'::DATE
#LIMIT 1
#'''

# A sensor task using a SqlSensor operator#
#sensor_task = SqlSensor(
#    task_id='check_data_avail',
#    conn_id=target_db,
#    sql=sensor_query_template,
#    poke_interval=30,
#    timeout=3600,
#    dag=dag)

# A minimalist idempotent aggregation query for clickstream data
#aggregation_query_template = '''
#BEGIN;
#DELETE FROM clickstream_aggregated
#WHERE click_date = '{{ execution_date }}'::DATE;
#INSERT INTO clickstream_aggregated
#SELECT click_timestamp::DATE AS click_date,
#    SUM(CASE WHEN is_ad_display_event THEN 1 ELSE 0 END) AS nb_ad_display_events,
#    SUM(CASE WHEN is_ad_search_event THEN 1 ELSE 0 END) AS nb_ad_search_events
#FROM clickstream
#WHERE click_timestamp::DATE = '{{ execution_date }}'::DATE
#GROUP BY 1;
#COMMIT;
#'''

# A simple postgres SQL task execution
#aggregation_task = PostgresOperator(
#    task_id='aggregate_clickstream_data',
#    postgres_conn_id=target_db,
#    sql=aggregation_query_template,
#    autocommit=False,
#    dag=dag)








# task created by instantiating operators DummmyOerator
dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# task created by instantiating operators PythonOperators
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag) ###

# custom sensor and operator
## cf plugins/operators/my_operators.py
sensor_task = MyFirstSensor(
                            task_id='my_sensor_task',
                            poke_interval=30, 
                            dag=dag)

operator_task = MyFirstOperator(
                                my_operator_param='This is a cat woman.',
                                task_id='my_first_operator_task', 
                                dag=dag)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)
# bucket = udacity-dend 
t3 = BashOperator(
    task_id="displaySourcePath",
    bash_command="echo {{ var.value.bucket }}", #set in variables
)
# variables_config=  { "aws_default_region": "eu-east-4", "s3_bucket": "udacity-dend", "source_path": "/usr/local/airflow/data", "s3_prefix": "data-pipelines" } 
t4 = BashOperator(
    task_id="displayData",
    bash_command="echo {{ var.json.variables_config.s3_bucket }}", #set in variables
)

#t5 = BashOperator(
#    task_id="get_dag_config",
#    bash_command='echo "{0}"'.format(dag_config),
#    dag=dag,
#)


templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t6 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

t7 = PythonOperator(
    task_id="get_context_variables",
    python_callable=_print_exec_date,
    dag=dag,
    provide_context=True
)


dummy_task >> t1
t1 >> t6
t1 >> t7
