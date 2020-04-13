from datetime import timedelta, datetime
import airflow
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import MyFirstOperator, MyFirstSensor


from helpers import CreateTables

# edit the .dags/config/variables.json
dag_config = Variable.get("variables_config", deserialize_json=True)
aws_default_region = dag_config["aws_default_region"]
s3_bucket = dag_config["s3_bucket"]
s3_prefix = dag_config["s3_prefix"]


def print_hello():
    return 'Hello Word!'

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
    'Udacity_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2020, 3, 20), catchup=False
)



# task created by instantiating operators DummmyOerator
dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# task created by instantiating operators PythonOperators
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
# custom sensor and operator
sensor_task = MyFirstSensor(
                            task_id='my_sensor_task',
                            poke_interval=30, 
                            dag=dag)
operator_task = MyFirstOperator(
                                my_operator_param='This is a test.',
                                task_id='my_first_operator_task', 
                                dag=dag)

# Create Staging tables
create_staging_events = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="sparkify",
    sql=CreateTables.staging_events_table_create
)

create_staging_songs = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="sparkify",
    sql=CreateTables.staging_songs_table_create
)



# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

hello_operator >> t1 
t1 >> [t2, t3]
t1 >> dummy_task >> sensor_task >> operator_task
t2 >> create_staging_events
t2 >> create_staging_songs