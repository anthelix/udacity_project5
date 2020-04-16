# with staging functions
# donr't use this .py
# prends et efface

from __future__ import print_function
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

## use with bash_command="echo {{ var.json.variables_config.s3_bucket }}"
dag_config = Variable.get("variables_config", deserialize_json=True)


#########################################################################
# FUNCTIONS
def print_hello():
    return 'Hello Word!'

default_args = {
    'owner': 'Chatagns',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False # Run only the last DAG
}

#########################################################################
# DAGS  ## changer starttime, schedule interval.
dag = DAG(
    'ETL_Sparkify',
    default_args=default_args,
    description='ETL with from S3 to Redshift with Airflow',
    schedule_interval='0 12 * * *',
    start_date=datetime(2020, 3, 20), catchup=False
)



####################################################################################
# TASKS

# task created by instantiating operators DummmyOerator
dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# task created by instantiating operators PythonOperators
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag) ###
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

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)
t3 = BashOperator(
    task_id="displaySourcePath",
    bash_command="echo {{ var.value.prefix }}", #set in variables
)

t4 = BashOperator(
    task_id="displayData",
    bash_command="echo {{ var.json.variables_config.s3_bucket }}", #set in variables
)

t5 = BashOperator(
    task_id="get_dag_config",
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag,
)
 
t1 >> t5 >> t3 >> t4 



