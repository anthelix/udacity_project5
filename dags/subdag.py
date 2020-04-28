import datetime
from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.udacity_plugin import HasRowsOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator
from airflow.operators.udacity_plugin import DataQualityOperator

from helpers import CreateTables, SqlQueries

def get_s3_to_redshift_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        create_tbl,
        target_table,
        s3_bucket,
        s3_key,
        custom,
        *args, **kwargs):

    dag= DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    templated_command = """
    echo "**************** {{ task.owner }}, {{ task.task_id }}"
    echo "**************** The execution date : {{ ds }}"
    echo "**************** {{ task_instance_key_str }} is running"
    """

    info_task = BashOperator(
        task_id=f"Info_about_{parent_dag_name}.{task_id}",
        dag=dag,
        depends_on_past=False,
        bash_command=templated_command,
    )

    copy_task = StageToRedshiftOperator(
        task_id=f"copy_{target_table}_from_s3_to_redshift",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        create_tbl=create_tbl,
        target_table=target_table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        custom=custom
    )

    check_staging = HasRowsOperator(
        task_id=f"check_{target_table}_rows",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        target_table=target_table,
    )

    info_task >> copy_task 
    copy_task >> check_staging
    return dag


def get_dimTables_to_Redshift_subdag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        create_tbl,
        target_table,
        source_table,
        append_data,
        pk,
        *args, **kwargs):
    dag= DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    templated_command = """
    echo "**************** The execution date : {{ ds }}"
    echo "**************** {{ task.owner }}, {{ task.task_id }}"
    echo "**************** {{ task_instance_key_str }} is running"
    """

    info_task = BashOperator(
        task_id=f"Info_about_{parent_dag_name}.{task_id}_running",
        dag=dag,
        depends_on_past=False,
        bash_command=templated_command,
    )

    load_dimension_table = LoadDimensionOperator(
        task_id=f"load_{target_table}_dim_table",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        create_tbl=create_tbl,
        target_table=target_table,
        source_table=source_table,
        append_data=append_data,
        pk=pk
    )

    check_dimension_quality = DataQualityOperator(
        task_id =f"{target_table}_quality",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        target_table = target_table,
        pk = pk,
    )

    info_task >> load_dimension_table 
    load_dimension_table >> check_dimension_quality
    return dag