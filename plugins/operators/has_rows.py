import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin


class HasRowsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('********** HasRowsOperator is processing')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.target_table}")
        self.log.info(f"********** Running for {self.target_table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"********** Data quality check failed. {self.target_table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"********** Data quality check failed. {self.target_table} contained 0 rows")
        logging.info(f"********** Data quality on table {self.target_table} check passed with {records[0][0]} records")
        self.log.info(f"********** HasRowsOperator end !!")

