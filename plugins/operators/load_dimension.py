from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import helpers

class LoadDimensionOperator(BaseOperator):
    """
    Get data from staging tables to dimension table

    :redshift_conn_id       Reshift cluster hook
    :target_table           Fact table in redshift to receive data
    :create_tbl             Sql statement to create dimension table
    :source_table           sql statement to insert data 
    :append_data            append only or truncate/append data
    """

    ui_color = '#80BD9E'
    insert_template = """
                    INSERT INTO {}
                    {}
                    ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 create_tbl="",
                 source_table="",
                 target_table="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_tbl = create_tbl
        self.source_table = source_table
        self.target_table = target_table
        self.append_data = append_data
       
    def execute(self, context):
        self.log.info('********** LoadDimensionOperator is processing')
        # get hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"********** Running for {self.target_table}")

        # create dim table if not exists
        self.log.info('********** Create {} if not exists'.format(self.target_table))
        redshift.run(self.create_tbl)

        self.log.info(f"********** Running for {self.target_table}")

        # delete data before inserting
        if self.append_data == False:
                redshift.run("TRUNCATE TABLE {}".format(self.target_table))
        # insert anyway
        self.log.info("********** Inserting data into {}".format(self.target_table))
 
        insert_formated = LoadDimensionOperator.insert_template.format(self.target_table, self.source_table) 
        redshift.run(insert_formated)
        #redshift.run("INSERT INTO {} {}".format(self.target_table, self.source_table))
        self.log.info("********** LoadDimensionOperator end !!")