from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import helpers

class LoadFactOperator(BaseOperator):
    """
    Get data from staging tables to fact table
    
    :redshift_conn_id       Reshift cluster hook
    :target_table           Fact table in redshift to receive data
    :create_tbl             Sql statement to create fact table
    :source                 sql statement to insert data 
    """

    ui_color = '#F98866'
    insert_template = """
                    INSERT INTO {}
                    {}
                    ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 target_table="",
                 create_tbl="",
                 source="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_tbl = create_tbl
        self.target_table = target_table
        self.source = source

    def execute(self, context):
        self.log.info("********** LoadFactOperator is processing")
        # get hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"********** Running for {self.target_table}")

        # create fact table if not exists
        self.log.info('********** Create {} if not exists'.format(self.target_table))
        redshift.run(self.create_tbl)

        self.log.info('********** Delete data from  {} '.format(self.target_table))
        redshift.run("TRUNCATE TABLE {}".format(self.target_table))

        self.log.info("********** Inserting data into {}".format(self.target_table))
        insert_formated = LoadFactOperator.insert_template.format(
            self.target_table,
            self.source
        )      
        redshift.run(insert_formated)
        self.log.info("********** LoadFactOperator end !!")
