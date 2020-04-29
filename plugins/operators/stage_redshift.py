from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.plugins_manager import AirflowPlugin
from helpers import CreateTables
import time

class StageToRedshiftOperator(BaseOperator):
    """
    Extract JSON data from s3Bucket to redshift staging tables.

    :s3_key                 Source s3Bucket prefix
    :aws_region             AWS region where is the redshift cluster
    :json_format            Source json format
    :redshift_conn_id       Reshift cluster hook
    :aws_credentials_id     AWS iam hook
    :create_tbl             Sql statement to create staging tables
    :target_table           Tables staging in redshift to receive data
    :s3_bucket              Source s3Bucket
    :custom                 parameters to complete the copy query
    """
    ui_color = '#358140'

    template_fields     = ("s3_key", )

    # templated copy_sql statement
    copy_query_template = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    {}
                    ;
    """
    @apply_defaults # define operators parameters
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_tbl="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 custom="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs) 
        # set the attributes on our class
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_tbl = create_tbl
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.custom = custom


    def execute(self, context):
        self.log.info('********** StageToRedshiftOperator is processing')

        # get hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        credentials = aws_hook.get_credentials()
        
        # clear target_table in redshift
        #dropTable = CreateTables.dropTable(self.target_table)
        #redshift_hook.run(dropTable)

        # clear data from table


        # create stage table if not exists
        self.log.info('********** Create {} if not exists'.format(self.target_table))
        redshift_hook.run(self.create_tbl)     
        time.sleep(10)   
        self.log.info(f"********** {self.target_table} create after 10 secondes  !!!!")

        self.log.info('********** Delete data from {} '.format(self.target_table))
        redshift_hook.run(f"TRUNCATE {self.target_table}")

        # copy data from s3 to redshift
        self.log.info('********** Copying data from s3 to Redshift in ' + self.target_table)
        #rendered_key = self.s3_key.format(**context)
        #self.log.info(f"**********  {rendered_key}")

        #self.log.info(f'rendered_key : {rendered_key}')
        s3_path = "s3://" + self.s3_bucket + "/" + self.s3_key
        self.log.info(f"********** {s3_path}")

        copy_formated = StageToRedshiftOperator.copy_query_template.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.custom,
        )
        redshift_hook.run(copy_formated)

        self.log.info("********** StageToRedshiftOperator end !!")





