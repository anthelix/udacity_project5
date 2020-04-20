from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    The STAGE operator is expected to be able to load any JSON formatted files from S3 to 
    Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters
    provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
    The parameters should be used to distinguish between JSON file. Another important requirement
     of the STAGE operator is containing a templated field that allows it to load timestamped files 
     from S3 based on the execution time and run backfills.

    _ charger n'importe quel fichier au format JSON de S3 vers Amazon Redshift.
    _ crée et exécute une instruction SQL COPY sur la base des paramètres fournis. 
    _ Les paramètres de l'opérateur doivent 
        _ préciser où le fichier est chargé en S3 et 
        _ quelle est la table cible.
    _ Les paramètres doivent être utilisés pour distinguer les fichiers JSON. 
    _ l'opérateur STAGE est de contenir un champ de modèle qui lui permet de 
        _ charger des fichiers horodatés à partir de S3 en fonction du temps d'exécution 
        _ exécuter des remplissages.

    """
    """
    Extract JSON data from s3Bucket to redshift staging tables.

    :s3_key                 Source s3Bucket prefix
    :arn_iam_role           AWS arn iam_role with permission to read data from s3
    :aws_region                 AWS region where is the redshift cluster
    :json_format            Source json format
    :redshift_conn_id       Reshift cluster hook
    :aws_credentials_id     AWS iam hook
    :target_table           Tables staging in redshift to receive data
    :s3_bucket              Source s3Bucket



    """
    ui_color = '#358140'

    # templated field to formated strings
    ## s3_key = 'song_data' and 'log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json
    ## execution_date.year, execution_date.month, ds are context variables
    template_fields     = ("s3_key",)

    # templated copy_sql statement
    copy_query_template = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    compupdate off
                    FORMAT AS JSON '{}';
    """
    @apply_defaults # define operators parameters
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs) 
        # set the attributes on our class
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format


    def execute(self, context):
        self.log.info('StageToRedshiftOperator is processing')
        # get hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        credentials = aws_hook.get_credentials()
        
        # clear target_table in redshift
        self.log.info('Clearing data from destination Redshift table')
        redshift_hook.run("DELETE FROM {}".format(self.target_table))

        # copy data from s3 to redshift
        self.log.info('Copying data from s3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("rendered_key: {}".format(rendered_key))

        sql_stmt = StageToRedshiftOperator.copy_query_template.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        self.log.info(f'Running {sql_stmt}')
        redshift_hook.run(sql_stmt)





