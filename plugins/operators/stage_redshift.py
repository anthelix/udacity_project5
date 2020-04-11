from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    The STAGE operator is expected to be able to load any JSON formatted files from S3 to 
    Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters
    provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
    The parameters should be used to distinguish between JSON file. Another important requirement
     of the STAGE operator is containing a templated field that allows it to load timestamped files 
     from S3 based on the execution time and run backfills.



    L'opérateur STAGE devrait pouvoir charger n'importe quel fichier au format JSON de 
    S3 vers Amazon Redshift. L'opérateur crée et exécute une instruction SQL COPY sur 
    la base des paramètres fournis. Les paramètres de l'opérateur doivent préciser où 
    le fichier est chargé en S3 et quelle est la table cible.
    Les paramètres doivent être utilisés pour distinguer les fichiers JSON. Une autre 
    exigence importante de l'opérateur STAGE est de contenir un champ de modèle qui lui 
    permet de charger des fichiers horodatés à partir de S3 en fonction du temps 
    d'exécution et d'exécuter des remplissages.

    """


    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')





