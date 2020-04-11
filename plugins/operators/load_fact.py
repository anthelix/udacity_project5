from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    With DIMENSION and FACT operators, you can utilize the provided SQL helper class to 
    run data transformations. Most of the logic is within the SQL transformations and 
    the operator is expected to take as input a SQL statement and target database on 
    which to run the query against. You can also define a target table that will contain 
    the results of the transformation.

   Avec les opérateurs DIMENSION et FACT, vous pouvez utiliser la classe d'aide SQL 
   fournie pour exécuter des transformations de données. La plupart de la logique se 
   trouve dans les transformations SQL et l'opérateur est censé prendre en entrée une 
   instruction SQL et une base de données cible sur lesquelles exécuter la requête. 
   Vous pouvez également définir une table cible qui contiendra les résultats de la 
   transformation.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
