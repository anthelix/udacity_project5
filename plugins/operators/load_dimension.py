from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    With DIMENSION and FACT operators, you can utilize the provided SQL helper class to 
    run data transformations. Most of the logic is within the SQL transformations and 
    the operator is expected to take as input a SQL statement and target database on 
    which to run the query against. You can also define a target table that will contain 
    the results of the transformation.
    Dimension loads are often done with the truncate-insert pattern where the target 
    table is emptied before the load. Thus, you could also have a parameter that allows 
    switching between insert modes when loading dimensions. Fact tables are usually so 
    massive that they should only allow append type functionality.

   Avec les opérateurs DIMENSION et FACT, vous pouvez utiliser la classe d'aide SQL 
   fournie pour exécuter des transformations de données. La plupart de la logique se 
   trouve dans les transformations SQL et l'opérateur est censé prendre en entrée une 
   instruction SQL et une base de données cible sur lesquelles exécuter la requête. 
   Vous pouvez également définir une table cible qui contiendra les résultats de la 
   transformation.
   Les chargements de DIMENSION sont souvent effectués avec le modèle de troncature-insertion 
   où la table cible est vidée avant le chargement. Ainsi, vous pouvez aussi avoir un 
   paramètre qui permet de passer d'un mode d'insertion à l'autre lors du chargement 
   des dimensions. Les tables de faits sont généralement si massives qu'elles ne 
   devraient permettre que la fonctionnalité de type append.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
