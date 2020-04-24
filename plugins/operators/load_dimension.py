from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import helpers

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
        self.log.info('LoadDimensionOperator is processing')
        delete_data = "TRUNCATE TABLE {}"
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data == False:
            redshift.run(delete_data.format.self.table)

        self.log.info("Inserting data into {}".format(self.target_table))
        insert_dim_formated = LoadDimensionOperator.insert_template.format(
            self.target_table,
            self.source_table
        )

        self.log.info(f"LoadDimensionOperator : {insert_dim_formated}")
        redshift.run(insert_dim_formated)
        self.log.info("LoadDimensionOperator end !!")