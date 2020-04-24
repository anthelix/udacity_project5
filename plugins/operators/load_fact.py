from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import helpers

class LoadFactOperator(BaseOperator):
    """
    With DIMENSION and FACT operators, you can utilize the provided SQL helper class to 
    run data transformations. Most of the logic is within the SQL transformations and 
    the operator is expected to take as input a SQL statement and target database on 
    which to run the query against. You can also define a target table that will contain 
    the results of the transformation.

   Avec les opérateurs DIMENSION et FACT, 
   - classe d'aide SQL fournie pour exécuter des transformations de données. 
   - La plupart de la logique se trouve dans les transformations SQL et 
   - prend en entrée une instruction SQL et une base de données cible sur lesquelles exécuter la requête. 
   - définir une table cible qui contiendra les résultats de la transformation.
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
                 create_tbl="",
                 target_table="",
                 source_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_tbl = create_tbl
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        self.log.info("LoadFactOperator is processing")
        delete_data = "TRUNCATE TABLE {}"

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inserting data into {}".format(self.target_table))
        redshift.run(self.create_tbl)
        redshift.run(delete_data.format(self.target_table))
        insert_facts_templated = LoadFactOperator.insert_template.format(
            self.target_table,
            self.source_table
        )
        redshift.run(insert_facts_templated)

        self.log.info("LoadFactOperator end !!")
