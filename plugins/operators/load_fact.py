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

        # create stage table if not exists
        self.log.info('********** Create {} if not exists'.format(self.target_table))
        redshift.run(self.create_tbl)

        self.log.info("********** Inserting data into {}".format(self.target_table))
        #redshift.run(self.create_tbl)
     
        redshift.run("INSERT INTO {} {}".format(self.target_table, self.source))
        self.log.info("********** LoadFactOperator end !!")
