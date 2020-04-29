from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

class DataQualityOperator(BaseOperator):
    """
    check data after loading from staging tables to dim table

    :redshift_conn_id       Reshift cluster hook
    :target_table           Dimension table in redshift to receive data
    :pk                     Primary key 
    """


    ui_color = '#89DA59'
    check_template = """
                    SELECT count(*) FROM users
                    WHERE(SELECT att.attname FROM pg_index ind, pg_class cl, pg_attribute att WHERE cl.oid = 'public.{}'::regclass AND ind.indrelid = cl.oid AND att.attrelid = cl.oid and att.attnum = ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' ')) and attnum > 0 AND ind.indisprimary order by att.attnum) is null
                    LIMIT 1;
                    
    """

    second = """
            SELECT COUNT(*) FROM {} WHERE {} is not NULL;
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                target_table="",
                pk="",
                sql_quality="",
                sql_row="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.pk = pk
        self.sql_row = sql_row
        self.sql_quality = sql_quality


    def execute(self, context):
        self.log.info(f'********** DataQualityOperator processing {self.target_table}')
        redshift = PostgresHook(self.redshift_conn_id)
       
        #check_records = redshift.get_records(f"SELECT att.attname FROM pg_index ind, pg_class cl, pg_attribute att WHERE cl.oid = 'public.{self.target_table}'::regclass AND ind.indrelid = cl.oid AND att.attrelid = cl.oid and att.attnum = ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' ')) and attnum > 0 AND ind.indisprimary order by att.attnum;")
        #self.log.info(f'Check Reccords is {check_records} for {self.target_table}')

        self.log.info(f"********** Running sql query {self.sql_row} ")
        records = redshift.get_records(f"{self.sql_row} " )        



        self.log.info(f"********** Running sql query {self.sql_quality} ")
        null_records = redshift.get_records(f"{self.sql_quality}" )
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"********** Data quality check failed. {self.target_table} returned no results")
        
        num = records[0][0]
        num_null = null_records[0][0]

        self.log.info(f"********** Table {self.target_table} passe with {null_records[0][0]} null records")
        if num_null > 0:
            raise ValueError(F"********** Data quality check failed. {self.target_table} return primary key with null value")

        if num_null > 0: 
            raise ValueError(f"********** Data quality check failed. {self.target_table} contained {null_records[0][0]} null values")
        self.log.info(f"********** Data quality on table {self.target_table} check passed with {records[0][0]} records")
        self.log.info(f"********** DataQualityOperator end !!")


        #check_formated = DataQualityOperator.check_template.format(self.target_table)
        #check_records2 = redshift.get_records(check_formated)[0]
        #self.log.info(f'Check Reccords2 is {check_records2} for {self.target_table}')
        



