from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

class DataQualityOperator(BaseOperator):
    """
    L'opérateur final à créer est l'OPÉRATEUR DE LA QUALITÉ DES DONNÉES, qui est utilisé 
    pour effectuer des contrôles sur les données elles-mêmes. La principale fonctionnalité 
    de l'opérateur consiste à recevoir un ou plusieurs cas de test basés sur SQL ainsi 
    que les résultats attendus et à exécuter les tests. Pour chaque test, le résultat du 
    test et le résultat attendu doivent être vérifiés et s'il n'y a pas de correspondance, 
    l'opérateur doit soulever une exception et la tâche doit être réessayée et échouer 
    éventuellement.

    Par exemple, un test pourrait être une instruction SQL qui vérifie si une certaine 
    colonne contient des valeurs NULL en comptant toutes les lignes qui ont NULL dans 
    la colonne. Nous ne voulons pas avoir de NULL, donc le résultat attendu serait 0 et 
    le test comparerait le résultat de l'instruction SQL au résultat attendu.
    SELECT COUNT(*) FROM () as foo
    WHERE foo is NULL;
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
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.pk = pk

    def execute(self, context):
        self.log.info(f'********** DataQualityOperator processing {self.target_table}')
        redshift = PostgresHook(self.redshift_conn_id)
       
        
        #check_records = redshift.get_records(f"SELECT att.attname FROM pg_index ind, pg_class cl, pg_attribute att WHERE cl.oid = 'public.{self.target_table}'::regclass AND ind.indrelid = cl.oid AND att.attrelid = cl.oid and att.attnum = ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' ')) and attnum > 0 AND ind.indisprimary order by att.attnum;")
        #self.log.info(f'Check Reccords is {check_records} for {self.target_table}')

        null_records = redshift.get_records(f"SELECT COUNT(*) FROM {self.target_table} WHERE {self.pk} is NULL")
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.target_table}")

        num_null = null_records[0][0]
        num = records[0][0]

        self.log.info(f"********** Table {self.target_table} passe with {null_records[0][0]} null records")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"********** Data quality check failed. {self.target_table} returned no results")
        
        if num_null > 0: 
            raise ValueError(f"********** Data quality check failed. {self.target_table} contained {null_records[0][0]} null values")
        self.log.info(f"********** Data quality on table {self.target_table} check passed with {records[0][0]} records none null")
        self.log.info(f"********** DataQualityOperator end !!")


        #check_formated = DataQualityOperator.check_template.format(self.target_table)
        #check_records2 = redshift.get_records(check_formated)[0]
        #self.log.info(f'Check Reccords2 is {check_records2} for {self.target_table}')
        



