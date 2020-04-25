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
    """
    ui_color = '#89DA59'
    check_template = """
                    SELECT COUNT(*) FROM (SELECT a.attname
                            FROM   pg_index i
                            JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                            WHERE  i.indrelid = 'contacts'::regclass
                            AND    i.indisprimary) as foo
                    WHERE foo is NULL;
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                target_table="",
                expected_ans="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.expected_ans = expected_ans

    def execute(self, context):
        self.log.info(f'DataQualityOperator processing {target_table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        check_records = redshift.get_records(check_template)[0]


