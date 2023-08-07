from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook



class PostgresFileOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                operation,
                config={},
                *args, 
                **kargs):
        super(PostgresFileOperator, self).__init__(*args, **kargs)
        self.operation = operation
        self.config = config
        self.postgres_hook = PostgresHook(postgres_conn_id='postgres')

    def execute(self, context):
        if self.operation == 'write':
            self.writeInTable()
        elif self.operation == 'read':
            pass
       
    def writeInTable(self):
        self.postgres_hook.bulk_load(self.config.get('table_name'),'plugins/operators/resultados.tsv')
        