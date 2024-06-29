from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 target_table,
                 query,
                 truncate_table=False,
                 aws_redshift_conn_id="redshift_default",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_conn_id = aws_redshift_conn_id
        self.query = query
        self.truncate_table = truncate_table
        self.target_table = target_table


    def execute(self, context):
        self.log.info(f'Initialising LoadFactOperator to load to the table {self.target_table}')

        self.log.info(f'Initialising PostgresHook with connection {self.aws_redshift_conn_id}')
        hook = PostgresHook(self.aws_redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Truncating table {self.target_table}')
            hook.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f'Loading {self.query}')
        insert_query = f'INSERT INTO {self.target_table} {self.query}'
        hook.run(insert_query)
        self.log.info(f'Loaded data {self.query}')




