from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """ 
    Load Dimension table operator
    Load and Transform data from staging table to Dimension table
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query = "",
                 table = "",
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator on table {self.table} starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncate table {self.table} starting...')
            redshift.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f'Truncate table {self.table} completed!')

        formatted_sql = f'INSERT INTO {self.table} {self.sql_query}'
        
        redshift.run(formatted_sql)

        self.log.info(f'LoadDimensionOperator on table {self.table} completed!')
