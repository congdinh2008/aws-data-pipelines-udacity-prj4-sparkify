from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.create_tables import CreateTableOperator
from udacity.common import final_project_sql_statements

SqlQueries = final_project_sql_statements.SqlQueries

@dag(
    description='Create tables in Redshift',
    start_date=pendulum.now(),
    schedule_interval='@once',
    max_active_runs=1
)
def create_tables():

    create_redshift_tables = CreateTableOperator(
        task_id='Create_tables',
        redshift_conn_id="redshift"
    )

    create_redshift_tables

create_tables_dag = create_tables()