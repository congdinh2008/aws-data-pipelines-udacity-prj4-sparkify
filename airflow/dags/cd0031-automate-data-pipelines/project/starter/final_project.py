from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common import final_project_sql_statements
SqlQueries = final_project_sql_statements.SqlQueries

region = "us-west-2"
redshift_conn_id = "redshift"
aws_credentials_id = "aws_credentials"
s3_bucket_name = "congdinh2023-sparkify"

default_args = {
    'owner': 'congdinh2023',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="public.staging_events",
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket_name,
        s3_key="log-data",
        region=region,
        file_format="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="public.staging_songs",
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket_name,
        s3_key="song-data",
        region=region,
        file_format="auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        table='public.songplays',
        truncate=True,
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        table='public.users',
        truncate=True,
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        table='public.songs',
        truncate=True,
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        table='public.artists',
        truncate=True,
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        table='public.time',
        truncate=True,
        sql_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        aws_credentials_id=aws_credentials_id,
        redshift_conn_id=redshift_conn_id,
        tables=["public.songplays", "public.users", "public.songs", "public.artists", "public.time"]
    )
    
    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table, 
                             load_song_dimension_table, 
                             load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()