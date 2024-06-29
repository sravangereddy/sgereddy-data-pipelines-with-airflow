from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    dag_id='final_project',
    schedule_interval=None,
    start_date=datetime.now(),
    end_date=None,
    catchup=False,
    default_args=default_args
)

start = DummyOperator(task_id='start', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    s3_path="s3://sgereddy-udacity-airflow-bucket-13/log-data/",
    target_table="staging_events",
    json_path='s3://sgereddy-udacity-airflow-bucket-13/log_json_path.json',
    aws_redshift_conn_id="redshift_default",
    aws_credentials_conn_id="aws_credentials",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    s3_path="s3://sgereddy-udacity-airflow-bucket-13/song-data-2/A/A/",
    target_table="staging_songs",
    aws_redshift_conn_id="redshift_default",
    aws_credentials_conn_id="aws_credentials",
    dag=dag
)

load_fact_song_plays = LoadFactOperator(task_id='load_fact_song_plays',
                                        target_table='songplays',
                                        query=SqlQueries.songplay_table_insert,
                                        truncate_table=True,
                                        aws_redshift_conn_id="redshift_default",
                                        dag=dag)

test_case_params = {
    'null_checks': [{'table_name': 'staging_events', 'column_name': 'artist', 'threshold': 0.25}],
    'duplicate_checks': [{'table_name': 'users', 'column_name': 'userid'}],
    'row_count_check': [{'table_name': 'songs'}, {'table_name': 'artists'}, {'table_name': 'users'}],
}
data_quality = DataQualityOperator(
    task_id='data_quality',
    test_case_params=test_case_params,
    aws_redshift_conn_id="redshift_default",
    dag=dag,
)

dimension_details = {
    'users': SqlQueries.user_table_insert,
    'songs': SqlQueries.song_table_insert,
    'time': SqlQueries.time_table_insert,
    'artists': SqlQueries.artist_table_insert,
}

for task, script in dimension_details.items():
    _task = LoadDimensionOperator(
        task_id=f'load_dimension_{task}',
        target_table=task,
        query=script,
        truncate_table=True,
        aws_redshift_conn_id="redshift_default",
        dag=dag
    )
    load_fact_song_plays.set_downstream(_task)
    _task.set_downstream(data_quality)

end = DummyOperator(task_id='end', dag=dag)


start.set_downstream(stage_events_to_redshift)
start.set_downstream(stage_songs_to_redshift)
stage_events_to_redshift.set_downstream(load_fact_song_plays)
stage_songs_to_redshift.set_downstream(load_fact_song_plays)
data_quality.set_downstream(end)
