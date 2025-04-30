from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from sql.sql_statements import SqlQueries as sqt_stm

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,             
    'retries': 3,                         
    'retry_delay': timedelta(minutes=5),  
    'email_on_retry': False  ,
    'catchup': False      
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    redshift_conn_id="redshift"
    aws_credentials_id="aws_credentials"

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_events",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="log-data",
        json_path="s3://udaprobucket/log_json_path.json",
        create_sql=sqt_stm.staging_events_table_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_songs",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="song-data",
        create_sql=sqt_stm.staging_songs_table_create
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        load_sql=sqt_stm.songplay_table_insert,
        create_sql=sqt_stm.songplays_table_create
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_sql=sqt_stm.user_table_insert,
        table='"users"',
        create_sql=sqt_stm.users_table_create
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_sql=sqt_stm.song_table_insert,
        table='songs',
        create_sql=sqt_stm.songs_table_create
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_sql=sqt_stm.artist_table_insert,
        table='artists',
        create_sql=sqt_stm.artists_table_create
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        load_sql=sqt_stm.time_table_insert,
        table='"time"',
        create_sql=sqt_stm.time_table_create
    )

    checks=[
        {
            "test_sql": "SELECT COUNT(*) FROM users",
            "expected_result": 0,
            "comparison": ">",
            "check_name": "Users table not empty"
        },
        {
            "test_sql": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL",
            "expected_result": 0,
            "comparison": "==",
            "check_name": "No NULL song IDs"
        }
    ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=redshift_conn_id,
        checks = checks
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()