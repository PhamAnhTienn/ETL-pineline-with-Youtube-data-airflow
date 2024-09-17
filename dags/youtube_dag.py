from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from youtube_etl import run_youtube_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'youtube_etl',
    default_args=default_args,
    description='A simple ETL process for extracting data from YouTube',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['youtube'],
)

run_etl = PythonOperator(
    task_id='complete_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag, 
)

load_csv = PostgresOperator(
    task_id='load_csv',
    sql="COPY Youtube_data FROM '/path/to/videos.csv' WITH CSV HEADER",
    postgres_conn_id='your_postgres_conn_id',
    dag=dag,
)

run_etl >> load_csv