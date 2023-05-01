from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from spotipy_extract_load import run_project_etl, upload_to_s3

default_args = {
    'owner': 'sanjiv',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'email':['tamusanjiv6773@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotipy_dag',
    default_args=default_args,
    description='DAG to extract and load data from spotify API',
    
)

extract = PythonOperator(
    task_id='extract',
    python_callable=run_project_etl,
    dag = dag
)

load = PythonOperator(
    task_id='load',
    python_callable=upload_to_s3,
    dag = dag
)

extract >> load