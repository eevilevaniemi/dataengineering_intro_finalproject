from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import sqlite3
from weather_etl import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='ETL DAG for weather data',
    schedule_interval='@daily',
    max_active_runs=1,
)


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    dag=dag,
)

load_to_sqlite_task = PythonOperator(
    task_id='load_to_sqlite_task',
    python_callable=load_to_sqlite,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_to_sqlite_task
