import sys
import os
sys.path.insert(0,os.path.abspath(os.getcwd()))

from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common_args import default_args
from spoty_etl_airflow.base_etl import run_etl


dag = DAG(
    dag_id='single_task',
    description='etl_pipeline_on_task',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(8),
    end_date=days_ago(6),
    tags=['example']
)

etl = PythonOperator(
    task_id = 'run_etl_process',
    python_callable=run_etl,
    dag=dag
)
