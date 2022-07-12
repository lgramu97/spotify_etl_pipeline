import sys
import os
sys.path.insert(0,os.path.abspath(os.getcwd()))

from datetime import timedelta
from email.policy import default
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from spoty_etl_airflow.base_etl import extract, transform, load
from dags.common_args import default_args

#Programatic way to define ETL.
@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
)

def taskflow_api_etl():
    @task
    def extract_task():
        context = get_current_context()
        return extract(context["execution_date"])
    
    @task
    def transform_task(json_df):
        context = get_current_context()
        return transform(json_df,context["execution_date"]).to_json()
    
    @task
    def load_task(json_df):
        import pandas as pd
        load(pd.read_json(json_df))
    
    #Get df, clean df, load df.
    df = extract_task()
    clean_df = transform_task(df)
    load_task(clean_df)

#Execute pipeline.
taskflow_api_etl_dag = taskflow_api_etl()