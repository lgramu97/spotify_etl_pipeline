import sys
import os
sys.path.insert(0,os.path.abspath(os.getcwd()))

from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common_args import default_args
from spoty_etl_airflow.base_etl import extract, transform, load


def transform_data(execution_date,**kwargs):
    """Transfor task for transform data

    Args:
        execution_date (_type_): Airflow macro. Data pass.

    Returns:
        pandas_dataframe_json: data transformed.
    """
    ti = kwargs['id']
    json_df = ti.xcom_pull(task_ids='extract_task')
    #Call transform function from base_etl.py
    return transform(json_df, execution_date).to_json()

def load_data(**kwargs):
    import pandas as pd

    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='transform_task')
    #Convert json to pandas object and load data (use load base_etl.py)
    load(pd.read_json(json_data)) 
    


dag = DAG(
    dag_id='x_com',
    description='etl_pipeline_xcom',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(5),
    end_date=days_ago(3),
    tags=['example']
)

#Execute transform data (base_etl.py)
t1_extract = PythonOperator(
    task_id = 'extract_task',
    python_callable=extract,
    dag=dag,
    provide_context=True
)

#Get json_data extracted and execute t2 (transform data.)
t2_transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag,
    provide_context = True
)

#Load data transformed to sql database.
t3_load = PythonOperator(
    task_id = 'load_task',
    python_callable=load_data,
    dag=dag   
)

t1_extract >> t2_transform >> t3_load