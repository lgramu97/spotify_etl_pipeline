from platform import python_compiler
from tracemalloc import start
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

#Use scopes to comunnicate tasks.

"""
    Model A --->                                     ->> Accurate (BASHOP)
    Model B --->    EVALUATE (BranchPythonOperator)  
    Model C --->                                     ->> Inaccurate (BASHOP)

"""


def _training_model():
    return randint(1,10)

def _choose_best_model(ti):
    #Get the values of the 3 tasks.
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    #Choose the best and evaluate the model.
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

#This direct acyclic graph (DAG) will start 2022-07-07 and execute daily.
with DAG("my_dag",start_date=datetime(2022,7,7),
         schedule_interval="@daily",catchup=False,) as dag:
    
    #Define Models
    training_model_tasks = [
        PythonOperator(
            task_id=f"training_model_{model_id}",
            python_callable=_training_model,
            op_kwargs={
                "model": model_id
            }
        ) for model_id in ['A', 'B', 'C']
    ]
    
    #Define Branch
    choose_best_model = BranchPythonOperator(
        task_id = "choose_best_model",
        python_callable=_choose_best_model
    )
    
    #Define Bash Operators
    accurate = BashOperator(
        task_id = "accurate",
        bash_command="echo accurate"
    )


    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command="echo inaccurate"
    )
    
    #Same level models. Then choose best model. Finally choose action (same level)
    training_model_tasks >> choose_best_model >> [inaccurate, accurate]

