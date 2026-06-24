from datetime import timedelta
from pendulum import datetime
import random


import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

def process_data():
    flower_df = pd.read_csv("/tmp/flowerdataset.csv")
    flower_df["volume"] = flower_df["sepal_length"] * flower_df["sepal_width"]
    print(flower_df.head())    

def branch_function():
    return random.choice(["task_option_1", "task_option_2"])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 6, 23),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    "dag-branching",
    default_args=default_args,
    schedule="@daily",
)

download_data_task = BashOperator(
    task_id="download_data",
    bash_command='curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv',
    dag=dag,
)

process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)

branching_test = BranchPythonOperator(
    task_id="branching",
    python_callable=branch_function,
    dag=dag
)

task_option_1_task = BashOperator(
    task_id="task_option_1",
    bash_command='echo option 1',
    dag=dag,
)

task_option_2_task = BashOperator(
    task_id="task_option_2",
    bash_command='echo option 2',
    dag=dag,
)


download_data_task >> process_data_task >> branching_test
branching_test >> [task_option_1_task, task_option_2_task]