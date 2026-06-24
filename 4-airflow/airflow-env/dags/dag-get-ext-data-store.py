from datetime import timedelta
from pendulum import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def process_data():
    flower_df = pd.read_csv("/tmp/flowerdataset.csv")
    flower_df["volume"] = flower_df["sepal_length"] * flower_df["sepal_width"]

    largest_flower = flower_df.sort_values("volume", ascending=False).iloc[0]

    return {
        "volume": largest_flower["volume"],
        "speal_length": largest_flower["sepal_length"],
        "speal_width": largest_flower["sepal_width"],
    }

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 6, 23),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    "dag-get-ext-data-store",
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

create_task = SQLExecuteQueryOperator(
    task_id="create_table",
    postgres_conn_id="postgres_localhost",
    sql="""
        CREATE TABLE IF NOT EXISTS flower (
        volume FLOAT,
        sepal_length FLOAT,
        sepal_width FLOAT
        )
    """,
    dag=dag,
)

insert_task = SQLExecuteQueryOperator(
    task_id="instert_into_postgres",
    postgres_conn_id="postgres_localhost",
    sql="""
        INSERT INTO flower (volume, sepal_length, sepal_width)
        VALUES (
            {{ task_instance.xcom_pull(task_ids='process_data')["volume"]}},
            {{ task_instance.xcom_pull(task_ids='process_data')["sepal_length"]}},
            {{ task_instance.xcom_pull(task_ids='process_data')["sepal_width"]}}
        )
    """,
    dag=dag,
)

download_data_task >> process_data_task >> create_task >> insert_task
