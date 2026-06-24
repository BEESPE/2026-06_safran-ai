from datetime import datetime, timedelta
from pathlib import Path
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator


INPUT_FOLDER = Path("/opt/airflow/input_folder")


def _sleep():
    random_number = random.random()
    time.sleep(10)


with DAG(
    dag_id="sleep_dag",
    description="Deletes all files from input_folder",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * 6",
    catchup=False,
    dagrun_timeout=timedelta(seconds=5),
    tags=["cleanup"],
) as dag:

    sleep_task_1 = PythonOperator(
        task_id="sleep_task_1",
        python_callable=_sleep,
    )

    sleep_task_2 = PythonOperator(
        task_id="sleep_task_2",
        python_callable=_sleep,
    )

    sleep_task_3 = PythonOperator(
        task_id="sleep_task_3",
        python_callable=_sleep,
    )

    sleep_task_4 = PythonOperator(
        task_id="sleep_task_4",
        python_callable=_sleep,
    )

    sleep_task_1 >> [sleep_task_2, sleep_task_3] >> sleep_task_4

