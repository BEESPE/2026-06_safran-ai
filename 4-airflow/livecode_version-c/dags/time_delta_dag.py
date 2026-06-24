from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.providers.standard.operators.python import PythonOperator


def dummy_task_function(task_name: str):
    logging.info("Executing task: %s", task_name)


with DAG(
    dag_id="timedelta_sensor_priority_demo",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["sensor", "priority"],
) as dag:

    wait_2_minutes = TimeDeltaSensor(
        task_id="wait_2_minutes",
        delta=timedelta(minutes=2),
    )

    high_priority_task = PythonOperator(
        task_id="high_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_name": "HIGH PRIORITY TASK"},
        priority_weight=100,
    )

    low_priority_task = PythonOperator(
        task_id="low_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_name": "LOW PRIORITY TASK"},
        priority_weight=1,
    )

    wait_2_minutes >> [high_priority_task, low_priority_task]