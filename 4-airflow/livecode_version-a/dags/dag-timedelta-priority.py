from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "retries": 0,
}


def dummy_task_function(task_label: str, **context):
    """
    Dummy task that logs execution at INFO level.
    """
    logging.info(f"Executing task: {task_label}")


with DAG(
    dag_id="dag-timedelta-priority",
    default_args=default_args,
    description="DAG with TimeDeltaSensor followed by high/low priority parallel tasks",
    start_date=datetime(2026, 6, 23),
    schedule="@daily",
    catchup=False,
) as dag:

    # 2-minute delay sensor
    wait_2_minutes = TimeDeltaSensor(
        task_id="wait_2_minutes",
        delta=timedelta(minutes=2),
    )

    # High priority task
    high_priority_task = PythonOperator(
        task_id="high_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_label": "HIGH priority task executed"},
        priority_weight=10,  # higher weight = higher priority in scheduler queue
    )

    # Low priority task
    low_priority_task = PythonOperator(
        task_id="low_priority_task",
        python_callable=dummy_task_function,
        op_kwargs={"task_label": "LOW priority task executed"},
        priority_weight=1,
    )

    # DAG structure
    wait_2_minutes >> [high_priority_task, low_priority_task]