from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bsp',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id="dag-cron",
    start_date=datetime(2026, 6, 23),
    schedule="0 10 * * Mon-Fri",
) as dag:
    task = BashOperator(
        task_id="task",
        bash_command="echo blabla"
    )
    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo blabla"
    )


    task >> task_2 >> task