from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


dag = DAG(
    dag_id="dag-timetout",
    schedule="@daily",
)

task = BashOperator(
    task_id="task",
    bash_command='sleep 120',
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)