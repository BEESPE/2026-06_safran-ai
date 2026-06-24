from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


def process_file():
    print("File is here.")

def process_file_2():
    file_path = "/opt/airflow/watch/trigger.txt"

    with open(file_path, "r") as f:
        content = f.read()

    print("File is here.")
    print("Content:")
    print(content)


with DAG(
    dag_id="dag-watch",
    start_date=datetime(2026, 6, 23),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/watch/*.txt",
        poke_interval=5,
        timeout=3600,
        deferrable=True,
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file_2,
    )

    wait_for_file >> process