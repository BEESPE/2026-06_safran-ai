from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


class FilePathSensor(FileSensor):
    def poke(self, context):
        files = list(Path("/opt/airflow/input_folder").glob("*"))

        for file in files:
            context["ti"].xcom_push(
                key="detected_file",
                value=str(file),
            )
            return True

        return False


def process_files(**context):
    file_path = context["ti"].xcom_pull(
        task_ids="wait_for_file",
        key="detected_file",
    )

    print(f"Processing file =======>>>>>>>>>>>>>>> {file_path}")


with DAG(
    dag_id="watch_folder_for_files",
    start_date=datetime(2026, 6, 1),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:

    wait_for_file = FilePathSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/input_folder/*",
        fs_conn_id="local_file_connection",
        poke_interval=30,
        timeout=3600,
        deferrable=True,
    )

    process_task = PythonOperator(
        task_id="process_file",
        python_callable=process_files,
    )

    wait_for_file >> process_task