from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


INPUT_FOLDER = Path("/opt/airflow/input_folder")


def clear_input_folder():
    if not INPUT_FOLDER.exists():
        raise FileNotFoundError(f"Folder not found: {INPUT_FOLDER}")

    deleted = 0

    for item in INPUT_FOLDER.rglob("*"):
        if item.is_file():
            item.unlink()
            deleted += 1

    print(f"Deleted {deleted} files from {INPUT_FOLDER}")


with DAG(
    dag_id="clear_input_folder_dag",
    description="Deletes all files from input_folder",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * 6",
    catchup=False,
    tags=["cleanup"],
) as dag:

    clear_task = PythonOperator(
        task_id="clear_input_folder",
        python_callable=clear_input_folder,
    )