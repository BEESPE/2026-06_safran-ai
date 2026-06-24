from datetime import datetime, timedelta
from pathlib import Path
import zipfile
import requests
import csv
import psycopg2
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


INPUT_FOLDER = Path("/opt/airflow/input_folder")
OUTPUT_FOLDER = Path("/opt/airflow/output_folder")

FILE_URL = "https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv"
FILE_NAME = "flowerdataset.csv"

SCHEMA = "airflow_data"
TABLE = "flowers"

default_args={
        "depends_on_past": False,
        "email": ["email@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


def download_file():
    INPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    file_path = INPUT_FOLDER / FILE_NAME

    response = requests.get(FILE_URL)
    response.raise_for_status()

    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded file to: {file_path}")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "airflow"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=os.getenv("DB_PORT", "5432")
    )



def load_csv_to_postgres():
    file_path = INPUT_FOLDER / FILE_NAME

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    # 2. Create table inside schema
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            sepal_length FLOAT,
            sepal_width FLOAT,
            petal_length FLOAT,
            petal_width FLOAT,
            species TEXT
        )
    """)

    # Clear old data (optional)
    cursor.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.{TABLE}
                VALUES (%s, %s, %s, %s, %s)
                """,
                row
            )

    conn.commit()
    cursor.close()
    conn.close()

    print("Data loaded into PostgreSQL")


def create_archive():
    if not INPUT_FOLDER.exists():
        raise FileNotFoundError(
            f"Input folder does not exist: {INPUT_FOLDER}"
        )

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    zip_path = OUTPUT_FOLDER / "archive.zip"

    files_added = 0

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in INPUT_FOLDER.rglob("*"):
            if file_path.is_file():
                zipf.write(
                    file_path,
                    arcname=file_path.relative_to(INPUT_FOLDER)
                )
                files_added += 1

    print(f"Created archive: {zip_path}")
    print(f"Files added: {files_added}")


def cleanup_input_folder():
    if not INPUT_FOLDER.exists():
        raise FileNotFoundError(f"Input folder not found: {INPUT_FOLDER}")

    deleted = 0

    for file_path in INPUT_FOLDER.rglob("*"):
        if file_path.is_file():
            file_path.unlink()
            deleted += 1

    print(f"Deleted {deleted} files from input folder")


with DAG(
    dag_id="zip_input_folder_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    dowmload_task = PythonOperator(
        task_id="download_file",
        python_callable=download_file,
    )

    load_task = PythonOperator(
        task_id="save_to_db",
        python_callable=load_csv_to_postgres,
    )

    create_zip_task = PythonOperator(
        task_id="create_zip_archive",
        python_callable=create_archive,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_input",
        python_callable=cleanup_input_folder,
    )

    show_data_task = SQLExecuteQueryOperator(
        task_id="show_flower_data",
        conn_id="postgres_local",
        sql="""
            SELECT *
            FROM airflow_data.flowers
            LIMIT 10;
        """,
    )

    # dowmload_task >> load_task
    dowmload_task >> load_task >> create_zip_task >> [cleanup_task, show_data_task]