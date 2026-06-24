from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from bitcoin_plugin.operators import BitcoinExtractOperator


@dag(
    dag_id="bitcoin_price_plugin_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bitcoin", "plugin", "etl"],
)
def bitcoin_price_plugin_pipeline():

    # ---------------------------
    # Extract via OPERATOR (uses HOOK inside)
    # ---------------------------
    extract = BitcoinExtractOperator(
        task_id="extract_bitcoin",
        days=10,
    )

    # ---------------------------
    # Store
    # ---------------------------
    @task
    def store(records):

        hook = PostgresHook(postgres_conn_id="postgres_local")

        hook.run("""
            CREATE SCHEMA IF NOT EXISTS airflow_data;
        """)

        hook.run("""
            CREATE TABLE IF NOT EXISTS airflow_data.bitcoin_history_30d (
                date DATE UNIQUE,
                price_usd NUMERIC,
                volume_usd NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        for r in records:
            hook.run(
                """
                INSERT INTO airflow_data.bitcoin_history_30d
                (date, price_usd, volume_usd)
                VALUES (%s, %s, %s)
                ON CONFLICT (date) DO NOTHING;
                """,
                parameters=(r["date"], r["price_usd"], r["volume_usd"]),
            )

    # ---------------------------
    # DAG FLOW
    # ---------------------------
    store_task = store(extract.output)
    extract >> store_task


bitcoin_price_plugin_pipeline()