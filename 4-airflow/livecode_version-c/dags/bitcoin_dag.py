from datetime import datetime, timedelta
import time
import random

import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import (
    dag,
    task,
)


def on_deadline_missed(context):
    dag_run = context["dag_run"]
    print(
        f"Deadline missed for DAG {dag_run.dag_id}, "
        f"run_id={dag_run.run_id}"
    )


def safe_get(url, params, max_retries=3):
    """
    Robust CoinGecko request handler with:
    - Retry-After support
    - exponential backoff
    - jitter
    """

    session = requests.Session()

    for attempt in range(max_retries):

        response = session.get(url, params=params, timeout=30)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            if retry_after:
                sleep_time = int(retry_after)
            else:
                sleep_time = min(60, (2 ** attempt) + random.uniform(1, 3))

            time.sleep(sleep_time)
            continue

        if response.status_code in [500, 502, 503, 504]:
            time.sleep(min(60, (2 ** attempt)))
            continue

        response.raise_for_status()

    raise Exception("CoinGecko API: Max retries exceeded (rate limit or blocking)")


@dag(
    dag_id="bitcoin_price_taskflow",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bitcoin", "coingecko", "rate-limit", "etl"],
)
def bitcoin_price_taskflow():

    @task
    def get_existing_dates():
        hook = PostgresHook(postgres_conn_id="postgres_local")

        hook.run("""
            CREATE SCHEMA IF NOT EXISTS airflow_data;
        """)

        hook.run("""
            CREATE TABLE IF NOT EXISTS airflow_data.bitcoin_history_30d (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price_usd NUMERIC,
                volume_usd NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        rows = hook.get_records("""
            SELECT date FROM airflow_data.bitcoin_history_30d;
        """)

        return {r[0].strftime("%Y-%m-%d") for r in rows if r[0]}

    @task
    def generate_dates(existing_dates):
        today = datetime.today()

        return [
            (today - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(10)
            if (today - timedelta(days=i)).strftime("%Y-%m-%d") not in existing_dates
        ]

    # ---------------------------
    # FETCH DATA (RATE LIMITED)
    # ---------------------------
    @task
    def extract(dates_to_fetch):
        base_url = "https://api.coingecko.com/api/v3/coins/bitcoin/history"

        results = []

        for date_str in dates_to_fetch:

            api_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

            raw = safe_get(
                base_url,
                params={"date": api_date}
            )

            market_data = raw.get("market_data", {})

            results.append({
                "date": date_str,
                "price_usd": market_data.get("current_price", {}).get("usd"),
                "volume_usd": market_data.get("total_volume", {}).get("usd"),
            })

            # time.sleep(1.2)  # safe baseline for CoinGecko

        return results

    # ---------------------------
    # STORE DATA
    # ---------------------------
    @task
    def store(records):
        hook = PostgresHook(postgres_conn_id="postgres_local")

        insert_sql = """
        INSERT INTO airflow_data.bitcoin_history_30d (date, price_usd, volume_usd)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
        """

        for r in records:
            hook.run(
                insert_sql,
                parameters=(r["date"], r["price_usd"], r["volume_usd"]),
            )

    existing = get_existing_dates()
    missing = generate_dates(existing)
    data = extract(missing)
    store(data)


bitcoin_price_taskflow()