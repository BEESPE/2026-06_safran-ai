from datetime import datetime

import numpy as np
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="bitcoin_rsi_from_db",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bitcoin", "rsi", "db", "feature-engineering"],
)
def bitcoin_rsi_from_db():

    # --------------------------------------------------
    # 1. EXTRACT from DB (last 10 days)
    # --------------------------------------------------
    @task
    def extract():
        hook = PostgresHook(postgres_conn_id="postgres_local")

        rows = hook.get_records("""
            SELECT date, price_usd
            FROM airflow_data.bitcoin_history_30d
            ORDER BY date DESC
            LIMIT 10;
        """)

        # reverse to chronological order
        rows = rows[::-1]

        prices = [r[1] for r in rows]
        latest_date = rows[-1][0]

        return {
            "date": latest_date,
            "prices": prices
        }

    # --------------------------------------------------
    # 2. PROCESS RSI
    # --------------------------------------------------
    @task
    def process(data):
        prices = data["prices"]

        gains = []
        losses = []

        for i in range(1, len(prices)):
            change = prices[i] - prices[i - 1]

            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = np.mean(gains) if gains else 0
        avg_loss = np.mean(losses) if losses else 0

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        return {
            "date": data["date"],
            "price_usd": prices[-1],
            "rsi": float(rsi),
        }

    # --------------------------------------------------
    # 3. STORE result
    # --------------------------------------------------
    @task
    def store(result):
        hook = PostgresHook(postgres_conn_id="postgres_local")

        hook.run("""
            CREATE SCHEMA IF NOT EXISTS airflow_data;
        """)

        hook.run("""
            CREATE TABLE IF NOT EXISTS airflow_data.bitcoin_rsi (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price_usd NUMERIC,
                rsi NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        hook.run("""
            INSERT INTO airflow_data.bitcoin_rsi (date, price_usd, rsi)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) DO UPDATE
            SET price_usd = EXCLUDED.price_usd,
                rsi = EXCLUDED.rsi;
        """, parameters=(
            result["date"],
            result["price_usd"],
            result["rsi"],
        ))

    data = extract()
    result = process(data)
    store(result)


bitcoin_rsi_from_db()