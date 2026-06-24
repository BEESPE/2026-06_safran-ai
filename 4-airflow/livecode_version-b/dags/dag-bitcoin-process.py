import sys
import os
import pendulum
import pandas as pd
from airflow.sdk import dag, task

sys.path.append(os.path.join(os.path.dirname(__file__), "../plagins"))

from bitcoin_plugin import BitcoinExtractOperator

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 24, tz="UTC"),
    catchup=False,
    tags=["coingecko", "taskflow_api", "fixed_v3"],
)
def coingecko_bitcoin_history_pipeline():

    extract_bitcoin_task = BitcoinExtractOperator(
        task_id="extract_bitcoin_price"
    )

    @task(multiple_outputs=True)
    def process_data(bitcoin_data: dict) -> dict:
        prices_raw = bitcoin_data.get("prices", [])
        volumes_raw = bitcoin_data.get("total_volumes", [])
        
        timestamps = [item[0] for item in prices_raw]
        prices = [item[1] for item in prices_raw]
        volumes = [item[1] for item in volumes_raw]
        
        gains = []
        losses = []
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(abs(diff) if diff < 0 else 0)
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        if avg_loss == 0:
            rsi_value = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi_value = 100.0 - (100.0 / (1 + rs))
            
        rsi_list = [50.0] + [rsi_value] * (len(prices) - 1) 

        return {
            "usd": prices[-1] if prices else 0.0,
            "volume": volumes[-1] if volumes else 0.0,
            "rsi": rsi_value,
            "all_timestamps": timestamps,
            "all_prices": prices,
            "all_rsi": rsi_list
        }

    @task()
    def store_data(usd: float, volume: float, rsi: float):
        print(f"--- STORE DATA ---")
        print(f"Price: {usd:.2f}$, Volume: {volume:.2f}$, RSI: {rsi:.2f}")

    @task()
    def create_dataframe(timestamps: list, prices: list, rsi_list: list) -> dict:
        dates = [pendulum.from_timestamp(ts / 1000).to_date_string() for ts in timestamps]
        df = pd.DataFrame({
            "date": dates,
            "price": prices,
            "rsi": rsi_list
        })
        print("--- DATAFRAME CREATED ---")
        print(df.to_string())
        return df.to_dict(orient="list")

    @task()
    def calculate_weekly_averages(df_dict: dict) -> dict:
        df = pd.DataFrame(df_dict)
        df['date'] = pd.to_datetime(df['date'])
        
        weekly_avg = df.groupby(df['date'].dt.to_period('W'))['price'].mean().reset_index()
        weekly_avg['date'] = weekly_avg['date'].astype(str)
        
        print("--- WEEKLY AVERAGE PRICES ---")
        print(weekly_avg.to_string(index=False))
        return weekly_avg.to_dict(orient="list")

    metrics = process_data(extract_bitcoin_task.output)
    
    store_data(usd=metrics["usd"], volume=metrics["volume"], rsi=metrics["rsi"])
    
    df_data = create_dataframe(
        timestamps=metrics["all_timestamps"],
        prices=metrics["all_prices"],
        rsi_list=metrics["all_rsi"]
    )
    
    calculate_weekly_averages(df_data)

coingecko_bitcoin_history_pipeline()