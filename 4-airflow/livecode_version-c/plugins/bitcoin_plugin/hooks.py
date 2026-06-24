import requests
from datetime import datetime
import time
import random

from airflow.hooks.base import BaseHook


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


class BitcoinAPIHook(BaseHook):
    """
    Hook for Bitcoin-related API or data access.
    Currently supports CoinGecko calls (optional extension).
    """

    def __init__(self, base_url="https://api.coingecko.com/api/v3"):
        super().__init__()
        self.base_url = base_url
        self.session = requests.Session()

    def get_history(self, date_str: str):
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