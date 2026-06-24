from datetime import datetime, timedelta
import time
import random

from airflow.models import BaseOperator
from bitcoin_plugin.hooks import BitcoinAPIHook


class BitcoinExtractOperator(BaseOperator):

    def __init__(self, days=10, **kwargs):
        super().__init__(**kwargs)
        self.days = days

    def execute(self, context):

        hook = BitcoinAPIHook()

        results = []
        today = datetime.today()

        for i in range(self.days):
            date = today - timedelta(days=i)
            formatted = date.strftime("%d-%m-%Y")

            raw = hook.get_history(formatted)

            market = raw.get("market_data", {})

            results.append({
                "date": date.strftime("%Y-%m-%d"),
                "price_usd": market.get("current_price", {}).get("usd"),
                "volume_usd": market.get("total_volume", {}).get("usd"),
            })

            # rate limit safety
            time.sleep(random.uniform(1.2, 2.5))

        return results[::-1]  # chronological order