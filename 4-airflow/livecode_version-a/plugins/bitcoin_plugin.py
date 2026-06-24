from datetime import timedelta
import random
import requests

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"


class BitcoinAPIHook(BaseHook):

    conn_name_attr = "bitcoin_api_conn_id"

    def __init__(self, bitcoin_api_conn_id="bitcoin_api_default"):
        super().__init__()
        self.bitcoin_api_conn_id = bitcoin_api_conn_id

    def get_price_on_date(self, date_str):
        url = API.format(date_str)
        return requests.get(url).json()["market_data"]["current_price"]["usd"]


class BitcoinExtractOperator(BaseOperator):

    def __init__(
        self,
        execution_date,
        bitcoin_api_conn_id="bitcoin_api_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.execution_date = execution_date
        self.bitcoin_api_conn_id = bitcoin_api_conn_id

    def execute(self, context):

        hook = BitcoinAPIHook(self.bitcoin_api_conn_id)

        formatted_date = self.execution_date.strftime("%d-%m-%Y")
        initial_price = hook.get_price_on_date(formatted_date)

        dates = []
        prices = []

        for n_day_before in range(10):
            date = self.execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(date)
            prices.append(initial_price + random_variation)

        return {"dates": dates, "prices": prices}


class BitcoinPlugin(AirflowPlugin):
    name = "bitcoin_plugin"
    hooks = [BitcoinAPIHook]
    operators = [BitcoinExtractOperator]
