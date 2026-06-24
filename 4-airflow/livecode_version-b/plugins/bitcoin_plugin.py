import pendulum
import random
from airflow.sdk import BaseOperator
from airflow.sdk import BaseHook

class BitcoinAPIHook(BaseHook):

    def __init__(self, conn_id: str = "coingecko_default"):
        super().__init__()
        self.conn_id = conn_id

    def get_bitcoin_history(self, execution_date) -> dict:
        prices = []
        volumes = []
        
        for i in range(9, -1, -1):
            day = execution_date.subtract(days=i)
            timestamp = int(day.timestamp() * 1000)
            
            dummy_price = random.uniform(60000, 65000)
            dummy_volume = random.uniform(15000000000, 25000000000)
            
            prices.append([timestamp, dummy_price])
            volumes.append([timestamp, dummy_volume])
            
        return {
            "prices": prices,
            "total_volumes": volumes
        }


class BitcoinExtractOperator(BaseOperator):
    def __init__(self, conn_id: str = "coingecko_default", **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        execution_date = context.get('logical_date') or context.get('execution_date')
        
        hook = BitcoinAPIHook(conn_id=self.conn_id)
        return hook.get_bitcoin_history(execution_date)