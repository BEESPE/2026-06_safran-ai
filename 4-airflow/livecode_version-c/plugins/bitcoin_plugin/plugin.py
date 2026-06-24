from airflow.plugins_manager import AirflowPlugin

from bitcoin_plugin.hooks import BitcoinAPIHook
from bitcoin_plugin.operators import BitcoinExtractOperator


class BitcoinPlugin(AirflowPlugin):
    name = "bitcoin_plugin"

    hooks = [BitcoinAPIHook]
    operators = [BitcoinExtractOperator]