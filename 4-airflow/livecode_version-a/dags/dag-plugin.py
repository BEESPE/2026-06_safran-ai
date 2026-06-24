"""
Test plugin import.
"""

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from bitcoin_plugin import BitcoinExtractOperator



with DAG(
    "dag-plugin",
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="echo nothing",
    )