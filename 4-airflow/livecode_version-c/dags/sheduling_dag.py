from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import BranchPythonOperator


@dag(
    dag_id="hourly_weekday_branch",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def hourly_weekday_branch():

    def choose_day() -> str:
        return datetime.now().strftime("%A").lower()

    branch = BranchPythonOperator(
        task_id="select_day",
        python_callable=choose_day,
    )

    @task(task_id="monday")
    def monday():
        print("Happy Monday!")

    @task(task_id="tuesday")
    def tuesday():
        print("It's Tuesday!")

    @task(task_id="wednesday")
    def wednesday():
        print("Happy Wednesday!")

    @task(task_id="thursday")
    def thursday():
        print("It's Thursday!")

    @task(task_id="friday")
    def friday():
        print("Happy Friday!")

    @task(task_id="saturday")
    def saturday():
        print("Enjoy your Saturday!")

    @task(task_id="sunday")
    def sunday():
        print("Relax, it's Sunday!")

    branch >> [
        monday(),
        tuesday(),
        wednesday(),
        thursday(),
        friday(),
        saturday(),
        sunday(),
    ]


hourly_weekday_branch()