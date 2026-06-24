from datetime import datetime
import random

from airflow.sdk import dag, task


RANDOM_FACTS = [
    "Honey never spoils. Archaeologists have found edible honey in ancient tombs.",
    "Octopuses have three hearts.",
    "Bananas are berries, but strawberries are not.",
    "A day on Venus is longer than a year on Venus.",
    "The Eiffel Tower can grow taller in summer due to thermal expansion.",
    "Wombats produce cube-shaped droppings.",
    "Sharks existed before trees.",
]


@dag(
    dag_id="weekend_random_fact",
    start_date=datetime(2026, 6, 1),
    schedule="0 10 * * 6,0",
    tags=["weekend", "demo"],
)
def weekend_random_fact():

    @task
    def print_random_fact():
        fact = random.choice(RANDOM_FACTS)
        print("=" * 50)
        print("Weekend Random Fact")
        print("=" * 50)
        print(fact)

    print_random_fact()


weekend_random_fact()