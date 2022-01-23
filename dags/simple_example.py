from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
default_args = {
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['abc@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


@task
def hello_name(name: str):
    print(f'Hello {name}!')


with DAG('daily_processing', default_args=default_args) as dag:
    task_1 = hello_name("Gigi")
    task_2 = hello_name("Gianni")

    task_1.set_downstream(task_2)
