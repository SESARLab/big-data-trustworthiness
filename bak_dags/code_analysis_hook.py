from datetime import datetime, timedelta
from pprint import pprint
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG("code_analysis_hook",
         default_args=default_args,
         description="Example of code analysis before execution",
         schedule_interval=timedelta(minutes=5),
         start_date=datetime(2022, 1, 1),
         catchup=False,
         tags=['big-data assurance']):

    def printer(**kwargs):
        for key, val in kwargs.items():
            print(key, val)

        print("dag vars:")
        pprint(vars(kwargs["dag"]))
        print("conf vars:")
        pprint(vars(kwargs["conf"]))
        print("dag_run vars:")
        pprint(vars(kwargs["dag_run"]))

    def http_request():
        import requests
        res = requests.get("https://example.com")
        return res.text

    t1 = PythonOperator(
        task_id="printer",
        python_callable=printer
    )

    t2 = PythonVirtualenvOperator(
        task_id="http_request",
        requirements=["requests"],
        python_callable=http_request)

    t1 >> t2
