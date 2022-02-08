from datetime import datetime, timedelta
from pprint import pprint
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.lineage import AUTO

from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
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
with DAG(
    'code_analysis_example',
    default_args=default_args,
    description='Example of code analysis hook before execution',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    @task.python()
    def check_config(**kwargs):
        """
        Configuration checker
        """
        for key, value in kwargs["conf"].items():
            print(key, value)
        print("Check for configuration errors here")

    @task.python()
    def printer(**kwargs):
        context = kwargs

        for key, val in context.items():
            print(key, val)

        print("dag vars:")
        pprint(vars(context["dag"]))
        print("conf vars:")
        pprint(vars(context["conf"]))
        print("dag_run vars:")
        pprint(vars(context["dag_run"]))

    @task.virtualenv(task_id="fasf", requirements=["requests"])
    def http_request():
        import requests
        res = requests.get("https://example.com")
        return res.text

    st1 = check_config()

    security_tasks = [st1]

    t1 = printer()

    t2 = http_request()

    security_tasks >> t1 >> t2
