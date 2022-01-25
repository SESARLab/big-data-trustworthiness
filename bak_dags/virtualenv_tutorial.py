from datetime import datetime, timedelta
from pprint import pprint
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
with DAG(
    'virtualenv_tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

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

        # print(context)
        # for dag_name, dag in DagBag().dags.items():
        #     print(f'DAG: {dag_name}')
        #     for task in dag.tasks:
        #         print(task.task_id)
        #         print(task.doc_md)
        #         print(task.executor_config)
        #         print(task.template_ext)
        #         print(task.template_fields)
        #         print(task.template_fields_renderers)

    t4 = PythonOperator(
        task_id="printer",
        python_callable=printer
    )

    def http_request():
        import requests
        res = requests.get("https://example.com")
        return res.text

    t5 = PythonVirtualenvOperator(
        task_id="http_request",
        requirements=["requests"],
        python_callable=http_request)

    t1 >> [t2, t3] >> t4 >> t5
