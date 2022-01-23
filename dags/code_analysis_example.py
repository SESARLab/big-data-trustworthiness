import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {"depends_on_past": False, "start_date": datetime.now(), "retries": 1,
                "retry_delay": timedelta(seconds=30)}


@task.virtualenv(requirements=["requests"])
def http_request(site: str = "https://example.com"):
    import requests
    res = requests.get(site)
    print(res.status_code)
    return res.text


@task.python()
def check_config(**kwargs):
    """
    Configuration checker
    :param kwargs:
    :return:
    """
    from airflow.configuration import AirflowConfigParser
    conf: AirflowConfigParser = kwargs["conf"]
    for key, value in conf.items():
        print(key, value)
    print("Check for configuration errors here")


@task.python()
def code_analysis(**kwargs):
    """
    Example function that will be performed in a virtual environment.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from pprint import pformat
    from airflow import DAG
    from airflow.models import DagRun, TaskInstance, BaseOperator
    from textwrap import dedent
    import inspect
    import os
    from tempfile import NamedTemporaryFile

    print("KWARGS:\n", pformat(kwargs.items()))

    dag_instance: DAG = kwargs["dag"]
    dag_run: DagRun = kwargs["dag_run"]

    for t in dag_run.get_task_instances():
        t: TaskInstance
        print("EXECUTOR CONFIG:\n", pformat(t.executor_config))

        b_op: BaseOperator = dag_instance.get_task(t.task_id)
        print("BASE OPERATOR:\n", pformat(vars(b_op)))

        if "requirements" in vars(b_op).keys():
            print("Checking virtualenv requirements")
            with NamedTemporaryFile(prefix=t.task_id, mode="w") as temp_f:
                temp_f.writelines(b_op.requirements)
                temp_f.seek(0)

                res = subprocess.call(['safety', "check", "-r", temp_f.name])

        if "python_callable" in vars(b_op).keys():
            print("Detected python operator")
            source_code = '\n'.join([
                "from airflow.decorators import task",
                dedent(inspect.getsource(b_op.python_callable))
            ])
            print("PYTHON SOURCE CODE:\n", pformat(source_code))

            with NamedTemporaryFile(prefix=t.task_id, mode="w") as temp_f:
                temp_f.writelines(source_code)
                temp_f.seek(0)

                os.system(f"pylint -E ${temp_f.name}")

        else:
            print("Unsupported operator type")

    print('Finished')


with DAG("code_analysis_example", default_args=default_args, tags=["big-data-assurance"]) as dag:
    check_config_t = check_config()
    python_code_analysis_t = code_analysis()

    security_tasks = [check_config_t, python_code_analysis_t]

    http_request_t = http_request()

    security_tasks >> http_request_t
