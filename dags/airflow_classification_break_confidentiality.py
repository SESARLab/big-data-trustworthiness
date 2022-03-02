from airflow.decorators import task
from airflow.lineage.entities import File
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from openlineage.airflow.dag import DAG
from typing import List

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
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
    # 'trigger_rule': 'all_success',
}

"""
Pipeline di esempio

Task that trains a regression model over a certain training set and saves it to HDFS
"""


@task.python()
def train_model_task(
    train_set,
    model_target,
    app_name="spark_classification",
):
    # from spark_classification import train_model
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName(app_name).master("yarn").getOrCreate()
    # train_set = spark.read.csv(train_set.url, header=True, inferSchema=True)
    # model = train_model(train_set=train_set)
    # model.write().overwrite().save(model_target)
    # res = {
    #     "app_id": spark.sparkContext.applicationId,
    #     "scores": model.avgMetrics,
    #     "summary": [str(param) for param in model.getEstimatorParamMaps()],
    # }

    res = {
        "app_id": "application_1645452963766_0161",
        "scores": [0.7925691081650833],
        "summary": [],
    }

    return res


"""
Utilities
"""


def python_task_source_extractor(
    dag: DAG,
    task_id: str,
):
    import inspect
    from textwrap import dedent
    from airflow.operators.python import PythonOperator

    p_op: PythonOperator = dag.get_task(task_id=task_id)
    python_source = dedent(inspect.getsource(p_op.python_callable))
    python_args = p_op.op_args
    python_kwargs = p_op.op_kwargs
    return {
        "source": python_source,
        "args": python_args,
        "kwargs": python_kwargs,
    }


"""
Probes and analysis
"""


def requirements_analysis(requirements: List[str]):
    """
    Given a list of requirements, returns a list of known vulnerabilities
    """
    from pprint import pprint
    from tempfile import NamedTemporaryFile
    import json
    import subprocess

    print("Checking requirements")
    with NamedTemporaryFile(mode="w") as temp_f:
        temp_f.writelines(requirements)
        temp_f.flush()

        data = (
            subprocess.check_output(
                ["safety", "check", "--json", "-r", temp_f.name])
            .decode()
            .strip()
        )

        res = json.loads(data)

        print("Warnings:")
        pprint(res)

        return {"evidence": requirements, "warnings": res}


def python_code_analysis(source: List[str]):
    """
    Run pylint on the source code
    """
    from tempfile import NamedTemporaryFile
    import json
    import subprocess

    print("Checking source")
    with NamedTemporaryFile("w") as temp_f:
        temp_f.writelines(source)
        temp_f.flush()

        data = (
            subprocess.run(
                ["pylint", "--output-format=json", temp_f.name],
                stdout=subprocess.PIPE,
                check=False,
            )
            .stdout.decode()
            .strip()
        )
        print(type(data), data)
        res = json.loads(data)

        return {"evidence": source, "warnings": res}


def spark_log_probe(
    app_id: str,
    spark_history_api: str = "http://localhost:18080/api/v1",
):
    """
    Extracts logs information of a spark application and returns it as a dictionary
    """
    import requests

    base_path = "%s/applications/%s" % (spark_history_api, app_id)
    print("Using", base_path)

    evidence = dict()
    evidence["allexecutors"] = requests.get(
        "%s/allexecutors" % (base_path)).json()
    evidence["jobs"] = requests.get("%s/jobs" % (base_path)).json()
    evidence["environment"] = requests.get(
        "%s/environment" % (base_path)).json()

    return evidence


def spark_log_analysis(evidence):
    from pprint import pprint

    warnings = []
    for k, v in evidence:
        print(k)
        pprint(v)

        # TODO

    return {"evidence": evidence, "warnings": warnings}


def hdfs_config_probe(hdfs_api: str = "http://localhost:9870"):
    """
    Extracts configuration information of a Hadoop cluster and returns it as a dictionary
    """
    import requests
    from xml.etree import ElementTree

    content = requests.get("%s/conf" % hdfs_api).content
    root_xml = ElementTree.fromstring(content)

    return {
        e.find("name").text: e.find("value").text for e in root_xml.findall("property")
    }


def hdfs_config_analysis(config):
    """
    Analyzes an Hadoop cluster configuration
    """
    warnings = []
    for k, v in config.items():
        print(k, v)
        # Encryption
        if k == "dfs.encrypt.data.transfer" and v == "false":
            warnings.append("In-transit data encryption is disabled")
        if k == "yarn.intermediate-data-encryption.enable" and v == "false":
            warnings.append("Intermediate data encryption is disabled")
        # Access control
        if k == "dfs.permissions.enabled" and v == "false":
            warnings.append("FS access control is disabled")
        if k == "dfs.permissions.superusergroup" and v == "supergroup":
            warnings.append(
                "Task is running with default unrestricted permissions")
        if k == "hadoop.registry.secure" and v == "false":
            warnings.append("Registry security is not enabled")
        if k == "hadoop.security.authorization" and v == "false":
            warnings.append("Authentication is disabled")

    return {"evidence": config, "warnings": warnings}


"""
Airflow task definitions
"""


def requirements_check():
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    with open("requirements.txt") as f:
        requirements = list(f.readlines())
    res = requirements_analysis(requirements=requirements)

    evidence = res["evidence"]
    warnings = res["warnings"]

    ti.xcom_push("evidence", {"requirements": evidence})
    ti.xcom_push("warnings", {"requirements": warnings})


def python_code_check(file_path: str):
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    with open(file=file_path) as f:
        source_code = list(f.readlines())
    res = python_code_analysis(source=source_code)

    evidence = {"code_analysis": {file_path: res["evidence"]}}
    warnings = {"code_analysis": {file_path: res["warnings"]}}

    ti.xcom_push("evidence", evidence)
    ti.xcom_push("warnings", warnings)


def hadoop_config_check():
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    config = hdfs_config_probe()
    res = hdfs_config_analysis(config)

    ti.xcom_push("evidence", res["evidence"])
    ti.xcom_push("warnings", res["warnings"])


def pre_execution_spark_check(
    target_task_id: str,
):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context
    from pprint import pprint

    context = get_current_context()
    ti: TaskInstance = context["ti"]

    res = python_task_source_extractor(
        dag=context["dag"], task_id=target_task_id
    )

    print(res["source"])
    print(res["args"])
    pprint(res["kwargs"])

    # dag_run: DagRun = ti.dag_run
    # target_ti: TaskInstance = dag_run.get_task_instance(task_id=target_task_id)

    # print("Executor config:\n", pformat(target_ti.executor_config))
    # print("Type of operator:", type(target_ti))
    # print("Vars operator:\n", pformat(vars(target_ti)))


def post_execution_spark_check(spark_history_api="http://localhost:18080/api/v1"):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context
    from pprint import pprint

    ti: TaskInstance = get_current_context()["ti"]

    model_data = ti.xcom_pull("train_model_task")
    app_id = model_data["app_id"]
    evidence = spark_log_probe(
        app_id=app_id, spark_history_api=spark_history_api)

    print(evidence)
    for k, v in evidence.items():
        print(k)
        pprint(v)

    warnings = []
    ti.xcom_push("evidence", evidence)
    ti.xcom_push("warnings", warnings)


with DAG(
    "classification_break_confidentiality",
    default_args=default_args,
    description="""
    Classification model pipeline that breaks the confidentiality property
    saving the data locally
    """,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["big-data assurance"],
) as dag:
    # Data from https://www.kaggle.com/c/titanic/

    train_model_t = train_model_task(
        train_set=File("hdfs://localhost:/titanic/train.csv"),
        model_target="/tmp/spark/model_unsafe",  # Local path
        app_name="spark_classification_break_confidentiality",
    )

    pre_exec_requirements_check = PythonOperator(
        task_id="pre_execution_requirements_check", python_callable=requirements_check
    )
    pre_exec_airflow_code_check = PythonOperator(
        task_id="pre_exec_airflow_code_check",
        python_callable=python_code_check,
        op_kwargs={
            "file_path": "./dags/airflow_classification_break_confidentiality.py"
        },
    )
    pre_exec_spark_code_check = PythonOperator(
        task_id="pre_exec_spark_code_check",
        python_callable=python_code_check,
        op_kwargs={"file_path": "./dags/spark_classification.py"},
    )
    pre_exec_hadoop_config_check = PythonOperator(
        task_id="pre_exec_airflow_config_check",
        python_callable=hadoop_config_check,
    )

    pre_exec_spark_check = PythonOperator(
        task_id="pre_exec_spark_check",
        python_callable=pre_execution_spark_check,
        op_kwargs={"target_task_id": train_model_t._operator.task_id},
    )
    post_exec_spark_check = PythonOperator(
        task_id="post_exec_spark_check",
        python_callable=post_execution_spark_check,
        op_kwargs={"target_task_id": train_model_t._operator.task_id},
    )

    (
        [
            pre_exec_airflow_code_check,
            pre_exec_hadoop_config_check,
            pre_exec_requirements_check,
            pre_exec_spark_code_check,
            pre_exec_spark_check,
        ]
        >> train_model_t
        >> [post_exec_spark_check]
    )

    # pre_execution_airflow_code_check_t = PythonOperator(
    #     task_id="pre_execution_airflow_code_check",
    #     python_callable=python_code_analysis,
    #     op_kwargs={"file_path": "dags/airflow_classification_break_confidentiality.py"},
    # )
    # pre_execution_spark_code_check_t = PythonOperator(
    #     task_id="pre_execution_spark_code_check",
    #     python_callable=python_code_analysis,
    #     op_kwargs={"file_path": "dags/spark_classification.py"},
    # )
    # pre_execution_requirements_t = pre_execution_requirements_check()
    # pre_execution_hadoop_t = pre_execution_hadoop_check()
    # pre_execution_spark_t = pre_execution_spark_check(
    #     target_task_id=train_model_t._operator.task_id
    # )

    # post_execution_spark_t = post_execution_spark_check()

    # (
    #     [
    #         pre_execution_airflow_code_check_t,
    #         pre_execution_spark_code_check_t,
    #         pre_execution_requirements_t,
    #         pre_execution_hadoop_t,
    #         pre_execution_spark_t,
    #     ]
    #     >> train_model_t
    #     >> [post_execution_spark_t]
    # )
