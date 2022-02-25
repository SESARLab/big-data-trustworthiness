from datetime import timedelta, datetime

from airflow.lineage.entities import File
from airflow.decorators import task
from openlineage.airflow.dag import DAG

from spark_classification import train_model


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


@task.python()
def train_model_task(
    train_set, model_target, app_name="spark_classification", **kwargs
):
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


@task.python()
def pre_execution_task(hadoop_api="http://localhost:9870/conf", **kwargs):
    import requests
    from xml.etree import ElementTree

    evidence = dict()

    content = requests.get(hadoop_api).content
    root = ElementTree.fromstring(content)
    config = {
        e.find("name").text: e.find("value").text for e in root.findall("property")
    }

    evidence["hdfs_config"] = config

    return evidence


@task.python()
def check_task_confidentiality(
    spark_history_api="http://localhost:18080/api/v1", **kwargs
):

    ti = kwargs["ti"]
    model_data = ti.xcom_pull("train_model_task")

    import requests

    print("model_data", model_data)
    base_path = "%s/applications/%s" % (spark_history_api, model_data["app_id"])
    print("Using", base_path)

    evidence = dict()

    evidence["allexecutors"] = requests.get("%s/allexecutors" % (base_path)).json()
    evidence["jobs"] = requests.get("%s/jobs" % (base_path)).json()
    evidence["environment"] = requests.get("%s/environment" % (base_path)).json()

    print(evidence)

    warnings = []

    from pprint import pprint

    print("hadoopProperties")
    pprint(evidence["environment"]["hadoopProperties"])

    for k, v in evidence["environment"]["hadoopProperties"]:
        print(k)
        if k == "dfs.permissions.enabled" and v == "false":
            warnings.append({"message": "FS access control is disabled"})
        if k == "dfs.permissions.superusergroup" and v == "supergroup":
            warnings.append(
                {"message": "Task is running with default unrestricted permissions"}
            )
        if k == "hadoop.registry.secure" and v == "false":
            warnings.append({"message": "Registry security is not enabled"})
        if k == "hadoop.security.authorization" and v == "false":
            warnings.append({"message": "Authentication is disabled"})

    ti.xcom_push("evidence", evidence)
    return warnings


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

    pre_execution_t = pre_execution_task()

    train_model_t = train_model_task(
        train_set=File("hdfs://localhost:/titanic/train.csv"),
        model_target="/tmp/spark/model_unsafe",  # Local path
        app_name="spark_classification_break_confidentiality",
    )

    post_execution_t = check_task_confidentiality()

    pre_execution_t >> train_model_t >> post_execution_t
