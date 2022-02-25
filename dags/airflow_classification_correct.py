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
def train_model_task(train_set, model_target, app_name="spark_classification"):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(app_name).master("yarn").getOrCreate()

    train_set = spark.read.csv(train_set.url, header=True, inferSchema=True)

    model = train_model(train_set=train_set)

    model.write().overwrite().save(model_target.url)

    return {
        "app_id": spark.sparkContext.applicationId,
        "scores": model.avgMetrics,
        "summary": [str(param) for param in model.getEstimatorParamMaps()],
    }


with DAG(
    "classification_correct",
    default_args=default_args,
    description="Correct implementation of the classification model",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["big-data assurance"],
) as dag:
    # Data from https://www.kaggle.com/c/titanic/data?select=train.csv

    train_model_t = train_model_task(
        train_set=File("hdfs://localhost:/titanic/train.csv"),
        model_target=File("hdfs://localhost:/tmp/spark/model"),
        app_name="spark_classification_correct",
    )

    train_model_t
