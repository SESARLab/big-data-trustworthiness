from datetime import timedelta, datetime

from airflow.lineage.entities import File
from airflow.operators.python import PythonOperator

from openlineage.airflow.dag import DAG

import pipeline_lib

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


with DAG(
    "classification_pipeline",
    default_args=default_args,
    description="Correct implementation of the classification model",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["big-data assurance"],
) as dag:
    # Data from https://www.kaggle.com/c/titanic/data?select=train.csv
    train_set = File("hdfs://localhost:/titanic/train.csv")

    train_model_t = PythonOperator(
        task_id="train_model_task",
        python_callable=pipeline_lib.train_model,
        op_kwargs={
            "train_set": train_set,
            "model_target": "/titanic/model",
            "results_target": "/titanic/results",
            "app_name": "spark_classification",
            "keep_last": '{{"train_model_task" in dag_run.conf.get("keep_last", [])}}',
        },
    )

    train_model_t
