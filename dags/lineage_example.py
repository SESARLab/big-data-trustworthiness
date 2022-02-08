from datetime import timedelta, datetime

from airflow import DAG
from airflow.lineage.entities import File
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    "lineage_example",
    default_args=default_args,
    description='Example of code analysis hook before execution',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    f_addresses = File("hdfs://localhost:/example/addresses.csv")

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        application="/home/bertof/Documenti/Papers/big-data-assurance/implementation/spark_example.py",
        name="spark-example",
        conn_id="spark_default",
        inlets={
            "datasets": [f_addresses]
        }
    )

    f_addresses > submit_job
