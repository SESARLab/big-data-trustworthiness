from airflow.lineage.entities import File
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from openlineage.airflow.dag import DAG
from pipeline_lib import (
    CachingPythonOperator,
    CachingSparkSubmitOperator,
    requirements_check,
    python_code_check,
    hadoop_config_check_encryption,
    hadoop_config_check_security,
    hdfs_paths_check,
    task_args_check,
    lineage_check,
    spark_config_check,
    hdfs_file_can_read,
    hdfs_file_can_write,
    airflow_config_check,
    spark_logs_check,
    kerberos_auth_check,
    openvas_check,
    acl_config_check,
    report_generator
)

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
    "verified_kmeans_pipeline",
    default_args=default_args,
    description="""
    Classification model pipeline
    """,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["big-data assurance"],
) as dag:
    with TaskGroup(group_id="pipeline") as p1:
        train_model_t = CachingSparkSubmitOperator(
            task_id="train_model",
            application="dags/spark_kmeans.py",
        )

    with TaskGroup(group_id="pre-execution-checks") as pre_p1:
        # p2
        hdfs_paths_check_t = CachingPythonOperator(
            task_id="hdfs_paths_check",
            python_callable=hdfs_paths_check,
            op_kwargs={
                "target_task_id": train_model_t.task_id,
                "spark_file_path": "dags/spark_kmeans.py",
                "expected_paths_re": [r"hdfs://localhost.+"],
            },
        )

        # p3
        task_args_check_t = CachingPythonOperator(
            task_id="task_args_check",
            python_callable=task_args_check,
            op_kwargs={
                "target_task_id": train_model_t.task_id,
                "expected_args": {
                    "application": "dags/spark_kmeans.py",
                    "args": None,
                },
            },
        )

        # p4
        lineage_check_t = CachingPythonOperator(
            task_id="lineage_check",
            python_callable=lineage_check,
        )

        # p5
        python_code_check_spark_t = CachingPythonOperator(
            task_id="python_code_check_spark",
            python_callable=python_code_check,
            op_kwargs={"file_path": "./dags/spark_kmeans.py"},
        )

        # p6
        requirements_check_t = CachingPythonOperator(
            task_id="requirements_check",
            python_callable=requirements_check,
        )

        # p7
        # TODO check sul dag

        # p8
        # read_permission_check_t = CachingPythonOperator(
        #     task_id="read_permission_check",
        #     python_callable=hdfs_file_can_read,
        #     op_kwargs={
        #         "path": "/titanic/train.csv",
        #         "user": "bertof",
        #         "group": "hadoop",
        #     },
        # )

        # p9
        write_permission_check_t = CachingPythonOperator(
            task_id="write_permission_check",
            python_callable=hdfs_file_can_write,
            op_kwargs={
                "path": "kmeans",
                "user": "bertof",
                "group": "hadoop",
            },
        )

        # p10
        hadoop_config_check_encryption_t = CachingPythonOperator(
            task_id="hadoop_config_check_encryption",
            python_callable=hadoop_config_check_encryption,
        )

        # p11
        hadoop_config_check_security_t = CachingPythonOperator(
            task_id="hadoop_config_check_security",
            python_callable=hadoop_config_check_security,
        )

        # p12
        spark_config_check_t = CachingPythonOperator(
            task_id="spark_config_check",
            python_callable=spark_config_check,
        )

        # p13
        airflow_config_check_t = CachingPythonOperator(
            task_id="airflow_config_check",
            python_callable=airflow_config_check,
        )

        # p14
        kerberos_auth_check_t = CachingPythonOperator(
            task_id="kerberos_auth_check",
            python_callable=kerberos_auth_check,
            op_kwargs={
                "principal": "bertof/my.engine",
                "keytab": "eng.keytab",
            },
        )

        # p15
        acl_config_check = CachingPythonOperator(
            task_id="acl_config_check",
            python_callable=acl_config_check,
            op_kwargs={
                "expected_acl": {},
            },
        )

        # p17
        openvas_check_t = CachingPythonOperator(
            task_id="openvas_check",
            python_callable=openvas_check,
            op_kwargs={
                "environment": {"OPENVAS_HOST": "172.20.28.178"},
                "config": {"config": {"host": "172.20.28.200"}},
            },
        )

        pass

    with TaskGroup(group_id="post-execution-checks") as post_p1:
        # p1
        spark_logs_check_t = CachingPythonOperator(
            task_id="spark_logs_check",
            python_callable=spark_logs_check,
            retry_delay=timedelta(seconds=5),
            retries=2,
            op_kwargs={
                "target_app_name": "spark_kmeans",
                "expected_jobs": {
                    "collect at AreaUnderCurve",
                    "collect at BinaryClassificationMetrics",
                    "collect at StringIndexer",
                    "count at BinaryClassificationMetrics",
                    "parquet at LinearSVC",
                    "parquet at StringIndexer",
                    "runJob at SparkHadoopWriter",
                    "showString at NativeMethodAccessorImpl.java:0",
                    "treeAggregate at RDDLossFunction",
                    "treeAggregate at Summarizer",
                },
                "master": "yarn",
            },
        )

        # p16
        # TODO: Monitoring probe su ranger

    with TaskGroup(group_id="assurance-report") as ass_p1:
        report_generator_t = CachingPythonOperator(
            task_id="report_generator", python_callable=report_generator
        )

    pre_p1 >> p1 >> post_p1
    [pre_p1, post_p1] >> ass_p1
