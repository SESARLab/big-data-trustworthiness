from airflow.lineage.entities import File
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from openlineage.airflow.dag import DAG
from typing import List, Set, Optional
import pipeline_lib
from airflow.utils.task_group import TaskGroup

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

#
# Probes and analysis
#


def requirements_analysis(requirements: Optional[List[str]]):
    """
    Given a list of requirements, returns a list of known vulnerabilities
    """
    from tempfile import NamedTemporaryFile
    import json
    import subprocess

    if requirements is not None:
        print("Evaluating requirements...")
        with NamedTemporaryFile(mode="w") as temp_f:
            temp_f.writelines(requirements)
            temp_f.flush()

            data = (
                subprocess.run(
                    ["safety", "check", "--json", "-r", temp_f.name],
                    stdout=subprocess.PIPE,
                    check=False,
                )
                .stdout.decode()
                .strip()
            )
    else:
        print("Evaluating installed libraries...")
        data = (
            subprocess.run(
                ["safety", "check", "--json"],
                stdout=subprocess.PIPE,
                check=False,
            )
            .stdout.decode()
            .strip()
        )

    res = json.loads(data)

    return {"evidence": requirements, "warnings": res}


def python_code_analysis(source: List[str]):
    """
    Run pylint on the source code
    """
    from tempfile import NamedTemporaryFile
    import json
    import subprocess

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
        res = json.loads(data)

        return {"evidence": source, "warnings": res}


def spark_log_probe(
    app_id: str,
    spark_history_api: str = "http://localhost:18080/api/v1",
):
    """
    Extracts logs information of a spark application and returns it as a
    dictionary
    """
    import requests

    base_path = f"{spark_history_api}/applications/{app_id}"

    return {
        "allexecutors": requests.get(f"{base_path}/allexecutors").json(),
        "jobs": requests.get(f"{base_path}/jobs").json(),
        "environment": requests.get(f"{base_path}/environment").json(),
    }


def spark_log_analysis(evidence, expected_jobs: Set[str] = set(), prev_evidence=None):
    warnings = []
    new_jobs = {job["name"].split(".scala")[0] for job in evidence["jobs"]}
    for job in new_jobs:
        if job not in expected_jobs:
            warnings.append(f"Unexpected job {job}")

    if prev_evidence is not None:
        old_jobs = {job["name"].split(".scala")[0] for job in prev_evidence["jobs"]}
        for job in new_jobs:
            if job not in old_jobs:
                warnings.append(f"New job {job} not present in previous logs")

    return {"evidence": evidence, "warnings": warnings}


def hdfs_config_probe(hdfs_api: str = "http://localhost:9870"):
    """
    Extracts configuration information of a Hadoop cluster and returns it as a
    dictionary
    """
    import requests
    from xml.etree import ElementTree

    content = requests.get(f"{hdfs_api}/conf").content
    root_xml = ElementTree.fromstring(content)

    return {
        e.find("name").text: e.find("value").text for e in root_xml.findall("property")
    }


def hdfs_config_analysis(config):
    """
    Analyzes an Hadoop cluster configuration
    """
    warnings = []
    scores = []
    for k, v in config.items():
        # Encryption
        if k == "dfs.encrypt.data.transfer" and v == "false":
            warnings.append("In-transit data encryption is disabled")
            scores.append(0.1)
        if k == "yarn.intermediate-data-encryption.enable" and v == "false":
            warnings.append("Intermediate data encryption is disabled")
            scores.append(0.1)
        # Access control
        if k == "dfs.permissions.enabled" and v == "false":
            warnings.append("FS access control is disabled")
            scores.append(0.1)
        if k == "dfs.permissions.superusergroup" and v == "supergroup":
            warnings.append("Task is running with default unrestricted permissions")
            scores.append(0.5)
        if k == "hadoop.registry.secure" and v == "false":
            warnings.append("Registry security is not enabled")
            scores.append(0.5)
        if k == "hadoop.security.authorization" and v == "false":
            warnings.append("Authentication is disabled")
            scores.append(0.1)

    return {"evidence": config, "warnings": warnings, "scores": scores}


def hdfs_paths_probe(source_code: str) -> List[str]:
    assert isinstance(source_code, str)

    def file_path_heuristic(h_source_code: str) -> List[str]:

        from itertools import chain
        import re

        r1 = r'["\']\s*(\w+:(\/?\/?)[^\s]+)\s*["\']'  # well formatted uri
        r2 = r'["\']\s*(.+/.+)\s*["\']'  # any string with a slash

        iterator = chain(re.finditer(r1, h_source_code), re.finditer(r2, h_source_code))

        return list(set(s.group(1).strip() for s in iterator))

    def url_map(maybe_path: str) -> Optional[str]:

        from urllib.parse import urlparse

        try:
            parsed_url = urlparse(maybe_path)
            if parsed_url.scheme is None:
                parsed_url.scheme = "hdfs"
            if parsed_url.netloc is None:
                parsed_url.netloc = "localhost"

            return parsed_url.geturl()

        except ValueError:
            return None

    return list(
        filter(
            lambda p: p is not None,
            map(
                url_map,
                file_path_heuristic(h_source_code=source_code),
            ),
        )
    )


#
# Airflow task definitions
#


def requirements_check(keep_last=False):
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        prev_res = pipeline_lib.load_prev_results(ti, ti.task_id)
        if prev_res is not None:
            return prev_res
        else:
            print("No previous results found...")

    with open("requirements.txt") as f:
        requirements = list(f.readlines())

    res = requirements_analysis(requirements=requirements)

    if len(res["warnings"]) == 0:  # No warnings
        score = 1.0
    else:
        ids = [(e[0], e[4]) for e in res["warnings"]]
        scores = list(map(pipeline_lib.cve_to_score, pipeline_lib.pyupio_to_cve(ids)))

        if all([s is None for s in scores]):  # All warnings don't have scores
            score = 0.5
        else:  # Default
            score = 1.0 - max([s / 10.0 for s in scores if s is not None])

        print(score)

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def python_code_check(file_path: str, keep_last=False):
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        prev_res = pipeline_lib.load_prev_results(ti, ti.task_id)
        if prev_res is not None:
            return prev_res
        else:
            print("No previous results found...")

    with open(file=file_path) as f:
        source_code = list(f.readlines())
    res = python_code_analysis(source=source_code)

    if len(res["warnings"]) == 0:
        score = 1.0
    elif len([w for w in res["warnings"] if w["type"] in ["error", "fatal"]]) > 0:
        score = 0.0
    elif len([w for w in res["warnings"] if w["type"] in ["refactor", "warning"]]) > 0:
        score = 0.75
    else:  # Some convention warnings remaining
        score = 0.90

    return {
        "file_path": file_path,
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def hadoop_config_check(keep_last=False):
    from airflow.operators.python import get_current_context

    ti = get_current_context()["ti"]

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        prev_res = pipeline_lib.load_prev_results(ti, ti.task_id)
        if prev_res is not None:
            return prev_res
        else:
            print("No previous results found...")

    config = hdfs_config_probe()
    res = hdfs_config_analysis(config)
    score = min(res["scores"] + [1.0])  # min of scores or default to 1.0

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def hdfs_paths_check(
    target_task_id: str,
    spark_file_path: str,
    expected_paths_re: List[str] = [],
    keep_last=False,
):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context
    import re

    context = get_current_context()
    ti: TaskInstance = context["ti"]

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        prev_res = pipeline_lib.load_prev_results(ti, ti.task_id)
        if prev_res is not None:
            return prev_res
        else:
            print("No previous results found...")

    airflow_task = pipeline_lib.python_task_source_extractor(
        dag=context["dag"], task_id=target_task_id
    )

    airflow_source = (
        [airflow_task["source"]]
        + list(map(str, airflow_task["args"]))
        + list(map(str, airflow_task["kwargs"].values()))
    )

    with open(spark_file_path) as r:
        spark_source = [r.read()]

    source_code = airflow_source + spark_source

    detected_paths = list(
        set(path for src in source_code for path in hdfs_paths_probe(source_code=src))
    )

    evidence = {
        "spark_source": spark_source,
        "airflow_source": airflow_source,
        "detected_paths": detected_paths,
    }
    warnings = list(
        set(
            f"Unexpected path {path}"
            for path in detected_paths
            if len({m for r in expected_paths_re for m in re.findall(r, path)}) == 0
        )
    )
    score = 1.0 if len(warnings) == 0 else 0.0

    return {"evidence": evidence, "warnings": warnings, "score": score}


def spark_logs_check(
    spark_history_api="http://localhost:18080/api/v1",
    expected_jobs: Set[str] = set(),
    keep_last=False,
):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context

    ti: TaskInstance = get_current_context()["ti"]

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        prev_res = pipeline_lib.load_prev_results(ti, ti.task_id)
        if prev_res is not None:
            return prev_res
        else:
            print("No previous results found...")

    model_data = ti.xcom_pull("pipeline.train_model")
    app_id = model_data["app_id"]
    evidence = spark_log_probe(app_id=app_id, spark_history_api=spark_history_api)

    try:
        prev_res = pipeline_lib.load_prev_results(ti, "train_model")
        if prev_res is None:
            prev_evidence = None
        else:
            prev_app_id = prev_res["app_id"]
            prev_evidence = spark_log_probe(
                app_id=prev_app_id, spark_history_api=spark_history_api
            )
    except KeyError:
        prev_evidence = None

    res = spark_log_analysis(
        evidence=evidence, expected_jobs=expected_jobs, prev_evidence=prev_evidence
    )

    score = 1.0 if len(res["warnings"]) == 0 else 0.0

    return {"evidence": res["evidence"], "warnings": res["warnings"], "score": score}


def report_generator(task_ids: Optional[Set[str]] = None):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context
    from functools import reduce
    import operator

    ctx = get_current_context()
    ti: TaskInstance = ctx["ti"]
    if task_ids is None:
        task_ids = ti.task.upstream_task_ids

    prev_results = {
        task_id: res for task_id, res in zip(task_ids, ti.xcom_pull(task_ids=task_ids))
    }

    task_scores = {task_id: res.get("score") for task_id, res in prev_results.items()}
    task_warnings = {
        task_id: res.get("warnings") for task_id, res in prev_results.items()
    }

    return {
        "score": min(list(task_scores.values()) + [1.0]),
        "prod_score": reduce(operator.mul, task_scores.values(), 1.0),
        "task_scores": task_scores,
        "task_warnings": task_warnings,
    }


with DAG(
    "verified_classification_pipeline",
    default_args=default_args,
    description="""
    Classification model pipeline
    """,
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=["big-data assurance"],
) as dag:
    # Data from https://www.kaggle.com/c/titanic/
    train_set = File("hdfs://localhost:/titanic/train.csv")

    with TaskGroup(group_id="pipeline") as p1:
        train_model_t = PythonOperator(
            task_id="train_model",
            python_callable=pipeline_lib.train_model,
            op_kwargs={
                "train_set": train_set,
                "model_target": "/titanic/model",
                "results_target": "/titanic/results",
                "app_name": "spark_classification",
                "keep_last": '{{"train_model" in dag_run.conf.get("keep_last", [])}}',
            },
        )

    with TaskGroup(group_id="pre-execution-checks") as pre_p1:
        requirements_check_t = PythonOperator(
            task_id="requirements_check",
            python_callable=requirements_check,
            op_kwargs={
                "keep_last": '{{"requirements_check" in dag_run.conf.get("keep_last", [])}}',
            },
        )

        python_code_check_airflow_t = PythonOperator(
            task_id="python_code_check_airflow",
            python_callable=python_code_check,
            op_kwargs={
                "file_path": "./dags/airflow_verified_classification.py",
                "keep_last": '{{"python_code_check_airflow" in dag_run.conf.get("keep_last", [])}}',
            },
        )

        python_code_check_spark_t = PythonOperator(
            task_id="python_code_check_spark",
            python_callable=python_code_check,
            op_kwargs={
                "file_path": "./dags/spark_classification.py",
                "keep_last": '{{"python_code_check_spark" in dag_run.conf.get("keep_last", [])}}',
            },
        )

        hadoop_config_check_t = PythonOperator(
            task_id="hadoop_config_check",
            python_callable=hadoop_config_check,
            op_kwargs={
                "keep_last": '{{"hadoop_config_check" in dag_run.conf.get("keep_last", [])}}',
            },
        )

        hdfs_paths_check_t = PythonOperator(
            task_id="hdfs_paths_check",
            python_callable=hdfs_paths_check,
            op_kwargs={
                "target_task_id": train_model_t.task_id,
                "spark_file_path": "./dags/spark_classification.py",
                "expected_paths_re": [r"hdfs://localhost.+"],
                "keep_last": '{{"hdfs_paths_check" in dag_run.conf.get("keep_last", [])}}',
            },
        )

    with TaskGroup(group_id="post-execution-checks") as post_p1:
        spark_logs_check_t = PythonOperator(
            task_id="spark_logs_check",
            python_callable=spark_logs_check,
            retry_delay=timedelta(seconds=5),
            retries=2,
            op_kwargs={
                "target_task_id": train_model_t.task_id,
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
                "keep_last": '{{"spark_logs_check" in dag_run.conf.get("keep_last", [])}}',
            },
        )

    with TaskGroup(group_id="assurance-report") as ass_p1:
        report_generator_t = PythonOperator(
            task_id="report_generator", python_callable=report_generator
        )

    pre_p1 >> p1 >> post_p1
    [pre_p1, post_p1] >> ass_p1
