from airflow.models import TaskInstance, DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from openlineage.airflow.dag import DAG
from pyspark.sql import SparkSession
from typing import Optional, List, Set


####################
# Custom operators #
####################


class CachingPythonOperator(PythonOperator):
    from airflow.utils.context import Context

    def execute(self, context: Context):
        dag_run: DagRun = context["dag_run"]
        keep_last = dag_run.conf.get("keep_last", [])

        if self.task_id in keep_last:
            print("Using last results...")
            prev_res = load_prev_results(context["ti"], self.task_id)
            if prev_res is not None:
                return prev_res
            else:
                print("No previous results found...")

        return super().execute(context=context)


class CachingSparkSubmitOperator(SparkSubmitOperator):
    from airflow.utils.context import Context

    def execute(self, context: Context) -> None:
        dag_run: DagRun = context["dag_run"]
        keep_last = dag_run.conf.get("keep_last", [])

        if self.task_id in keep_last:
            print("Using last results...")
            prev_res = load_prev_results(context["ti"], self.task_id)
            if prev_res is not None:
                return prev_res
            else:
                print("No previous results found...")

        return {"res": super().execute(context=context)}


class CachingDockerOperator(DockerOperator):
    from airflow.utils.context import Context

    def execute(self, context: Context):
        dag_run: DagRun = context["dag_run"]
        keep_last = dag_run.conf.get("keep_last", [])

        if self.task_id in keep_last:
            print("Using last results...")
            prev_res = load_prev_results(context["ti"], self.task_id)
            if prev_res is not None:
                return prev_res
            else:
                print("No previous results found...")

        return super().execute(context=context)


#############
# UTILITIES #
#############


def get_hdfs_file_permissions(path: str, ss: Optional[SparkSession] = None):
    """
    Get HDFS file permission

    Only works with files, not folders
    """
    if ss is None:
        ss = SparkSession.builder.appName("permission_checker").getOrCreate()
    fs = ss._jvm.org.apache.hadoop.fs.FileSystem.get(
        ss._jsc.hadoopConfiguration())
    file = fs.getFileStatus(ss._jvm.org.apache.hadoop.fs.Path(path))
    return {
        "file": file.getPath().getName(),
        "owner": file.getOwner(),
        "group": file.getGroup(),
        "permissions": file.getPermission().toOctal(),
        "pad_permissions": f"{file.getPermission().toOctal():04}",
    }


def python_task_source_extractor(
    dag: DAG,
    task_id: str,
):
    """
    Source code extractort for python airflow opertors
    """
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


def operator_args_extractor(dag: DAG, task_id: str):
    """
    Get the operator arguments
    """
    task = dag.get_task(task_id=task_id)
    if issubclass(type(task), PythonOperator):
        task: PythonOperator
        return {"args": task.op_args, "kwargs": task.op_kwargs}
    elif issubclass(type(task), SparkSubmitOperator):
        task: SparkSubmitOperator
        return {"application": task._application, "args": task._application_args}
    else:
        raise NotImplementedError


def spark_submit_task_source_extractor(
    dag: DAG,
    task_id: str,
):
    """
    Source code extractort for python airflow opertors
    """
    from airflow.providers.apache.spark.operators.spark_submit import (
        SparkSubmitOperator,
    )

    p_op: SparkSubmitOperator = dag.get_task(task_id=task_id)
    with open(p_op._application) as f:
        spark_source = f.read()
    spark_args = p_op._application_args
    spark_args = {} if spark_args is None else spark_args
    return {
        "source": spark_source,
        "args": spark_args,
    }


def load_prev_results(ti: TaskInstance, prev_task: str):
    """
    Utility function to return a previous task XCOM results if present, None otherwise
    """
    from airflow.utils.state import State

    prev_ti = ti.get_previous_ti(state=State.SUCCESS)
    if prev_ti is None:
        return None
    return prev_ti.xcom_pull(prev_task)


def pyupio_to_cve(ids: [(str, str)]) -> [str]:
    """
    Converts pyupio ids to cves

    `ids` is a list of (<library name>, <pyupio id>)
    """
    if len(ids) == 0:
        return []

    import requests

    db = requests.get(
        "https://raw.githubusercontent.com/pyupio/safety-db/master/data/insecure_full.json"
    ).json()

    return [
        e.get("cve")
        for lib, pid in ids
        for e in db.get(lib, [])
        if e["id"] == f"pyup.io-{pid}" and e.get("cve") is not None
    ]


def cve_to_score(cve: str) -> Optional[float]:
    """
    Returns the score of a registered CVE if present, None otherwise
    """
    import cve_lookup

    try:
        res = cve_lookup.cve(cve)
        return res.cvss2.score_overall
    except (AssertionError, ValueError):
        return None


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

#########
# TASKS #
#########


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


def spark_logs_probe(
    target_app_name: str,
    spark_history_api: str = "http://localhost:18080/api/v1",
):
    """
    Extracts logs information of a spark application and returns it as a
    dictionary
    """
    import requests

    applications = requests.get(f"{spark_history_api}/applications").json()

    try:
        app = next(
            filter(lambda e: (e.get("name") == target_app_name), applications))
    except StopIteration as e:
        print(f"No {target_app_name} attempt found")
        raise e

    base_path = f"{spark_history_api}/applications/{app['id']}"

    return {
        "allexecutors": requests.get(f"{base_path}/allexecutors").json(),
        "jobs": requests.get(f"{base_path}/jobs").json(),
        "environment": requests.get(f"{base_path}/environment").json(),
    }


def spark_logs_analysis(evidence, expected_jobs: Set[str] = set(), prev_evidence=None):
    warnings = []
    new_jobs = {job["name"].split(".scala")[0] for job in evidence["jobs"]}
    for job in new_jobs:
        if job not in expected_jobs:
            warnings.append(f"Unexpected job {job}")

    if prev_evidence is not None:
        old_jobs = {job["name"].split(".scala")[0]
                    for job in prev_evidence["jobs"]}
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


def hdfs_config_analysis_encryption(config):
    """
    Analyzes an Hadoop cluster configuration
    """
    warnings = []
    scores = []
    if config.get("dfs.encrypt.data.transfer", "false") == "false":
        warnings.append("In-transit data encryption is disabled")
        scores.append(0.1)
    if config.get("yarn.intermediate-data-encryption.enable", "false") == "false":
        warnings.append("Intermediate data encryption is disabled")
        scores.append(0.1)

    return {"evidence": config, "warnings": warnings, "scores": scores}


def hdfs_config_analysis_security(config):
    """
    Analyzes an Hadoop cluster configuration
    """
    warnings = []
    scores = []
    if config.get("dfs.permissions.enabled", "false") == "false":
        warnings.append("FS access control is disabled")
        scores.append(0.1)
    if config.get("dfs.permissions.superusergroup", "supergroup") == "supergroup":
        warnings.append(
            "Task is running with default unrestricted permissions")
        scores.append(0.5)
    if config.get("hadoop.registry.secure", "false") == "false":
        warnings.append("Registry security is not enabled")
        scores.append(0.5)
    if config.get("hadoop.security.authorization", "false") == "false":
        warnings.append("Authentication is disabled")
        scores.append(0.1)

    return {"evidence": config, "warnings": warnings, "scores": scores}


def spark_config_probe(master: str = "spark://localhost:7077"):
    """
    Extracts configuration information of a Spark cluster and returns it as a
    dictionary
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "conf-checker").master(master).getOrCreate()
    return dict(spark.sparkContext.getConf().getAll())


def spark_config_analysis(config):
    warnings = []
    scores = []

    if config.get("spark.network.crypto.enabled", "false") == "false":
        warnings.append("Network encryption is disabled")
        scores.append(0.1)
    if config.get("spark.io.encryption.enable", "false") == "false":
        warnings.append("IO encryption is disabled")
        scores.append(0.1)

    return {"evidence": config, "warnings": warnings, "scores": scores}


def airflow_config_analysis(config):
    warnings = []
    scores = []

    if config.get("core", {}).get("spark.network.crypto.enabled", "") == "":
        warnings.append("Fernet key is not set")
        scores.append(0.1)
    if config.get("kubernetes", {}).get("verify_ssl", "False") == "False":
        warnings.append("SSL cert. check is disabled")
        scores.append(0.1)

    return {"evidence": config, "warnings": warnings, "scores": scores}


def hdfs_paths_probe(source_code: str) -> List[str]:
    assert isinstance(source_code, str)

    def file_path_heuristic(h_source_code: str) -> List[str]:

        from itertools import chain
        import re

        r1 = r'["\']\s*(\w+:(\/?\/?)[^\s]+)\s*["\']'  # well formatted uri
        r2 = r'["\']\s*(.+/.+)\s*["\']'  # any string with a slash

        iterator = chain(re.finditer(r1, h_source_code),
                         re.finditer(r2, h_source_code))

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
