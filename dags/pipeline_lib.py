from airflow.models import TaskInstance, DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from openlineage.airflow.dag import DAG
from pyspark.sql import SparkSession
from typing import List, Set, Optional, Any, Dict


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

    print("Requirements:", requirements)

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

    print("Data:", data)

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

    if config.get("core", {}).get("fernet_key", "") == "":
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

#########
# TASKS #
#########


def requirements_check():
    """
    Check the requirements.txt file for vulnerable libraries
    """
    with open("requirements.txt") as f:
        requirements = list(f.readlines())

    res = requirements_analysis(requirements=requirements)

    print("Analysis results", res)

    if len(res["warnings"]) == 0:  # No warnings
        score = 1.0
    else:
        ids = [(e[0], e[4]) for e in res["warnings"]]
        scores = list(map(cve_to_score, pyupio_to_cve(ids)))

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


def python_code_check(file_path: str):
    """
    Check a python file for errors
    """
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


def hadoop_config_check_encryption():
    config = hdfs_config_probe()
    res = hdfs_config_analysis_encryption(config)
    score = min(res["scores"] + [1.0])  # min of scores or default to 1.0

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def hadoop_config_check_security():
    config = hdfs_config_probe()
    res = hdfs_config_analysis_security(config)
    score = min(res["scores"] + [1.0])  # min of scores or default to 1.0

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def spark_config_check(master: str = "spark://localhost:7077"):
    config = spark_config_probe(master)
    res = spark_config_analysis(config)
    score = min(res["scores"] + [1.0])  # min of scores or default to 1.0

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def airflow_config_check():
    from airflow.configuration import conf

    config = conf.as_dict(display_sensitive=True)
    res = airflow_config_analysis(config)
    score = min(res["scores"] + [1.0])  # min of scores or default to 1.0

    return {
        "evidence": res["evidence"],
        "warnings": res["warnings"],
        "score": score,
    }


def acl_config_check(expected_acl):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context

    ti: TaskInstance = get_current_context()["ti"]

    acl = ti.task.dag.access_control
    if acl is None:
        acl = dict()
    evidence = acl
    warnings = [
        f"Unexpected ({k},{v})" for k, v in acl.items() if expected_acl.get(k, {}) != v
    ]
    score = 1.0 if len(warnings) == 0 else 0.0

    return {
        "evidence": evidence,
        "warnings": warnings,
        "score": score,
    }


def task_args_check(
    target_task_id: str,
    expected_args: Dict[str, Any] = [],
):
    from airflow.operators.python import get_current_context
    from pprint import pprint

    args = operator_args_extractor(dag=get_current_context()[
                                   "dag"], task_id=target_task_id)

    print("FOUND ARGS:")
    pprint(args)

    print("EXPECTED ARGS:")
    pprint(expected_args)

    evidence = {"args": args}
    warnings = list(
        set(
            f"Unexpected arg {arg} with value {val}"
            for arg, val in args.items()
            if arg not in expected_args.keys() or val != expected_args.get(arg)
        )
    )
    score = 1.0 if len(warnings) == 0 else 0.0

    return {"evidence": evidence, "warnings": warnings, "score": score}


def hdfs_paths_check(
    target_task_id: str,
    spark_file_path: str,
    expected_paths_re: List[str] = [],
):
    from airflow.operators.python import get_current_context
    import re

    context = get_current_context()

    airflow_task = spark_submit_task_source_extractor(
        dag=context["dag"], task_id=target_task_id
    )

    airflow_source = [airflow_task["source"]] + \
        list(map(str, airflow_task["args"]))

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
    target_app_name: str,
    spark_history_api="http://localhost:18080/api/v1",
    expected_jobs: Set[str] = set(),
):
    from airflow.models import TaskInstance
    from airflow.operators.python import get_current_context

    ti: TaskInstance = get_current_context()["ti"]

    evidence = spark_logs_probe(
        target_app_name=target_app_name, spark_history_api=spark_history_api
    )

    try:
        prev_res = load_prev_results(ti, "pipeline.train_model")
        if prev_res is None:
            prev_evidence = None
        else:
            prev_app_id = prev_res["app_id"]
            prev_evidence = spark_logs_probe(
                app_name=prev_app_id, spark_history_api=spark_history_api
            )
    except KeyError:
        prev_evidence = None

    res = spark_logs_analysis(
        evidence=evidence, expected_jobs=expected_jobs, prev_evidence=prev_evidence
    )

    score = 1.0 if len(res["warnings"]) == 0 else 0.0

    return {"evidence": res["evidence"], "warnings": res["warnings"], "score": score}


def lineage_check():
    # from airflow.models import TaskInstance
    # from airflow.operators.python import get_current_context

    # ti: TaskInstance = get_current_context()["ti"]

    # TODO

    return {"evidence": {}, "warnings": [], "score": 1.0}


def hdfs_file_permission_check(
    path: str,
    owner: Optional[str] = None,
    group: Optional[str] = None,
    expected_permissions: Optional[int] = None,
):
    res = get_hdfs_file_permissions(path)
    assert len(res) == 1
    res = res[0]
    warnings = []
    score = 1.0
    if owner is not None and res["owner"] != owner:
        warnings.append(f"Unexpected owner {res['owner']} for {path}")
        score = 0.0
    if group is not None and res["group"] != group:
        warnings.append(f"Unexpected group {res['group']} for {path}")
        score = 0.0
    if expected_permissions is not None and res["permissions"] != expected_permissions:
        score = 1.0
        warnings = [f"Unexpected permissions {res['permissions']} for {path}"]

    return {"evidence": res, "warnings": warnings, "score": score}


def hdfs_file_can_write(
    path: str, user: Optional[str] = None, group: Optional[str] = None
):
    res = get_hdfs_file_permissions(path)
    perms = res["pad_permissions"]
    WRITE_OCTALS = "2367"

    if (
        (user == res["owner"] and perms[-3] in WRITE_OCTALS)
        or (group == res["group"] and perms[-2] in WRITE_OCTALS)
        or (perms[-1] in WRITE_OCTALS)
    ):
        score = 1.0
        warnings = []
    else:
        score = 0.0
        warnings = [f"Cannot write to {path}"]

    return {"evidence": res, "warnings": warnings, "score": score}


def hdfs_file_can_read(
    path: str, user: Optional[str] = None, group: Optional[str] = None
):
    res = get_hdfs_file_permissions(path)
    perms = res["pad_permissions"]
    READ_OCTALS = "4567"

    if (
        (user == res["owner"] and perms[-3] in READ_OCTALS)
        or (group == res["group"] and perms[-2] in READ_OCTALS)
        or (perms[-1] in READ_OCTALS)
    ):
        score = 1.0
        warnings = []
    else:
        score = 0.0
        warnings = [f"Cannot read from {path}"]

    return {"evidence": res, "warnings": warnings, "score": score}


def kerberos_auth_check(principal: Optional[str] = None, keytab: Optional[str] = None):
    import subprocess

    args = ["kinit"]
    if keytab is not None:
        args += ["-kt", keytab]
    if principal is not None:
        args.append(principal)

    res = subprocess.run(
        args=args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    warnings = []
    score = 1.0
    if res.returncode != 0:
        warnings.append(res.stdout.decode().strip())
        score = 0.0

    return {
        "evidence": res.stdout.decode().strip(),
        "warnings": warnings,
        "score": score,
    }


def openvas_check(
    config: Dict[str, Any], environment: Dict[str, str] = {}, timeout: float = 600.0
):
    from subprocess import TimeoutExpired,  PIPE, Popen
    import json

    args = ["docker", "run", "--rm", "-i", "--pull=missing"]
    for k, v in environment.items():
        args += ["-e", f"{k}={v}"]
    args += ["repository.v2.moon-cloud.eu:4567/probes/openvas"]
    args = " ".join(args)
    print("args:", args)

    in_data = json.dumps(config) + "\n"
    print("in_data:", in_data)

    p = Popen(
        args=args,
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        text=True,
        shell=True,
    )

    try:
        out, err = p.communicate(input=in_data, timeout=timeout)
    except TimeoutExpired:
        p.kill()
        out, err = p.communicate()

    out = out.strip()
    err = err.strip()

    print("OUT:", out)
    print("ERR:", err)

    if p.returncode != 0:
        evidence = {"out": out, "err": err}
        warnings = {"returncode": p.returncode}
        score = 0.0
    else:
        evidence = json.loads(out.split("\n")[-1])
        evidence.update(json.loads("\n".join(out.split("\n")[1:-1])))
        warnings = [(host, data)
                    for host, data in evidence.get("data", {}).items()]
        score = 1.0 - max(
            [
                desc.get("cvss", 10.0) * 0.1
                for host, data in evidence.get("data", {}).items()
                for id, desc in data.get("vulnerabilities", {}).items()
            ]
            + [0.0]
        )

    return {
        "evidence": evidence,
        "warnings": warnings,
        "score": score,
    }


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

    task_scores = {task_id: res.get("score")
                   for task_id, res in prev_results.items()}
    task_warnings = {
        task_id: res.get("warnings") for task_id, res in prev_results.items()
    }

    return {
        "score": min(list(task_scores.values()) + [1.0]),
        "prod_score": reduce(operator.mul, task_scores.values(), 1.0),
        "task_scores": task_scores,
        "task_warnings": task_warnings,
    }
