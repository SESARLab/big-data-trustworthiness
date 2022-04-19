from airflow.lineage.entities import File
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from openlineage.airflow.dag import DAG
from typing import List, Set, Optional, Any, Dict
from pipeline_lib import (
    airflow_config_analysis,
    CachingPythonOperator,
    CachingSparkSubmitOperator,
    cve_to_score,
    get_hdfs_file_permissions,
    hdfs_config_analysis_encryption,
    hdfs_config_analysis_security,
    hdfs_paths_probe,
    load_prev_results,
    operator_args_extractor,
    python_code_analysis,
    pyupio_to_cve,
    requirements_analysis, hdfs_config_probe,
    spark_config_analysis,
    spark_config_probe,
    spark_logs_analysis,
    spark_logs_probe,
    spark_submit_task_source_extractor,
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

#
# Airflow task definitions
#


def requirements_check():
    with open("requirements.txt") as f:
        requirements = list(f.readlines())

    res = requirements_analysis(requirements=requirements)

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
    train_set = File("titanic/train.csv")

    with TaskGroup(group_id="pipeline") as p1:
        train_model_t = CachingSparkSubmitOperator(
            task_id="train_model",
            application="dags/spark_classification.py",
        )

    with TaskGroup(group_id="pre-execution-checks") as pre_p1:
        # p2
        hdfs_paths_check_t = CachingPythonOperator(
            task_id="hdfs_paths_check",
            python_callable=hdfs_paths_check,
            op_kwargs={
                "target_task_id": train_model_t.task_id,
                "spark_file_path": "dags/spark_classification.py",
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
                    "application": "dags/spark_classification.py",
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
            op_kwargs={"file_path": "./dags/spark_classification.py"},
        )

        # p6
        requirements_check_t = CachingPythonOperator(
            task_id="requirements_check",
            python_callable=requirements_check,
        )

        # p7
        # TODO check sul dag

        # p8
        read_permission_check_t = CachingPythonOperator(
            task_id="read_permission_check",
            python_callable=hdfs_file_can_read,
            op_kwargs={
                "path": "/titanic/train.csv",
                "user": "bertof",
                "group": "hadoop",
            },
        )

        # p9
        write_permission_check_t = CachingPythonOperator(
            task_id="write_permission_check",
            python_callable=hdfs_file_can_write,
            op_kwargs={
                "path": "/titanic",
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
                "target_app_name": "spark_classification",
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
