from airflow.models import TaskInstance, DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from openlineage.airflow.dag import DAG
from pyspark.sql import SparkSession
from typing import Optional, List, Any


#
# Custom operators
#


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


#
# Training task
#


def train_model(
    train_set,
    model_target,
    results_target,
    app_name="spark_classification",
    master="local",
    ss: Optional[SparkSession] = None,
    keep_last=False,
):
    """
    Model training pipeline task.

    Example of ML pipeline task.
    """
    from spark_classification import train_model
    from pyspark.sql import Row
    from pyspark.ml.tuning import CrossValidatorModel
    import urllib.request

    gc_jars = [
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.1.1/gcs-connector-hadoop3-2.1.1-shaded.jar",
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/bigquery-connector/hadoop3-1.2.0/bigquery-connector-hadoop3-1.2.0-shaded.jar",
        "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.22.2/spark-bigquery-with-dependencies_2.12-0.22.2.jar",
    ]

    files = [urllib.request.urlretrieve(url)[0] for url in gc_jars]

    # Set these to your own project and bucket
    project_id = "pipeline-assurance"
    gcs_bucket = "pipeline-assurance-bucket"
    credentials_file = "/home/jovyan/notebooks/gcs/bq-spark-demo.json"

    if ss is None:
        ss = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.jars", ",".join(files))
            # Install and set up the OpenLineage listener
            .config("spark.jars.packages", "io.openlineage:openlineage-spark:0.3.+")
            .config(
                "spark.extraListeners",
                "io.openlineage.spark.agent.OpenLineageSparkListener",
            )
            .config("spark.openlineage.host", "http://localhost:5000")
            .config("spark.openlineage.namespace", "spark_integration")
            # Configure the Google credentials and project id
            # .config("spark.executorEnv.GCS_PROJECT_ID", project_id)
            # .config(
            #     "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS",
            #     "/home/jovyan/notebooks/gcs/bq-spark-demo.json",
            # )
            # .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            # .config(
            #     "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            #     credentials_file,
            # )
            # .config(
            #     "spark.hadoop.fs.gs.impl",
            #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            # )
            # .config(
            #     "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            # )
            # .config("spark.hadoop.fs.gs.project.id", project_id)
            .getOrCreate()
        )

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        model = CrossValidatorModel.load(model_target)  # load saved model
        data = ss.read.json(results_target)  # load saved results
    else:
        train_set = ss.read.csv(train_set.url, header=True, inferSchema=True)
        print("Training the model...")
        model = train_model(train_set=train_set)  # train model
        print("Saving the model...")
        model.write().overwrite().save(model_target)  # save model
        data = ss.createDataFrame(
            data=[
                Row(
                    app_id=ss.sparkContext.applicationId,
                    scores=model.avgMetrics,
                    summary=[str(param) for param in model.getEstimatorParamMaps()],
                )
            ]
        )  # prepare results
        data.write.json(results_target, mode="overwrite")  # save results

    res = [{k: e[k] for k in data.columns} for e in data.collect()][0]

    return res


def get_hdfs_file_permissions(path: str, ss: Optional[SparkSession] = None):
    if ss is None:
        ss = SparkSession.builder.appName("permission_checker").getOrCreate()
    fs = ss._jvm.org.apache.hadoop.fs.FileSystem.get(ss._jsc.hadoopConfiguration())
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


def task_args_extractor(dag: DAG, task_id: str):
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
        return vars(task)


def spark_submit_task_source_extractor(
    dag: DAG,
    task_id: str,
):
    """
    Source code extractort for python airflow opertors
    """
    import inspect
    from textwrap import dedent
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
