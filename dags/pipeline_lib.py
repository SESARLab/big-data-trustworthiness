from openlineage.airflow.dag import DAG


def train_model_task(
    train_set,
    model_target,
    results_target,
    app_name="spark_classification",
    keep_last=False,
):
    """
    Model training pipeline task.

    Example of ML pipeline task.
    """
    from spark_classification import train_model
    from pyspark.sql import SparkSession, Row
    from pyspark.ml.tuning import CrossValidatorModel

    spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()

    if str(keep_last).strip().lower() == "true":
        print("Using last results...")
        model = CrossValidatorModel.load(model_target)  # load saved model
        data = spark.read.json(results_target)  # load saved results
    else:
        train_set = spark.read.csv(train_set.url, header=True, inferSchema=True)
        print("Training the model...")
        model = train_model(train_set=train_set)  # train model
        print("Saving the model...")
        model.write().overwrite().save(model_target)  # save model
        data = spark.createDataFrame(
            data=[
                Row(
                    app_id=spark.sparkContext.applicationId,
                    scores=model.avgMetrics,
                    summary=[str(param) for param in model.getEstimatorParamMaps()],
                )
            ]
        )  # prepare results
        data.write.json(results_target, mode="overwrite")  # save results

    res = [{k: e[k] for k in data.columns} for e in data.collect()][0]

    return res


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
