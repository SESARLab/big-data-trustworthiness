def train_kmeans_model(train_set):
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

    train_set.printSchema()

    # Evaluator
    evaluator = ClusteringEvaluator(predictionCol="target")

    # Assembler
    assembler = VectorAssembler(
        inputCols=[
            "pixel_0_0",
            "pixel_0_1",
            "pixel_0_2",
            "pixel_0_3",
            "pixel_0_4",
            "pixel_0_5",
            "pixel_0_6",
            "pixel_0_7",
            "pixel_1_0",
            "pixel_1_1",
            "pixel_1_2",
            "pixel_1_3",
            "pixel_1_4",
            "pixel_1_5",
            "pixel_1_6",
            "pixel_1_7",
            "pixel_2_0",
            "pixel_2_1",
            "pixel_2_2",
            "pixel_2_3",
            "pixel_2_4",
            "pixel_2_5",
            "pixel_2_6",
            "pixel_2_7",
            "pixel_3_0",
            "pixel_3_1",
            "pixel_3_2",
            "pixel_3_3",
            "pixel_3_4",
            "pixel_3_5",
            "pixel_3_6",
            "pixel_3_7",
            "pixel_4_0",
            "pixel_4_1",
            "pixel_4_2",
            "pixel_4_3",
            "pixel_4_4",
            "pixel_4_5",
            "pixel_4_6",
            "pixel_4_7",
            "pixel_5_0",
            "pixel_5_1",
            "pixel_5_2",
            "pixel_5_3",
            "pixel_5_4",
            "pixel_5_5",
            "pixel_5_6",
            "pixel_5_7",
            "pixel_6_0",
            "pixel_6_1",
            "pixel_6_2",
            "pixel_6_3",
            "pixel_6_4",
            "pixel_6_5",
            "pixel_6_6",
            "pixel_6_7",
            "pixel_7_0",
            "pixel_7_1",
            "pixel_7_2",
            "pixel_7_3",
            "pixel_7_4",
            "pixel_7_5",
            "pixel_7_6",
            "pixel_7_7",
        ],
        outputCol="features",
    )

    # Classifier
    estimator = KMeans(featuresCol="features", predictionCol="pred", seed=12345)

    # Pipeline
    pipeline = Pipeline(
        stages=[
            # sex_string_indexer,
            # cabin_string_indexer,
            # embarked_string_indexer,
            assembler,
            estimator,
        ]
    )

    # Cross validation
    # grid = ParamGridBuilder().addGrid(
    #     estimator.regParam, [0.1, 0.3, 0.7]).build()
    # grid = ParamGridBuilder().addGrid(estimator.maxIter, [10, 20, 50]).build()
    grid = ParamGridBuilder().build()

    cv = CrossValidator(
        estimator=pipeline,
        evaluator=evaluator,
        numFolds=7,
        estimatorParamMaps=grid,
        parallelism=2,
        seed=123,
    )

    # Model fitting
    # model = pipeline.fit(train_set)
    model = cv.fit(train_set)

    # Test set prediction
    df = model.transform(train_set)

    # Train set evaluation
    print("Training set:", evaluator.evaluate(model.transform(train_set)))
    # print("Params:", pformat(model.avgMetrics))
    print("Summary:", model.getEstimatorParamMaps())
    df.show()

    return model


if __name__ == "__main__":
    from pyspark.sql import SparkSession, Row
    from sklearn.datasets import load_digits

    spark = SparkSession.builder.appName("spark_kmeans").master("yarn").getOrCreate()

    base_folder = "kmeans"
    model_target_url = "kmeans/model"
    results_target_url = "kmeans/results"

    data, target = load_digits(return_X_y=True, as_frame=True)
    data["target"] = target

    data_df = spark.createDataFrame(data)

    train_set, test_set = data_df.randomSplit([0.8, 0.2], seed=12345)

    model = train_kmeans_model(train_set=train_set)

    model.write().overwrite().save(model_target_url)

    data = spark.createDataFrame(
        data=[
            Row(
                app_id=spark.sparkContext.applicationId,
                scores=model.avgMetrics,
                summary=[str(param) for param in model.getEstimatorParamMaps()],
            )
        ]
    )

    data.write.json(results_target_url, mode="overwrite")

    # from pyspark.ml.tuning import CrossValidatorModel
    # model2 = CrossValidatorModel.load("hdfs://localhost:/titanic/model")
    # model2.transform(test_set).show()
