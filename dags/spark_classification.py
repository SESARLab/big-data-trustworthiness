def train_model(train_set):
    from pprint import pformat
    from pyspark.ml.classification import LinearSVC
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.sql import functions, types

    train_set.printSchema()

    # Evaluator
    evaluator = BinaryClassificationEvaluator(labelCol="Survived")

    # Convert cabin null to string
    dumb_to_string = functions.udf(lambda x: str(x), types.StringType())
    train_set = train_set.withColumn(
        "Cabin", dumb_to_string(functions.col("Cabin")))

    # Drop NA
    train_set = train_set.dropna()

    # Index strings
    sex_string_indexer = StringIndexer(
        inputCol="Sex", outputCol="SexId"
    ).setHandleInvalid("keep")
    cabin_string_indexer = StringIndexer(
        inputCol="Cabin", outputCol="CabinId"
    ).setHandleInvalid("keep")
    embarked_string_indexer = StringIndexer(
        inputCol="Embarked", outputCol="EmbarkedId"
    ).setHandleInvalid("keep")

    # Assembler
    assembler = VectorAssembler(
        inputCols=[
            "Pclass",
            "Age",
            "SibSp",
            "Parch",
            "Fare",
            "SexId",
            "CabinId",
            "EmbarkedId",
        ],
        outputCol="features",
    ).setHandleInvalid("keep")

    # Classifier

    estimator = LinearSVC(labelCol="Survived", featuresCol="features")

    # estimator = LogisticRegression(
    #     labelCol="Survived",
    #     maxIter=10,
    #     regParam=0.3,
    #     elasticNetParam=0.8)

    # estimator = DecisionTreeClassifier(
    #     labelCol="Survived",
    #     maxDepth=3)

    # Pipeline
    pipeline = Pipeline(
        stages=[
            sex_string_indexer,
            cabin_string_indexer,
            embarked_string_indexer,
            assembler,
            estimator,
        ]
    )

    # Cross validation
    # grid = ParamGridBuilder().addGrid(
    #     estimator.regParam, [0.1, 0.3, 0.7]).build()
    grid = ParamGridBuilder().addGrid(
        estimator.regParam, [0.1, 0.3, 0.7]).build()
    cv = CrossValidator(
        estimator=pipeline,
        evaluator=evaluator,
        numFolds=9,
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
    print("Params:", pformat(model.avgMetrics))
    print("Summary:", model.getEstimatorParamMaps())
    df.show()

    return model


if __name__ == "__main__":
    from pyspark.sql import SparkSession, Row

    spark = (
        SparkSession.builder.appName("spark_classification")
        .master("yarn")
        .getOrCreate()
    )

    train_set_url = "titanic/train.csv"
    test_set_url = "titanic/test.csv"
    model_target_url = "titanic/model"
    results_target_url = "titanic/results"

    train_set = spark.read.csv(train_set_url, header=True, inferSchema=True)
    test_set = spark.read.csv(test_set_url, header=True, inferSchema=True)

    model = train_model(train_set=train_set)
    model.write().overwrite().save(model_target_url)

    data = spark.createDataFrame(
        data=[
            Row(
                app_id=spark.sparkContext.applicationId,
                scores=model.avgMetrics,
                summary=[str(param)
                         for param in model.getEstimatorParamMaps()],
            )
        ]
    )

    data.write.json(results_target_url, mode="overwrite")

    # from pyspark.ml.tuning import CrossValidatorModel
    # model2 = CrossValidatorModel.load("hdfs://localhost:/titanic/model")
    # model2.transform(test_set).show()
