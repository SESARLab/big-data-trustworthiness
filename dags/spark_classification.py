import sys
from pprint import pformat

# from airflow.decorators import task
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession


def train_model(train_set):
    train_set.printSchema()

    # Evaluator
    evaluator = BinaryClassificationEvaluator(labelCol="Survived")

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
    )

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
    grid = ParamGridBuilder().addGrid(estimator.regParam, [0.1, 0.3, 0.7]).build()
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

    print(sys.argv)

    spark = (
        SparkSession.builder.appName("spark_classification")
        .master("yarn")
        .getOrCreate()
    )

    train_set_url = "hdfs://localhost:/titanic/train.csv"
    # test_set_url = "hdfs://localhost:/titanic/test.csv"

    train_set = spark.read.csv(train_set_url, header=True, inferSchema=True)
    # test_set = spark.read.csv(test_set_url, header=True, inferSchema=True)

    model = train_model(train_set=train_set)

    model.write().overwrite().save("hdfs://localhost:/titanic/model")
