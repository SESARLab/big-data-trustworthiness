from pyspark.sql import SparkSession, Row
from pyspark.ml.tuning import CrossValidatorModel
import pyspark
import numpy
import os
import sys

print(sys.executable)
print(sys.exec_prefix)
print(numpy.version.full_version)
print(os.environ)

spark = (
    SparkSession.builder.appName("spark_classification")
    .config("spark.kerberos.principal", "bertof/my.engine")
    .config(
        "spark.kerberos.keytab",
        "/home/bertof/Documenti/Papers/big-data-assurance/implementation/eng.keyset",
    )
    .master("yarn")
    .getOrCreate()
)

train_set_url = "hdfs://localhost:/titanic/train.csv"
test_set_url = "hdfs://localhost:/titanic/test.csv"


train_set = spark.read.csv(train_set_url, header=True, inferSchema=True)
