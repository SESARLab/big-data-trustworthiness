from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lower, trim, length
from pprint import pprint


def explode_col(weight):
    return int(weight // 10) * [10.0] + ([] if weight % 10 == 0 else [weight % 10])


spark = SparkSession.builder \
    .appName("spark_test") \
    .master("spark://localhost:7077") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs://localhost:/example/addresses.csv", inferSchema=True)

df = df.select(lower(trim(df._c0)).alias("name"),
               lower(trim(df._c1)).alias("surname"))
rdd = df.rdd.map(lambda row: (row[0], row[1]))

print(rdd.toDebugString().decode("utf-8"))

df = rdd.toDF()
df.printSchema()

df.show()
df.write.csv("hdfs://localhost:/example/addresses_lower.csv",
             header=True, mode="overwrite")
