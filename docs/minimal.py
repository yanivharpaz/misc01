from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TestRun").getOrCreate()
data = [(1, "a"), (2, "b"), (3, "c")]
df = spark.createDataFrame(data, ["id", "value"])
df.show()
