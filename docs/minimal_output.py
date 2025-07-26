from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestRun").getOrCreate()

# Create sample data
data = [(1, "a"), (2, "b"), (3, "c")]
df = spark.createDataFrame(data, ["id", "value"])
df.show()

# Write DataFrame to OCI Object Storage bucket
bucket_path = "oci://df-output@idub0tfy0whp/"

# Write as Parquet format (recommended for performance)
df.write \
  .mode("overwrite") \
  .parquet(bucket_path + "parquet_output")

# Alternative: Write as CSV format
df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(bucket_path + "csv_output")

# Alternative: Write as JSON format
df.write \
  .mode("overwrite") \
  .json(bucket_path + "json_output")

print(f"Data successfully written to bucket: {bucket_path}")

# Stop Spark session
spark.stop()