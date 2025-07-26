from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import random

# Create Spark session
spark = SparkSession.builder.appName("GroupByExample").getOrCreate()

# Generate sample data (e.g., 100 rows of [group, value])
data = [(random.choice(['A', 'B', 'C']), random.randint(1, 100)) for _ in range(100)]
df = spark.createDataFrame(data, ["group", "value"])

# Show the generated data
print("Sample Data:")
df.show()

# Perform groupBy calculation (e.g., average value per group)
result = df.groupBy("group").agg(avg("value").alias("average_value"))

# Show result
print("Grouped Result:")
result.show()

# Optional: write to Object Storage (replace with your path if needed)
# result.write.csv("oci://your-bucket@namespace/group_by_result", mode="overwrite")
