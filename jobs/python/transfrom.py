from pyspark.sql import SparkSession
import os
from datetime import datetime

spark = SparkSession.builder.appName("TransformAirTravel").getOrCreate()

# MinIO S3-style endpoint
minio_endpoint = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Input from MinIO raw-data bucket
raw_path = "s3a://raw-data/*.csv"
df = spark.read.option("header", "true").csv(raw_path)

# Example transformation: rename column, filter rows
df_transformed = df.withColumnRenamed("JAN", "January").filter("`1958` IS NOT NULL")

# Save back to MinIO transformed-data bucket
output_object = f"airtravel_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
output_path = f"s3a://transformed-data/{output_object}.csv"

df_transformed.write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
