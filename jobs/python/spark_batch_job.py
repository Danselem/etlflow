import logging
import os
import tempfile
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, sum as spark_sum, avg, count

# --------------------------
# Logging
# --------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --------------------------
# MinIO config
# --------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"
RAW_KEY = "orders/orders.csv"
TRANSFORMED_KEY = "orders/orders_transformed.parquet"
AGGREGATED_KEY = "orders/orders_aggregated.parquet"

# --------------------------
# MinIO helpers
# --------------------------
def ensure_bucket(bucket_name):
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    try:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logging.info(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logging.warning(f"Bucket check/create skipped: {e}")

def upload_part_file(local_folder, bucket, key):
    files = [f for f in os.listdir(local_folder) if f.startswith("part-") and f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No part file found in {local_folder}")
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    s3.upload_file(os.path.join(local_folder, files[0]), bucket, key)
    logging.info(f"Uploaded '{files[0]}' to 's3://{bucket}/{key}'")

# --------------------------
# Main ETL
# --------------------------
def main():
    try:
        # Ensure buckets exist
        for bucket in [RAW_BUCKET, PROCESSED_BUCKET]:
            ensure_bucket(bucket)

        # Spark session in LOCAL mode
        spark = SparkSession.builder \
            .appName("BatchETL") \
            .master("local[*]") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()

        # --------------------------
        # Read CSV from MinIO using pandas
        # --------------------------
        s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=MINIO_ACCESS_KEY,
                          aws_secret_access_key=MINIO_SECRET_KEY)
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
        df_pd = pd.read_csv(obj['Body'])
        logging.info(f"Read {len(df_pd)} rows from raw data.")

        # Convert to Spark DataFrame
        df = spark.createDataFrame(df_pd)

        # --------------------------
        # Transform
        # --------------------------
        df_transformed = df.dropDuplicates(["order_id"]).fillna({"amount": 0.0}) \
                           .withColumn("processed_timestamp", current_timestamp())

        # --------------------------
        # Aggregate
        # --------------------------
        df_agg = df_transformed.groupBy("customer_id").agg(
            spark_sum("amount").alias("total_spent"),
            avg("amount").alias("avg_order_amount"),
            count("order_id").alias("order_count")
        )

        # --------------------------
        # Write to MinIO (Parquet)
        # --------------------------
        with tempfile.TemporaryDirectory() as tmpdir:
            # Transformed
            transformed_local = os.path.join(tmpdir, "transformed")
            df_transformed.coalesce(1).write.mode("overwrite").parquet(transformed_local)
            upload_part_file(transformed_local, PROCESSED_BUCKET, TRANSFORMED_KEY)

            # Aggregated
            aggregated_local = os.path.join(tmpdir, "aggregated")
            df_agg.coalesce(1).write.mode("overwrite").parquet(aggregated_local)
            upload_part_file(aggregated_local, PROCESSED_BUCKET, AGGREGATED_KEY)

        logging.info("ETL completed successfully.")
        spark.stop()

    except Exception as e:
        logging.error(f"ETL failed: {e}")

if __name__ == "__main__":
    main()
