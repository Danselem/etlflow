from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import requests
from minio import Minio
import pandas as pd
from io import BytesIO

# ------------------------
# MinIO Config
# ------------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_RAW = "raw-data"
BUCKET_TRANSFORMED = "transformed-data"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# ------------------------
# Extract: Download + Save to MinIO
# ------------------------
def extract_to_minio(**kwargs):
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"
    response = requests.get(url)
    response.raise_for_status()
    data = response.content

    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)

    object_name = f"airtravel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    client.put_object(
        BUCKET_RAW,
        object_name,
        data=BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )
    # Push object name for downstream tasks
    kwargs["ti"].xcom_push(key="raw_object", value=object_name)

# ------------------------
# Load: Read transformed from MinIO → Postgres
# ------------------------
def load_to_postgres(**kwargs):
    ti = kwargs["ti"]
    object_name = ti.xcom_pull(key="transformed_object", task_ids="spark_transform")

    # Download transformed data
    response = client.get_object(BUCKET_TRANSFORMED, object_name)
    df = pd.read_csv(response)

    # Insert into Postgres
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql("airtravel", engine, if_exists="replace", index=False)

# ------------------------
# DAG Definition
# ------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_web_minio_spark_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_to_minio",
        python_callable=extract_to_minio,
        provide_context=True,
    )

    # Spark job will pull raw data from MinIO, transform, and save back to MinIO
    transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/airflow/jobs/transform_job.py",  # Spark script
        conn_id="spark_default",
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    extract >> transform >> load
