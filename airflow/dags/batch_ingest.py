from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "Daniel Egbo",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


def extract_data_from_mysql(**kwargs):
    import logging
    import pandas as pd
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    logging.info("Starting extraction from MySQL...")
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    df = mysql_hook.get_pandas_df("SELECT * FROM orders;")
    logging.info(f"Extracted {len(df)} records from MySQL.")
    return df.to_json()  # pass DataFrame via XCom as JSON string


def validate_data_with_pandera(**kwargs):
    import logging
    import pandas as pd
    import pandera as pa
    from pandera import Column, Check

    ti = kwargs["ti"]
    df = pd.read_json(ti.xcom_pull(task_ids="extract_mysql"))
    logging.info(f"Running Pandera validation on {len(df)} records...")

    schema = pa.DataFrameSchema(
        {
            "order_id": Column(int, nullable=False),
            "amount": Column(float, checks=Check.gt(0)),
            "customer_id": Column(int, nullable=True),
        }
    )

    schema.validate(df, lazy=True)
    logging.info("Pandera validations passed.")
    return df.to_json()


def load_to_minio(**kwargs):
    import logging
    import pandas as pd
    import boto3
    from io import StringIO

    ti = kwargs["ti"]
    df = pd.read_json(ti.xcom_pull(task_ids="validate_data"))

    logging.info("Uploading DataFrame to MinIO (raw-data bucket)...")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )

    bucket_name = "raw-data"
    try:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created or already exists.")
    except Exception as e:
        logging.info(f"Bucket creation skipped: {e}")

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket_name, Key="orders/orders.csv", Body=csv_buffer.getvalue()
    )
    logging.info("File successfully uploaded to MinIO.")


def load_to_postgres(**kwargs):
    import logging
    import pandas as pd
    import boto3
    from io import BytesIO
    import psycopg2

    logging.info("Downloading transformed Parquet from MinIO...")

    # -------------------------
    # MinIO client
    # -------------------------
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )

    bucket_name = "processed-data"
    key = "orders/orders_transformed.parquet"  # parquet file

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    logging.info(f"Loaded {len(df)} records from MinIO")

    # -------------------------
    # Postgres connection (jobs db)
    # -------------------------
    conn = psycopg2.connect(
        host="postgres",
        dbname="jobs",       # <-- use jobs db
        user="airflow",
        password="airflow",
        port=5432,
    )
    cur = conn.cursor()

    logging.info("Creating target table if not exists...")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders_transformed (
            order_id INT,
            customer_id INT,
            amount DECIMAL(10,2),
            processed_timestamp TIMESTAMP
        );
        """
    )
    cur.execute("TRUNCATE TABLE orders_transformed;")

    # Insert data
    rows = [
        (row.order_id, row.customer_id, row.amount, row.processed_timestamp)
        for row in df.itertuples(index=False)
    ]
    args_str = ",".join(cur.mogrify("(%s,%s,%s,%s)", row).decode("utf-8") for row in rows)
    cur.execute(f"INSERT INTO orders_transformed VALUES {args_str}")

    conn.commit()
    cur.close()
    conn.close()

    logging.info("Data successfully loaded into Postgres (jobs db).")

    


with DAG(
    dag_id="batch_ingest",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_data_from_mysql,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data_with_pandera,
    )

    load_to_minio_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
    )

    # spark_transform_task = BashOperator(
    # task_id="spark_transform",
    # bash_command=(
    #     'docker exec -e HOME=/tmp spark '
    #     'spark-submit --master local[2] '
    #     '--conf spark.jars.ivy=/tmp/.ivy2 '
    #     '/opt/spark_jobs/spark_batch_job.py'
    # ),
    # )

    spark_transform_task = SparkSubmitOperator(
    task_id="spark_transform",
    conn_id="spark-conn",
    application="jobs/python/spark_batch_job.py",
    conf={"spark.master": "local[*]"},
)



    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # DAG dependencies
    extract_task >> validate_task >> load_to_minio_task >> spark_transform_task >> load_postgres_task