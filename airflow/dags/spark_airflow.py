from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "Daniel Egbo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 9, 6),  # static start date for Airflow 3
}

with DAG(
    dag_id="sparking_flow",
    default_args=default_args,
    description="DAG to run Python, Scala, and Java Spark jobs",
    schedule="@daily",      # replaces schedule_interval
    catchup=False,          # don’t backfill from start_date
    tags=["spark", "etl"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Jobs started"),
    )

    python_job = SparkSubmitOperator(
        task_id="python_job",
        conn_id="spark-conn",
        application="jobs/python/wordcountjob.py",
    )

    
    # python_job = SparkSubmitOperator(
    # task_id="python_job",
    # conn_id="spark-conn",
    # application="jobs/python/wordcountjob.py",
    # conf={
    #     "spark.master": "local[*]"
    # }
    # )


    # scala_job = SparkSubmitOperator(
    #     task_id="scala_job",
    #     conn_id="spark-conn",
    #     application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    # )

    java_job = SparkSubmitOperator(
        task_id="java_job",
        conn_id="spark-conn",
        application="jobs/java/spark-job/target/spark-job-1.0-SNAPSHOT.jar",
        java_class="com.airscholar.spark.WordCountJob",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    start >> [python_job, java_job] >> end #scala_job
