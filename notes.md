## Airflow Spark

docker exec -it sparkflow-airflow-worker-1 /bin/bash


export SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
export PATH=$SPARK_HOME/bin:$PATH


/home/airflow/.local/bin/python3 jobs/python/wordcountjob.py

## Spark master

docker exec -it sparkflow-spark-master-1 /bin/bash


spark-submit --version
WARNING: Using incubator modules: jdk.incubator.vector
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 4.0.0
      /_/
                        
Using Scala version 2.13.16, OpenJDK 64-Bit Server VM, 17.0.16
Branch HEAD
Compiled by user wenchen on 2025-05-19T07:58:03Z
Revision fa33ea000a0bda9e5a3fa1af98e8e85b8cc5e4d4
Url https://github.com/apache/spark