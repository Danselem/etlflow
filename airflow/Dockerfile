FROM apache/airflow:3.0.6-python3.11

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Set SPARK_HOME and update PATH
# ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
# ENV PATH=$PATH:$SPARK_HOME/bin

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH


USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt
