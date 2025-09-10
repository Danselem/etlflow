### etlflow

Production-like ETL playground that runs locally with Docker Compose. It wires together Apache Airflow, Spark, Kafka, MySQL, Postgres, Redis and MinIO to demonstrate an end‑to‑end batch pipeline and Spark job orchestration.

- **Batch DAG (`batch_ingest`)**: MySQL ➜ Pandera validation ➜ land CSV to MinIO ➜ Spark transform ➜ load Parquet to Postgres.
- **Spark jobs DAG (`sparking_flow`)**: Submits Python and Java Spark jobs via Airflow `SparkSubmitOperator`.


## Prerequisites
- Docker Desktop (>= 4 CPUs, >= 4GB RAM allocated)
- Make (optional, convenience)


## Quick start
1) Build and start the stack
```
make up
```
or
```
docker compose up -d --build
```

2) UIs and endpoints
- Airflow API server: http://localhost:8080 (user: `airflow` / pass: `airflow`)
- Spark Master UI: http://localhost:9090
- Postgres: localhost:5432 (`airflow`/`airflow` db: `airflow`); jobs DB is created as `jobs`
- PGAdmin: http://localhost:8050 (user: `root@root.com` / pass: `root`)
- MinIO Console: http://localhost:9001 (access: `minio` / secret: `minio123`)
- MinIO S3 endpoint: http://localhost:9000
- Kafka broker: advertised as `${KAFKA_BROKER}` (see Config section)

3) Initialize Airflow volumes (first run only)
This is handled automatically by the `airflow-init` service during `docker compose up`.


## What gets deployed
- Spark master/worker (Bitnami image)
- Zookeeper & Kafka (Confluent images)
- MySQL with a sample `orders` table seeded from `include/scripts/init_db.sql`
- Postgres with `jobs` database from `include/scripts/pq_db.sql`
- Redis (Celery broker)
- MinIO (S3-compatible object store) with health checks
- Airflow components: API server, Scheduler, Dag Processor, Celery Worker, Triggerer, CLI

Shared volumes mount your code and DAGs into the containers:
- `./airflow/dags` ➜ `/opt/airflow/dags`
- `./jobs` ➜ Spark jobs location (mounted in Airflow and Spark containers)


## Project layout
```
airflow/
  dags/
    batch_ingest.py        # MySQL ➜ validate ➜ MinIO ➜ Spark ➜ Postgres
    spark_airflow.py       # Submit Python and Java Spark jobs
  airflow.env              # Airflow and connection env vars
  requirements.txt         # Providers & libs used by Airflow tasks
jobs/
  python/
    spark_batch_job.py     # Reads CSV from MinIO, transforms, writes Parquet
    wordcountjob.py        # Minimal Spark word-count example
  java/spark-job/          # Maven project (WordCount example)
  scala/                   # Scala Spark sample (optional)
include/scripts/
  init_db.sql              # Seeds MySQL `orders`
  pq_db.sql                # Creates Postgres `jobs` DB
docker-compose.yaml        # All services
Makefile                   # build/up/down helpers
```


## Configuration
Key variables are defined in `airflow/airflow.env` and referenced by Compose:
- `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow`
- `AIRFLOW_CONN_MYSQL_DEFAULT=mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@mysql:${MYSQL_PORT}/${MYSQL_DATABASE}`
- `AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql+psycopg2://airflow:airflow@postgres:5432/jobs`
- `AIRFLOW_CONN_SPARK_CONN=spark://spark-master:7077`
- `SPARK_MASTER_URL=spark://spark-master:7077`

Kafka advertised listeners rely on `${KAFKA_BROKER}` in `docker-compose.yaml`:
- For local host access (clients on your laptop), export: `KAFKA_BROKER=localhost:9092`
- For in-cluster clients (containers), `kafka:9092` is used via `bootstrap_servers`. If you want outside clients too, set `KAFKA_BROKER=localhost:9092` before `docker compose up`.

You can create a `.env` file in repo root to persist these exports.


## Running the pipelines
1) Start the stack: `make up`
2) Open Airflow UI at http://localhost:8080
3) Unpause and trigger:
   - `batch_ingest`
   - `sparking_flow`

Connections used by the DAGs (preconfigured from env):
- `mysql_default` ➜ MySQL seeded with `orders`
- `spark-conn` ➜ Spark master URL
- `postgres_default` ➜ Postgres `jobs` database

Data flow (batch_ingest):
1. Extract from MySQL `orders` using `MySqlHook`
2. Validate with Pandera schema
3. Write CSV to MinIO bucket `raw-data` at `orders/orders.csv`
4. Spark job (`jobs/python/spark_batch_job.py`) reads CSV from MinIO, dedupes, adds timestamp, aggregates, writes Parquet to `processed-data`
5. Load Parquet from MinIO into Postgres table `orders_transformed`


## Useful commands
```
make build         # docker compose build
make up            # start services
make down          # stop and remove services
make restart       # restart services
```

Run the Kafka producer (from your host) to emit sample messages:
```
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --list | cat"
python3 kafka/producer.py  # requires local kafka-python installed, or run inside a container
```

Run Spark wordcount inside Airflow worker (debug):
```
docker exec -it <airflow-worker-container> /bin/bash
export SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
export PATH=$SPARK_HOME/bin:$PATH
/home/airflow/.local/bin/python3 jobs/python/wordcountjob.py
```


## Troubleshooting
- Ensure Docker has ≥ 4GB RAM and ≥ 2 CPUs (Airflow init checks this).
- If Airflow web is not up yet, wait for `airflow-init` and `scheduler` to become healthy.
- If Kafka is unreachable from host, set `KAFKA_BROKER=localhost:9092` before `docker compose up`.
- If MinIO buckets are missing, the Spark job and DAG create them on first run.
- Logs are under `airflow/logs/` and service logs via `docker compose logs -f <service>`.


## License
This project is licensed under the terms of the [MIT LICENSE](/LICENSE) file in the repository.
