from pyspark.sql import SparkSession
import os

# Force local mode; no cluster needed
spark = SparkSession.builder \
    .appName("PythonWordCount") \
    .master("local[*]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

# spark = SparkSession.builder \
#     .appName("PythonWordCount") \
#     .master(spark_master) \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.driver.memory", "1g") \
#     .getOrCreate()

# spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

# Input text
text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

# Split words and parallelize
words = spark.sparkContext.parallelize(text.split(" "))

# Count words
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print results
for word, count in wordCounts.collect():
    print(word, count)

spark.stop()
