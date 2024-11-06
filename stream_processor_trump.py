from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Define the schema based on the data structure expected in the JSON
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("selftext", StringType(), True),
    StructField("month_key", StringType(), True)
])

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("TrumpRedditKafkaConsumer") \
    .getOrCreate()

# Set the log level to WARN to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_posts_trump") \
    .load()

# Convert the Kafka message value (binary) to a string and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Aggregate data by month_key
df_aggregated = df_parsed.groupBy("month_key") \
    .agg(
        count("*").alias("total_entries"),
        spark_sum("num_comments").alias("total_comments")
    )

# Output the aggregated data to console
query = df_aggregated \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
