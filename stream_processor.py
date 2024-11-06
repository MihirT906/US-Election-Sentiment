from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

#Define the schema based on the data structure expected in the JSON
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
    .appName("RedditKafkaStreaming") \
    .getOrCreate()

# Set the log level to WARN to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_posts_harris") \
    .load()

# Convert the Kafka message value (binary) to a string and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

df_selected = df_parsed.select("title", "num_comments")
#df_grouped = df_parsed.groupBy("candidate_key").count()
# df_candidate_counts = df_parsed \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy("candidate_key") \
#     .count()
# Output to console
query = df_selected \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()
