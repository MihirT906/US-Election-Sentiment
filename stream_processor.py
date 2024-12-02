from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, count, sum as spark_sum, to_timestamp, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)
from textblob import TextBlob


def compute_sentiment(text):
    if not text or text.strip() == "":
        return 0.0  # Return neutral sentiment for empty text
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

sentiment_udf = udf(compute_sentiment, DoubleType())

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


df_with_timestamp = df_parsed.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

df_with_combined_text = df_with_timestamp.withColumn(
    "combined_text",
    coalesce(col("title"), lit(""))  # Replace NULL in 'title' with an empty string
)

# Apply sentiment UDF
df_with_sentiment = df_with_combined_text.withColumn(
    "sentiment", sentiment_udf(col("combined_text"))
)

df_with_watermark = df_with_sentiment.withWatermark("timestamp", "10 minutes")

# Aggregate by month_key including sentiment
df_aggregated = df_with_watermark.groupBy("month_key") \
    .agg(
        count("*").alias("total_entries"),
        spark_sum("num_comments").alias("total_comments"),
        spark_sum("sentiment").alias("total_sentiment")  # Aggregate sentiment
    )


df_selected = df_parsed.select("title", "num_comments")
#df_grouped = df_parsed.groupBy("candidate_key").count()
# df_candidate_counts = df_parsed \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy("candidate_key") \
#     .count()
# Output to console

# query = df_selected \
query = df_aggregated \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()


# Wait for the streaming query to terminate
query.awaitTermination()




