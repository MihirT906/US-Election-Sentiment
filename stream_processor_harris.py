from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum as spark_sum, to_date, udf, coalesce, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from textblob import TextBlob
from config import SUBREDDIT_NAME

# Sentiment Analysis Function
def compute_sentiment(text):
    if not text or text.strip() == "":
        return 0.0  # Neutral sentiment for empty text
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

# Register UDF for Sentiment Analysis
sentiment_udf = udf(compute_sentiment, DoubleType())

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
    .appName("HarrisRedditKafkaConsumer") \
    .getOrCreate()

# Set the log level to WARN to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", f"reddit_posts_harris_{SUBREDDIT_NAME}") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert the Kafka message value (binary) to a string and parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Add a proper timestamp column for watermarking
df_with_timestamp = df_parsed.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Combine title and selftext into a single column, handling nulls
df_with_combined_text = df_with_timestamp.withColumn(
    "combined_text",
    coalesce(col("title"), lit(""))  # Replace NULL in 'title' with an empty string
)

# Apply sentiment analysis
df_with_sentiment = df_with_combined_text.withColumn(
    "sentiment", sentiment_udf(col("combined_text"))
)

df_with_date = df_with_sentiment.withColumn("date", to_date(col("timestamp")))

# Add watermark for handling late data
df_with_watermark = df_with_sentiment.withWatermark("timestamp", "10 minutes")

# Aggregate data by month_key, including sentiment
df_aggregated = df_with_date.groupBy("date") \
    .agg(
        count("*").alias("number_of_posts"),
        spark_sum("num_comments").alias("total_comments")
    )
    
# Write aggregated data to a Parquet sink
water_mark_query = df_with_watermark \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"posts_output/{SUBREDDIT_NAME}/Harris/watermark_data") \
    .option("checkpointLocation", f"posts_checkpoint/{SUBREDDIT_NAME}/Harris/watermark_data") \
    .start()
    
# aggregated_query = df_aggregated \
#     .writeStream \
#     .outputMode("complete") \
#     .format("delta") \
#     .option("path", "my_output/USpolitics/Trump/aggregated_data") \
#     .option("checkpointLocation", "checkpoint/USpolitics/Trump/aggregated_data") \
#     .start()

# Wait for the streaming query to terminate
water_mark_query.awaitTermination()
# aggregated_query.awaitTermination()
