from kafka import KafkaConsumer
import json

# Initialize Kafka consumer for raw posts
raw_consumer = KafkaConsumer(
    'reddit_posts_raw',
    bootstrap_servers='localhost:9092',
    group_id='raw_posts_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in raw_consumer:
    data = message.value
    # Process raw data (you can choose to clean or enrich this data)
    print(f"Raw Post: {data['title'][:10]} on {data['month_key']}")
    # Here, you might write this data to a file or send it to PySpark
