from kafka import KafkaConsumer
import json

# Initialize Kafka consumer for policy posts
policy_consumer = KafkaConsumer(
    'reddit_policy',
    bootstrap_servers='localhost:9092',
    group_id='policy_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in policy_consumer:
    data = message.value
    # Process policy-related data
    policy = data['policy_key']
    print(f"Policy Post: {data['title']} discusses {policy}")
    # Additional processing, e.g., sending to a database or PySpark
