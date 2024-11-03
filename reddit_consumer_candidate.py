from kafka import KafkaConsumer
import json

# Initialize Kafka consumer for candidate posts
candidate_consumer = KafkaConsumer(
    'reddit_candidate',
    bootstrap_servers='localhost:9092',
    group_id='candidate_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in candidate_consumer:
    data = message.value
    # Process candidate-related data
    candidate = data['candidate_key']
    print(f"Candidate Post: {data['title'][:10]} mentions {candidate}")
    # Additional processing, e.g., sending to a database or PySpark
