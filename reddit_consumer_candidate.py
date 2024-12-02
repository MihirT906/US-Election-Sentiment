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
    title = data.get('title', None) #M
    selftext = data.get('selftext', None) #M
    sentiment = data.get('sentiment', None) #M
    print(f"Candidate Post: {data['title'][:10]} mentions {candidate}")
    #M
    print(f"Title: {title[:30] if title else 'NULL'} | Selftext: {selftext[:30] if selftext else 'NULL'} | Sentiment: {sentiment}")

    # Additional processing, e.g., sending to a database or PySpark
