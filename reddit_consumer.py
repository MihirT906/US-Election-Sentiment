from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'redditcomments',  # Topic name
    bootstrap_servers='localhost:9092',  # Kafka broker
    auto_offset_reset='earliest',  # Start reading at the beginning of the topic
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def process_data(message):
    # Process the incoming data
    message['text'] = message['text'].replace('\n', ' ').strip()
    
    # Print or process message further
    print(f"Processed Message: {message['title']}")
    
print("Listening for messages in 'redditcomments'...")

# Process incoming messages
for message in consumer:
    print(f"Received message: {message.value}")
