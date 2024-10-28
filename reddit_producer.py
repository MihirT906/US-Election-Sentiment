import praw
import json
from kafka import KafkaProducer
import configparser
import time

# Load Reddit API credentials from config file
config = configparser.ConfigParser()
config.read('credentials.cfg')


client_id = config['DEFAULT']['client_id']
client_secret = config['DEFAULT']['SECRET_KEY']
username = config['DEFAULT']['USERNAME']
password = config['DEFAULT']['PASSWORD']
user_agent = config['DEFAULT']['USER_AGENT']

#Initialize Reddit API with PRAW
reddit = praw.Reddit(
    client_id=client_id, 
    client_secret=client_secret, 
    username=username,
    password=password,
    user_agent=user_agent)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to stream Reddit comments to Kafka
def stream_reddit_comments(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)

    for comment in subreddit.stream.comments(skip_existing=True):
        comment_data = {
            'id': comment.id,
            'body': comment.body,
            'created_utc': comment.created_utc,
            'author': str(comment.author),
            'score': comment.score,
        }
        print(f'Sending comment to Kafka: {comment.body[:30]}...')
        producer.send('redditcomments', value=comment_data)
        time.sleep(1)  # Adjust the rate as needed

if __name__ == "__main__":
    stream_reddit_comments('politics') 