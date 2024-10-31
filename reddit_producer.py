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
def stream_reddit_comments(subreddit_name, keywords):
    subreddit = reddit.subreddit(subreddit_name)

    for submission in subreddit.stream.submissions(skip_existing=False):
        #check if keyword in submission title
        if any(keyword in submission.title for keyword in keywords):
            post_data = {
                'id': submission.id,
                'title': submission.title,
                'created_utc': submission.created_utc,
                'author': str(submission.author),
                'score': submission.score,
                'num_comments': submission.num_comments,
                'selftext': submission.selftext
            }
            print(f'Sending comment to Kafka: {submission.title[:30]}...')
            producer.send('redditcomments', value=post_data)
            time.sleep(4)


if __name__ == "__main__":
    keywords = ["Harris", "Trump"]
    stream_reddit_comments('USpolitics', keywords) 