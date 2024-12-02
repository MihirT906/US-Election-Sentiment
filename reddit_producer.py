import praw
import json
from kafka import KafkaProducer
import configparser
import time
from datetime import datetime
#Mansi
from textblob import TextBlob  # Install: pip install textblob

# Load Reddit API credentials from config file
config = configparser.ConfigParser()
config.read('credentials.cfg')


client_id = config['DEFAULT']['CLIENT_ID']
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

#Mansi
def get_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity  # Returns a score between -1 and 1

def is_within_date_range(post_time, start_date, end_date):
    return start_date <= post_time <= end_date

# Function to stream Reddit comments to Kafka
def stream_reddit_comments(subreddit_name, candidates, policy_keywords, start_date, end_date):
    subreddit = reddit.subreddit(subreddit_name)
    posts_fetched = 0
    # for submission in subreddit.stream.submissions(skip_existing=False):
    for submission in subreddit.new(limit=None):
        posts_fetched += 1
        post_time = datetime.fromtimestamp(submission.created_utc)
        # Check if the post falls within the date range
        if not is_within_date_range(post_time, start_date, end_date):
            continue
        
        #check if keyword in submission title
        if any(candidate in submission.title.lower() for candidate in candidates):
            month_key = None
            candidate_key = None
            policy_key = None
            
            post_time = datetime.fromtimestamp(submission.created_utc)
            month_key = post_time.strftime('%Y-%m')
            
            for candidate in candidates:
                if candidate in submission.title.lower():
                    candidate_key = candidate.lower()
                    post_data = {
                        'id': submission.id,
                        'title': submission.title,
                        'selftext': submission.selftext, #M
                        'timestamp': post_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'created_utc': submission.created_utc,
                        'author': str(submission.author),
                        'score': submission.score,
                        'num_comments': submission.num_comments,
                        'selftext': submission.selftext,
                        'month_key': month_key,
                        'candidate_key': candidate_key,
                        'sentiment': get_sentiment(submission.title + " " + submission.selftext) #M
                    }
                    print(f'{candidate_key}: {post_time}')
                    producer.send(
                        f'reddit_posts_{candidate_key}', key=bytes(candidate_key, encoding='utf-8'), value=post_data
                    )
                    #producer.flush()
    
    return posts_fetched

if __name__ == "__main__":
    candidates = ["harris", "trump"]
    policy_keywords = ["economy", "healthcare", "education", "tax"]
    #policy_keywords = ["economy"]
    start_date = datetime(2024, 8, 1)  # Start date in the format (year, month, day)
    end_date = datetime(2024, 11, 5)  # End date in the format (year, month, day)
    start_time = time.time()
    posts_fetched = stream_reddit_comments('USpolitics', candidates, policy_keywords, start_date, end_date) 
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # convert to milliseconds
    print(f"API Request Latency: {latency:.2f} ms")
    print(f"Throughput: {posts_fetched / latency:.2f} posts/ms")