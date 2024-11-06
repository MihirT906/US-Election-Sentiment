import praw
import json
from kafka import KafkaProducer
import configparser
import time
from datetime import datetime

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
def stream_reddit_comments(subreddit_name, candidates, policy_keywords):
    subreddit = reddit.subreddit(subreddit_name)

    for submission in subreddit.stream.submissions(skip_existing=False):
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
                        'timestamp': post_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'created_utc': submission.created_utc,
                        'author': str(submission.author),
                        'score': submission.score,
                        'num_comments': submission.num_comments,
                        'selftext': submission.selftext,
                        'month_key': month_key,
                        'candidate_key': candidate_key
                    }
                    print(f'{candidate_key}: {submission.title[:30]}...')
                    producer.send(
                        f'reddit_posts_{candidate_key}', key=bytes(candidate_key, encoding='utf-8'), value=post_data
                    )
            time.sleep(1)
            # for keyword in policy_keywords:
            #     if keyword in submission.title.lower():
            #         policy_key = keyword.lower()
            #         break
                
            
            # producer.send(
            #     'reddit_posts_raw', key=bytes(month_key, encoding='utf-8'), value=post_data
            # )
            # print(f'Sending comment to raw: {submission.title[:30]}...')
            
            # if policy_key:
            #     producer.send(
            #         'reddit_policy', key=bytes(policy_key, encoding='utf-8'), value=post_data
            #     )
            #     #print(f'Sending comment to policy: {submission.title[:30]}...')
            
if __name__ == "__main__":
    candidates = ["harris", "trump"]
    policy_keywords = ["economy", "healthcare", "education", "tax"]
    #policy_keywords = ["economy"]
    stream_reddit_comments('USpolitics', candidates, policy_keywords) 