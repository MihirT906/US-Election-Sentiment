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

# Initialize Reddit API with PRAW
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

def is_within_date_range(post_time, start_date, end_date):
    return start_date <= post_time <= end_date

# Function to stream Reddit posts and comments to Kafka
def stream_reddit_posts_and_comments(subreddit_name, candidates, policy_keywords, start_date, end_date):
    subreddit = reddit.subreddit(subreddit_name)
    posts_checked = 0
    posts_fetched = 0
    comments_processed = 0  # Track total comments processed

    for submission in subreddit.new(limit=None):
        posts_checked += 1  # Increment the total number of posts checked
        post_time = datetime.fromtimestamp(submission.created_utc)

        # Check if the post falls within the date range
        if not is_within_date_range(post_time, start_date, end_date):
            continue

        # Check if any candidate's name is in the submission title
        if any(candidate in submission.title.lower() for candidate in candidates):
            posts_fetched += 1
            month_key = post_time.strftime('%Y-%m')
            candidate_key = None

            for candidate in candidates:
                if candidate in submission.title.lower():
                    candidate_key = candidate.lower()
                    # Print only the candidate and the date of the post
                    print(f"{candidate_key}: {post_time.strftime('%Y-%m-%d')}")

                    # Send post data to Kafka
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
                    producer.send(
                        f'reddit_posts_{candidate_key}', key=bytes(candidate_key, encoding='utf-8'), value=post_data
                    )

                    # Fetch and process comments
                    submission.comments.replace_more(limit=None)  # Expand all comments
                    for comment in submission.comments.list():
                        comments_processed += 1  # Increment total comments processed
                        comment_data = {
                            'post_id': submission.id,
                            'comment_id': comment.id,
                            'comment_body': comment.body,
                            'comment_author': str(comment.author),
                            'comment_score': comment.score,
                            'comment_created_utc': comment.created_utc
                        }
                        # Send comment data to Kafka
                        producer.send(
                            f'reddit_comments_{candidate_key}', key=bytes(candidate_key, encoding='utf-8'), value=comment_data
                        )

    return posts_checked, posts_fetched, comments_processed

if __name__ == "__main__":
    candidates = ["harris", "trump"]
    policy_keywords = ["economy", "healthcare", "education", "tax"]
    start_date = datetime(2024, 8, 1)  # Start date in the format (year, month, day)
    end_date = datetime(2024, 11, 5)  # End date in the format (year, month, day)
    start_time = time.time()
    posts_checked, posts_fetched, comments_processed = stream_reddit_posts_and_comments('USpolitics', candidates, policy_keywords, start_date, end_date)
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # Convert to milliseconds
    print(f"\nAPI Request Latency: {latency:.2f} ms")
    print(f"Throughput: {posts_fetched / latency:.2f} posts/ms")
    print(f"Comments processed throughput: {comments_processed / latency:.2f} comments/ms")
