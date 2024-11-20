import praw
import configparser
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

# Define subreddit and time range
subreddit = reddit.subreddit('USpolitics')
start_date = datetime(2024, 10, 1).timestamp()  # Start of the range (01-10-2024)
end_date = datetime(2024, 11, 15).timestamp()  # End of the range (15-11-2024)

# Fetch posts
posts = []
last_post_time = None  # To track the timestamp of the last fetched post

# Loop until all relevant posts are fetched
while True:
    # Query the subreddit
    if last_post_time:
        # Use the 'before' parameter to get posts before the last timestamp
        submission_generator = subreddit.new(limit=1000, params={'before': last_post_time})
    else:
        # First request (no 'before' parameter)
        submission_generator = subreddit.new(limit=1000)

    # Loop through the posts and filter by date range
    for submission in submission_generator:
        if submission.created_utc < start_date:
            # Stop if the posts are outside the desired date range
            break
        if start_date <= submission.created_utc <= end_date:
            posts.append(submission)
        
        # Update the 'before' timestamp
        last_post_time = submission.created_utc

    # Break the loop if no posts are found or the start date is reached
    if not posts or submission.created_utc < start_date:
        break

# Print out the extracted posts (or save to a file)
for post in posts:
    print(f"Title: {post.title}, Date: {datetime.utcfromtimestamp(post.created_utc)}")
