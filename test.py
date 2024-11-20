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
start_date = datetime(2024, 10, 25).timestamp()  # Start of the range
end_date = datetime(2024, 11, 15).timestamp()  # End of the range

# Fetch posts
posts = []
total_posts = 0  # Counter for total posts processed
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
        total_posts += 1  # Increment total posts counter
        if submission.created_utc < start_date:
            # Stop if the posts are outside the desired date range
            break
        if start_date <= submission.created_utc <= end_date:
            # Check if the title contains "harris" or "trump" in any case
            title_lower = submission.title.lower()
            if "harris" in title_lower or "trump" in title_lower:
                posts.append(submission)
        
        # Update the 'before' timestamp
        last_post_time = submission.created_utc

    # Break the loop if no more posts or the start date is reached
    if not posts or submission.created_utc < start_date:
        break

# Print out the extracted posts
for post in posts:
    print(f"Title: {post.title}, Date: {datetime.utcfromtimestamp(post.created_utc)}")

# Print total number of posts processed
print(f"\nTotal posts processed: {total_posts}")
