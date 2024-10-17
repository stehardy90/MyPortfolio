import praw
import time
import json
import os
from datetime import datetime
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from .env file
load_dotenv()

# Retry configuration
max_retries = 10
retry_delay = 5  

# Function to initialize Kafka Producer with retry logic
def create_kafka_producer():
    retries = 0
    while retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka")
            return producer  
        except NoBrokersAvailable:
            retries += 1
            print(f"Kafka broker not available, retrying {retries}/{max_retries}...")
            time.sleep(retry_delay)
    raise Exception("Failed to connect to Kafka after multiple retries.")

# Initialise Kafka Producer
producer = create_kafka_producer()

# Initialise Reddit API with PRAW
reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    user_agent=os.getenv('USER_AGENT'),
)

# Stream comments from a subreddit
subreddit = reddit.subreddit("news")
for comment in subreddit.stream.comments(skip_existing=True):
    parent_submission = comment.submission

    # Structure the comment data
    structured_output = {
        "comment": {
            "text": comment.body,
            "author": str(comment.author),
            "posted_at": datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
            "permalink": comment.permalink
        },
        "parent_post": {
            "title": parent_submission.title,
            "author": str(parent_submission.author),
            "subreddit": str(parent_submission.subreddit),
            "posted_at": datetime.utcfromtimestamp(parent_submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        }
    }

    # Send data to Kafka
    producer.send('reddit-comments', value=structured_output)
