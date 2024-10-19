import spacy
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, ArrayType
from transformers import pipeline

# Initialise Spark session
spark = SparkSession.builder \
    .appName("RedditCommentProcessor") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Define a UDF to generate UUIDs
def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

# Define schema for Reddit comments
schema = StructType() \
    .add("comment", StructType()
        .add("text", StringType())
        .add("author", StringType())
        .add("posted_at", StringType())
        .add("permalink", StringType())
    ) \
    .add("parent_post", StructType()
        .add("title", StringType())
        .add("author", StringType())
        .add("subreddit", StringType())
        .add("posted_at", StringType())
    )

# Load spacy NER model
nlp = spacy.load("en_core_web_sm")

# Load the Hugging Face emotion classification model
emotion_classifier = pipeline('text-classification', model='j-hartmann/emotion-english-distilroberta-base', return_all_scores=False)

# Define a UDF to extract named entities using spacy
def extract_entities(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ in {"PERSON", "GPE", "ORG"}]

extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

# Define a UDF for emotion analysis using the Hugging Face model
def analyze_emotion(text):
    result = emotion_classifier(text)
    # Return the label of the most likely emotion
    return result[0]['label']

emotion_udf = udf(analyze_emotion, StringType())

# Read from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit-comments") \
    .load()

# Convert Kafka value (which is a string) to a JSON object
comments_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.comment.text").alias("text"),
        col("data.comment.author").alias("comment_author"),
        col("data.comment.posted_at").alias("comment_posted_at"),
        col("data.comment.permalink").alias("permalink"),
        col("data.parent_post.title").alias("title"),
        col("data.parent_post.author").alias("post_author"),
        col("data.parent_post.subreddit").alias("subreddit"),
        col("data.parent_post.posted_at").alias("post_posted_at")
    )

# Apply spacy NER UDF to extract keywords from the parent post title
keywordData = comments_df.withColumn("keywords", extract_entities_udf(col("title")))

# Apply Hugging Face emotion analysis to the comment text
emotionData = keywordData.withColumn("emotion", emotion_udf(col("text")))

# Add the UUID column to your DataFrame before writing to Cassandra
emotionData = emotionData.withColumn("id", uuid_udf())

# Write the processed data to Cassandra in a streaming manner
query = emotionData.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .option("table", "reddit_comments") \
    .option("keyspace", "reddit_data") \
    .outputMode("append") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
