import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import boto3
import json

# --- Configurations ---
AWS_ACCESS_KEY = "" # replace with your access key
AWS_SECRET_KEY = "" # replace with your secret key
AWS_REGION = "us-east-1"

S3_BUCKET = "youtube-live-streams-bucket"
S3_PREFIX = "live_data"
REDSHIFT_IAM_ROLE = "arn:aws:iam::122610497416:role/service-role/AmazonRedshift-CommandsAccessRole-20250522T185327"

YOUTUBE_API_KEY = '' ##replace with your youtube API key

# --- Spark Setup ---
spark = SparkSession.builder.appName("YouTubeLiveStreamETL").getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# --- Step 1: Get YouTube Live Streams ---
url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&eventType=live&type=video&relevanceLanguage=en&hl=en&key={YOUTUBE_API_KEY}'
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"Failed to fetch YouTube data: {response.status_code}")

streams = []
for item in response.json().get('items', []):
    video_id = item['id']['videoId']
    snippet = item['snippet']
    title = snippet['title']
    channel_title = snippet['channelTitle']
    published_at = snippet['publishedAt']
    stream_url = f"https://www.youtube.com/watch?v={video_id}"

    # Optional: Get video stats
    stats_url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    stats_resp = requests.get(stats_url).json()
    stats = stats_resp['items'][0].get('statistics', {}) if stats_resp['items'] else {}

    streams.append({
        "video_id": video_id,
        "title": title,
        "channel_title": channel_title,
        "stream_url": stream_url,
        "published_at": published_at,
        "view_count": int(stats.get("viewCount", 0)),
        "like_count": int(stats.get("likeCount", 0)),
        "comment_count": int(stats.get("commentCount", 0)),
        "fetched_at": datetime.utcnow().isoformat()
    })

# --- Step 2: Save as Parquet ---
df = pd.DataFrame(streams)
sdf = spark.createDataFrame(df)

timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
s3_folder = f"{S3_PREFIX}/batch_{timestamp}/"
parquet_key = f"{s3_folder}data.parquet"
s3_path = f"s3a://{S3_BUCKET}/{parquet_key}"
sdf.coalesce(1).write.mode("overwrite").parquet(s3_path)

# --- Step 3: Generate Manifest File ---
s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Find actual parquet file
objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_folder)
parquet_obj = next((o for o in objects.get("Contents", []) if o["Key"].endswith(".parquet")), None)
if not parquet_obj:
    raise Exception("Parquet file not found")

manifest = {
    "entries": [
        {
            "url": f"s3://{S3_BUCKET}/{parquet_obj['Key']}",
            "mandatory": True,
            "meta": {"content_length": parquet_obj['Size']}
        }
    ]
}
manifest_key = f"{s3_folder}manifest.json"
s3_client.put_object(Bucket=S3_BUCKET, Key=manifest_key, Body=json.dumps(manifest), ContentType="application/json")

# --- Step 4: Create COPY command and save as .txt ---
copy_sql = f"""
COPY youtube_live_streams
FROM 's3://{S3_BUCKET}/{manifest_key}'
IAM_ROLE '{REDSHIFT_IAM_ROLE}'
FORMAT AS PARQUET
MANIFEST;
"""

copy_txt_key = f"{s3_folder}copy_command.txt"
s3_client.put_object(Bucket=S3_BUCKET, Key=copy_txt_key, Body=copy_sql, ContentType="text/plain")

print("✅ All steps completed. Data, manifest, and COPY command saved to S3.")

# --- Step 5: Publish to SNS to Trigger Lambda ---
sns_client = boto3.client("sns", region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:122610497416:TriggerRedshiftLoader"  #Replace with your actual SNS ARN

# Build message for Lambda
sns_message = {
    "copy_command": f"s3://{S3_BUCKET}/{copy_txt_key}"
}

response = sns_client.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="YouTube Data Ready: Trigger Redshift Load",
    Message=json.dumps(sns_message)
)

print("📩 SNS notification sent to trigger Lambda:", response)
