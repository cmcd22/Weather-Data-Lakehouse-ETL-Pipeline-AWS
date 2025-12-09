import boto3
import json

s3 = boto3.client("s3")
bucket = "cmcd-etl-weather-lake"  # <-- replace with your bucket name

payload = {"test": "ok"}

s3.put_object(
    Bucket=bucket,
    Key="bronze/raw/test/test_file.json",
    Body=json.dumps(payload),
    ContentType="application/json"
)

print("Upload successful!")
