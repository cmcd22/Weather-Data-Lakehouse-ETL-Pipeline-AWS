import json
import requests
import boto3
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

S3_BUCKET = "cmcd-etl-weather-lake"
S3_PREFIX = "bronze/raw"

s3 = boto3.client("s3")

def load_cities():
    with open("config/cities.json") as f:
        return json.load(f)

def build_url(lat, lon):
    return (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,relativehumidity_2m,precipitation,wind_speed_10m"
        "&timezone=UTC"
    )

def upload_raw_to_s3(data, city, ts):
    key = f"{S3_PREFIX}/{city}/{ts:%Y/%m/%d/%H}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))
    logging.info(f"Uploaded: s3://{S3_BUCKET}/{key}")

def fetch_city_weather(city_info):
    city, lat, lon = city_info["city"], city_info["lat"], city_info["lon"]
    url = build_url(lat, lon)
    logging.info(f"Requesting: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"API failed for {city}")
        return

    data = response.json()
    ts = datetime.utcnow()
    upload_raw_to_s3(data, city, ts)

def main():
    for c in load_cities():
        fetch_city_weather(c)

if __name__ == "__main__":
    main()
