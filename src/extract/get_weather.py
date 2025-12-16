import json
import requests
import boto3
from datetime import datetime, timezone
import logging
import os
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger()

# Define base directory and paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(BASE_DIR, "config", "config.json")
CITIES_FILE = os.path.join(BASE_DIR, "config", "cities.json")

with open(CONFIG_FILE, "r") as f:
    CONFIG = json.load(f)

S3_BUCKET = CONFIG["s3_bucket"]
S3_PREFIX = CONFIG["bronze_prefix"]
API_BASE = CONFIG["api_base"]
PARALLEL_WORKERS = CONFIG["parallel_workers"]
RETRY_ATTEMPTS = CONFIG["retry_attempts"]
TIMEOUT_SECONDS = CONFIG["timeout_seconds"]
HOURLY_FIELDS = CONFIG["hourly_parameters"]

s3 = boto3.client("s3")

# Load city configuration
def load_cities():
    with open(CITIES_FILE, "r") as f:
        return json.load(f)


# Fetch data with retry logic
def fetch_with_retry(url, city):
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        except RequestException as e:
            log.warning(f"[CITY={city}] Attempt {attempt}/{RETRY_ATTEMPTS} failed: {e}")

            if attempt == RETRY_ATTEMPTS:
                log.error(f"[CITY={city}] Max retries reached — skipping.")
                raise
    return None


# Build the API URL for a given latitude and longitude
def build_url(lat, lon):
    hourly_str = ",".join(HOURLY_FIELDS)
    return (
        f"{API_BASE}"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly={hourly_str}"
        f"&timezone={CONFIG['timezone']}"
    )

def s3_object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# Upload data to S3
def upload_to_s3(data, city, ts):
    key = (
        f"{S3_PREFIX}/city={city}/year={ts:%Y}/month={ts:%m}/"
        f"day={ts:%d}/hour={ts:%H}/data.json"
    )

    tag_city = quote(city, safe='')
    tag_timestamp = quote(ts.isoformat(), safe='')

    if s3_object_exists(S3_BUCKET, key):
        log.info(f"[CITY={city}] Data already exists, skipping → {key}")
        return

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
        Tagging=f"city={tag_city}&timestamp={tag_timestamp}"
    )

    log.info(f"[CITY={city}] Uploaded to s3://{S3_BUCKET}/{key}")

REQUIRED_KEYS = ["latitude", "longitude", "hourly"]

# Validate API response
def validate_response(data, city):
    if not all(k in data for k in REQUIRED_KEYS):
        log.error(f"[CITY={city}] Missing required keys in API response.")
        return False
    return True


# Fetch weather data and upload to S3
def fetch_and_upload(city_info):
    city = city_info["city"]
    lat = city_info["latitude"]
    lon = city_info["longitude"]

    url = build_url(lat, lon)
    log.info(f"Fetching weather data for {city}: {url}")

    response = requests.get(url)

    if response.status_code != 200:
        log.error(f"Failed for {city}, status_code={response.status_code}")
        return

    data = fetch_with_retry(url, city)
    if not validate_response(data, city):
        return

    ts = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    upload_to_s3(data, city, ts)


# Main function to process all cities
def main():
    cities = load_cities()

    log.info(f"Processing {len(cities)} cities...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_and_upload, city): city for city in cities}

        for fut in as_completed(futures):
            city = futures[fut]["city"]
            try:
                fut.result()
                log.info(f"[CITY={city}] Completed successfully.")
            except Exception as e:
                log.error(f"[CITY={city}] Failed: {e}")


if __name__ == "__main__":
    main()
