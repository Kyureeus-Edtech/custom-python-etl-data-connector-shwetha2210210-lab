import requests
import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

def extract_data(url, user_agent, retries=3, delay=10):
    """
    Fetch raw ASCII IP data from DShield endpoint with retry and rate limit handling.
    - Retries on network errors or HTTP 429 (Too Many Requests).
    - Respects Retry-After header if present.
    """
    headers = {'User-Agent': user_agent}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay))
                print(f"[Rate Limit] Sleeping for {retry_after} seconds before retry...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            return response.text

        except requests.exceptions.RequestException as e:
            print(f"[Error] Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                print(f"[Retry] Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Error fetching data after {retries} attempts: {e}")

def transform_data(raw_text):
    """
    Transform ASCII text from DShield into a list of structured dicts.
    Each dict contains IP, counts, date range, and ingestion timestamp.
    """
    lines = raw_text.strip().splitlines()
    records = []

    for line in lines:
        # Skip comments or empty lines
        if line.startswith('#') or not line.strip():
            continue

        parts = line.split()
        if len(parts) != 5:
            continue

        ip, count1, count2, start_date, end_date = parts
        try:
            records.append({
                "ip": ip,
                "count1": int(count1),
                "count2": int(count2),
                "start_date": start_date,
                "end_date": end_date,
                "_ingested_at": datetime.utcnow()
            })
        except ValueError:
            continue

    return records

def load_to_mongo(records, mongo_uri, db_name, collection_name):
    """
    Load transformed records into MongoDB.
    """
    if not records:
        print("[Load] No records to insert.")
        return

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        coll = db[collection_name]
        coll.insert_many(records)
        print(f"[Load] Inserted {len(records)} records into {db_name}.{collection_name}")
    except Exception as e:
        raise RuntimeError(f"Error inserting to MongoDB: {e}")

def main():
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME", "dshield")
    collection_name = os.getenv("COLLECTION_NAME", "ipsascii_raw")
    user_agent = os.getenv("USER_AGENT")

    if not mongo_uri or not user_agent:
        raise ValueError("MONGO_URI and USER_AGENT must be set in .env")

    url = "https://www.dshield.org/ipsascii.html"

    print("[1/3] Extracting data...")
    raw_data = extract_data(url, user_agent)

    print("[2/3] Transforming data...")
    records = transform_data(raw_data)

    print("[3/3] Loading data to MongoDB...")
    load_to_mongo(records, mongo_uri, db_name, collection_name)

    print("[Done] ETL process completed successfully.")

if __name__ == "__main__":
    main()
