import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
import os

# Configuration
BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
BUCKET_NAME = os.environ["BUCKET_NAME"]
S3_PREFIX = os.environ.get("S3_PREFIX", "")  # Set to 'bls/' via CloudFormation
CONTACT_INFO = "Brandon Young (brandon@jsbsolutions.io)"
USER_AGENT = f"DataSyncBot/1.0 ({CONTACT_INFO})"
HEADERS = {"User-Agent": USER_AGENT}

def main():
    s3 = boto3.client("s3")

    # Load existing metadata (from S3)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=S3_PREFIX + "metadata.json")
        old_metadata = {item["file_name"]: item for item in json.load(obj["Body"])}
    except s3.exceptions.NoSuchKey:
        old_metadata = {}

    # Scrape live directory
    response = requests.get(BASE_URL, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    pre_block = soup.find("pre")
    if not pre_block:
        raise RuntimeError("No <pre> block found on the page.")

    new_metadata = {}
    for a_tag in pre_block.find_all("a"):
        href = a_tag.get("href")
        if not href or "Parent Directory" in href or href.endswith("/"):
            continue

        file_name = os.path.basename(href)
        if not file_name.strip():
            continue

        file_url = BASE_URL + file_name
        s3_key = S3_PREFIX + "files/" + file_name  # bls/files/<file>

        sibling = a_tag.find_next_sibling(string=True)
        date_str, time_str, file_size, timestamp = "", "", None, None
        if sibling:
            match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)\s+([\d,]+)", sibling.strip())
            if match:
                date_str, time_str, size_str = match.groups()
                file_size = int(size_str.replace(",", ""))
                dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %I:%M %p")
                timestamp = dt.isoformat()

        existing = old_metadata.get(file_name)
        should_upload = (
            existing is None or
            existing.get("last_updated_timestamp") != timestamp
        )

        if should_upload:
            print(f"Uploading: {file_name}")
            file_response = requests.get(file_url, headers=HEADERS)
            if file_response.status_code == 200:
                s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=file_response.content)
            else:
                print(f"Failed to download {file_name}")
                continue

        new_metadata[file_name] = {
            "file_name": file_name,
            "url": file_url,
            "last_updated_date": date_str,
            "last_updated_time": time_str,
            "last_updated_timestamp": timestamp,
            "file_size_bytes": file_size
        }

    # Delete removed files from S3
    deleted_files = set(old_metadata) - set(new_metadata)
    for file_name in deleted_files:
        s3_key = S3_PREFIX + "files/" + file_name
        print(f"Deleting removed file from S3: {file_name}")
        s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)

    # Upload updated metadata.json
    metadata_json = json.dumps(list(new_metadata.values()), indent=2)
    s3.put_object(Bucket=BUCKET_NAME, Key=S3_PREFIX + "metadata.json", Body=metadata_json.encode("utf-8"))
    print(f"Sync complete. {len(new_metadata)} files now in S3.")
    return len(new_metadata)

def lambda_handler(event, context):
    try:
        file_count = main()
        return {
            "statusCode": 200,
            "body": f"Success. {file_count} files now in S3."
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }