import os
import json
import boto3
import requests
import botocore.exceptions

# Config
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
BUCKET_NAME = os.environ["BUCKET_NAME"]
S3_KEY = "datausa/population.json"

def main():
    s3 = boto3.client("s3")

    # Fetch data from DataUSA
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        json_data = response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch population data: {e}")

    # Upload to S3
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
            Body=json.dumps(json_data, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Failed to upload to S3: {e}")

    print(f"Success: Saved population data to s3://{BUCKET_NAME}/{S3_KEY}")
    return True

def lambda_handler(event, context):
    try:
        main()
        return {
            "statusCode": 200,
            "body": "DataUSA population data synced successfully."
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }