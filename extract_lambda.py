import boto3
import pandas as pd
import json
from datetime import datetime, timezone

# --- Configuration (in real Lambda, these come from environment variables) ---
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
RAW_BUCKET = "spotify-raw-data"
SOURCE_CSV = "data/dataset.csv"


def lambda_handler(event, context=None):
    """
    Simulates an AWS Lambda 'extraction' function.
    In production, this would call an external API (like Spotify's).
    Here, it reads the Kaggle dataset and 'extracts' it into the raw zone,
    partitioned by an ingestion timestamp — the same pattern real pipelines use
    so you can track when each batch of data arrived.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Read the source data
    df = pd.read_csv(SOURCE_CSV)
    record_count = len(df)

    # Build a timestamped key — mirrors how real pipelines partition raw data by ingestion date
    ingestion_time = datetime.now(timezone.utc)
    date_prefix = ingestion_time.strftime("%Y/%m/%d")
    object_key = f"raw/{date_prefix}/spotify_tracks_{ingestion_time.strftime('%H%M%S')}.csv"

    # Upload to the raw bucket
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=RAW_BUCKET, Key=object_key, Body=csv_bytes)

    result = {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Extraction complete",
            "records_extracted": record_count,
            "destination_bucket": RAW_BUCKET,
            "destination_key": object_key,
            "ingestion_timestamp": ingestion_time.isoformat(),
        }),
    }
    print(result["body"])
    return result


if __name__ == "__main__":
    # This simulates Lambda invoking the handler with an empty event
    lambda_handler(event={}, context=None)