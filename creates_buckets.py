import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
)

buckets = ["spotify-raw-data", "spotify-transformed-data"]

for bucket_name in buckets:
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket already exists: {bucket_name}")
        else:
            raise

print("\nCurrent buckets:")
for b in s3.list_buckets()["Buckets"]:
    print(f" - {b['Name']}")