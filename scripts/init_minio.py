import os
import boto3
from botocore.exceptions import ClientError


endpoint = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
access_key = os.getenv("S3_ACCESS_KEY", "taxiadmin")
secret_key = os.getenv("S3_SECRET_KEY", "taxiadmin-secret")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
)

buckets = ["bronze", "silver", "gold"]

for bucket in buckets:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket déjà présent : {bucket}")

    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket créé : {bucket}")

print("Buckets RustFS prêts.")