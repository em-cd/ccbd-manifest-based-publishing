# test_s3.py
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION"))
bucket = os.getenv("S3_BUCKET_NAME")

print(f"🪣 Bucket: {bucket}\n")

# List everything in the bucket
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=bucket):
    for obj in page.get("Contents", []):
        size_mb = obj["Size"] / 1e6
        print(f"  📄 {obj['Key']}  ({size_mb:.1f} MB)")