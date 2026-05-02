import os
import time
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env into environment variables
load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_DEFAULT_REGION")

if not bucket:
    raise ValueError("S3_BUCKET_NAME is missing in .env")

s3 = boto3.client("s3", region_name=region)

def download_file(local_path: str, s3_key: str = None):
    if not s3_key:
        s3_key = os.path.basename(local_path)

    # Ensure local directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        s3.download_file(bucket, s3_key, local_path)
    except ClientError as e:
        raise RuntimeError(f"Download failed: {e}")
    
    size = os.path.getsize(local_path)

    return size

def get_prefix(dataset_id, version=None):
    if version is None:
        return f"bench/{dataset_id}/"
    return f"staging/{dataset_id}/{version}/"

def list_objects(prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys

def download_dataset(dataset_id: str, local_dir: str, version: str = None):
    """
    Download full dataset version from staging/<dataset_id>/<version>/.
    """
    prefix = get_prefix(dataset_id, version)
    keys = list_objects(prefix)

    total_bytes = 0
    file_count = 0
    start = time.time()

    for s3_key in keys:
        relative_path = s3_key.replace(prefix, "")
        local_path = os.path.join(local_dir, relative_path)

        size = download_file(local_path, s3_key)

        total_bytes += size
        file_count += 1

    elapsed = time.time() - start
    total_mb = total_bytes / (1024 * 1024)
    throughput = (total_bytes / total_mb) / elapsed if elapsed > 0 else 0

    print(f"Downloaded {get_prefix(dataset_id, version)}")
    print(f"Total: {total_bytes/1e6:.2f} MB in {elapsed:.2f}s")
    print(f"Throughput: {throughput:.2f} MB/s")

    return {
        "total_bytes": total_bytes,
        "total_mb": total_mb,
        "elapsed_s": elapsed,
        "throughput_mb_s": throughput,
        "file_count": file_count
    }