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

def upload_file(file_path: str, s3_key: str = None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")

    if not s3_key:
        s3_key = os.path.basename(file_path)

    try:
        s3.upload_file(file_path, bucket, s3_key)
    except ClientError as e:
        raise RuntimeError(f"Upload failed: {e}")

    size = os.path.getsize(file_path)

    return size

def get_prefix(dataset_id, version=None):
    if version is None:
        return f"bench/{dataset_id}/"
    return f"staging/{dataset_id}/{version}/"

def list_files(local_dir, prefix):
    files = []
    keys = []

    for root, _, local_files in os.walk(local_dir):
        for f in local_files:
            local_path = os.path.join(root, f)
            rel_path = os.path.relpath(local_path, local_dir)

            s3_key = prefix + rel_path

            files.append(local_path)
            keys.append(s3_key)

    return files, keys

def upload_dataset(dataset_id: str, local_dir: str, version: str = None):
    """
    Upload a full Parquet dataset folder to S3 staging area. If version is not
    provided, uploads to bench area.
    """
    prefix = get_prefix(dataset_id, version)
    files, keys = list_files(local_dir, prefix)

    total_bytes = 0
    file_count = 0
    start = time.time()

    for local_path, s3_key in zip(files, keys):
        size = upload_file(local_path, s3_key)

        total_bytes += size
        file_count += 1

    elapsed = time.time() - start
    total_mb = total_bytes / (1024 * 1024)
    throughput = (total_bytes / total_mb) / elapsed if elapsed > 0 else 0


    print(f"Uploaded {get_prefix(dataset_id, version)}")
    print(f"Total: {total_bytes/1e6:.2f} MB in {elapsed:.2f}s")
    print(f"Throughput: {throughput:.2f} MB/s")

    return {
        "total_bytes": total_bytes,
        "total_mb": total_mb,
        "elapsed_s": elapsed,
        "throughput_mb_s": throughput,
        "file_count": file_count
    }