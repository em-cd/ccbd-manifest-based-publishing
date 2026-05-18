import os
import json
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env into environment variables
load_dotenv()

class S3Backend:
  def __init__(self):
    self.bucket = os.getenv("S3_BUCKET_NAME")
    self.region = os.getenv("AWS_DEFAULT_REGION")

    if not self.bucket:
      raise ValueError("S3_BUCKET_NAME is missing in .env")

    self.s3 = boto3.client("s3", region_name=self.region)

  def filesystem(self):
    import pyarrow.fs as fs
    return fs.S3FileSystem()

  def get_root(self):
    return self.bucket

  def list_objects(self, prefix):
    """
    List objects and page count
    """
    keys = []
    page_count = 0

    paginator = self.s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
      page_count += 1
      for obj in page.get("Contents", []):
        keys.append(obj["Key"])

    return keys, page_count

  def download_file(self, local_path: str, key: str = None):
    """
    Download a single file from S3 object store
    """
    if not key:
      key = os.path.basename(local_path)

    # Ensure local directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
      self.s3.download_file(self.bucket, key, local_path)
    except ClientError as e:
      raise RuntimeError(f"Download failed: {e}")
    
    return os.path.getsize(local_path)
 
  def upload_file(self, file_path: str, key: str = None):
    """
    Upload a single file to an S3 bucket
    """
    if not os.path.exists(file_path):
      raise FileNotFoundError(f"{file_path} does not exist")

    if not key:
      key = os.path.basename(file_path)

    try:
      self.s3.upload_file(file_path, self.bucket, key)
    except ClientError as e:
      raise RuntimeError(f"Upload failed: {e}")

    return os.path.getsize(file_path)

  def write_json(self, key: str, data: dict):
    """
    Write a Python dict as JSON to AWS S3.
    """
    self.s3.put_object(
        Bucket=self.bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8")
    )

  def read_json(self, key: str):
    """
    Read JSON blob and return as dict. Returns None if not found.
    """
    try:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())
    except self.s3.exceptions.NoSuchKey:
        return None

  def delete_prefix(self, prefix: str):
    """
    Delete all objects under a prefix.
    """
    keys, _ = self.list_objects(prefix)

    if not keys:
        print(f"No objects found under {prefix}")
        return 0

    deleted = 0

    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i+1000]]

        self.s3.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": batch}
        )

        deleted += len(batch)

    print(f"Deleted {deleted} objects under {prefix}")
    return deleted