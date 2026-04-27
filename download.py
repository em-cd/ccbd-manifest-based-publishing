import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env into environment variables
load_dotenv()

bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_DEFAULT_REGION")

if not bucket:
    raise ValueError("S3_BUCKET_NAME is missing in .env")

# Create S3 client
s3 = boto3.client(
    "s3",
    region_name=region
)

def download(file_path: str, s3_key: str = None):
    """
    Download a file from S3.

    :param file_path: Local path where the file will be saved
    :param s3_key: S3 object key (defaults to basename of file_path)
    """
    if not s3_key:
        s3_key = os.path.basename(file_path)

    # Ensure local directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        s3.download_file(bucket, s3_key, file_path)
        print(f"Downloaded from s3://{bucket}/{s3_key} to {file_path}")
    except ClientError as e:
        print(f"Download failed: {e}")