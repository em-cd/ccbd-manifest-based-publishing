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

def upload(file_path: str, s3_key: str = None):
    """
    Upload a file to S3.

    :param file_path: Local path where the file is stored
    :param s3_key: S3 object key (defaults to basename of file_path)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")

    if not s3_key:
        s3_key = os.path.basename(file_path)

    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"Uploaded to s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"Upload failed: {e}")