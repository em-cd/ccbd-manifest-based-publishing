# download_from_s3.py
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION"))
bucket = os.getenv("S3_BUCKET_NAME")

def download_dataset(size):
    prefix = f"curated/{size}"
    local_dir = f"data/{size}"
    os.makedirs(local_dir, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            local_path = os.path.join(local_dir, filename)
            
            if os.path.exists(local_path):
                print(f"  ⏭️  {filename} already exists, skipping")
                continue
                
            print(f"  📥 Downloading {key}...")
            s3.download_file(bucket, key, local_path)
            size_mb = os.path.getsize(local_path) / 1e6
            print(f"     Done ({size_mb:.1f} MB)")

    print(f"\n✅ {size} dataset downloaded to {local_dir}/")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], required=True)
    args = parser.parse_args()
    download_dataset(args.size)