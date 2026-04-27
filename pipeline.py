import os
import argparse
import boto3
from dataset_gen import generate_dataset
from upload import upload

bucket = os.getenv("S3_BUCKET_NAME")
if not bucket:
    raise ValueError("S3_BUCKET_NAME environment variable is not set")

s3 = boto3.client("s3")
zone = "curated"

# TODO: We only check that the dataset folder is there, we should probably
# check that it's complete, e.g. by checking that the correct files are there
# and the amount of data is correct
def dataset_exists(prefix):
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )
    return "Contents" in resp

def main(size):
    if size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [size]

    for s in sizes:
        prefix = f"{zone}/{s}"

        if dataset_exists(prefix):
            print(f"[SKIP] Dataset {s} exists.")
            continue

        result = generate_dataset(s)
        print(f"\nUploading dataset {s} to S3...")

        for file_path in result["file_paths"]:
            key = f"{prefix}/{os.path.basename(file_path)}"
            upload(file_path, key)

        print(f"[DONE] Dataset {s} ready.")

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--size",
        choices=["test", "S", "M", "L", "all"],
        default="test",
        help="Dataset size"
    )
    args = parser.parse_args()

    main(args.size)