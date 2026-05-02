import os
import json
import boto3
from datetime import datetime, timezone
import pyarrow.dataset as ds

s3 = boto3.client("s3")

bucket = os.getenv("S3_BUCKET_NAME")
if not bucket:
    raise ValueError("S3_BUCKET_NAME environment variable is not set")

def publish(dataset_id, version):
    """
    Publish a dataset version using a manifest-based approach, to ensure
    readers always see a consistent dataset version.

    Validates data in staging/<dataset_id>/<version>/, then updates
    published/<dataset_id>/latest.json to point to the new version.
    The previous manifest is saved to previous.json for rollback.

    :param dataset_id (str): Dataset identifier.
    :param version (str): Version name (e.g., "v1").
    """
    staging_prefix = f"staging/{dataset_id}/{version}/"
    latest_key = f"published/{dataset_id}/latest.json"
    previous_key = f"published/{dataset_id}/previous.json"

    s3_path = f"s3://{BUCKET}/{staging_prefix}"

    # 1. Validate
    validation = validate_dataset(s3_path)

    # 2. Read current latest
    current_latest = read_json(latest_key)

    # 3. Move latest -> previous
    if current_latest:
        write_json(previous_key, current_latest)

    # 4. Write new latest
    new_manifest = {
        "version": version,
        "prefix": staging_prefix,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "validation": validation
    }

    write_json(latest_key, new_manifest)

    print(f"Published {dataset_id} {version} successfully.")


def validate_dataset(s3_path):
    dataset = ds.dataset(s3_path, format="parquet")

    table = dataset.to_table(columns=["ts", "event_type", "value"])

    num_rows = table.num_rows
    ts_col = table.column("ts")

    min_ts = ts_col.to_pylist()[0]
    max_ts = ts_col.to_pylist()[-1]

    return {
        "rows": num_rows,
        "min_ts": str(min_ts),
        "max_ts": str(max_ts)
    }

def write_json(key, data):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8")
    )

def read_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None