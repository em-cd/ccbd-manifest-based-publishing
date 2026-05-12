from datetime import datetime, timezone
from data_transfer import upload
import pyarrow.dataset as ds
import pyarrow.compute as pc

DATASET_SCHEMA = {
    "ts": "timestamp[us]",
    "user_id": "int64",
    "character": "string",
    "region": "string",
    "event_type": "string",
    "topic": "string",
    "mood": "string",
    "value": "double",
    "payload": "string",
}

def publish(backend, dataset_id, local_path, version, sleep=0):
    """
    Publish a dataset version using a manifest-based approach, to ensure
    readers always see a consistent dataset version.

    :param backend: the backend to publish to
    :param dataset_id (str): Dataset identifier.
    :param local_path (str): Path of dataset to upload.
    :param version (str): Version name (e.g., "v1").
    :param sleep (float): Time to sleep for during upload (for demo purposes)
    """
    if version is None:
        raise("Must provide a version number to publish")
    # 1. Upload new dataset
    upload(backend, dataset_id, local_path, version, sleep=sleep)

    staging_prefix = f"staging/{dataset_id}/{version}/"
    published_prefix = f"published/{dataset_id}/"

    # 2. Validate it
    validation = validate_dataset(
            f"{backend.get_root()}/{staging_prefix}",
            filesystem=backend.filesystem()
        )

    # 3. Write manifest
    write_manifest(backend, version, validation, staging_prefix, published_prefix)

    print(f"Published {dataset_id} {version} successfully.")


def validate_dataset(dataset_path, filesystem):
    """
    Validate a dataset in staging. Part of the safe publishing flow.
    """
    dataset = ds.dataset(dataset_path, format="parquet", filesystem=filesystem)

    # Validate schema
    schema = dataset.schema
    actual_columns = set(schema.names)
    missing = DATASET_SCHEMA.keys() - actual_columns
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Read only necessary columns
    table = dataset.to_table(columns=["ts", "event_type", "region", "value"])

    # Check dataset not empty
    rows = table.num_rows
    if rows == 0:
        raise ValueError("Dataset is empty")

    # Validate timestamps & compute stats
    ts_col = table.column("ts")
    min_ts = pc.min(ts_col).as_py()
    max_ts = pc.max(ts_col).as_py()
    if min_ts > max_ts:
        raise ValueError("Invalid timestamp range")

    # Validate values & compute stats
    value_col = table.column("value")
    min_value = pc.min(value_col).as_py()
    max_value = pc.max(value_col).as_py()
    avg_value = pc.mean(value_col).as_py()
    if min_value is None or max_value is None:
        raise ValueError("Invalid numeric stats")

    # Check events distribution
    event_counts = pc.value_counts(
        table.column("event_type")
    )
    top_events = {
        row["values"]: row["counts"]
        for row in event_counts.to_pylist()[:5]
    }

    return {
        "rows": rows,
        "columns": schema.names,
        "min_ts": min_ts.isoformat(),
        "max_ts": max_ts.isoformat(),
        "min_value": min_value,
        "max_value": max_value,
        "avg_value": round(avg_value, 2),
        "top_event_types": top_events,
    }

def write_manifest(backend, version, validation, staging_prefix, published_prefix):
    """
    Write a new manifest for a validated dataset. Part of the safe publishing flow.
    The previous manifest is saved to previous.json for rollback.
    """
    latest_key = f"{published_prefix}latest.json"
    previous_key = f"{published_prefix}previous.json"

    # 1. Read current latest manifest
    current_latest = backend.read_json(latest_key)

    # 2. Move latest -> previous
    if current_latest:
        backend.write_json(previous_key, current_latest)

    # 3. Write new latest manifest
    new_manifest = {
        "version": version,
        "prefix": staging_prefix,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "validation": validation
    }

    backend.write_json(latest_key, new_manifest)

def naive_publish(backend, dataset_id, local_path, sleep=0):
    """
    Naive publishing implementation for demo purposes. No manifest,
    no versioning, just deletes old version then writes to the curated
    zone. Sleep can be included to ensure we see inconsistent reads
    during demo.
    """
    backend.delete_prefix(f"curated/{dataset_id}/")
    upload(backend, dataset_id, local_path, version=None, zone="curated", sleep=sleep)