import os
import time
import csv
import argparse
import boto3
import pyarrow.dataset as ds
import pyarrow.fs as fs
from datetime import datetime
from dotenv import load_dotenv
from upload import upload_dataset
from download import download_dataset

load_dotenv()

# Config
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")

if not BUCKET:
    raise ValueError("S3_BUCKET_NAME is missing in .env")

s3 = boto3.client("s3", region_name=REGION)
s3fs = fs.S3FileSystem()

DATA_DIR = "./data"
NUM_RUNS = 3  # Number of times to repeat each benchmark

def get_prefix(size):
    return f"bench/{size}/"

# Listing time
def bench_listing(size):
    prefix = get_prefix(size)

    start = time.time()
    count = 0
    total_bytes = 0

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
            total_bytes += obj.get("Size", 0)

    elapsed = time.time() - start
    total_mb = total_bytes / (1024 * 1024)

    return {
        "object_count": count,
        "total_bytes": total_bytes,
        "total_mb": total_mb,
        "elapsed_s": elapsed
    }


# Parquet scan (analytics query)
def bench_scan(size):
    dataset = ds.dataset(
        f"{BUCKET}/{get_prefix(size)}",
        filesystem=s3fs,
        format="parquet"
    )

    start = time.time()

    filter_expr = (
        (ds.field("region") == "Pemberley") &
        (ds.field("ts") >= datetime(1812, 1, 1)) &
        (ds.field("ts") < datetime(1812, 7, 1))
    )

    table = dataset.to_table(filter=filter_expr)

    result = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])

    elapsed = time.time() - start

    return {
        "rows_matched": table.num_rows,
        "num_groups": result.num_rows,
        "elapsed_s": elapsed
    }

def run_repeated(name, func, num_runs):
    """Run a benchmark multiple times and collect results."""
    print(f"\n── {name} ({num_runs} runs) ──")
    results = []

    for i in range(num_runs):
        result = func()
        results.append(result)
        print(f"  Run {i + 1}/{num_runs} ✅")

    return results

def save_results(all_results, output_file="results.csv"):
    fieldnames = [
        "size", "operation", "run", "total_bytes", "total_mb", "elapsed_s",
        "throughput_mb_s", "file_count", "object_count",
        "rows_matched", "num_groups", "avg_elapsed_s", "avg_throughput_mb_s"
    ]

    file_exists = os.path.exists(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\n💾 Results saved to {output_file}")

def process_results(size, operation, results, value_keys, extra_keys):
    """
    Generic benchmark result processor.

    - size: dataset size (S/M/L)
    - operation: upload/download/listing/scan
    - results: list of dicts (one per run)
    - value_keys: numeric fields to average (e.g. ["elapsed", "throughput"])
    - extra_keys: non-numeric fields to carry over (e.g. counts)
    """
    rows = []
    accum = {k: [] for k in value_keys}

    for i, r in enumerate(results):
        for k in value_keys:
            accum[k].append(r[k])

        row = {
            "size": size,
            "operation": operation,
            "run": i + 1,
        }

        for k in value_keys:
            row[k] = r.get(k)

        for k in extra_keys:
            row[k] = r.get(k)

        rows.append(row)

    avg_row = {
        "size": size,
        "operation": operation,
        "run": "avg",
    }
    
    for k in value_keys:
        avg_row[f"avg_{k}"] = sum(accum[k]) / max(len(accum[k]), 1)

    for k in extra_keys:
        avg_row[k] = results[0].get(k)

    rows.append(avg_row)

    return rows

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="📊 Benchmark Harness")
    parser.add_argument("--size", choices=["test", "S", "M", "L", "all"], required=True)
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of runs per benchmark")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    if args.size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [args.size]

    all_results = []

    for size in sizes:
        print(f"\n{'═' * 50}")
        print(f"📊 Benchmarking: {size} ({args.runs} runs each)")
        print(f"   Local: {DATA_DIR}/{size}")
        print(f"   S3:    s3://{BUCKET}/{get_prefix(size)}")
        print(f"{'═' * 50}")

        # Upload
        upload_results = run_repeated("Upload", lambda: upload_dataset(size, f"./data/{size}"), args.runs)
        processed_upload_results = process_results(
            size,
            "upload",
            upload_results,
            value_keys=["elapsed_s", "throughput_mb_s"],
            extra_keys=["file_count", "total_bytes", "total_mb"]
        )
        all_results.extend(processed_upload_results)

        # Download
        download_results = run_repeated("Download", lambda: download_dataset(size, f"./data/{size}"), args.runs)
        processed_download_results = process_results(
            size,
            "download",
            download_results,
            value_keys=["elapsed_s", "throughput_mb_s"],
            extra_keys=["file_count", "total_bytes", "total_mb"]
        )
        all_results.extend(processed_download_results)

        # Listing
        listing_results = run_repeated("Listing", lambda: bench_listing(size), args.runs)
        processed_listing_results = process_results(
            size,
            "listing",
            listing_results,
            value_keys=["elapsed_s"],
            extra_keys=["object_count", "total_bytes"]
        )
        all_results.extend(processed_listing_results)

        # Scan
        scan_results = run_repeated("Scan", lambda: bench_scan(size), args.runs)
        processed_scan_results = process_results(
            size,
            "scan",
            scan_results,
            value_keys=["elapsed_s"],
            extra_keys=["rows_matched", "num_groups"]
        )
        all_results.extend(processed_scan_results)

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")