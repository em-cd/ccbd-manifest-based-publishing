import os
import time
import csv
import argparse
import boto3
import pyarrow.dataset as ds
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Config
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")

if not BUCKET:
    raise ValueError("S3_BUCKET_NAME is missing in .env")

s3 = boto3.client("s3", region_name=REGION)

DATA_DIR = "./data"
NUM_RUNS = 3  # Number of times to repeat each benchmark


def get_prefix(size):
    return f"raw/{size}" if size == "test" else f"curated/{size}"


# Upload throughput
def bench_upload(size):
    local_dir = os.path.join(DATA_DIR, size)
    prefix = get_prefix(size)
    files = sorted([f for f in os.listdir(local_dir) if f.endswith(".parquet")])

    total_bytes = sum(os.path.getsize(os.path.join(local_dir, f)) for f in files)

    start = time.time()
    for f in files:
        local_path = os.path.join(local_dir, f)
        s3_key = f"{prefix}/{f}"
        s3.upload_file(local_path, BUCKET, s3_key)
    elapsed = time.time() - start

    throughput = (total_bytes / 1e6) / elapsed
    return total_bytes, elapsed, throughput, len(files)


# Download throughput
def bench_download(size):
    prefix = get_prefix(size)
    download_dir = os.path.join(DATA_DIR, f"{size}")
    os.makedirs(download_dir, exist_ok=True)

    # List objects
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(obj)

    total_bytes = 0
    start = time.time()
    for obj in objects:
        key = obj["Key"]
        filename = key.split("/")[-1]
        local_path = os.path.join(download_dir, filename)
        s3.download_file(BUCKET, key, local_path)
        total_bytes += os.path.getsize(local_path)
    elapsed = time.time() - start

    throughput = (total_bytes / 1e6) / elapsed
    return total_bytes, elapsed, throughput, len(objects)


# Listing time
def bench_listing(size):
    prefix = get_prefix(size)

    start = time.time()
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
    elapsed = time.time() - start

    return count, elapsed


# Parquet scan (analytics query)
def bench_scan(size):
    local_dir = os.path.join(DATA_DIR, size)
    dataset = ds.dataset(local_dir, format="parquet")

    start = time.time()

    filter_expr = (
        (ds.field("region") == "Pemberley") &
        (ds.field("ts") >= datetime(1812, 1, 1)) &
        (ds.field("ts") < datetime(1812, 7, 1))
    )

    table = dataset.to_table(filter=filter_expr)
    df = table.to_pandas()

    result = df.groupby("event_type").agg(
        count=("value", "count"),
        avg_value=("value", "mean")
    ).reset_index()

    elapsed = time.time() - start

    return len(df), len(result), elapsed


# Run with repeats
def run_repeated(name, func, size, num_runs):
    """Run a benchmark multiple times and collect results."""
    print(f"\n── {name} ({num_runs} runs) ──")
    results = []

    for i in range(num_runs):
        result = func(size)
        results.append(result)
        print(f"  Run {i + 1}/{num_runs} ✅")

    return results


# Save results
def save_results(all_results, output_file="results.csv"):
    fieldnames = [
        "size", "operation", "run", "total_mb", "elapsed_s",
        "throughput_mb_s", "file_count", "object_count",
        "rows_matched", "groups", "avg_elapsed_s", "avg_throughput_mb_s"
    ]

    file_exists = os.path.exists(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\n💾 Results saved to {output_file}")


# Process results for one benchmark
def process_upload_results(size, results):
    rows = []
    times = []
    throughputs = []

    for i, (total_bytes, elapsed, throughput, file_count) in enumerate(results):
        times.append(elapsed)
        throughputs.append(throughput)
        rows.append({
            "size": size,
            "operation": "upload",
            "run": i + 1,
            "total_mb": round(total_bytes / 1e6, 2),
            "elapsed_s": round(elapsed, 2),
            "throughput_mb_s": round(throughput, 2),
            "file_count": file_count,
        })

    # Add average row
    rows.append({
        "size": size,
        "operation": "upload",
        "run": "avg",
        "total_mb": rows[0]["total_mb"],
        "avg_elapsed_s": round(sum(times) / len(times), 2),
        "avg_throughput_mb_s": round(sum(throughputs) / len(throughputs), 2),
        "file_count": rows[0]["file_count"],
    })

    print(f"  📤 Upload avg: {rows[-1]['avg_elapsed_s']}s, {rows[-1]['avg_throughput_mb_s']} MB/s")
    return rows


def process_download_results(size, results):
    rows = []
    times = []
    throughputs = []

    for i, (total_bytes, elapsed, throughput, file_count) in enumerate(results):
        times.append(elapsed)
        throughputs.append(throughput)
        rows.append({
            "size": size,
            "operation": "download",
            "run": i + 1,
            "total_mb": round(total_bytes / 1e6, 2),
            "elapsed_s": round(elapsed, 2),
            "throughput_mb_s": round(throughput, 2),
            "file_count": file_count,
        })

    rows.append({
        "size": size,
        "operation": "download",
        "run": "avg",
        "total_mb": rows[0]["total_mb"],
        "avg_elapsed_s": round(sum(times) / len(times), 2),
        "avg_throughput_mb_s": round(sum(throughputs) / len(throughputs), 2),
        "file_count": rows[0]["file_count"],
    })

    print(f"  📥 Download avg: {rows[-1]['avg_elapsed_s']}s, {rows[-1]['avg_throughput_mb_s']} MB/s")
    return rows


def process_listing_results(size, results):
    rows = []
    times = []

    for i, (count, elapsed) in enumerate(results):
        times.append(elapsed)
        rows.append({
            "size": size,
            "operation": "listing",
            "run": i + 1,
            "object_count": count,
            "elapsed_s": round(elapsed, 4),
        })

    rows.append({
        "size": size,
        "operation": "listing",
        "run": "avg",
        "object_count": rows[0]["object_count"],
        "avg_elapsed_s": round(sum(times) / len(times), 4),
    })

    print(f"  📋 Listing avg: {rows[-1]['avg_elapsed_s']}s ({rows[0]['object_count']} objects)")
    return rows


def process_scan_results(size, results):
    rows = []
    times = []

    for i, (rows_matched, groups, elapsed) in enumerate(results):
        times.append(elapsed)
        rows.append({
            "size": size,
            "operation": "scan",
            "run": i + 1,
            "rows_matched": rows_matched,
            "groups": groups,
            "elapsed_s": round(elapsed, 4),
        })

    rows.append({
        "size": size,
        "operation": "scan",
        "run": "avg",
        "rows_matched": rows[0]["rows_matched"],
        "groups": rows[0]["groups"],
        "avg_elapsed_s": round(sum(times) / len(times), 4),
    })

    print(f"  🔍 Scan avg: {rows[-1]['avg_elapsed_s']}s ({rows[0]['rows_matched']} rows matched)")
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
        upload_results = run_repeated("Upload", bench_upload, size, args.runs)
        all_results.extend(process_upload_results(size, upload_results))

        # Listing
        listing_results = run_repeated("Listing", bench_listing, size, args.runs)
        all_results.extend(process_listing_results(size, listing_results))

        # Download
        download_results = run_repeated("Download", bench_download, size, args.runs)
        all_results.extend(process_download_results(size, download_results))

        # Scan
        scan_results = run_repeated("Scan", bench_scan, size, args.runs)
        all_results.extend(process_scan_results(size, scan_results))

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")