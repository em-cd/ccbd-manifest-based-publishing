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

def get_prefix(size):
    return f"raw/{size}" if size == "test" else f"curated/{size}"


#Upload throughput
def bench_upload(size):
    #Upload all parquet files to S3 and measure throughput
    local_dir = os.path.join(DATA_DIR, size)
    
    # Test goes to raw, everything else to curated
    prefix = get_prefix(size)
    
    files = sorted([f for f in os.listdir(local_dir) if f.endswith(".parquet")])

    total_bytes = 0
    start = time.time()

    for f in files:
        local_path = os.path.join(local_dir, f)
        s3_key = f"{prefix}/{f}"
        file_size = os.path.getsize(local_path)
        total_bytes += file_size
        s3.upload_file(local_path, BUCKET, s3_key)
        print(f"  📤 {s3_key} ({file_size / 1e6:.1f} MB)")

    elapsed = time.time() - start
    throughput = (total_bytes / 1e6) / elapsed

    print(f"  ⏱️  {total_bytes / 1e6:.1f} MB in {elapsed:.1f}s = {throughput:.1f} MB/s")

    return {
        "size": size,
        "operation": "upload",
        "total_mb": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 2),
        "throughput_mb_s": round(throughput, 2),
        "file_count": len(files),
    }


#Download throughput
def bench_download(size):
    #Download all parquet files from S3 and measure throughput.
    prefix = get_prefix(size)
    download_dir = os.path.join(DATA_DIR, f"{size}_downloaded")
    os.makedirs(download_dir, exist_ok=True)

    # List all objects
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
        file_size = os.path.getsize(local_path)
        total_bytes += file_size
        print(f"  📥 {key} ({file_size / 1e6:.1f} MB)")

    elapsed = time.time() - start
    throughput = (total_bytes / 1e6) / elapsed

    print(f"  ⏱️  {total_bytes / 1e6:.1f} MB in {elapsed:.1f}s = {throughput:.1f} MB/s")

    return {
        "size": size,
        "operation": "download",
        "total_mb": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 2),
        "throughput_mb_s": round(throughput, 2),
        "file_count": len(objects),
    }


#Listing time
def bench_listing(size):
    #List all objects under a prefix and measure time.
    prefix = get_prefix(size)

    start = time.time()
    count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
    elapsed = time.time() - start

    print(f"  📋 Listed {count} objects in {elapsed:.3f}s")

    return {
        "size": size,
        "operation": "listing",
        "object_count": count,
        "elapsed_s": round(elapsed, 4),
    }


#Analytics query
def bench_scan(size):
    """
    Run an analytics query on local parquet files:
    - Filter: region = 'Pemberley' AND ts in first half of 1812
    - Group by: event_type
    - Aggregate: count + average value
    """
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

    print(f"  🔍 {len(df):,} rows matched, {len(result)} groups, {elapsed:.2f}s")
    print(result.to_string())

    return {
        "size": size,
        "operation": "scan",
        "rows_matched": len(df),
        "groups": len(result),
        "elapsed_s": round(elapsed, 4),
    }


#Save results
def save_results(results, output_file="results.csv"):
    #Append benchmark results to CSV
    file_exists = os.path.exists(output_file)

    fieldnames = [
        "size", "operation", "total_mb", "elapsed_s",
        "throughput_mb_s", "file_count", "object_count",
        "rows_matched", "groups"
    ]

    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\n💾 Results saved to {output_file}")


#Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="📊 Benchmark Harness")
    parser.add_argument("--size", choices=["test", "S", "M", "L", "all"], required=True)
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    if args.size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [args.size]

    all_results = []

    for size in sizes:
        print(f"\n{'═' * 50}")
        print(f"📊 Benchmarking size: {size}")
        print(f"   Local: {DATA_DIR}/{size}")
        print(f"   S3:    s3://{BUCKET}/{get_prefix(size)}")
        print(f"{'═' * 50}")

        print("\n── Upload ──")
        all_results.append(bench_upload(size))

        print("\n── Listing ──")
        all_results.append(bench_listing(size))

        print("\n── Download ──")
        all_results.append(bench_download(size))

        print("\n── Parquet Scan ──")
        all_results.append(bench_scan(size))

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")