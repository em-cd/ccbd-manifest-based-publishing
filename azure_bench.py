
import os
import time
import csv
import argparse
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobBlock
import pyarrow.dataset as ds

load_dotenv()

# Config
ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
KEY = os.getenv("AZURE_STORAGE_KEY")
CONTAINER = os.getenv("AZURE_CONTAINER")

blob_service = BlobServiceClient(
    account_url=f"https://{ACCOUNT}.blob.core.windows.net",
    credential=KEY,
    max_block_size=4 * 1024 * 1024,        # 4 MB chunks (default is 4MB)
    max_single_put_size=8 * 1024 * 1024,    # Files under 8MB upload in one shot
    max_page_size=4 * 1024 * 1024,
    connection_timeout=300,
    read_timeout=300,
)
container_client = blob_service.get_container_client(CONTAINER)

DATA_DIR = "./data"
NUM_RUNS = 3


def get_prefix(size):
    return f"bench/{size}/"



# Upload throughput
def bench_upload(size):
    local_dir = os.path.join(DATA_DIR, size)
    prefix = get_prefix(size)
    files = sorted([f for f in os.listdir(local_dir) if f.endswith(".parquet")])

    total_bytes = sum(os.path.getsize(os.path.join(local_dir, f)) for f in files)

    start = time.time()
    for f in files:
        local_path = os.path.join(local_dir, f)
        blob_name = f"{prefix}{f}"
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                max_concurrency=8,    # parallel chunk uploads within one file
            )

    elapsed = time.time() - start

    throughput = (total_bytes / 1e6) / elapsed

    return {
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 2),
        "throughput_mb_s": round(throughput, 2),
        "file_count": len(files),
    }


# Download throughput
def bench_download(size):
    prefix = get_prefix(size)
    download_dir = os.path.join(DATA_DIR, f"{size}_azure_downloaded")
    os.makedirs(download_dir, exist_ok=True)

    blobs = list(container_client.list_blobs(name_starts_with=prefix))

    total_bytes = 0
    start = time.time()
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        local_path = os.path.join(download_dir, filename)
        blob_client = container_client.get_blob_client(blob.name)
        with open(local_path, "wb") as f:
            data = blob_client.download_blob(max_concurrency=8)
            data.readinto(f)
        file_size = os.path.getsize(local_path)
        total_bytes += file_size
    elapsed = time.time() - start

    throughput = (total_bytes / 1e6) / elapsed

    return {
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 2),
        "throughput_mb_s": round(throughput, 2),
        "file_count": len(blobs),
    }


# Listing time
def bench_listing(size):
    prefix = get_prefix(size)

    start = time.time()
    count = 0
    total_bytes = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        count += 1
        total_bytes += blob.size
    elapsed = time.time() - start

    return {
        "object_count": count,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 4),
    }


# Parquet scan; reads directly from Azure
def bench_scan(size):
    from adlfs import AzureBlobFileSystem

    azfs = AzureBlobFileSystem(
        account_name=ACCOUNT,
        account_key=KEY
    )

    dataset = ds.dataset(
        f"{CONTAINER}/{get_prefix(size)}",
        filesystem=azfs,
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
        "elapsed_s": round(elapsed, 4),
    }


# Run with repeats
def run_repeated(name, func, num_runs):
    print(f"\n── {name} ({num_runs} runs) ──")
    results = []
    for i in range(num_runs):
        result = func()
        results.append(result)
        print(f"  Run {i + 1}/{num_runs} ✅")
    return results


# Process results
def process_results(size, operation, results, value_keys, extra_keys):
    rows = []
    accum = {k: [] for k in value_keys}

    for i, r in enumerate(results):
        for k in value_keys:
            accum[k].append(r[k])

        row = {
            "size": size,
            "provider": "azure",
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
        "provider": "azure",
        "operation": operation,
        "run": "avg",
    }
    for k in value_keys:
        avg_row[f"avg_{k}"] = round(sum(accum[k]) / len(accum[k]), 4)
    for k in extra_keys:
        avg_row[k] = results[0].get(k)
    rows.append(avg_row)

    return rows


# Save results
def save_results(all_results, output_file):
    fieldnames = [
        "size", "provider", "operation", "run", "total_bytes", "total_mb",
        "elapsed_s", "throughput_mb_s", "file_count", "object_count",
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


# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="☁️ Azure Benchmark Harness")
    parser.add_argument("--size", choices=["test", "S", "M", "L", "all"], required=True)
    parser.add_argument("--runs", type=int, default=NUM_RUNS)
    parser.add_argument("--output", default="results/results-azure.csv")
    args = parser.parse_args()

    os.makedirs("results", exist_ok=True)

    if args.size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [args.size]

    all_results = []

    for size in sizes:
        print(f"\n{'═' * 50}")
        print(f"☁️  Azure Benchmarking: {size} ({args.runs} runs each)")
        print(f"   Local: {DATA_DIR}/{size}")
        print(f"   Azure: {CONTAINER}/{get_prefix(size)}")
        print(f"{'═' * 50}")

        # Upload
        upload_results = run_repeated("Upload", lambda s=size: bench_upload(s), args.runs)
        all_results.extend(process_results(size, "upload", upload_results,
            value_keys=["elapsed_s", "throughput_mb_s"],
            extra_keys=["file_count", "total_bytes", "total_mb"]))

        # Listing
        listing_results = run_repeated("Listing", lambda s=size: bench_listing(s), args.runs)
        all_results.extend(process_results(size, "listing", listing_results,
            value_keys=["elapsed_s"],
            extra_keys=["object_count", "total_bytes"]))

        # Download
        download_results = run_repeated("Download", lambda s=size: bench_download(s), args.runs)
        all_results.extend(process_results(size, "download", download_results,
            value_keys=["elapsed_s", "throughput_mb_s"],
            extra_keys=["file_count", "total_bytes", "total_mb"]))

        # Scan
        scan_results = run_repeated("Scan", lambda s=size: bench_scan(s), args.runs)
        all_results.extend(process_results(size, "scan", scan_results,
            value_keys=["elapsed_s"],
            extra_keys=["rows_matched", "num_groups"]))

    save_results(all_results, args.output)
    print("\n✅ All Azure benchmarks complete!")
