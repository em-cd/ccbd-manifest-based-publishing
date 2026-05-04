import os
import time
import csv
import argparse
import pyarrow.dataset as ds
from datetime import datetime
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend

DATA_DIR = "./data"
NUM_RUNS = 3  # Number of times to repeat each benchmark
BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

def get_prefix(size):
    return f"bench/{size}/"

def get_local_dir(size):
    return f"{DATA_DIR}/{size}"

def bench_upload(store, size):
    prefix = get_prefix(size)
    local_dir = get_local_dir(size)

    files = []
    keys = []

    for root, _, fs in os.walk(local_dir):
        for f in fs:
            local_path = os.path.join(root, f)
            rel = os.path.relpath(local_path, local_dir)

            files.append(local_path)
            keys.append(prefix + rel)

    total_bytes = 0
    start = time.time()

    for local_path, key in zip(files, keys):
        total_bytes += store.upload_file(local_path, key)

    elapsed = time.time() - start
    total_mb = total_bytes / (1024 * 1024)
    throughput = total_mb / elapsed if elapsed > 0 else 0

    return {
        "total_bytes": total_bytes,
        "total_mb": total_mb,
        "elapsed_s": elapsed,
        "throughput_mb_s": throughput,
        "file_count": len(files)
    }

def bench_download(store, size):
    prefix = get_prefix(size)
    local_dir = get_local_dir(size)

    keys = store.list_objects(prefix)

    total_bytes = 0
    start = time.time()

    for key in keys:
        relative_path = key.replace(prefix, "")
        local_path = os.path.join(local_dir, relative_path)

        total_bytes += store.download_file(local_path, key)

    elapsed = time.time() - start
    total_mb = total_bytes / (1024 * 1024)
    throughput = total_mb / elapsed if elapsed > 0 else 0

    return {
        "total_bytes": total_bytes,
        "total_mb": total_mb,
        "elapsed_s": elapsed,
        "throughput_mb_s": throughput,
        "file_count": len(keys)
    }

def bench_listing(store, size):
    prefix = get_prefix(size)

    start = time.time()

    keys = store.list_objects(prefix)

    elapsed = time.time() - start

    return {
        "object_count": len(keys),
        "elapsed_s": elapsed
    }

# Parquet scan (analytics query)
def bench_scan(store, size):
    fs = backend.filesystem()

    dataset = ds.dataset(
        f"{backend.get_root()}/bench/{size}/",
        filesystem=fs,
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
        "backend", "size", "operation", "run", "total_bytes", "total_mb", "elapsed_s",
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

def process_results(backend_name, size, operation, results, value_keys, extra_keys):
    """
    Generic benchmark result processor.

    - backend_name: name of backend data store (azure or s3)
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
            accum[k].append(r.get(k, 0))

        row = {
            "backend": backend_name,
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
        "backend": backend_name,
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
    parser.add_argument("--backend", choices=["s3", "azure", "all"], required=True)
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of runs per benchmark")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    if args.size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [args.size]

    backends = list(BACKEND_MAP) if args.backend == "all" else [args.backend]

    all_results = []

    for backend_name in backends:
        backend_cls = BACKEND_MAP[backend_name]
        backend = backend_cls()

        for size in sizes:
            print(f"\n{'═' * 50}")
            print(f"📊 Benchmarking: {size} ({args.runs} runs each)")
            print(f"   Local: {DATA_DIR}/{size}")
            print(f"   Remote: {backend_name}://{backend.get_root()}/{get_prefix(size)}")
            print(f"{'═' * 50}")

            # Upload
            upload_results = run_repeated("Upload", lambda: bench_upload(backend, size), args.runs)
            processed_upload_results = process_results(
                backend,
                size,
                "upload",
                upload_results,
                value_keys=["elapsed_s", "throughput_mb_s"],
                extra_keys=["file_count", "total_bytes", "total_mb"]
            )
            all_results.extend(processed_upload_results)

            # Download
            download_results = run_repeated("Download", lambda: bench_download(backend, size), args.runs)
            processed_download_results = process_results(
                backend,
                size,
                "download",
                download_results,
                value_keys=["elapsed_s", "throughput_mb_s"],
                extra_keys=["file_count", "total_bytes", "total_mb"]
            )
            all_results.extend(processed_download_results)

            # Listing
            listing_results = run_repeated("Listing", lambda: bench_listing(backend, size), args.runs)
            processed_listing_results = process_results(
                backend,
                size,
                "listing",
                listing_results,
                value_keys=["elapsed_s"],
                extra_keys=["object_count", "total_bytes"]
            )
            all_results.extend(processed_listing_results)

            # Scan
            scan_results = run_repeated("Scan", lambda: bench_scan(backend, size), args.runs)
            processed_scan_results = process_results(
                backend,
                size,
                "scan",
                scan_results,
                value_keys=["elapsed_s"],
                extra_keys=["rows_matched", "num_groups"]
            )
            all_results.extend(processed_scan_results)

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")