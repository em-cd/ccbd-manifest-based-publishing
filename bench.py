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

def bench_upload(backend, size):
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
    start = time.perf_counter()

    for local_path, key in zip(files, keys):
        total_bytes += backend.upload_file(local_path, key)

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "total_bytes": total_bytes,
        "elapsed_ms": elapsed_ms,
        "object_count": len(files),
    }

def bench_download(backend, size):
    prefix = get_prefix(size)
    local_dir = get_local_dir(size)

    keys, page_count = backend.list_objects(prefix)

    total_bytes = 0
    start = time.perf_counter()

    for key in keys:
        relative_path = key.replace(prefix, "")
        local_path = os.path.join(local_dir, relative_path)

        total_bytes += backend.download_file(local_path, key)

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "total_bytes": total_bytes,
        "elapsed_ms": elapsed_ms,
        "object_count": len(keys),
    }

def bench_listing(backend, size):
    prefix = get_prefix(size)

    start = time.perf_counter()

    keys, page_count = backend.list_objects(prefix)

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "elapsed_ms": elapsed_ms,
        "object_count": len(keys),
        "list_requests": page_count
    }

# Parquet scan (analytics query)
def bench_scan(backend, size, region=None, date_from=None, date_to=None):
    fs = backend.filesystem()
    dataset = ds.dataset(
        f"{backend.get_root()}/bench/{size}/",
        filesystem=fs,
        format="parquet"
    )

    start = time.perf_counter()

    filters = []
    if region:
        filters.append(ds.field("region") == region)
    if date_from:
        filters.append(ds.field("ts") >= datetime.fromisoformat(date_from))
    if date_to:
        filters.append(ds.field("ts") < datetime.fromisoformat(date_to))

    if filters:
        filter_expr = filters[0]
        for f in filters[1:]:
            filter_expr = filter_expr & f
        table = dataset.to_table(filter=filter_expr)
    else:
        table = dataset.to_table()  # ← this is the only to_table call now

    result = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "total_bytes": None,
        "elapsed_ms": elapsed_ms,
        "object_count": None,
        "rows_matched": table.num_rows,
        "num_groups": result.num_rows,
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
        "backend", "size_label", "test_type", "run", "total_bytes", "total_mb", "elapsed_ms",
        "throughput_mb_s", "object_count", "object_size_mb", "list_requests",
        "rows_matched", "num_groups"
    ]

    file_exists = os.path.exists(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\n💾 Results saved to {output_file}")

def process_results(backend_name, size, test_type, results):
    """
    Generic benchmark result processor.
    """
    rows = []

    for i, r in enumerate(results):
        total_bytes = r.get("total_bytes")
        elapsed_ms = r.get("elapsed_ms")
        object_count = r.get("object_count")

        # Compute derived keys
        if total_bytes is not None and elapsed_ms > 0:
            total_mb = total_bytes / (1024 * 1024)
            throughput_mb_s = (total_mb * 1000) / elapsed_ms
        else:
            total_mb = None
            throughput_mb_s = None

        if total_mb is not None and object_count:
            object_size_mb = total_mb / object_count
        else:
            object_size_mb = None

        row = {
            "backend": backend_name,
            "size_label": size,
            "test_type": test_type,
            "run": i + 1,
            "elapsed_ms": elapsed_ms,
            "total_bytes": total_bytes,
            "total_mb": total_mb,
            "throughput_mb_s": throughput_mb_s,
            "object_count": object_count,
            "object_size_mb": object_size_mb,
            "list_requests": r.get("list_requests"),
            "rows_matched": r.get("rows_matched"),
            "num_groups": r.get("num_groups"),
        }

        rows.append(row)

    return rows

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="📊 Benchmark Harness")
    parser.add_argument("--size", choices=["test", "S", "M", "L", "all"], required=True)
    parser.add_argument("--backend", choices=["s3", "azure", "all"], required=True)
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of runs per benchmark")
    parser.add_argument("--region", default="Pemberley")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
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
            processed_upload_results = process_results(backend_name, size, "upload", upload_results)
            all_results.extend(processed_upload_results)

            # Download
            download_results = run_repeated("Download", lambda: bench_download(backend, size), args.runs)
            processed_download_results = process_results(backend_name, size, "download", download_results)
            all_results.extend(processed_download_results)

            # Listing
            listing_results = run_repeated("Listing", lambda: bench_listing(backend, size), args.runs)
            processed_listing_results = process_results(backend_name, size, "listing", listing_results)
            all_results.extend(processed_listing_results)

            # Scan
            scan_results = run_repeated("Scan", lambda: bench_scan(backend, size), args.runs)
            processed_scan_results = process_results(backend_name, size, "scan", scan_results)
            all_results.extend(processed_scan_results)

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")