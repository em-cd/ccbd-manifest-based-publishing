import os
import time
import csv
import argparse
import pyarrow.dataset as ds
from datetime import datetime, timezone
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from publish import validate_dataset, write_manifest

DATA_DIR = "./data"
NUM_RUNS = 3  # Number of times to repeat each benchmark

DATASET_INFO = {
    "test": {"rows": 1000, "objects": 1}, # Only for debugging
    "S": {"rows": 3_580_000, "objects": 4},
    "M": {"rows": 17_900_000, "objects": 18},
    "L": {"rows": 35_800_000, "objects": 36}
}

BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

BENCH_FUNCS = {
    "upload": lambda b, s: bench_upload(b, s),
    "download": lambda b, s: bench_download(b, s),
    "listing": lambda b, s: bench_listing(b, s),
    "scan": lambda b, s: bench_scan(b, s, region="Pemberley"),
    "publish": lambda b, s: bench_publish(b, s),
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

    keys, _ = backend.list_objects(prefix)

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
        table = dataset.to_table()

    result = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "elapsed_ms": elapsed_ms,
        "rows_matched": table.num_rows,
        "num_groups": result.num_rows,
    }

def bench_publish(backend, size):
    """
    Benchmarks manifest-based publish:
    - validation time
    - manifest write time
    - total time
    """
    version = "v1" # fixed for benchmark
    staging_prefix = f"bench/{size}/" # read from bench data, not staging
    published_prefix = f"bench/published/{size}/"

    start = time.perf_counter()

    # 1. Validation only timing
    validation = validate_dataset(
        f"{backend.get_root()}/{staging_prefix}",
        filesystem=backend.filesystem()
    )
    mid = time.perf_counter()

    # 2. Manifest timing
    write_manifest(backend, version, validation, staging_prefix, published_prefix)
    end = time.perf_counter()

    validation_ms = (mid - start) * 1000
    meta_ms = (end - mid) * 1000
    elapsed_ms = (end - start) * 1000

    return {
        "validation_ms": validation_ms,
        "metadata_ms": meta_ms,
        "elapsed_ms": elapsed_ms
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
        "backend", "size_label", "test_type", "session_ts", "run", "row_count", "object_count", "elapsed_ms", # all benchmarks
        "total_bytes", "total_mb", "object_size_mb", "throughput_mb_s", # upload/download
        "list_requests", # listing
        "rows_matched", "num_groups", # scan
        "validation_ms", "metadata_ms" # publish
    ]

    file_exists = os.path.exists(output_file)

    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\n💾 Results saved to {output_file}")

def process_results(backend_name, size, test_type, results, session_ts):
    """
    Generic benchmark result processor.
    """
    rows = []

    for i, r in enumerate(results):
        total_bytes = r.get("total_bytes")
        elapsed_ms = r.get("elapsed_ms")
        object_count = r.get("object_count") or DATASET_INFO[size]["objects"]

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
            "session_ts": session_ts,
            "run": i + 1,
            "elapsed_ms": elapsed_ms,
            "total_bytes": total_bytes,
            "total_mb": total_mb,
            "throughput_mb_s": throughput_mb_s,
            "row_count": DATASET_INFO[size]["rows"],
            "object_count": object_count,
            "object_size_mb": object_size_mb,
            "list_requests": r.get("list_requests"),
            "rows_matched": r.get("rows_matched"),
            "num_groups": r.get("num_groups"),
            "validation_ms": r.get("validation_ms"),
            "metadata_ms": r.get("metadata_ms"),
        }

        rows.append(row)

    return rows

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="📊 Benchmark Harness")
    parser.add_argument("--size", choices=["test", "S", "M", "L", "all"], required=True)
    parser.add_argument("--backend", choices=["s3", "azure", "all"], required=True)
    parser.add_argument(
        "--tests",
        nargs="+",
        choices=list(BENCH_FUNCS) + ["all"],
        default=["all"],
        help="Which benchmarks to run"
    )
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

    selected_tests = args.tests
    if "all" in selected_tests:
        selected_tests = list(BENCH_FUNCS)

    # Run the benchmarks
    session_ts = datetime.now(timezone.utc).isoformat()
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

            for test in selected_tests:
                if test not in BENCH_FUNCS:
                    continue

                results = run_repeated(
                    test,
                    lambda t=test: BENCH_FUNCS[t](backend, size),
                    args.runs
                )

                all_results.extend(process_results(backend_name, size, test, results, session_ts))

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")