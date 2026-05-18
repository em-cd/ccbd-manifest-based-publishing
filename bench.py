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
    "scan": None, #handled separately with scan presets
    "publish": lambda b, s: bench_publish(b, s),
}

SCAN_PRESETS = {

    # Region + date range (original benchmark query)
    "v1": {
        "region":     "Pemberley",
        "date_from":  "1812-01-01",
        "date_to":    "1812-07-01",
    },
 
    # Event type + date range
    "v2": {
        "event_type": "ball_attendance",
        "date_from":  "1812-06-01",
        "date_to":    "1812-12-31",
    },
 
    # Character + region (no date)
    "v3": {
        "character":  "Mr. Darcy",
        "region":     "Pemberley",
    },
 
    # Mood + event type (high selectivity combo)
    "v4": {
        "mood":       "mortified",
        "event_type": "proposal_rejected",
    },
 
    # Wide date range only
    "v5": {
        "date_from":  "1812-03-01",
        "date_to":    "1812-09-01",
    },
 
    # Single region, no date
    "v6": {
        "region":     "Longbourn",
    },
 
    # Single event type only
    "v7": {
        "event_type": "dramatic_hand_flex",
    },
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
def bench_scan(backend, size, preset_name="v1", region=None, event_type=None,
               character=None, mood=None, date_from=None, date_to=None):
 
    fs = backend.filesystem()
    dataset = ds.dataset(
        f"{backend.get_root()}/bench/{size}/",
        filesystem=fs,
        format="parquet",
    )
 
    # Total rows in dataset before any filtering (approximates rows scanned)
    total_rows_in_dataset = dataset.count_rows()
 
    # Build filter expression from whichever args are set
    filters = []
    if region:
        filters.append(ds.field("region") == region)
    if event_type:
        filters.append(ds.field("event_type") == event_type)
    if character:
        filters.append(ds.field("character") == character)
    if mood:
        filters.append(ds.field("mood") == mood)
    if date_from:
        filters.append(ds.field("ts") >= datetime.fromisoformat(date_from))
    if date_to:
        filters.append(ds.field("ts") < datetime.fromisoformat(date_to))
 
    if filters:
        filter_expr = filters[0]
        for f in filters[1:]:
            filter_expr = filter_expr & f
 
    # Scan timing (load + filter)
    scan_start = time.perf_counter()
    table      = dataset.to_table(filter=filter_expr) if filters else dataset.to_table()
    scan_ms    = (time.perf_counter() - scan_start) * 1000
 
    # Aggregation timing 
    agg_start  = time.perf_counter()
    result     = table.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean"),
    ])
    agg_ms     = (time.perf_counter() - agg_start) * 1000
 
    return {
        "elapsed_ms":            scan_ms + agg_ms,
        "scan_ms":               scan_ms,
        "agg_ms":                agg_ms,
        "rows_scanned":          total_rows_in_dataset,
        "rows_matched":          table.num_rows,
        "num_groups":            result.num_rows,
        # Which filters were active
        "preset_name":           preset_name,
        "filter_region":         region,
        "filter_event_type":     event_type,
        "filter_character":      character,
        "filter_mood":           mood,
        "filter_date_from":      date_from,
        "filter_date_to":        date_to,
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
        "backend", "size_label", "test_type", "session_ts", "run", "row_count",
        "elapsed_ms",
        "total_bytes", "total_mb", "object_count", "object_size_mb", "throughput_mb_s",
        "list_requests",
        "scan_ms", "agg_ms", "rows_scanned", "rows_matched", "num_groups",
        "preset_name", "filter_region", "filter_event_type", "filter_character",
        "filter_mood", "filter_date_from", "filter_date_to",
        "validation_ms", "metadata_ms",
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
            "backend":           backend_name,
            "size_label":        size,
            "test_type":         test_type,
            "session_ts":        session_ts,
            "run":               i + 1,
            "row_count":         DATASET_INFO[size]["rows"],
            "elapsed_ms":        elapsed_ms,
            "total_bytes":       total_bytes,
            "total_mb":          total_mb,
            "throughput_mb_s":   throughput_mb_s,
            "object_count":      object_count,
            "object_size_mb":    object_size_mb,
            "list_requests":     r.get("list_requests"),
            "scan_ms":           r.get("scan_ms"),
            "agg_ms":            r.get("agg_ms"),
            "rows_scanned":      r.get("rows_scanned"),
            "rows_matched":      r.get("rows_matched"),
            "num_groups":        r.get("num_groups"),
            "preset_name":       r.get("preset_name"),
            "filter_region":     r.get("filter_region"),
            "filter_event_type": r.get("filter_event_type"),
            "filter_character":  r.get("filter_character"),
            "filter_mood":       r.get("filter_mood"),
            "filter_date_from":  r.get("filter_date_from"),
            "filter_date_to":    r.get("filter_date_to"),
            "validation_ms":     r.get("validation_ms"),
            "metadata_ms":       r.get("metadata_ms"),
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
    parser.add_argument(
        "--query",
        nargs="+",
        choices=list(SCAN_PRESETS) + ["all"],
        default=["v1"],
        help="Scan query preset(s) to run. Use 'all' to run every preset.",
    )
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of runs per benchmark")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    sizes    = ["S", "M", "L"] if args.size == "all" else [args.size]
    backends = list(BACKEND_MAP) if args.backend == "all" else [args.backend]

    selected_tests   = list(BENCH_FUNCS) if "all" in args.tests else args.tests
    selected_presets = list(SCAN_PRESETS) if "all" in args.query else args.query

    session_ts  = datetime.now(timezone.utc).isoformat()
    all_results = []

    for backend_name in backends:
        backend_cls = BACKEND_MAP[backend_name]
        backend     = backend_cls()

        for size in sizes:
            print(f"\n{'═' * 50}")
            print(f"📊 Benchmarking: {size} ({args.runs} runs each)")
            print(f"   Local: {DATA_DIR}/{size}")
            print(f"   Remote: {backend_name}://{backend.get_root()}/{get_prefix(size)}")
            print(f"{'═' * 50}")

            for test in selected_tests:
                if test not in BENCH_FUNCS:
                    continue

                if test == "scan":
                    for preset_name in selected_presets:
                        filters = SCAN_PRESETS[preset_name]
                        results = run_repeated(
                            f"scan/{preset_name}",
                            lambda p=preset_name, f=filters: bench_scan(
                                backend, size,
                                preset_name=p,
                                region=f.get("region"),
                                event_type=f.get("event_type"),
                                character=f.get("character"),
                                mood=f.get("mood"),
                                date_from=f.get("date_from"),
                                date_to=f.get("date_to"),
                            ),
                            args.runs,
                        )
                        all_results.extend(
                            process_results(backend_name, size, f"scan/{preset_name}", results, session_ts)
                        )
                else:
                    results = run_repeated(
                        test,
                        lambda t=test: BENCH_FUNCS[t](backend, size),
                        args.runs,
                    )
                    all_results.extend(
                        process_results(backend_name, size, test, results, session_ts)
                    )

    save_results(all_results, args.output)
    print("\n✅ All benchmarks complete!")