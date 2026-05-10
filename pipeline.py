import argparse
import os
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from dataset_gen import generate_dataset
from data_transfer import upload
from publish import publish, naive_publish
from reader import read_current_dataset, naive_read_dataset

DATA_DIR = "./data"
BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

def run_pipeline(backend, size):
    dataset_id = size
    version = "v1"

    # 1. Generate + upload data
    local_path = f"{DATA_DIR}/{size}"
    if not (os.path.exists(local_path) and len(os.listdir(local_path)) > 0):
        local_path = generate_dataset(size)

    # 2. Safe publish
    upload(backend, dataset_id, local_path, version)
    publish(backend, dataset_id, version)

    # 3. Naive publish
    naive_publish(backend, dataset_id)

    # 4. Read results
    safe_table = read_current_dataset(backend, dataset_id)
    naive_table = naive_read_dataset(backend, dataset_id)

    return {
        "safe_rows": safe_table.num_rows,
        "safe_cols": safe_table.column_names,
        "naive_rows": naive_table.num_rows,
        "naive_cols": naive_table.column_names,
    }

def main(backend_name, size):
    if size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [size]

    backend_cls = BACKEND_MAP[backend_name]
    backend = backend_cls()

    for s in sizes:
        result = run_pipeline(backend, s)

        print(f"SAFE PUBLISH    rows: {result['safe_rows']}, cols: {result['safe_cols']}")
        print(f"NAIVE PUBLISH   rows: {result['naive_rows']}, cols: {result['naive_cols']}")
        print(f"CONSISTENT?     ", result["safe_rows"] == result["naive_rows"])

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["s3", "azure"], required=True)
    parser.add_argument(
        "--size",
        choices=["test", "S", "M", "L", "all"],
        default="test",
        help="Dataset size"
    )
    args = parser.parse_args()

    main(args.backend, args.size)