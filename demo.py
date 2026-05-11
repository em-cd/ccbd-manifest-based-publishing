import argparse
import os
import threading
import time
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from dataset_gen import generate_dataset
from publish import publish, naive_publish
from reader import read_current_dataset, naive_read_dataset

DATA_DIR = "./data"
BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

def get_local_dataset_path(size):
    return f"{DATA_DIR}/{size}"

def run_safe_publish_demo(backend, size):
    dataset_id = size
    version = "v1"

    # Generate data if not present
    local_path = get_local_dataset_path(size)
    if not (os.path.exists(local_path) and len(os.listdir(local_path)) > 0):
        local_path = generate_dataset(size)


    # Read results
    table = read_current_dataset(backend, dataset_id)

    return {
        "rows": table.num_rows,
        "cols": table.column_names
    }

def run_publish_demo(backend):
    dataset_id = "demo"
    result = {}

    def writer(local_path, safe=True, version=None, sleep=0.5):
        # Sleep included to ensure we see inconsistent reads during demo
        if safe:
            # Safe publish: upload new version, then publish manifest
            publish(backend, dataset_id, local_path, version, sleep=sleep)
        else:
            # Naive publish: deletes first, then uploads, no manifest
            naive_publish(backend, dataset_id, local_path, sleep=sleep)

    def reader(safe, sleep=0.2):
        time.sleep(sleep)  # ensure it starts mid-write
        try:
            if safe:
                table = read_current_dataset(backend, dataset_id)
                result["safe_rows"] = table.num_rows
                print("ROWS READ from staging with manifest:           ", table.num_rows)

            else:
                table = naive_read_dataset(backend, dataset_id)
                result["naive_rows"] = table.num_rows
                print("ROWS READ from curated without manifest:        ", table.num_rows)
        except Exception as e:
            print(f"READ FAILED with{"out" if not safe else "   "} manifest:", str(e))

    # Generate demo datasets locally
    for version in ["v1", "v2"]:
        size_for_gen = f"{dataset_id}/{version}"
        local_path = get_local_dataset_path(size_for_gen)
        if not (os.path.exists(local_path) and len(os.listdir(local_path)) > 0):
            local_path = generate_dataset(size_for_gen)

    v1_local_path = get_local_dataset_path(f"{dataset_id}/v1")
    v2_local_path = get_local_dataset_path(f"{dataset_id}/v2")

    # Publish v1 to staging and curated zones
    print(f"{55*"="}\nBEGIN DEMO: publishing v1 (safe and unsafe)\n{55*"="}")
    writer(v1_local_path, safe=True, version="v1")
    writer(v1_local_path, safe=False)

    # Read after publishing v1
    reader(True)
    reader(False)

    # Now write v2 and read concurrently
    print(f"{55*"="}\nDEMO: safe publishing v2\n{55*"="}")
    t1 = threading.Thread(target=writer, args=(v2_local_path, True, "v2"))
    t2 = threading.Thread(target=reader, args=(True, 0.2))
    t3 = threading.Thread(target=reader, args=(True, 1.2))
    t4 = threading.Thread(target=reader, args=(True, 2))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    # Read once safely after everything is published
    reader(True)

    print(f"{55*"="}\nDEMO: unsafe publishing v2\n{55*"="}")
    t1 = threading.Thread(target=writer, args=(v2_local_path, False, "v2"))
    t2 = threading.Thread(target=reader, args=(False, 0.2))
    t3 = threading.Thread(target=reader, args=(False, 1.2))
    t4 = threading.Thread(target=reader, args=(False, 2))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    # Read once unsafely after everything is published
    reader(False)
    print(f"{55*"="}\nEND DEMO\n{55*"="}")


def main(backend_name):
    backend_cls = BACKEND_MAP[backend_name]
    backend = backend_cls()

    run_publish_demo(backend)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["s3", "azure"], required=True)
    args = parser.parse_args()

    main(args.backend)