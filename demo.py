import argparse
import os
import threading
import time
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from dataset_gen import generate_dataset
from publish import publish, naive_publish
from read import read_current_dataset, naive_read_dataset
from demo_viz import generate_demo_viz

DATA_DIR = "./data"

BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

def get_local_dataset_path(size):
    return f"{DATA_DIR}/{size}"

def run_publish_demo(backend):
    dataset_id = "demo"

    result = {
        "safe_rows": [],
        "naive_rows": []
    }
    # Writer
    def writer(local_path, safe=True, version=None, sleep=0.5):
        if safe:
            publish(
                backend,
                dataset_id,
                local_path,
                version,
                sleep=sleep
            )
        else:
            naive_publish(
                backend,
                dataset_id,
                local_path,
                sleep=sleep
            )

    # Reader
    def reader(safe=True, sleep=0.2):
        time.sleep(sleep)

        try:
            if safe:
                table = read_current_dataset(backend, dataset_id)
                rows = table.num_rows
                result["safe_rows"].append(rows)
                print(
                    f"[SAFE ] read at t={sleep:.1f}s "
                    f"-> {rows:,} rows"
                )
            else:
                table = naive_read_dataset(backend, dataset_id)
                rows = table.num_rows
                result["naive_rows"].append(rows)
                print(
                    f"[NAIVE] read at t={sleep:.1f}s "
                    f"-> {rows:,} rows"
                )

        except Exception as e:
            if safe:
                result["safe_rows"].append(0)
            else:
                result["naive_rows"].append(0)
            print(
                f"[{'SAFE ' if safe else 'NAIVE'}] "
                f"READ FAILED at t={sleep:.1f}s -> {str(e)}"
            )

    # Generate datasets if missing
    for version in ["v1", "v2"]:

        size_for_gen = f"{dataset_id}/{version}"

        local_path = get_local_dataset_path(size_for_gen)

        if not (
            os.path.exists(local_path)
            and len(os.listdir(local_path)) > 0
        ):
            generate_dataset(size_for_gen)

    v1_local_path = get_local_dataset_path(f"{dataset_id}/v1")
    v2_local_path = get_local_dataset_path(f"{dataset_id}/v2")

    # Publish v1 first
    print(f"\n{'=' * 65}")
    print("INITIAL PUBLISH OF v1")
    print(f"{'=' * 65}\n")

    writer(v1_local_path, safe=True, version="v1")
    writer(v1_local_path, safe=False)

    # baseline read
    reader(True, 0)
    reader(False, 0)

    # SAFE DEMO
    print(f"\n{'=' * 65}")
    print("SAFE MANIFEST-BASED PUBLISHING")
    print(f"{'=' * 65}\n")

    safe_threads = [
        threading.Thread(
            target=writer,
            args=(v2_local_path, True, "v2", 0.8)
        ),

        threading.Thread(
            target=reader,
            args=(True, 0.2)
        ),

        threading.Thread(
            target=reader,
            args=(True, 1.2)
        ),

        threading.Thread(
            target=reader,
            args=(True, 2.0)
        ),
    ]

    for t in safe_threads:
        t.start()

    for t in safe_threads:
        t.join()

    # final stable read
    reader(True, 3.8)

    # NAIVE DEMO
    print(f"\n{'=' * 65}")
    print("NAIVE OVERWRITE PUBLISHING")
    print(f"{'=' * 65}\n")

    naive_threads = [
        threading.Thread(
            target=writer,
            args=(v2_local_path, False, "v2", 0.8)
        ),

        threading.Thread(
            target=reader,
            args=(False, 0.2)
        ),

        threading.Thread(
            target=reader,
            args=(False, 1.2)
        ),

        threading.Thread(
            target=reader,
            args=(False, 2.0)
        ),
    ]

    for t in naive_threads:
        t.start()

    for t in naive_threads:
        t.join()

    # final stable read
    reader(False, 3.8)

    # Visualization
    print(f"\n{'=' * 65}")
    print("GENERATING VISUALIZATION")
    print(f"{'=' * 65}\n")

    print(result)

    generate_demo_viz(result)

    print(f"\n{'=' * 65}")
    print("END DEMO")
    print(f"{'=' * 65}\n")


def main(backend_name):
    backend_cls = BACKEND_MAP[backend_name]
    backend = backend_cls()
    run_publish_demo(backend)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backend",
        choices=["s3", "azure"],
        required=True
    )
    args = parser.parse_args()

    main(args.backend)