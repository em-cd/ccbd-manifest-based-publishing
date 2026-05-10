import argparse
from dataset_gen import generate_dataset
from data_transfer import upload, dataset_exists
from publish import publish
from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend

BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

def main(backend_name, size):
    if size == "all":
        sizes = ["S", "M", "L"]
    else:
        sizes = [size]

    backend_cls = BACKEND_MAP[backend_name]
    backend = backend_cls()

    for s in sizes:
        dataset_id = s
        version = 'v1'

        if dataset_exists(backend, dataset_id, version):
            print(f"[SKIP DATAGEN] Dataset {s} exists.")
        else:
            result = generate_dataset(s)

            print(f"Uploading dataset {s} to S3...")
            upload(backend, dataset_id, result["out_path"], version)

        print(f"Publishing dataset {s}...")
        publish(backend, dataset_id, version)

        print(f"[DONE] Dataset {s} ready.")

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