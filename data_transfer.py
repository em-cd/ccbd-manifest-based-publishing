import os
import time

def get_prefix(dataset_id, version=None):
    return f"staging/{dataset_id}/{version}/"

def upload(backend, dataset_id: str, local_dir: str, version: str = None, zone: str="staging", sleep: float = None):
    """
    Upload a full Parquet dataset folder to staging area. If version is not
    provided, uploads to bench area.
    """
    if version is None:
        prefix = f"{zone}/{dataset_id}/"
    else:
        prefix = f"{zone}/{dataset_id}/{version}/"

    files, keys = list_files(local_dir, prefix)

    for local_path, key in zip(files, keys):
        backend.upload_file(local_path, key)

        # For demo purposes
        if sleep:
            time.sleep(sleep)

    print(f"Uploaded {prefix}")

def download(backend, dataset_id: str, local_dir: str, version: str = None):
    """
    Download full dataset version from staging/<dataset_id>/<version>/.
    """
    prefix = get_prefix(dataset_id, version)
    keys, _ = backend.list_objects(prefix)

    for key in keys:
        relative_path = key.replace(prefix, "")
        local_path = os.path.join(local_dir, relative_path)

        backend.download_file(local_path, key)

    print(f"Downloaded {prefix}")

# TODO: We only check that the dataset folder is there, we should probably
# check that it's complete, e.g. by checking that the correct files are there
# and the amount of data is correct
def dataset_exists(backend, dataset_id: str, version: str = None):
    prefix = get_prefix(dataset_id, version)
    keys, _ = backend.list_objects(prefix)
    return len(keys) > 0

def list_files(local_dir, prefix):
    files = []
    keys = []

    for root, _, local_files in os.walk(local_dir):
        for f in local_files:
            local_path = os.path.join(root, f)
            rel_path = os.path.relpath(local_path, local_dir)

            key = prefix + rel_path

            files.append(local_path)
            keys.append(key)

    return files, keys