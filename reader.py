import pyarrow.dataset as ds

def read_current_dataset(backend, dataset_id):
    """
    Read the current dataset. Checks the latest manifest to locate the
    current dataset.
    """
    manifest_key = f"published/{dataset_id}/latest.json"
    manifest = backend.read_json(manifest_key)

    if not manifest:
        raise RuntimeError("No published dataset")

    prefix = manifest["prefix"]

    dataset = ds.dataset(
        f"{backend.get_root()}/{prefix}",
        format="parquet",
        filesystem=backend.filesystem()
        )

    return dataset.to_table()

def naive_read_dataset(backend, dataset_id):
    """
    Naive reader for comparison purposes. Reads whatever data is under
    `curated/` without checking for a manifest.
    """
    dataset = ds.dataset(
        f"{backend.get_root()}/curated/{dataset_id}",
        format="parquet",
        filesystem=backend.filesystem()
        )

    return dataset.to_table()