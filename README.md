# Cloud Computing & Big Data Project
## Data Lake Benchmarking Harness & Manifest-Based Publishing

## Overview

This project implements a benchmark framework for evaluating safe dataset publishing and object storage performance in cloud data lake environments. It compares a naive overwrite approach with a manifest-based publishing strategy that enables atomic dataset versioning across object storage backends.

The system generates synthetic Parquet datasets at multiple scales and benchmarks upload, download, listing, scan, and publishing workflows, including analytical queries with configurable predicates. Results are logged in CSV format and analysed in Python notebooks to study throughput, query performance, and publishing behaviour.

## Storage Layout

The system supports both Amazon S3 and Azure Blob Storage backends with identical logical namespace structures.

| Area                                   | Path                                                      | Description                                                                                       |
| -------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Bench**                              | `<backend>://<container>/bench/<dataset_id>/`             | Used only for benchmarking workloads                                                              |
| **Staging**                            | `<backend>://<container>/staging/<dataset_id>/<version>/` | Versioned datasets are uploaded and validated here before publication                           |
| **Published**                          | `<backend>://<container>/published/<dataset_id>/`         | Contains only manifest files defining the active dataset version (`latest.json`, `previous.json`) |
| **Curated**                            | `<backend>://<container>/curated/`                        | Direct overwrite-based publishing without versioning                                              |

## Configuration

Create a `.env` file with the following variables:

- `AWS_DEFAULT_REGION`: AWS region (e.g. eu-central-1)
- `AWS_ACCESS_KEY_ID`: IAM user access key
- `AWS_SECRET_ACCESS_KEY`: IAM user secret key
- `S3_BUCKET_NAME`: target S3 bucket
- `AZURE_STORAGE_ACCOUNT`: storage account name for Azure
- `AZURE_STORAGE_KEY`: storage key for Azure account (can be an account key or a SAS token)
- `AZURE_CONTAINER`: target Azure Blob Storage container

You can configure the benchmark harness and publishing demos to use either AWS S3 or Azure Blob Storage. If you only use one provider, you only need to set the corresponding credentials and can leave out the other.

## Generating Datasets

This project uses Docker. If you have Docker installed, open a terminal and from within the project directory run the following commands to build the image and generate the datasets:

```bash
docker build -t ccbd-project .
docker run ccbd-project:latest dataset_gen.py
```

By default this will generate the small, medium and large datasets and store them locally under `./data/`. To generate a specific size dataset, you can do the following:

```bash
docker run ccbd-project:latest dataset_gen.py --size S
```

The supported dataset sizes are **S** (~1GB), **M** (~5GB), **L** (~10GB) and **test** (~283K, for debugging purposes).


## 🚀 Running Benchmarks

Run a full benchmark suite:

```bash
docker run --env-file .env ccbd-project:latest bench.py
```

### CLI options:

- `--size`: which size dataset to test against (`S | M | L | all`, default: `all`)
- `--backend`: the backend to use (`s3 | azure | all`, default: `all`)
- `--tests`: which benchmark tests to run (`upload | download | listing | scan | publish | all`, default: `all`)
- `--query`: which scan preset to use (`v1 | v2 | v3 | v4 | v5 | v6 | v7 | all`, default: `v1`)
- `--runs`: number of repetitions per test (default: 3)
- `--output`: CSV output file (default: results.csv)

### Scan query presets

Scan benchmarks simulate different analytical access patterns:

- `v1`: Region + date range (baseline workload)
- `v2`: Event type + date range
- `v3`: Character + region
- `v4`: Mood + event type (high selectivity)
- `v5`: Wide date range filter
- `v6`: Region only filter
- `v7`: Event type only filter

These presets are used to evaluate how filter selectivity impacts scan and aggregation performance.

## 📝 Publishing modes demonstration

To run the demo comparing naive overwrite publishing to manifest-based publishing on S3, run:

```bash
docker run --env-file .env ccbd-project:latest demo.py --backend s3
```

You can also use Azure by passing in `--backend azure`.

The demo generates two versions of a small dataset and runs a concurrent workload consisting of one writer thread and three reader threads. The writer publishes an initial version and then updates it to a second version, while readers continuously attempt to access the dataset during the update process.

In the naive overwrite mode, readers may observe partial or inconsistent dataset states due to files being replaced in-place. In the manifest-based mode, readers always resolve the dataset through a manifest pointer, ensuring they see either the old or new version, but never a mixed state.