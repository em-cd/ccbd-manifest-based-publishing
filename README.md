# Cloud Computing & Big Data Project
## Data Lake Benchmarking Harness & Manifest-Based Publishing

## Overview

This project consists of two main components:

1. A cloud storage benchmarking harness for data lake operations on Amazon S3 and Azure Blob Storage
2. A manifest-based dataset publishing system for safe, versioned data release

The system is designed to evaluate performance across multiple dataset sizes (S, M, L) and provide reproducible measurements for ingestion, analytical querying, and publishing workflows across different object storage backends.


## Storage Layout

The system supports both Amazon S3 and Azure Blob Storage backends with identical logical namespace structures.

### Benchmark data

```
<backend>://<container>/bench/<dataset_id>/
```

Used only for benchmarking.

### Dataset publishing

#### Staging (versioned datasets)

```
<backend>://<container>/staging/<dataset_id>/<version>/
```

New dataset versions are uploaded and validated here.

#### Published (active dataset pointer)

```
<backend>://<container>/published/<dataset_id>/
```

Contains only dataset manifests:

- `latest.json`: pointer to the currently active dataset version
- `previous.json`: pointer to the last published dataset version, for rollback


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