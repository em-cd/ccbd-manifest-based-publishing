# Cloud Computing & Big Data Project
## S3 Data Lake Benchmarking Harness & Manifest-Based Publishing

----

## Overview

This project has two parts:

1. A benchmarking harness for S3-based data lake operations
2. A manifest-based dataset publishing system for safe versioned data release

It is designed to compare performance across dataset sizes (S, M, L) and produce reproducible results for ingestion, query and publishing workloads.


## Storage Layout

The system uses the following S3 namespaces:

### Benchmark data

```
s3://bucket/bench/<dataset_id>/
```

Used only for benchmarking.

### Dataset publishing

#### Staging (versioned datasets)

```
s3://bucket/staging/<dataset_id>/<version>/
```

New dataset versions are uploaded and validated here.

#### Published (active dataset pointer)

```
s3://bucket/published/<dataset_id>/
```

Contains:

- `latest.json`: current active version
- `previous.json`: previous version for rollback


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

## Generating datasets

This project uses Docker. If you have Docker installed, open a terminal and from within the project directory run the following commands:

```bash
docker build -t ccbd-project .
docker run --env-file .env ccbd-project:latest
```

By default this will generate a small test dataset, upload it to S3, and delete the local copy. To generate different sizes, you can pass in a size argument like this:

```bash
docker run --env-file .env ccbd-project:latest python -u pipeline.py --size S
```

The supported dataset sizes are **test** (~283K), **S** (~1GB), **M** (~5GB) and **L** (~10GB).


## 🚀 Running Benchmarks

Run a full benchmark suite:

```bash
python bench.py --size=S
```

Options:

- --size: S | M | L | all
- --runs: number of repetitions per test (default: 3)
- --output: CSV output file (default: results.csv)