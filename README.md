# CCBD Project: Manifest-based publishing

## Endpoint Configuration

The project uses AWS S3 as the storage backend.

Configuration is provided via environment variables:

- AWS_DEFAULT_REGION: AWS region (e.g. eu-central-1)
- AWS_ACCESS_KEY_ID: IAM user access key
- AWS_SECRET_ACCESS_KEY: IAM user secret key
- S3_BUCKET_NAME: target S3 bucket

## How to run  it

First you will need a `.env` file with the configuration listed above.

This project uses Docker. If you have Docker installed, open a terminal and from within the project directory run the following commands:

```bash
docker build -t ccbd-project .
docker run --env-file .env ccbd-project:latest
```

By default this will generate a small test dataset, upload it to S3, and delete the local copy. To generate different sizes, you can pass in a size argument like this:

```bash
docker run --env-file .env ccbd-project:latest python -u pipeline.py --size S
```

The supported dataset sizes are test (~283K), S (~1GB), M (~5GB) and L (~10GB).