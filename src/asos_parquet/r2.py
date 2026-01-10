"""Cloudflare R2 storage operations (S3-compatible)."""

import os
from pathlib import Path

import boto3
from botocore.config import Config


def get_r2_client():
    """Create an R2 client using environment variables.

    Expected env vars:
        R2_ACCOUNT_ID: Cloudflare account ID
        R2_ACCESS_KEY_ID: R2 access key
        R2_SECRET_ACCESS_KEY: R2 secret key
    """
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        raise ValueError(
            "Missing R2 credentials. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, "
            "and R2_SECRET_ACCESS_KEY environment variables."
        )

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload_file(
    local_path: Path,
    bucket: str,
    key: str,
    client=None,
) -> str:
    """Upload a file to R2.

    Args:
        local_path: Local file path
        bucket: R2 bucket name
        key: Object key (path in bucket)
        client: Optional boto3 client (created if not provided)

    Returns:
        The S3 URI of the uploaded file
    """
    if client is None:
        client = get_r2_client()

    client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def upload_partition(
    partition_path: Path,
    bucket: str,
    prefix: str = "asos",
    client=None,
) -> list[str]:
    """Upload all files in a partition directory to R2.

    Merges all parquet files in the partition into a single data.parquet file.
    This creates predictable URLs for browser access (DuckDB-WASM can't glob over HTTP).

    Args:
        partition_path: Local partition directory (e.g., data/asos/date=2024-01-01)
        bucket: R2 bucket name
        prefix: Prefix for keys in bucket
        client: Optional boto3 client

    Returns:
        List of uploaded S3 URIs
    """
    import pyarrow.parquet as pq

    if client is None:
        client = get_r2_client()

    partition_name = partition_path.name  # e.g., "date=2024-01-01"
    parquet_files = list(partition_path.glob("*.parquet"))

    if not parquet_files:
        return []

    # Merge all parquet files into one and upload as data.parquet
    if len(parquet_files) == 1:
        # Single file - upload directly with fixed name
        key = f"{prefix}/{partition_name}/data.parquet"
        uri = upload_file(parquet_files[0], bucket, key, client)
        return [uri]
    else:
        # Multiple files - merge first
        import tempfile
        tables = [pq.read_table(f) for f in parquet_files]
        import pyarrow as pa
        merged = pa.concat_tables(tables)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pq.write_table(merged, tmp.name)
            key = f"{prefix}/{partition_name}/data.parquet"
            uri = upload_file(Path(tmp.name), bucket, key, client)
            Path(tmp.name).unlink()
            return [uri]


def list_partitions(
    bucket: str,
    prefix: str = "asos",
    client=None,
) -> list[str]:
    """List all partition months in the R2 bucket.

    Args:
        bucket: R2 bucket name
        prefix: Prefix for the dataset
        client: Optional boto3 client

    Returns:
        List of year_month strings (e.g., ["2024-01", "2024-02"])
    """
    if client is None:
        client = get_r2_client()

    # Use delimiter to get "directories"
    paginator = client.get_paginator("list_objects_v2")

    months = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/year_month=", Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            # Extract year_month from prefix like "asos/year_month=2024-01/"
            partition = prefix_info["Prefix"].rstrip("/").split("/")[-1]
            if partition.startswith("year_month="):
                months.add(partition.replace("year_month=", ""))

    return sorted(months)
