"""Thin S3 I/O layer for the Airflow DAG.

Two groups of functions:
  * Pure (de)serialization helpers (rows <-> CSV bytes, DataFrame <-> CSV
    bytes) — no Airflow dependency, unit-testable on their own.
  * S3 read/write/list wrappers built on Airflow's S3Hook. S3Hook is imported
    lazily so importing this module (and the pure helpers) does not require
    Airflow to be installed.

The same functions work against real AWS S3 and MinIO — the only difference is
the Airflow Connection referenced by `aws_conn_id` (for MinIO the connection's
`extra` sets `endpoint_url`).
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_AWS_CONN_ID = "aws_default"


# --------------------------------------------------------------------------- #
# Pure (de)serialization helpers
# --------------------------------------------------------------------------- #
def rows_to_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Serialize a list of row dicts to CSV bytes.

    The header is the union of all keys, in first-seen order, so rows with
    heterogeneous keys (e.g. some enriched, some not) still serialize safely.
    """
    if not rows:
        return b""
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def csv_bytes_to_rows(data: bytes) -> list[dict[str, Any]]:
    """Parse CSV bytes into a list of row dicts."""
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def csv_bytes_to_df(data: bytes) -> pd.DataFrame:
    """Parse CSV bytes into a pandas DataFrame (used by the graph step)."""
    return pd.read_csv(io.BytesIO(data))


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to CSV bytes."""
    return df.to_csv(index=False).encode("utf-8")


# --------------------------------------------------------------------------- #
# S3 wrappers (S3Hook imported lazily)
# --------------------------------------------------------------------------- #
def _hook(aws_conn_id: str) -> Any:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=aws_conn_id)


def _read_bytes(bucket: str, key: str, aws_conn_id: str) -> bytes:
    obj = _hook(aws_conn_id).get_key(key, bucket_name=bucket)
    data: bytes = obj.get()["Body"].read()
    return data


def read_rows(bucket: str, key: str, aws_conn_id: str = DEFAULT_AWS_CONN_ID) -> list[dict[str, Any]]:
    """Read a CSV object from S3 into a list of row dicts."""
    logger.info("Reading rows from s3://%s/%s", bucket, key)
    return csv_bytes_to_rows(_read_bytes(bucket, key, aws_conn_id))


def read_text(bucket: str, key: str, aws_conn_id: str = DEFAULT_AWS_CONN_ID) -> str:
    """Read a CSV/text object from S3 as a decoded string (for load_r_groups)."""
    logger.info("Reading text from s3://%s/%s", bucket, key)
    return _read_bytes(bucket, key, aws_conn_id).decode("utf-8")


def read_df(bucket: str, key: str, aws_conn_id: str = DEFAULT_AWS_CONN_ID) -> pd.DataFrame:
    """Read a CSV object from S3 into a DataFrame."""
    logger.info("Reading dataframe from s3://%s/%s", bucket, key)
    return csv_bytes_to_df(_read_bytes(bucket, key, aws_conn_id))


def write_rows(
    rows: list[dict[str, Any]],
    bucket: str,
    key: str,
    aws_conn_id: str = DEFAULT_AWS_CONN_ID,
) -> str:
    """Serialize rows to CSV and upload to S3. Returns the key."""
    _hook(aws_conn_id).load_bytes(rows_to_csv_bytes(rows), key=key, bucket_name=bucket, replace=True)
    logger.info("Wrote %d row(s) to s3://%s/%s", len(rows), bucket, key)
    return key


def write_bytes(
    data: bytes,
    bucket: str,
    key: str,
    aws_conn_id: str = DEFAULT_AWS_CONN_ID,
) -> str:
    """Upload raw bytes to S3 (used for the graph .zip). Returns the key."""
    _hook(aws_conn_id).load_bytes(data, key=key, bucket_name=bucket, replace=True)
    logger.info("Wrote %d bytes to s3://%s/%s", len(data), bucket, key)
    return key


def list_keys(bucket: str, prefix: str = "", aws_conn_id: str = DEFAULT_AWS_CONN_ID) -> list[str]:
    """List object keys under a prefix."""
    return _hook(aws_conn_id).list_keys(bucket_name=bucket, prefix=prefix) or []


def object_exists(bucket: str, key: str, aws_conn_id: str = DEFAULT_AWS_CONN_ID) -> bool:
    """Return True if an object exists at the given key."""
    return bool(_hook(aws_conn_id).check_for_key(key, bucket_name=bucket))
