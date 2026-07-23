"""Configuration and S3 key layout for dwh_load. No IO here."""

from __future__ import annotations

import logging
import os


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("dwh_load")


def get_release() -> str:
    release = os.environ.get("CHEMBL_RELEASE")
    if not release:
        raise RuntimeError("CHEMBL_RELEASE is not set.")
    return release


def s3_bucket() -> str:
    bucket = os.environ.get("DE_SCHOOL_S3_BUCKET")
    if not bucket:
        raise RuntimeError("DE_SCHOOL_S3_BUCKET is not set.")
    return bucket


def s3_root_prefix() -> str:
    return os.environ.get("S3_ROOT_PREFIX", "").strip("/")


def dwh_dsn() -> str:
    dsn = os.environ.get("DWH_DSN")
    if not dsn:
        raise RuntimeError(
            "DWH_DSN is not set, e.g. postgresql://airflow:airflow@localhost:5432/dwh"
        )
    return dsn


def top_similar_key(release: str) -> str:
    """The top-10 table produced by similarity_computation — this task's input."""
    path = f"silver/top_similar/release={release}/top10.parquet"
    root = s3_root_prefix()
    return f"{root}/{path}" if root else path
