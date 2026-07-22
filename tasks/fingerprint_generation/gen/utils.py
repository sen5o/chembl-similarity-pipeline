"""Configuration and S3 key layout for fingerprint_generation. No IO here."""

from __future__ import annotations

import logging
import os

DEFAULT_PARTITIONS = 16


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("fingerprint_generation")


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


def num_partitions() -> int:
    """How many partitions the corpus is split into for parallel fingerprinting.

    Configurable (CORPUS_PARTITIONS) rather than hardcoded: the optimum
    depends on the runner's cores/memory and is tuned by measurement, not
    baked into source. Default 16 (~155k molecules each on ChEMBL 35), sized
    so a single partition stays comfortably within the container's memory
    budget (~7.9 GB shared with Postgres + Airflow).
    """
    return int(os.environ.get("CORPUS_PARTITIONS", DEFAULT_PARTITIONS))


def fingerprints_prefix(release: str) -> str:
    """S3 prefix for this release's fingerprints (Silver artifact).

    Layout: <root>/silver/fingerprints/release=<release>/part-<NNN>.parquet
    plus a per-partition _SUCCESS marker, so a partition that completed is
    skipped on re-run and a partition that failed midway is redone in full.
    """
    path = f"silver/fingerprints/release={release}"
    root = s3_root_prefix()
    return f"{root}/{path}" if root else path


def partition_key(prefix: str, part: int) -> str:
    return f"{prefix}/part-{part:03d}.parquet"


def partition_success_key(prefix: str, part: int) -> str:
    return f"{prefix}/part-{part:03d}._SUCCESS"
