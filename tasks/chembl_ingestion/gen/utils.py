"""Configuration resolution and S3 key layout.

No I/O happens in this module — only reading environment variables and
building deterministic paths. Actual reads/writes live in repository.py.
"""

from __future__ import annotations

import logging
import os


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("chembl_ingestion")


def get_release() -> str:
    """Resolve the ChEMBL release this run is pinned to.

    Read from the CHEMBL_RELEASE environment variable (set as an Airflow
    Variable and exported into the task's environment). Every S3 key and
    cache decision is keyed by this value — see README ADR 6 (version
    pinning): it is what makes acquire_chembl idempotent for free and what
    proves the pipeline is a repeatable release-refresh, not a one-off.
    """
    release = os.environ.get("CHEMBL_RELEASE")
    if not release:
        raise RuntimeError(
            "CHEMBL_RELEASE is not set. Set it as an Airflow Variable (see "
            "local_deployment/.env.example) so this run is pinned to a "
            "specific ChEMBL release."
        )
    return release


def s3_bucket() -> str:
    bucket = os.environ.get("DE_SCHOOL_S3_BUCKET")
    if not bucket:
        raise RuntimeError("DE_SCHOOL_S3_BUCKET is not set.")
    return bucket


def s3_root_prefix() -> str:
    """Course-mandated S3 root, e.g. 'final_task/surname_name'.

    Configured via S3_ROOT_PREFIX so it isn't hardcoded into source. Empty
    string is valid (bucket used at its root) for local/dev testing.
    """
    return os.environ.get("S3_ROOT_PREFIX", "").strip("/")


def bronze_chembl_prefix(release: str) -> str:
    """S3 prefix for this release's Bronze ChEMBL artifacts.

    Layout: <root>/bronze/chembl/release=<release>/<table>/part-*.parquet
    A `_SUCCESS` marker at this prefix's root signals a *complete* load —
    acquire_chembl() checks for the marker, not bare prefix existence, so a
    run that failed partway through upload is correctly treated as a cache
    miss and retried in full.
    """
    path = f"bronze/chembl/release={release}"
    root = s3_root_prefix()
    return f"{root}/{path}" if root else path
