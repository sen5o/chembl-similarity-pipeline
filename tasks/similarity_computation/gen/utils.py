"""Configuration and S3 layout for similarity_computation. No IO here."""

from __future__ import annotations

import logging
import os


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("similarity_computation")


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


def _with_root(path: str) -> str:
    root = s3_root_prefix()
    return f"{root}/{path}" if root else path


def fingerprints_prefix(release: str) -> str:
    """Where fingerprint_generation wrote the corpus (this task's input)."""
    return _with_root(f"silver/fingerprints/release={release}")


def source_set_key() -> str:
    """resolved.parquet from source_resolution: the ~56 query molecules."""
    return _with_root("silver/source_set/resolved.parquet")


def similarity_prefix(release: str) -> str:
    """Full per-source similarity tables (brief step 4).

    Layout: <root>/silver/similarity/release=<r>/source_chembl_id=<id>/part-000.parquet
    One partition per source molecule — literally "the full similarity table
    for that molecule" from the brief.
    """
    return _with_root(f"silver/similarity/release={release}")


def source_similarity_key(release: str, source_chembl_id: str) -> str:
    prefix = similarity_prefix(release)
    return f"{prefix}/source_chembl_id={source_chembl_id}/part-000.parquet"


def top_similar_key(release: str) -> str:
    """Top-10 neighbours for all sources, one file (brief step 5).

    Small (56 x 10 rows) — the direct input to the DWH fact table.
    """
    return _with_root(f"silver/top_similar/release={release}/top10.parquet")


def success_key(release: str) -> str:
    return _with_root(f"silver/top_similar/release={release}/_SUCCESS")
