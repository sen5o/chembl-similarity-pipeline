"""Unit tests for chembl_ingestion.gen.services.

No network and no real S3 calls: repository functions are mocked out.
These tests verify orchestration logic (cache-skip behaviour, manifest
shape, one extract+upload per projected table) — not IO correctness.
IO correctness against the real dump is exercised manually via
`python run.py inspect` (see README note on the projection).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from gen import services


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("CHEMBL_RELEASE", "35")
    monkeypatch.setenv("DE_SCHOOL_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("DWH_DSN", "postgresql://u:p@localhost:5432/dwh")


def test_acquire_chembl_skips_download_on_cache_hit():
    with (
        patch.object(services.repository, "cache_hit", return_value=True) as cache_hit,
        patch.object(services.repository, "download_dump") as download_dump,
    ):
        result = services.acquire_chembl()

    cache_hit.assert_called_once_with("test-bucket", "bronze/chembl/release=35")
    download_dump.assert_not_called()
    assert result == "bronze/chembl/release=35"


def test_acquire_chembl_extracts_and_uploads_every_table_on_cache_miss(tmp_path):
    fake_sqlite = tmp_path / "chembl.db"
    fake_sqlite.touch()

    with (
        patch.object(services.repository, "cache_hit", return_value=False),
        patch.object(services.repository, "download_dump", return_value=fake_sqlite),
        patch.object(services.repository, "extract_table", return_value=10) as extract_table,
        patch.object(services.repository, "upload_file") as upload_file,
        patch.object(services.repository, "write_success_marker") as write_marker,
    ):
        services.acquire_chembl()

    # one extract + one upload per projected table — no table silently skipped
    assert extract_table.call_count == len(services.TABLE_COLUMNS)
    assert upload_file.call_count == len(services.TABLE_COLUMNS)

    write_marker.assert_called_once()
    _, _, manifest = write_marker.call_args.args
    assert manifest["release"] == "35"
    assert set(manifest["tables"]) == set(services.TABLE_COLUMNS)
    assert all(t["rows"] == 10 for t in manifest["tables"].values())


def test_success_marker_written_only_after_all_tables_uploaded():
    """Guards the ordering that makes the cache marker trustworthy: if any
    extract/upload raises, write_success_marker must never be called.
    """
    with (
        patch.object(services.repository, "cache_hit", return_value=False),
        patch.object(services.repository, "download_dump", return_value="fake.db"),
        patch.object(services.repository, "extract_table", side_effect=RuntimeError("boom")),
        patch.object(services.repository, "upload_file"),
        patch.object(services.repository, "write_success_marker") as write_marker,
        pytest.raises(RuntimeError, match="boom"),
    ):
        services.acquire_chembl()

    write_marker.assert_not_called()


def test_load_staging_downloads_and_copies_every_table():
    with (
        patch.object(services.repository, "download_file") as download_file,
        patch.object(services.repository, "truncate_and_copy", return_value=42) as copy,
    ):
        counts = services.load_staging()

    # one download + one copy per table — no table skipped
    assert download_file.call_count == len(services.TABLE_COLUMNS)
    assert copy.call_count == len(services.TABLE_COLUMNS)
    assert set(counts) == set(services.TABLE_COLUMNS)
    assert all(n == 42 for n in counts.values())
