"""Orchestration tests for generate_partition. Postgres and S3 are mocked;
these verify the wiring: streaming write, the structures_in/out reconciliation
metric (unparseable SMILES counted, not silently dropped), and cache-skip.
Real fingerprint math is covered in test_fingerprints.py.
"""

from __future__ import annotations

from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from gen import repository, services


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("CHEMBL_RELEASE", "35")
    monkeypatch.setenv("DE_SCHOOL_S3_BUCKET", "bucket")
    monkeypatch.setenv("S3_ROOT_PREFIX", "final_task/x")
    monkeypatch.setenv("DWH_DSN", "postgresql://u:p@localhost/dwh")
    monkeypatch.setenv("CORPUS_PARTITIONS", "4")


# a valid and an invalid SMILES so the in/out gap is exercised
_CORPUS = [
    ("CHEMBL25", 1, "CC(=O)Oc1ccccc1C(=O)O"),  # aspirin — valid
    ("CHEMBL2", 2, "Cn1cnc2c1c(=O)n(C)c(=O)n2C"),  # caffeine — valid
    ("CHEMBL_BAD", 3, "not_a_smiles"),  # unparseable
]


def _run(corpus, cached=False):
    uploaded = {}

    def fake_upload(local_path, bucket, key):
        uploaded["path"] = str(local_path)
        uploaded["key"] = key
        # read the parquet the service actually streamed, before tmpdir vanishes
        uploaded["table"] = pq.read_table(local_path)

    with (
        patch.object(repository, "partition_cached", return_value=cached),
        patch.object(repository, "read_corpus_partition", return_value=iter(corpus)),
        patch.object(repository, "upload_file", side_effect=fake_upload),
        patch.object(repository, "write_success_marker") as marker,
    ):
        metrics = services.generate_partition(0)
    return metrics, uploaded, marker


def test_reconciliation_counts_unparseable_without_dropping_silently():
    metrics, uploaded, marker = _run(_CORPUS)

    assert metrics["structures_in"] == 3
    assert metrics["fingerprints_out"] == 2  # aspirin + caffeine
    assert metrics["unparseable_smiles"] == 1  # the bad one, counted
    # only the 2 valid fingerprints were written to parquet
    assert uploaded["table"].num_rows == 2
    assert set(uploaded["table"].column("chembl_id").to_pylist()) == {"CHEMBL25", "CHEMBL2"}
    marker.assert_called_once()


def test_written_fingerprints_are_nonempty_bytes():
    _metrics, uploaded, _marker = _run(_CORPUS)
    fps = uploaded["table"].column("fingerprint").to_pylist()
    assert all(isinstance(b, bytes) and len(b) > 0 for b in fps)


def test_cache_hit_skips_all_work():
    with (
        patch.object(repository, "partition_cached", return_value=True),
        patch.object(repository, "read_corpus_partition") as read,
        patch.object(repository, "upload_file") as upload,
    ):
        metrics = services.generate_partition(0)

    assert metrics == {"partition": 0, "skipped": True}
    read.assert_not_called()
    upload.assert_not_called()


def test_marker_not_written_if_read_fails():
    """If the corpus read raises mid-stream, no _SUCCESS marker is written, so
    the partition is retried in full rather than looking complete."""

    def boom(*a, **k):
        yield ("CHEMBL25", 1, "CC(=O)Oc1ccccc1C(=O)O")
        raise RuntimeError("db dropped")

    with (
        patch.object(repository, "partition_cached", return_value=False),
        patch.object(repository, "read_corpus_partition", side_effect=boom),
        patch.object(repository, "upload_file"),
        patch.object(repository, "write_success_marker") as marker,
        pytest.raises(RuntimeError, match="db dropped"),
    ):
        services.generate_partition(0)

    marker.assert_not_called()
