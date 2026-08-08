"""Orchestration test for similarity_computation.run.

Uses REAL RDKit fingerprints for a tiny corpus (so Tanimoto is genuine) but
mocks all S3 IO. Verifies: source fingerprint is pulled from the corpus (self
score 1.0 excluded from its own top-10), full table written per source, and the
combined top-10 assembled with source_chembl_id attached.
"""

from __future__ import annotations

from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from gen import repository, services
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator

_gen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)


def _fp(smi):
    return _gen.GetFingerprint(Chem.MolFromSmiles(smi))


# small real corpus: aspirin, salicylic acid (close), ethanol, benzene (far)
_CORPUS_IDS = ["CHEMBL_ASP", "CHEMBL_SAL", "CHEMBL_ETH", "CHEMBL_BEN"]
_CORPUS_MOL = [1, 2, 3, 4]
_CORPUS_FP = [
    _fp("CC(=O)Oc1ccccc1C(=O)O"),
    _fp("O=C(O)c1ccccc1O"),
    _fp("CCO"),
    _fp("c1ccccc1"),
]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("CHEMBL_RELEASE", "35")
    monkeypatch.setenv("DE_SCHOOL_S3_BUCKET", "bucket")
    monkeypatch.setenv("S3_ROOT_PREFIX", "final_task/x")


def test_run_produces_top10_and_full_tables():
    full_tables = {}
    top10_written = {}

    def fake_upload(local_path, bucket, key):
        full_tables[key] = pq.read_table(local_path)

    def fake_write_top10(rows, bucket, key):
        top10_written["rows"] = rows

    with (
        patch.object(
            repository, "load_corpus", return_value=(_CORPUS_IDS, _CORPUS_MOL, _CORPUS_FP)
        ),
        patch.object(repository, "load_source_chembl_ids", return_value=["CHEMBL_ASP"]),
        patch.object(repository, "object_exists", return_value=False),
        patch.object(repository, "upload_file", side_effect=fake_upload),
        patch.object(repository, "write_top10", side_effect=fake_write_top10),
        patch.object(repository, "write_success_marker") as marker,
    ):
        manifest = services.run()

    # one source computed, none missing
    assert manifest["sources_computed"] == 1
    assert manifest["sources_missing_from_corpus"] == []

    # full similarity table for aspirin: all 4 corpus rows incl. self
    (full_key,) = full_tables
    tbl = full_tables[full_key]
    assert tbl.num_rows == 4

    # top-10 rows: self excluded, salicylic acid ranks first (closest), source id attached
    rows = top10_written["rows"]
    assert all(r["source_chembl_id"] == "CHEMBL_ASP" for r in rows)
    ids = [r["target_chembl_id"] for r in rows]
    assert "CHEMBL_ASP" not in ids  # self-match removed
    assert ids[0] == "CHEMBL_SAL"  # most similar to aspirin
    marker.assert_called_once()


def test_source_missing_from_corpus_is_recorded():
    with (
        patch.object(
            repository, "load_corpus", return_value=(_CORPUS_IDS, _CORPUS_MOL, _CORPUS_FP)
        ),
        patch.object(repository, "load_source_chembl_ids", return_value=["CHEMBL_NOT_IN_CORPUS"]),
        patch.object(repository, "object_exists", return_value=False),
        patch.object(repository, "upload_file"),
        patch.object(repository, "write_top10"),
        patch.object(repository, "write_success_marker"),
    ):
        manifest = services.run()

    assert manifest["sources_computed"] == 0
    assert manifest["sources_missing_from_corpus"] == ["CHEMBL_NOT_IN_CORPUS"]


def test_global_cache_hit_skips_everything():
    """If the _SUCCESS marker exists, run() returns the stored manifest without
    loading the corpus or recomputing anything."""
    cached = {"release": "35", "sources_computed": 56, "top10_rows": 560}
    with (
        patch.object(repository, "object_exists", return_value=True),
        patch.object(repository, "read_manifest", return_value=cached),
        patch.object(repository, "load_corpus") as load_corpus,
    ):
        manifest = services.run()

    assert manifest == cached
    load_corpus.assert_not_called()  # corpus never touched on the fast path


def test_per_source_cache_reuses_existing_table():
    """With no global marker but an existing per-source table, the source is
    re-ranked from S3 (not recomputed) and counted as reused."""
    fake_table = [("CHEMBL_SAL", 2, 0.4483), ("CHEMBL_ETH", 3, 0.1111), ("CHEMBL_ASP", 1, 1.0)]

    def exists(bucket, key):
        return not key.endswith("_SUCCESS")  # global marker absent, source table present

    top10 = {}

    def capture_top10(rows, bucket, key):
        top10["rows"] = rows

    with (
        patch.object(
            repository, "load_corpus", return_value=(_CORPUS_IDS, _CORPUS_MOL, _CORPUS_FP)
        ),
        patch.object(repository, "load_source_chembl_ids", return_value=["CHEMBL_ASP"]),
        patch.object(repository, "object_exists", side_effect=exists),
        patch.object(repository, "read_full_table", return_value=iter(fake_table)),
        patch.object(repository, "upload_file") as upload,
        patch.object(repository, "write_top10", side_effect=capture_top10),
        patch.object(repository, "write_success_marker"),
    ):
        manifest = services.run()

    assert manifest["sources_reused_from_cache"] == 1
    assert manifest["sources_computed"] == 0
    upload.assert_not_called()  # nothing recomputed/rewritten
    # top-10 still assembled for the cached source (self excluded)
    ids = [r["target_chembl_id"] for r in top10["rows"]]
    assert "CHEMBL_ASP" not in ids
    assert ids[0] == "CHEMBL_SAL"
