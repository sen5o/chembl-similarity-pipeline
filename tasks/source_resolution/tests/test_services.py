"""Tests for services orchestration and the DQ gate.

S3 and Postgres are mocked; these test the wiring: dedup by chembl_id with
flag union, quarantine routing, and gate thresholds. End-to-end against real
S3/Postgres is a manual run (python run.py resolve).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from gen import repository, services
from gen.repository import Candidate


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("DE_SCHOOL_S3_BUCKET", "bucket")
    monkeypatch.setenv("INPUT_S3_PREFIX", "input/x/")
    monkeypatch.setenv("S3_ROOT_PREFIX", "final_task/x")
    monkeypatch.setenv("DWH_DSN", "postgresql://u:p@localhost/dwh")


def _run_with(csv_text: str, candidates: list[Candidate], tmp_path: Path) -> tuple[dict, MagicMock]:
    """Drive resolve_sources with one in-memory CSV and a fixed candidate set."""
    csv_file = tmp_path / "batch.csv"
    csv_file.write_text(csv_text)

    def fake_download(bucket, key, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(csv_text)
        return dest

    with (
        patch.object(repository, "list_input_csvs", return_value=["input/x/batch.csv"]),
        patch.object(repository, "download_file", side_effect=fake_download),
        patch.object(repository, "fetch_candidates", return_value=candidates),
        patch.object(repository, "upload_file"),
        patch.object(repository, "write_parquet") as write_parquet,
    ):
        metrics = services.resolve_sources()
    return metrics, write_parquet


def _cand(name, chembl_id, mw, structure=True):
    return Candidate(
        normalised_name=name,
        chembl_id=chembl_id,
        mw_freebase=mw,
        full_mwt=None,
        has_structure=structure,
    )


def test_happy_path_resolves_and_writes_resolved(tmp_path):
    csv = "compound_name,molecular_weight\nAspirin,180.16\nIbuprofen,206.28\n"
    cands = [_cand("ASPIRIN", "CHEMBL25", 180.16), _cand("IBUPROFEN", "CHEMBL521", 206.28)]
    metrics, write_parquet = _run_with(csv, cands, tmp_path)

    assert metrics["resolved"] == 2
    assert metrics["resolution_rate"] == 1.0
    # first write_parquet call is resolved.parquet
    resolved_rows = write_parquet.call_args_list[0].args[0]
    assert {r["chembl_id"] for r in resolved_rows} == {"CHEMBL25", "CHEMBL521"}


def test_dedup_by_chembl_id_unions_flags_and_collects_sources(tmp_path):
    # Same compound twice: once with MW (verified), once without (mw_unverified).
    csv = "compound_name,molecular_weight\nAspirin,180.16\nAspirin,\n"
    cands = [_cand("ASPIRIN", "CHEMBL25", 180.16)]
    metrics, write_parquet = _run_with(csv, cands, tmp_path)

    assert metrics["resolved"] == 1  # collapsed to one chembl_id
    resolved_rows = write_parquet.call_args_list[0].args[0]
    assert len(resolved_rows) == 1
    row = resolved_rows[0]
    assert "mw_unverified" in row["dq_flags"]  # flag from the second row preserved
    # both source rows recorded
    import json

    assert len(json.loads(row["source_refs"])) == 2


def test_empty_name_is_quarantined_not_counted_against_rate(tmp_path):
    # One valid, one empty-name (DQ reject). Rate must be 100% (empty name is
    # input dirtiness, not a resolution failure).
    csv = "compound_name,molecular_weight\nAspirin,180.16\n,150.0\n"
    cands = [_cand("ASPIRIN", "CHEMBL25", 180.16)]
    metrics, write_parquet = _run_with(csv, cands, tmp_path)

    assert metrics["resolved"] == 1
    assert metrics["dq_rejected"] == 1
    assert metrics["resolution_rate"] == 1.0  # denominator excludes DQ rejects
    quarantine_rows = write_parquet.call_args_list[1].args[0]
    assert quarantine_rows[0]["reject_reason"] == "empty_name"


def test_gate_fails_when_nothing_resolves(tmp_path):
    csv = "compound_name,molecular_weight\nMysteryX,100.0\n"
    cands: list[Candidate] = []  # no candidate -> unresolved
    with pytest.raises(RuntimeError, match="0 molecules resolved"):
        _run_with(csv, cands, tmp_path)


def test_gate_fails_below_50_percent(tmp_path):
    # 1 resolved, 2 unresolved -> 33% -> below fail threshold
    csv = "compound_name,molecular_weight\nAspirin,180.16\nMysteryX,100.0\nMysteryY,200.0\n"
    cands = [_cand("ASPIRIN", "CHEMBL25", 180.16)]
    with pytest.raises(RuntimeError, match="below 50%"):
        _run_with(csv, cands, tmp_path)
