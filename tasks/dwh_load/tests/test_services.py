"""Tests for dwh_load orchestration. S3 and Postgres are mocked.

These cover the wiring and the guards: which molecules end up in the dimension,
what happens when a fact references a molecule staging doesn't know, and that
the manifest reconciles facts in vs facts loaded.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from gen import repository, services

_FACTS = [
    {
        "source_chembl_id": "CHEMBL25",
        "target_chembl_id": "CHEMBL521",
        "target_molregno": 2,
        "tanimoto_score": 0.83,
        "has_duplicates_of_last_largest_score": False,
    },
    {
        "source_chembl_id": "CHEMBL25",
        "target_chembl_id": "CHEMBL113",
        "target_molregno": 3,
        "tanimoto_score": 0.71,
        "has_duplicates_of_last_largest_score": True,
    },
    {
        "source_chembl_id": "CHEMBL521",
        "target_chembl_id": "CHEMBL25",
        "target_molregno": 1,
        "tanimoto_score": 0.83,
        "has_duplicates_of_last_largest_score": False,
    },
]


def _props(chembl_id):
    return {
        "chembl_id": chembl_id,
        "molecule_type": "Small molecule",
        "mw_freebase": 180.16,
        "alogp": 1.2,
        "psa": 63.6,
        "cx_logp": 1.1,
        "molecular_species": "ACID",
        "full_mwt": 180.16,
        "aromatic_rings": 1,
        "heavy_atoms": 13,
    }


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("CHEMBL_RELEASE", "35")
    monkeypatch.setenv("DE_SCHOOL_S3_BUCKET", "bucket")
    monkeypatch.setenv("S3_ROOT_PREFIX", "final_task/x")
    monkeypatch.setenv("DWH_DSN", "postgresql://u:p@localhost/dwh")


def test_dimension_covers_sources_and_targets_deduplicated():
    """CHEMBL25 is both a source and a target; it must appear once."""
    captured = {}

    def fake_fetch(dsn, ids):
        captured["ids"] = ids
        return [_props(i) for i in ids]

    with (
        patch.object(repository, "read_top10", return_value=_FACTS),
        patch.object(repository, "fetch_molecule_properties", side_effect=fake_fetch),
        patch.object(repository, "load_star_schema", return_value=(3, 3)),
    ):
        manifest = services.run()

    # three distinct molecules across sources+targets, no duplicates
    assert captured["ids"] == ["CHEMBL113", "CHEMBL25", "CHEMBL521"]
    assert manifest["dimension_rows"] == 3
    assert manifest["facts_in"] == 3
    assert manifest["facts_loaded"] == 3
    assert manifest["sources"] == 2


def test_fact_referencing_unknown_molecule_fails_loudly():
    """A fact pointing at a molecule staging doesn't have must abort the load
    with a clear message, not a KeyError inside the insert."""
    with (
        patch.object(repository, "read_top10", return_value=_FACTS),
        # staging only knows two of the three molecules
        patch.object(
            repository,
            "fetch_molecule_properties",
            return_value=[_props("CHEMBL25"), _props("CHEMBL521")],
        ),
        patch.object(repository, "load_star_schema") as load,
        pytest.raises(RuntimeError, match="missing from"),
    ):
        services.run()

    load.assert_not_called()  # nothing written when the guard trips


def test_empty_top10_is_rejected():
    with (
        patch.object(repository, "read_top10", return_value=[]),
        patch.object(repository, "load_star_schema") as load,
        pytest.raises(RuntimeError, match="empty"),
    ):
        services.run()

    load.assert_not_called()


def test_facts_passed_through_with_tie_flag_intact():
    captured = {}

    def fake_load(dsn, dim_rows, fact_rows):
        captured["facts"] = fact_rows
        captured["dim"] = dim_rows
        return len(dim_rows), len(fact_rows)

    with (
        patch.object(repository, "read_top10", return_value=_FACTS),
        patch.object(
            repository,
            "fetch_molecule_properties",
            return_value=[_props(i) for i in ("CHEMBL113", "CHEMBL25", "CHEMBL521")],
        ),
        patch.object(repository, "load_star_schema", side_effect=fake_load),
    ):
        services.run()

    flags = [f["has_duplicates_of_last_largest_score"] for f in captured["facts"]]
    assert flags == [False, True, False]  # tie flag survives the hand-off
