"""Tests for parser_factory + file_parser: robustness to arbitrary input."""

from __future__ import annotations

from pathlib import Path

from gen import file_parser, parser_factory

FIXTURES = Path(__file__).parent / "fixtures"


def test_column_map_is_case_and_separator_insensitive():
    header = ["IC50_nM", "compound_name", "Molecular Weight", "collection_date"]
    cmap = parser_factory.build_column_map(header)
    assert cmap["ic50"] == "IC50_nM"
    assert cmap["compound_name"] == "compound_name"
    assert cmap["molecular_weight"] == "Molecular Weight"
    # unmapped canonical fields are None, not errors
    assert cmap["compound_id"] is None


def test_column_map_handles_reordered_and_missing_columns():
    # batch_004 shape: no logp column at all
    header = ["compound_id", "compound_name", "molecular_weight", "ic50_nm", "lab_id"]
    cmap = parser_factory.build_column_map(header)
    assert cmap["compound_name"] == "compound_name"
    assert parser_factory.has_name_column(cmap) is True


def test_has_name_column_false_when_absent():
    cmap = parser_factory.build_column_map(["compound_id", "molecular_weight"])
    assert parser_factory.has_name_column(cmap) is False


def test_parse_valid_file_extracts_canonical_fields():
    rows = list(file_parser.parse_file(FIXTURES / "valid_batch_001.csv"))
    assert len(rows) == 3
    aspirin = rows[0]
    assert aspirin.compound_name == "Aspirin"
    assert aspirin.molecular_weight == 180.16
    assert aspirin.ic50 == 2500.0
    assert aspirin.compound_id == "CPD-001"
    assert aspirin.source_file == "valid_batch_001.csv"
    assert aspirin.row_number == 1


def test_parse_handles_empty_name_and_na_weight():
    rows = {r.compound_id: r for r in file_parser.parse_file(FIXTURES / "batch_004.csv")}
    assert rows["CPD-043"].compound_name is None  # empty name -> None
    assert rows["CPD-053"].molecular_weight is None  # "N/A" -> None
    assert rows["CPD-053"].compound_name == "Bupropion"


def test_parse_handles_nonnumeric_ic50_without_crashing():
    rows = {r.compound_name: r for r in file_parser.parse_file(FIXTURES / "batch_005_edge.csv")}
    assert rows["Escitalopram"].ic50 is None  # "notanumber" -> None, no crash
    assert rows["Venlafaxine"].ic50 == 0.0
    assert rows["Mirtazapine"].ic50 == -5.0


def test_parse_flags_file_without_name_column():
    rows = list(file_parser.parse_file(FIXTURES / "no_name_column.csv"))
    assert len(rows) == 1
    assert "schema_drift_no_name" in rows[0].dq_flags
    assert rows[0].compound_name is None
