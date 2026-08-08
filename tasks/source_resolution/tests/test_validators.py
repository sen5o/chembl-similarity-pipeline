"""Tests for validators: the DQ taxonomy (reject vs warn)."""

from __future__ import annotations

from gen import validators
from gen.file_parser import SourceRow


def _row(**kwargs) -> SourceRow:
    base = dict(
        source_file="f.csv",
        row_number=1,
        compound_id="CPD-1",
        compound_name="Aspirin",
        molecular_weight=180.16,
        ic50=2500.0,
    )
    base.update(kwargs)
    return SourceRow(**base)


def test_valid_row_has_no_reject_and_no_flags():
    result = validators.validate(_row())
    assert result.reject_reason is None
    assert result.dq_flags == []


def test_empty_name_is_rejected():
    result = validators.validate(_row(compound_name=None))
    assert result.reject_reason == "empty_name"


def test_missing_name_column_is_rejected():
    row = _row(compound_name=None)
    row.dq_flags.append("schema_drift_no_name")
    result = validators.validate(row)
    assert result.reject_reason == "schema_drift_no_name"


def test_ic50_non_positive_is_warned_not_rejected():
    # Kristina's ruling: corrupt ic50 still references a valid compound.
    result = validators.validate(_row(ic50=0.0))
    assert result.reject_reason is None
    assert "ic50_non_positive" in result.dq_flags

    result_neg = validators.validate(_row(ic50=-5.0))
    assert result_neg.reject_reason is None
    assert "ic50_non_positive" in result_neg.dq_flags


def test_ic50_unavailable_is_warned_not_rejected():
    result = validators.validate(_row(ic50=None))
    assert result.reject_reason is None
    assert "ic50_unavailable" in result.dq_flags


def test_mw_unavailable_is_warned_not_rejected():
    result = validators.validate(_row(molecular_weight=None))
    assert result.reject_reason is None
    assert "mw_unavailable" in result.dq_flags
    # a compound with no MW is still a valid source — MW is only for verification
