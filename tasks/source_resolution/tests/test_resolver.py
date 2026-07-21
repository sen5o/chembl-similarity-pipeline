"""Tests for resolver: tier logic and MW verification/disambiguation.

Pure unit tests — Candidates are constructed directly, no Postgres. The DB
query itself is exercised separately as an integration test.
"""

from __future__ import annotations

from gen.file_parser import SourceRow
from gen.repository import Candidate
from gen.resolver import resolve


def _row(name="Aspirin", mw=180.16, **kw) -> SourceRow:
    base = dict(
        source_file="f.csv",
        row_number=1,
        compound_id="CPD-1",
        compound_name=name,
        molecular_weight=mw,
        ic50=100.0,
    )
    base.update(kw)
    return SourceRow(**base)


def _cand(chembl_id="CHEMBL25", mw=180.16, full=None, structure=True) -> Candidate:
    return Candidate(
        normalised_name="ASPIRIN",
        chembl_id=chembl_id,
        mw_freebase=mw,
        full_mwt=full,
        has_structure=structure,
    )


def test_single_candidate_matching_mw_resolves():
    r = resolve(_row(), [_cand()])
    assert r.chembl_id == "CHEMBL25"
    assert r.method == "tier1_exact"
    assert r.reject_reason is None


def test_single_candidate_gross_mw_mismatch_is_rejected():
    # input MW 180 vs candidate 999 -> outside tolerance
    r = resolve(_row(mw=180.16), [_cand(mw=999.0)])
    assert r.chembl_id is None
    assert r.reject_reason == "mw_mismatch"


def test_mw_within_relative_tolerance_accepts():
    # 800 vs 805 -> within 1% (allowed = max(0.5, 8.0) = 8.0)
    r = resolve(_row(mw=800.0), [_cand(mw=805.0)])
    assert r.chembl_id == "CHEMBL25"


def test_full_mwt_salt_weight_can_confirm():
    # freebase far off, but salt weight (full_mwt) matches input -> accept
    r = resolve(_row(mw=250.0), [_cand(mw=180.0, full=250.2)])
    assert r.chembl_id == "CHEMBL25"


def test_missing_input_mw_accepts_on_name_alone_with_flag():
    r = resolve(_row(mw=None), [_cand()])
    assert r.chembl_id == "CHEMBL25"
    assert "mw_unverified" in r.dq_flags


def test_ambiguous_name_disambiguated_by_mw():
    cands = [
        _cand(chembl_id="CHEMBL1", mw=180.16),  # matches input MW
        _cand(chembl_id="CHEMBL2", mw=500.0),  # does not
    ]
    r = resolve(_row(mw=180.16), cands)
    assert r.chembl_id == "CHEMBL1"
    assert r.method == "tier1_mw_disambig"
    assert "disambiguated_by_mw" in r.dq_flags


def test_ambiguous_name_unresolvable_is_quarantined():
    # two candidates both within tolerance -> MW can't pick one
    cands = [_cand(chembl_id="CHEMBL1", mw=180.16), _cand(chembl_id="CHEMBL2", mw=180.16)]
    r = resolve(_row(mw=180.16), cands)
    assert r.chembl_id is None
    assert r.reject_reason == "ambiguous"


def test_candidate_without_structure_is_quarantined():
    r = resolve(_row(), [_cand(structure=False)])
    assert r.chembl_id is None
    assert r.reject_reason == "no_structure"


def test_no_candidates_is_unresolved():
    r = resolve(_row(), [])
    assert r.chembl_id is None
    assert r.reject_reason == "unresolved"
