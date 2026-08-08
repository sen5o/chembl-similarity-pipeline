"""Tests for the two pure cores: similarity and ranking.

Ranking gets the most attention — the tie flag (step 5) is the subtlest part
of the brief, so it's exercised with constructed boundary cases: no tie, a tie
that spills past the cut-off, a tie that sits entirely inside the top-10, and
self-match exclusion.
"""

from __future__ import annotations

from gen import ranking, similarity
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator

_gen = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)


def _fp(smiles: str):
    return _gen.GetFingerprint(Chem.MolFromSmiles(smiles))


# --- similarity core -------------------------------------------------------


def test_bulk_tanimoto_reference_values():
    aspirin = _fp("CC(=O)Oc1ccccc1C(=O)O")
    salicylic = _fp("O=C(O)c1ccccc1O")
    ethanol = _fp("CCO")
    scores = similarity.bulk_tanimoto(aspirin, [aspirin, salicylic, ethanol])
    assert scores[0] == 1.0  # self
    assert round(scores[1], 4) == 0.4483  # structurally close
    assert round(scores[2], 4) == 0.1111  # far


# --- ranking: self-match ---------------------------------------------------


def test_self_match_excluded():
    rows = [("SRC", 1, 1.0), ("A", 2, 0.9), ("B", 3, 0.8)]
    ranked = ranking.rank_top_n(rows, "SRC", top_n=2)
    ids = [r.target_chembl_id for r in ranked]
    assert "SRC" not in ids
    assert ids == ["A", "B"]


# --- ranking: the tie flag -------------------------------------------------


def test_no_tie_flag_all_false():
    # distinct scores, clean cut-off — nobody flagged
    rows = [(f"T{i}", i, 1.0 - i * 0.1) for i in range(15)]
    ranked = ranking.rank_top_n(rows, "SRC", top_n=10)
    assert all(not r.has_duplicates_of_last_largest_score for r in ranked)


def test_tie_spills_past_cutoff_flags_boundary_rows():
    # ranks 1-8 distinct high; ranks 9,10,11,12 all share score 0.50.
    # cut-off (10th) = 0.50 and 0.50 also appears at 11th/12th -> spill.
    rows = [(f"H{i}", i, 0.9 - i * 0.01) for i in range(8)]  # 8 distinct
    rows += [("E1", 101, 0.50), ("E2", 102, 0.50), ("E3", 103, 0.50), ("E4", 104, 0.50)]
    ranked = ranking.rank_top_n(rows, "SRC", top_n=10)

    assert len(ranked) == 10
    flagged = [r.target_chembl_id for r in ranked if r.has_duplicates_of_last_largest_score]
    # the two 0.50 rows that made the top-10 are flagged (the other two spilled out)
    assert len(flagged) == 2
    # the 8 high-score rows are not flagged
    for r in ranked:
        if r.tanimoto_score > 0.50:
            assert not r.has_duplicates_of_last_largest_score


def test_tie_entirely_inside_top10_not_flagged():
    # 10th score is unique; a tie exists but sits above the cut-off, fully
    # inside the top-10 -> no spill past boundary -> no flag.
    rows = [("A", 1, 0.9), ("B", 2, 0.9)]  # tie, but high and inside
    rows += [(f"C{i}", 10 + i, 0.8 - i * 0.01) for i in range(8)]  # fill to 10 distinct
    ranked = ranking.rank_top_n(rows, "SRC", top_n=10)
    assert all(not r.has_duplicates_of_last_largest_score for r in ranked)


def test_fewer_than_top_n_candidates():
    rows = [("A", 1, 0.9), ("B", 2, 0.8)]
    ranked = ranking.rank_top_n(rows, "SRC", top_n=10)
    assert len(ranked) == 2
    assert all(not r.has_duplicates_of_last_largest_score for r in ranked)


def test_deterministic_order_on_score_ties():
    # equal scores must break ties by chembl_id so output is stable across runs
    rows = [("Z", 1, 0.5), ("A", 2, 0.5), ("M", 3, 0.5)]
    ranked = ranking.rank_top_n(rows, "SRC", top_n=3)
    assert [r.target_chembl_id for r in ranked] == ["A", "M", "Z"]
