"""Pure ranking logic for step 5: top-10 nearest neighbours + tie flag.

No IO. Operates on the full similarity list for a single source —
(target_chembl_id, target_molregno, score) — and returns the top-10 plus the
`has_duplicates_of_last_largest_score` flag.

Performance: the full per-source table is 2.47M rows, and this runs 56 times.
We avoid a full sort (~6s each -> ~5.6 min total). heapq.nlargest picks the
top-10 (~0.12s), then a single linear pass counts rows at the cut-off score.
Same result, ~50x faster, no full sorted copy beside the resident corpus.

Tie flag (subtlest part of the brief): after excluding the source itself, take
the top 10 by score. Let S be the 10th (cut-off) score. If more rows have
score == S than fit inside the top-10 (i.e. ties spill past the boundary),
every top-10 row with score == S is flagged True. Grain is per-row because
fact_similarity is (source, target).
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass

TOP_N = 10


@dataclass
class RankedRow:
    target_chembl_id: str
    target_molregno: int
    tanimoto_score: float
    has_duplicates_of_last_largest_score: bool


def rank_top_n(rows, source_chembl_id: str, top_n: int = TOP_N) -> list[RankedRow]:
    """Top-`top_n` neighbours of one source, with the tie flag set.

    `rows` is any iterable of (target_chembl_id, target_molregno, score),
    consumed once into a transient list (freed on return).
    """
    candidates = [r for r in rows if r[0] != source_chembl_id]
    if not candidates:
        return []

    # top-N by (score desc, chembl_id asc), deterministic, without a full sort
    top = _deterministic_top(candidates, top_n)
    top.sort(key=lambda r: (-r[2], r[0]))  # stable, readable final order

    cutoff_score = top[-1][2]
    total_at_cutoff = sum(1 for r in candidates if r[2] == cutoff_score)
    in_top_at_cutoff = sum(1 for r in top if r[2] == cutoff_score)
    ties_spill_past_cutoff = total_at_cutoff > in_top_at_cutoff

    return [
        RankedRow(
            target_chembl_id=cid,
            target_molregno=mol,
            tanimoto_score=score,
            has_duplicates_of_last_largest_score=(ties_spill_past_cutoff and score == cutoff_score),
        )
        for cid, mol, score in top
    ]


def _deterministic_top(candidates, top_n):
    """Select the top_n rows by (score desc, chembl_id asc) deterministically,
    without a full sort: nlargest on a compound key that orders score desc then
    chembl_id asc. heapq.nlargest is deterministic given a total-order key.
    """
    # key: higher score first; for equal scores, smaller chembl_id first ->
    # invert the string via a wrapper so "largest" means smaller id.
    return heapq.nlargest(top_n, candidates, key=lambda r: (r[2], _IdAsc(r[0])))


class _IdAsc:
    """Order wrapper: makes a smaller string compare as 'larger' so that under
    heapq.nlargest, equal scores resolve toward the alphabetically smaller id.
    """

    __slots__ = ("s",)

    def __init__(self, s: str) -> None:
        self.s = s

    def __lt__(self, other: _IdAsc) -> bool:
        return self.s > other.s

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _IdAsc) and self.s == other.s
