"""Pure Tanimoto similarity. No IO, no config.

Thin wrapper over RDKit's BulkTanimotoSimilarity: one query fingerprint vs the
whole corpus in a single C++ call. Kept separate so it can be tested against
known reference values (aspirin vs salicylic acid = 0.4483, etc.) with no S3
or database.
"""

from __future__ import annotations

from rdkit import DataStructs
from rdkit.DataStructs import ExplicitBitVect


def bulk_tanimoto(query: ExplicitBitVect, corpus: list[ExplicitBitVect]) -> list[float]:
    """Tanimoto of `query` against every fingerprint in `corpus`, in order.

    Returns a plain list of floats aligned 1:1 with `corpus`. A molecule
    compared with itself scores exactly 1.0 (self-match), which the ranking
    stage removes by chembl_id.
    """
    return DataStructs.BulkTanimotoSimilarity(query, corpus)
