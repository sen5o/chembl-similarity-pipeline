"""Fingerprint decode, local to this task.

Container-per-task means we don't import across tasks; the encode side lives in
fingerprint_generation. This is the matching decode: reconstruct an
ExplicitBitVect from the bytes stored in the fingerprints parquet, so the
similarity stage can feed them to BulkTanimotoSimilarity. Kept in one function
to make the pairing with the producer obvious.
"""

from __future__ import annotations

from rdkit.DataStructs import ExplicitBitVect


def from_binary(data: bytes) -> ExplicitBitVect:
    """Inverse of RDKit's ExplicitBitVect.ToBinary()."""
    return ExplicitBitVect(data)  # type: ignore[call-overload]
