"""Pure fingerprint computation. No IO, no config — just chemistry.

Morgan fingerprint, radius 2, 2048 bits, per the brief (step 2). Uses RDKit's
current generator API (rdFingerprintGenerator.GetMorganGenerator); the older
GetMorganFingerprintAsBitVect is deprecated.

Kept deliberately small and side-effect-free so it can be unit-tested against
known reference values (aspirin -> 24 on-bits, etc.) without any database,
S3, or filesystem.
"""

from __future__ import annotations

from functools import lru_cache

from rdkit.Chem import rdFingerprintGenerator
from rdkit.DataStructs import ExplicitBitVect

RADIUS = 2
N_BITS = 2048


@lru_cache(maxsize=1)
def _generator() -> rdFingerprintGenerator.FingerprintGenerator64:
    """Morgan generator is reusable and thread-independent; build it once.

    Cached so we don't reconstruct it per molecule across a 155k-row
    partition.
    """
    return rdFingerprintGenerator.GetMorganGenerator(radius=RADIUS, fpSize=N_BITS)


def compute_fingerprint(mol) -> ExplicitBitVect:
    """RDKit Mol -> Morgan(radius=2, 2048-bit) ExplicitBitVect.

    Caller is responsible for passing a valid, non-None Mol (see
    molecule_parser.parse_smiles). This function does not guard against None
    on purpose: a None here is a programming error upstream, not data dirt.
    """
    return _generator().GetFingerprint(mol)


def to_binary(fp: ExplicitBitVect) -> bytes:
    """Serialise a fingerprint to RDKit's native compact binary form.

    This is what we persist (parquet BYTES column). RDKit compresses sparse
    vectors, so a 2048-bit fp is far smaller than 256 bytes (aspirin ~41 B).
    Round-trips exactly via from_binary(), which is what the similarity stage
    needs to reconstruct ExplicitBitVects for BulkTanimotoSimilarity.
    """
    return fp.ToBinary()


def from_binary(data: bytes) -> ExplicitBitVect:
    """Inverse of to_binary(): reconstruct an ExplicitBitVect from stored
    bytes. Lives here (next to to_binary) so the encode/decode pair can never
    drift apart; the similarity task imports this.
    """
    # ExplicitBitVect(bytes) round-trips ToBinary(); RDKit stubs omit this overload.
    return ExplicitBitVect(data)  # type: ignore[call-overload]
