"""SMILES -> RDKit Mol, with explicit handling of unparseable input.

Separated from fingerprints.py so the "can this string become a molecule?"
concern is isolated and testable on its own. RDKit prints parse errors to its
own C++ logger on failure; we silence that here and surface failures as a
plain None return plus our own structured log, so a handful of bad SMILES in a
2.4M-row corpus never crash a partition — they're counted as a DQ metric
(structures_in vs fingerprints_out) instead.
"""

from __future__ import annotations

import logging

from rdkit import Chem, RDLogger

log = logging.getLogger("fingerprint_generation")

# RDKit logs every parse failure to its native logger; at 2.4M molecules a
# few thousand failures would flood stderr. Silence RDKit's own logger — we
# report failures ourselves, aggregated.
RDLogger.DisableLog("rdApp.*")  # type: ignore[attr-defined] # method exists; RDKit stubs incomplete


def parse_smiles(smiles: str | None):
    """Return an RDKit Mol, or None if the SMILES is missing/unparseable.

    None is a normal, expected outcome for a small fraction of ChEMBL rows
    (malformed or exotic structures); the caller counts these rather than
    treating them as errors.
    """
    if not smiles:
        return None
    return Chem.MolFromSmiles(smiles)
