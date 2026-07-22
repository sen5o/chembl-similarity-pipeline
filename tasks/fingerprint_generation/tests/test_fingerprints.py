"""Tests for the pure fingerprint core and the SMILES parser.

Reference on-bit counts are pinned against RDKit 2026.03 — if a future RDKit
changes Morgan output, these fail loudly rather than silently shifting every
similarity score downstream.
"""

from __future__ import annotations

from gen import fingerprints, molecule_parser

# name -> (SMILES, expected Morgan(2,2048) on-bit count)
REFERENCE = {
    "aspirin": ("CC(=O)Oc1ccccc1C(=O)O", 24),
    "caffeine": ("Cn1cnc2c1c(=O)n(C)c(=O)n2C", 25),
    "ethanol": ("CCO", 6),
}


def test_reference_on_bit_counts():
    for _name, (smi, expected) in REFERENCE.items():
        mol = molecule_parser.parse_smiles(smi)
        fp = fingerprints.compute_fingerprint(mol)
        assert fp.GetNumOnBits() == expected


def test_binary_round_trip_is_exact():
    mol = molecule_parser.parse_smiles(REFERENCE["aspirin"][0])
    fp = fingerprints.compute_fingerprint(mol)
    restored = fingerprints.from_binary(fingerprints.to_binary(fp))
    # same set bits before and after serialise/deserialise
    assert list(fp.GetOnBits()) == list(restored.GetOnBits())


def test_binary_is_compact():
    # RDKit compresses sparse vectors; aspirin is far under the naive 256 bytes.
    mol = molecule_parser.parse_smiles(REFERENCE["aspirin"][0])
    data = fingerprints.to_binary(fingerprints.compute_fingerprint(mol))
    assert isinstance(data, bytes)
    assert len(data) < 256


def test_parse_invalid_smiles_returns_none():
    assert molecule_parser.parse_smiles("not_a_smiles") is None


def test_parse_empty_and_none_return_none():
    assert molecule_parser.parse_smiles("") is None
    assert molecule_parser.parse_smiles(None) is None
