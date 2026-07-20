"""Column mapping: from an arbitrary CSV header to our canonical fields.

The input files are not guaranteed to share a schema — observed drift:
columns reordered, `logp` present or absent, `ic50_nm` vs `IC50_nM`,
`assay_date` vs `collection_date`. We therefore never key off column
position or count; we match header names to canonical fields
case-insensitively, tolerating spaces/underscores. Anything we don't
recognise is ignored; the only field we truly require is the compound name.
"""

from __future__ import annotations

# Canonical field -> accepted header spellings (compared after normalisation:
# lowercased, spaces and underscores stripped). Extend these sets rather than
# adding position/count logic if a new input spelling appears.
_SYNONYMS: dict[str, set[str]] = {
    "compound_name": {"compoundname", "name", "moleculename"},
    "molecular_weight": {"molecularweight", "mw", "molweight", "mwt"},
    "ic50": {"ic50", "ic50nm", "ic50um", "ic50value"},
    "compound_id": {"compoundid", "cpdid", "id"},
}


def _normalise(header: str) -> str:
    return header.strip().lower().replace(" ", "").replace("_", "")


def build_column_map(header: list[str]) -> dict[str, str | None]:
    """Map each canonical field to the actual header column that provides it.

    Returns {canonical_field: actual_column_name or None}. A field is None
    when no column matches — callers must treat missing compound_name as a
    file-level schema problem (every row quarantined), and missing
    molecular_weight/ic50 as simply "unavailable" (rows still valid).
    """
    normalised = {_normalise(col): col for col in header if col}
    mapping: dict[str, str | None] = {}
    for canonical, spellings in _SYNONYMS.items():
        actual = next(
            (normalised[n] for n in normalised if n in spellings),
            None,
        )
        mapping[canonical] = actual
    return mapping


def has_name_column(column_map: dict[str, str | None]) -> bool:
    """Whether the file exposes a compound-name column at all. If not, the
    file cannot yield any resolvable rows and all of them are quarantined.
    """
    return column_map.get("compound_name") is not None
