"""CSV -> normalised SourceRow records.

Designed to survive arbitrary input: BOM, surrounding whitespace, quoted
fields, ragged rows (too few/too many columns), and unknown extra columns
are all tolerated. Nothing here rejects a row — parsing only extracts and
normalises; DQ decisions (reject vs warn) live in validators.py.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from . import parser_factory

# Tokens that mean "no value" in the numeric input columns.
_NULLISH = {"", "n/a", "na", "null", "none", "nan", "-"}


@dataclass
class SourceRow:
    source_file: str
    row_number: int
    compound_id: str | None
    compound_name: str | None
    molecular_weight: float | None
    ic50: float | None
    dq_flags: list[str] = field(default_factory=list)
    reject_reason: str | None = None


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_number(value: str | None) -> float | None:
    """Parse a numeric cell, returning None for nullish/garbage rather than
    raising — a bad number never crashes the parse.
    """
    cleaned = _clean(value)
    if cleaned is None or cleaned.lower() in _NULLISH:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_file(path: Path) -> Iterator[SourceRow]:
    """Yield one SourceRow per data row. If the file has no compound-name
    column, every row is still yielded but flagged `schema_drift_no_name`
    (validators will reject it) — we never silently drop rows.
    """
    source_file = path.name
    # utf-8-sig transparently strips a BOM if present.
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        column_map = parser_factory.build_column_map(list(header))
        name_col = column_map.get("compound_name")
        mw_col = column_map.get("molecular_weight")
        ic50_col = column_map.get("ic50")
        id_col = column_map.get("compound_id")
        file_has_name = parser_factory.has_name_column(column_map)

        for i, raw in enumerate(reader, start=1):
            row = SourceRow(
                source_file=source_file,
                row_number=i,
                compound_id=_clean(raw.get(id_col)) if id_col else None,
                compound_name=_clean(raw.get(name_col)) if name_col else None,
                molecular_weight=_parse_number(raw.get(mw_col)) if mw_col else None,
                ic50=_parse_number(raw.get(ic50_col)) if ic50_col else None,
            )
            if not file_has_name:
                row.dq_flags.append("schema_drift_no_name")
            yield row
