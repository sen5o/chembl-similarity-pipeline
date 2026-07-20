"""Data-quality rules applied to a parsed SourceRow.

Principle (README DQ section): REJECT only on fields we consume. A source
row exists to yield a chembl_id that has a structure, so only a missing
name (or a file with no name column at all) is a reject at this stage. A
corrupt ic50 or an unavailable molecular_weight is a WARN — the row stays
valid, because we never use ic50 downstream and molecular_weight is only a
*verification* signal for the resolver, not a requirement.

Resolution-level rejects (name not found, ambiguous, no structure, MW
mismatch) are decided later, in resolver.py — not here.
"""

from __future__ import annotations

from .file_parser import SourceRow


def validate(row: SourceRow) -> SourceRow:
    """Annotate `row` in place with dq_flags / reject_reason and return it.

    reject_reason set  -> row goes to quarantine.
    reject_reason None -> row proceeds to the resolver (possibly with warns).
    """
    # File-level: no compound-name column at all (flagged by the parser).
    if "schema_drift_no_name" in row.dq_flags:
        row.reject_reason = "schema_drift_no_name"
        return row

    # Row-level REJECT: no usable name -> nothing to resolve.
    if not row.compound_name:
        row.reject_reason = "empty_name"
        return row

    # Row-level WARNs: kept as valid, just flagged.
    if row.ic50 is None:
        # Either the column was absent or the value was non-numeric/nullish.
        row.dq_flags.append("ic50_unavailable")
    elif row.ic50 <= 0:
        # Kristina: a corrupt ic50 still references a valid compound -> flag,
        # keep. We don't use ic50 for similarity anyway.
        row.dq_flags.append("ic50_non_positive")

    if row.molecular_weight is None:
        # MW can't be used to verify the resolved candidate for this row.
        row.dq_flags.append("mw_unavailable")

    return row
