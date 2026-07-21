"""Orchestration for source resolution.

resolve_sources()  parse every input CSV -> validate (DQ) -> resolve names to
                   chembl_id -> dedup by chembl_id (union flags, collect
                   sources) -> write resolved.parquet + quarantine.parquet to
                   silver/source_set/ -> run the DQ gate.

The flow processes all valid unique compounds in the input, whatever their
number — no molecule count is hardcoded (the brief's "100" is illustrative;
the source set is "all valid unique compounds", per mentor guidance).

Processing is batch-in-memory, not streamed, and that is deliberate: the
input is a set of compound names (tens to low thousands of rows, a few
hundred KB), and the candidate lookup is a narrow IN-query returning only
matches — never the millions of rows a full ChEMBL table holds. Streaming
would add generators/chunking to guard against a memory pressure that cannot
occur at this scale, so we don't. (Contrast chembl_ingestion, which streams
because it genuinely moves millions of rows.)
"""

from __future__ import annotations

import json
import logging
import os
from collections import OrderedDict
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from . import file_parser, repository, resolver, utils, validators

log = logging.getLogger("source_resolution")

RESOLVED_SCHEMA = pa.schema(
    [
        ("chembl_id", pa.string()),
        ("input_name", pa.string()),
        ("molecular_weight", pa.float64()),
        ("resolution_method", pa.string()),
        ("dq_flags", pa.string()),  # comma-separated; flag is a row attribute
        ("source_refs", pa.string()),  # JSON array of {source_file,row,compound_id}
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        ("input_name", pa.string()),
        ("compound_id", pa.string()),
        ("source_file", pa.string()),
        ("row_number", pa.int64()),
        ("reject_reason", pa.string()),
        ("dq_flags", pa.string()),
    ]
)

# DQ gate thresholds (design C-3): the gate catches a *systemic* failure, not
# a slightly-lower-than-usual rate. Rate is computed over rows that reached
# the resolver (passed DQ), never over raw input — dirty rows correctly
# quarantined at parse time must not count as resolution failures.
GATE_FAIL_BELOW = 0.50
GATE_WARN_BELOW = 0.90


def _source_ref(row: file_parser.SourceRow) -> dict:
    return {
        "source_file": row.source_file,
        "row_number": row.row_number,
        "compound_id": row.compound_id,
        "input_name": row.compound_name,
    }


def resolve_sources() -> dict:
    """Run the full resolution flow and return the DQ metrics dict."""
    in_bucket = os.environ["DE_SCHOOL_S3_BUCKET"]
    in_prefix = os.environ["INPUT_S3_PREFIX"]  # e.g. input/hovhannes-karapetian/
    out_prefix = os.environ["S3_ROOT_PREFIX"].strip("/")  # final_task/<name>
    dsn = utils.dwh_dsn()

    with TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        # 1. Parse + validate every input CSV.
        parsed: list[file_parser.SourceRow] = []
        for key in repository.list_input_csvs(in_bucket, in_prefix):
            local = tmp_dir / Path(key).name
            repository.download_file(in_bucket, key, local)
            for row in file_parser.parse_file(local):
                parsed.append(validators.validate(row))

        dq_rejected = [r for r in parsed if r.reject_reason]
        to_resolve = [r for r in parsed if not r.reject_reason]

        # 2. Batch candidate lookup: one query for all names. (compound_name
        # is guaranteed non-None here — empty names were rejected in DQ — but
        # we narrow explicitly so the type checker agrees.)
        names = sorted(
            {
                utils.normalise_name(r.compound_name)
                for r in to_resolve
                if r.compound_name is not None
            }
        )
        candidates = repository.fetch_candidates(dsn, names)
        by_name: dict[str, list[repository.Candidate]] = {}
        for c in candidates:
            by_name.setdefault(c.normalised_name, []).append(c)

        # 3. Resolve each row.
        resolved_rows: list[tuple[file_parser.SourceRow, resolver.Resolution]] = []
        resolution_quarantine: list[tuple[file_parser.SourceRow, resolver.Resolution]] = []
        for row in to_resolve:
            name = row.compound_name
            if name is None:  # defensive: DQ already rejected empty names
                continue
            res = resolver.resolve(row, by_name.get(utils.normalise_name(name), []))
            if res.chembl_id:
                resolved_rows.append((row, res))
            else:
                resolution_quarantine.append((row, res))

        # 4. Dedup resolved by chembl_id: union flags, collect all sources.
        merged: OrderedDict[str, dict] = OrderedDict()
        for row, res in resolved_rows:
            chembl_id = res.chembl_id
            if chembl_id is None:  # only truthy ids reach resolved_rows
                continue
            entry = merged.get(chembl_id)
            if entry is None:
                merged[chembl_id] = {
                    "chembl_id": chembl_id,
                    "input_name": row.compound_name,
                    "molecular_weight": row.molecular_weight,
                    "resolution_method": res.method,
                    "flags": set(res.dq_flags),
                    "sources": [_source_ref(row)],
                }
            else:
                entry["flags"].update(res.dq_flags)
                entry["sources"].append(_source_ref(row))

        resolved_out = [
            {
                "chembl_id": e["chembl_id"],
                "input_name": e["input_name"],
                "molecular_weight": e["molecular_weight"],
                "resolution_method": e["resolution_method"],
                "dq_flags": ",".join(sorted(e["flags"])),
                "source_refs": json.dumps(e["sources"]),
            }
            for e in merged.values()
        ]

        # DQ rejects and resolution-failures both go to quarantine.
        quarantine_out = [
            {
                "input_name": r.compound_name,
                "compound_id": r.compound_id,
                "source_file": r.source_file,
                "row_number": r.row_number,
                "reject_reason": r.reject_reason,
                "dq_flags": ",".join(sorted(r.dq_flags)),
            }
            for r in dq_rejected
        ]
        quarantine_out += [
            {
                "input_name": row.compound_name,
                "compound_id": row.compound_id,
                "source_file": row.source_file,
                "row_number": row.row_number,
                "reject_reason": res.reject_reason,
                "dq_flags": ",".join(sorted(res.dq_flags)),
            }
            for row, res in resolution_quarantine
        ]

        # 5. Write parquet + upload.
        resolved_path = tmp_dir / "resolved.parquet"
        quarantine_path = tmp_dir / "quarantine.parquet"
        repository.write_parquet(resolved_out, RESOLVED_SCHEMA, resolved_path)
        repository.write_parquet(quarantine_out, QUARANTINE_SCHEMA, quarantine_path)

        base = f"{out_prefix}/silver/source_set"
        repository.upload_file(resolved_path, in_bucket, f"{base}/resolved.parquet")
        repository.upload_file(quarantine_path, in_bucket, f"{base}/quarantine.parquet")

        metrics = _dq_gate(
            parsed=len(parsed),
            dq_rejected=len(dq_rejected),
            resolved=len(merged),
            resolution_quarantined=len(resolution_quarantine),
            quarantine_reasons=_reason_counts(dq_rejected, resolution_quarantine),
        )
        return metrics


def _reason_counts(dq_rejected, resolution_quarantine) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in dq_rejected:
        counts[r.reject_reason] = counts.get(r.reject_reason, 0) + 1
    for _row, res in resolution_quarantine:
        counts[res.reject_reason] = counts.get(res.reject_reason, 0) + 1
    return counts


def _dq_gate(
    parsed: int,
    dq_rejected: int,
    resolved: int,
    resolution_quarantined: int,
    quarantine_reasons: dict[str, int],
) -> dict:
    """Log metrics always; fail the task only on a systemic collapse.

    Rate denominator is rows that reached the resolver (resolved +
    resolution_quarantined), not raw parsed rows — DQ rejects (empty name,
    schema drift) are input dirtiness, not resolution failures.
    """
    reached_resolver = resolved + resolution_quarantined
    rate = resolved / reached_resolver if reached_resolver else 0.0

    metrics = {
        "parsed": parsed,
        "dq_rejected": dq_rejected,
        "reached_resolver": reached_resolver,
        "resolved": resolved,
        "resolution_quarantined": resolution_quarantined,
        "resolution_rate": round(rate, 4),
        "quarantine_reasons": quarantine_reasons,
    }
    log.info("DQ metrics: %s", metrics)

    if resolved == 0:
        raise RuntimeError(f"DQ gate: 0 molecules resolved — systemic failure. Metrics: {metrics}")
    if rate < GATE_FAIL_BELOW:
        raise RuntimeError(
            f"DQ gate: resolution rate {rate:.1%} below {GATE_FAIL_BELOW:.0%}. Metrics: {metrics}"
        )
    if rate < GATE_WARN_BELOW:
        log.warning("DQ gate: resolution rate %.1f%% below warn threshold", rate * 100)

    return metrics
