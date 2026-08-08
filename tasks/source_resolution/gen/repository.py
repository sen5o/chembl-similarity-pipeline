"""IO boundary for source resolution: S3 input/output + the ChEMBL lookup.

The ChEMBL candidate lookup runs one narrow query: given the normalised
input names, return every matching molecule_dictionary row joined to its
properties (MW) and a flag for whether it has a structure. Returning *all*
matches per name (not LIMIT 1) is deliberate — the resolver needs to see
multiplicity to detect ambiguity. Everything the resolver decides is
computed in Python from this result; this module only reads/writes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("source_resolution")


# --------------------------------------------------------------------------
# S3 — input CSVs and output parquet
# --------------------------------------------------------------------------


def list_input_csvs(bucket: str, prefix: str) -> list[str]:
    """List keys of *.csv objects under the input prefix (any count)."""
    s3 = boto3.client("s3")
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                keys.append(obj["Key"])
    log.info("Found %s input CSV(s) under s3://%s/%s", len(keys), bucket, prefix)
    return keys


def download_file(bucket: str, key: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    boto3.client("s3").download_file(bucket, key, str(dest))
    return dest


def write_parquet(rows: list[dict], schema: pa.Schema, dest: Path) -> None:
    """Write rows (list of dicts) to a parquet file with an explicit schema.
    An empty list still writes a valid empty parquet with the right schema.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        table = pa.Table.from_pylist(rows, schema=schema)
    else:
        table = schema.empty_table()
    pq.write_table(table, dest)
    log.info("Wrote %s rows -> %s", len(rows), dest)


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    boto3.client("s3").upload_file(str(local_path), bucket, key)
    log.info("Uploaded %s -> s3://%s/%s", local_path, bucket, key)


# --------------------------------------------------------------------------
# ChEMBL candidate lookup (Postgres)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Candidate:
    """A ChEMBL molecule that matched an input name by pref_name."""

    normalised_name: str
    chembl_id: str
    mw_freebase: float | None
    full_mwt: float | None
    has_structure: bool


def fetch_candidates(dsn: str, normalised_names: list[str]) -> list[Candidate]:
    """Return all Tier-1 candidates for the given normalised names.

    Matches upper(trim(pref_name)) against the names (mirroring
    utils.normalise_name). Joins compound_properties for MW and left-joins
    compound_structures to flag structure availability. One round trip.
    """
    if not normalised_names:
        return []

    import psycopg2

    query = """
        SELECT
            upper(btrim(md.pref_name))          AS normalised_name,
            md.chembl_id,
            cp.mw_freebase,
            cp.full_mwt,
            (cs.molregno IS NOT NULL)           AS has_structure
        FROM staging.molecule_dictionary md
        LEFT JOIN staging.compound_properties cp ON cp.molregno = md.molregno
        LEFT JOIN staging.compound_structures cs
               ON cs.molregno = md.molregno
              AND cs.canonical_smiles IS NOT NULL
        WHERE upper(btrim(md.pref_name)) = ANY(%s)
    """

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (normalised_names,))
            rows = cur.fetchall()
    finally:
        conn.close()

    candidates = [
        Candidate(
            normalised_name=r[0],
            chembl_id=r[1],
            mw_freebase=r[2],
            full_mwt=r[3],
            has_structure=r[4],
        )
        for r in rows
    ]
    log.info(
        "Fetched %s candidate rows for %s names",
        len(candidates),
        len(normalised_names),
    )
    return candidates
