"""IO boundary for fingerprint_generation: Postgres reads, parquet writes, S3.

read_corpus_partition() is the single place the "corpus" is defined. We chose
NOT to materialise a silver/corpus artifact (on ChEMBL 35 the filters remove
nothing — all structured molecules are ACTIVE COMPOUND — so it would duplicate
2.4M rows without cleaning them). Instead the corpus is a filtered, partitioned
read over staging, expressed here once. This function is also the seam where a
materialised corpus would slot in later: swap the query body to read
silver/corpus/ and nothing else changes.
"""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
import pyarrow as pa

log = logging.getLogger("fingerprint_generation")


# --------------------------------------------------------------------------
# Corpus read (the contract)
# --------------------------------------------------------------------------

# Curated compound universe for similarity: ACTIVE compounds that have a 2-D
# structure. Filters verified against ChEMBL 35 (they remove nothing there,
# but encode the rule explicitly and defend against a dirtier future release).
# Partitioned by molregno % :partitions so N workers each read a disjoint,
# balanced slice with no range arithmetic and no contention.
_CORPUS_SQL = """
    SELECT md.chembl_id, cs.molregno, cs.canonical_smiles
    FROM staging.compound_structures cs
    JOIN staging.molecule_dictionary md
        ON md.molregno = cs.molregno
    JOIN staging.chembl_id_lookup cil
        ON cil.entity_id = cs.molregno
       AND cil.entity_type = 'COMPOUND'
    WHERE cil.status = 'ACTIVE'
      AND cs.canonical_smiles IS NOT NULL
      AND cs.molregno %% %(partitions)s = %(part)s
"""


def read_corpus_partition(dsn: str, part: int, partitions: int):
    """Return (chembl_id, molregno, canonical_smiles) rows for one partition.

    Read in one shot with a plain client-side cursor, not a server-side named
    cursor. A partition is ~155k rows (~12 MB) and the join itself runs in ~1.4s
    (measured with EXPLAIN ANALYZE), so materialising it client-side is cheap.

    A named cursor was tried first to "stream" the slice, but it forced Postgres
    onto an incremental cursor plan and split the read into round-tripped FETCH
    batches — turning a 1.4s join into 15+ minutes per partition. The memory it
    saved (~12 MB) never mattered under the container budget; the slowdown did.
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(_CORPUS_SQL, {"partitions": partitions, "part": part})
            return cur.fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Parquet write + S3
# --------------------------------------------------------------------------

FINGERPRINT_SCHEMA = pa.schema(
    [
        ("chembl_id", pa.string()),
        ("molregno", pa.int64()),
        ("fingerprint", pa.binary()),
    ]
)


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    boto3.client("s3").upload_file(str(local_path), bucket, key)
    log.info("Uploaded %s -> s3://%s/%s", local_path, bucket, key)


def partition_cached(bucket: str, success_key: str) -> bool:
    """True if this partition's _SUCCESS marker already exists (skip re-work).

    Only a real 404 is a cache miss; other S3 errors (e.g. 403) are re-raised.
    """
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=success_key)
        return True
    except s3.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in {"404", "NoSuchKey"}:
            return False
        raise


def write_success_marker(bucket: str, success_key: str, manifest: dict) -> None:
    import json

    boto3.client("s3").put_object(
        Bucket=bucket, Key=success_key, Body=json.dumps(manifest, indent=2).encode()
    )
    log.info("Wrote marker s3://%s/%s", bucket, success_key)
