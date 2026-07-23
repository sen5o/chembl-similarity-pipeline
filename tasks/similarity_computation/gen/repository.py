"""IO boundary for similarity_computation: S3 reads/writes only.

Corpus fits in memory (measured ~1.1 GB for 2.47M fingerprints), so we load it
once and reuse it for every source — see README. Full per-source similarity
tables are large (2.47M rows each), so they are written streaming, in batches,
never held whole in a Python list on top of the resident corpus.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from . import fingerprints

log = logging.getLogger("similarity_computation")

# --- schemas ---------------------------------------------------------------

FULL_TABLE_SCHEMA = pa.schema(
    [
        ("target_chembl_id", pa.string()),
        ("target_molregno", pa.int64()),
        ("tanimoto_score", pa.float64()),
    ]
)

TOP10_SCHEMA = pa.schema(
    [
        ("source_chembl_id", pa.string()),
        ("target_chembl_id", pa.string()),
        ("target_molregno", pa.int64()),
        ("tanimoto_score", pa.float64()),
        ("has_duplicates_of_last_largest_score", pa.bool_()),
    ]
)


# --- corpus + sources (reads) ----------------------------------------------


def load_corpus(bucket: str, fp_prefix: str):
    """Load the whole fingerprint corpus into memory once.

    Returns three parallel lists (chembl_ids, molregnos, fps) kept in the same
    order so index i refers to one molecule across all three — that alignment
    is what lets BulkTanimotoSimilarity results map straight back to ids.
    """
    s3 = boto3.client("s3")
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=bucket, Prefix=fp_prefix).get("Contents", [])
        if o["Key"].endswith(".parquet")
    ]
    chembl_ids: list[str] = []
    molregnos: list[int] = []
    fps: list = []
    for key in sorted(keys):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        chembl_ids.extend(table.column("chembl_id").to_pylist())
        molregnos.extend(table.column("molregno").to_pylist())
        fps.extend(fingerprints.from_binary(b) for b in table.column("fingerprint").to_pylist())
    log.info("Loaded corpus: %s fingerprints", len(fps))
    return chembl_ids, molregnos, fps


def load_source_chembl_ids(bucket: str, resolved_key: str) -> list[str]:
    """The resolved query molecules (chembl_id) from source_resolution."""
    body = boto3.client("s3").get_object(Bucket=bucket, Key=resolved_key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return table.column("chembl_id").to_pylist()


def object_exists(bucket: str, key: str) -> bool:
    """True if an S3 object exists. Only a real 404 is 'no'; other errors raise."""
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in {"404", "NoSuchKey"}:
            return False
        raise


def read_full_table(bucket: str, key: str):
    """Read a previously-written full similarity table back as row tuples.

    Used on the warm/partial path: a source whose table is already in S3 is not
    recomputed, but its rows are read back so its top-10 can still be ranked
    into the combined output.
    """
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    return zip(
        table.column("target_chembl_id").to_pylist(),
        table.column("target_molregno").to_pylist(),
        table.column("tanimoto_score").to_pylist(),
        strict=True,
    )


def read_manifest(bucket: str, key: str) -> dict:
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


# --- writes ----------------------------------------------------------------


def write_full_table_streaming(rows_iter, dest: Path, batch_size: int = 200_000) -> int:
    """Stream a source's full similarity table to parquet in batches.

    rows_iter yields (target_chembl_id, target_molregno, score). Batched so the
    2.47M-row table is never fully materialised in Python alongside the corpus.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(dest, FULL_TABLE_SCHEMA)
    total = 0
    batch: list[dict] = []
    try:
        for cid, mol, score in rows_iter:
            batch.append({"target_chembl_id": cid, "target_molregno": mol, "tanimoto_score": score})
            if len(batch) >= batch_size:
                writer.write_table(pa.Table.from_pylist(batch, schema=FULL_TABLE_SCHEMA))
                total += len(batch)
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=FULL_TABLE_SCHEMA))
            total += len(batch)
    finally:
        writer.close()
    return total


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    boto3.client("s3").upload_file(str(local_path), bucket, key)
    log.info("Uploaded s3://%s/%s", bucket, key)


def write_top10(rows: list[dict], bucket: str, key: str) -> None:
    """Write the combined top-10 table (all sources) to S3 in one shot.

    Small: 56 sources x 10 rows. Buffered in memory on purpose.
    """
    table = pa.Table.from_pylist(rows, schema=TOP10_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info("Wrote top-10 (%s rows) -> s3://%s/%s", len(rows), bucket, key)


def write_success_marker(bucket: str, key: str, manifest: dict) -> None:
    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=json.dumps(manifest, indent=2).encode()
    )
    log.info("Wrote marker s3://%s/%s", bucket, key)
