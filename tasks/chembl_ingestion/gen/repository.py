"""IO boundary: S3, the local filesystem, and the downloaded SQLite dump.

Every function here does exactly one read or write. Orchestration (what to
call, in what order, whether to skip) lives in services.py — keeping that
split makes each function trivially mockable in tests (see
tests/test_service.py, which never touches the network or a real bucket).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("chembl_ingestion")


# --------------------------------------------------------------------------
# S3 — cache marker
# --------------------------------------------------------------------------


def _success_marker_key(prefix: str) -> str:
    return f"{prefix}/_SUCCESS"


def cache_hit(bucket: str, prefix: str) -> bool:
    """True if a *complete* load already exists at this prefix.

    Checked via the `_SUCCESS` marker rather than bare prefix presence —
    see gen.utils.bronze_chembl_prefix for why (partial-upload safety).
    Only a real 404 (object not found) is treated as a cache miss; any
    other S3 error (e.g. 403 - no permission) is a real problem and is
    re-raised rather than silently reinterpreted as "no cache".
    """
    s3 = boto3.client("s3")
    key = _success_marker_key(prefix)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        log.info("Cache hit: s3://%s/%s", bucket, key)
        return True
    except s3.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey"}:
            return False
        raise


def write_success_marker(bucket: str, prefix: str, manifest: dict) -> None:
    """Write the completion marker. Called only after every table has been
    uploaded — this ordering is what makes the marker trustworthy.
    """
    s3 = boto3.client("s3")
    key = _success_marker_key(prefix)
    body = json.dumps(manifest, indent=2).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    log.info("Wrote cache marker: s3://%s/%s", bucket, key)


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)
    log.info("Uploaded %s -> s3://%s/%s", local_path, bucket, key)


def download_file(bucket: str, key: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, str(dest))
    log.info("Downloaded s3://%s/%s -> %s", bucket, key, dest)
    return dest


# --------------------------------------------------------------------------
# Postgres — staging load (TRUNCATE + COPY FROM STDIN)
# --------------------------------------------------------------------------


def truncate_and_copy(
    dsn: str,
    table: str,
    parquet_path: Path,
    columns: list[str],
    batch_size: int = 100_000,
) -> int:
    """Full-refresh load: TRUNCATE staging.<table>, then stream the parquet
    into it via COPY FROM STDIN. Both happen in one transaction, so a failed
    COPY rolls the TRUNCATE back — staging is never left empty on error.

    Streams the parquet in record batches and feeds each as CSV to COPY, so
    memory stays bounded even for chembl_id_lookup (~5.4M rows). CSV is used
    with `NULL ''`: pyarrow writes real strings quoted (empty string -> "")
    and nulls as bare empty, so the two remain distinguishable (verified).

    Returns rows copied.
    """
    import io

    import psycopg2
    import pyarrow.csv as pacsv

    col_list = ", ".join(columns)
    copy_sql = f"COPY staging.{table} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"

    parquet_file = pq.ParquetFile(parquet_path)
    total = 0
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE staging.{table}")
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                buf = io.BytesIO()
                pacsv.write_csv(batch, buf, write_options=pacsv.WriteOptions(include_header=False))
                buf.seek(0)
                cur.copy_expert(copy_sql, buf)
                total += batch.num_rows
        # `with conn` commits on success / rolls back on exception
    finally:
        conn.close()

    log.info("Loaded %s rows into staging.%s", total, table)
    return total


# --------------------------------------------------------------------------
# ChEMBL dump acquisition (EBI release, via chembl_downloader)
# --------------------------------------------------------------------------


def download_dump(release: str) -> Path:
    """Download (or reuse) the ChEMBL SQLite dump for `release`.

    chembl_downloader caches the extracted file on local disk (via pystow,
    typically under ~/.data/chembl) — a second call for the same release is
    a filesystem hit, not a re-download. This is a *separate* cache from our
    own S3 `_SUCCESS` marker: this one avoids re-hitting the EBI server,
    ours avoids redoing the extract-to-parquet-to-S3 work.
    """
    import chembl_downloader

    log.info(
        "Resolving ChEMBL release %s (first run downloads/unpacks the "
        "release dump — several GB, several minutes)",
        release,
    )
    path = chembl_downloader.download_extract_sqlite(version=release)
    if not isinstance(path, Path):
        # download_extract_sqlite returns Path | VersionPathPair depending on
        # return_version; we never pass return_version=True, but assert it
        # explicitly rather than trusting the default silently.
        raise TypeError(f"Expected a Path from chembl_downloader, got {type(path)!r}")
    return path


def table_columns(sqlite_path: Path, table: str) -> list[str]:
    """Real column names for `table`, read from the dump itself.

    Used by services.inspect_schema() to verify the projection in
    services.TABLE_COLUMNS against the actual release schema before it is
    trusted — see project plan, "developer decides projection against the
    real schema, not from memory".
    """
    with sqlite3.connect(sqlite_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def extract_table(
    sqlite_path: Path,
    table: str,
    schema: pa.Schema,
    dest_parquet: Path,
    chunk_size: int = 200_000,
) -> int:
    """Stream `table`, projected to `schema`'s columns, from the sqlite dump
    into a local parquet file. Streams in chunks (default 200k rows) rather
    than loading the full table into memory — chembl_id_lookup alone is
    ~5.4M rows. `chunk_size` is exposed mainly so tests can force multiple
    small chunks without loading real ChEMBL-sized data.

    `schema` is applied explicitly to every chunk (not inferred per-chunk):
    ChEMBL's property columns are sparse, so an all-NULL first chunk would
    otherwise infer as pa.null() and break the write on the next chunk that
    has real values. See services.TABLE_SCHEMAS for why this matters.

    Returns the row count written.
    """
    dest_parquet.parent.mkdir(parents=True, exist_ok=True)
    columns = schema.names
    col_list = ", ".join(columns)

    with sqlite3.connect(sqlite_path) as conn:
        # table/columns come from our own constants (services.TABLE_COLUMNS),
        # never from user input.
        cursor = conn.execute(f"SELECT {col_list} FROM {table}")
        writer: pq.ParquetWriter | None = None
        total = 0
        try:
            while True:
                chunk = cursor.fetchmany(chunk_size)
                if not chunk:
                    break
                batch = pa.table(
                    {col: [row[i] for row in chunk] for i, col in enumerate(columns)},
                    schema=schema,
                )
                if writer is None:
                    writer = pq.ParquetWriter(dest_parquet, schema)
                writer.write_table(batch)
                total += len(chunk)
        finally:
            if writer is not None:
                writer.close()

    log.info("Extracted %s rows: %s -> %s", total, table, dest_parquet)
    return total
