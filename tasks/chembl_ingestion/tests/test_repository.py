"""Regression tests for gen.repository — real sqlite and real pyarrow, no
mocks. Complements test_service.py, which mocks repository entirely to test
orchestration.

test_truncate_and_copy_* is an integration test against a real Postgres and
is skipped unless DWH_DSN points at one (e.g. the docker-compose DWH):
    DWH_DSN=postgresql://airflow:airflow@localhost:5432/dwh pytest
"""

from __future__ import annotations

import os
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gen import repository


def _make_sqlite_with_sparse_first_chunk(path):
    """A table where the first two rows are NULL in a numeric column and
    later rows are not — this is exactly the shape of ChEMBL's property
    columns (many molecules have no cx_logp/alogp/etc.) that broke
    ParquetWriter before extract_table took an explicit schema.
    """
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE compound_properties (molregno INTEGER, alogp REAL)")
        conn.executemany(
            "INSERT INTO compound_properties VALUES (?, ?)",
            [(1, None), (2, None), (3, 1.5), (4, 2.7)],
        )


def test_extract_table_survives_all_null_first_chunk(tmp_path):
    """Regression test: an all-NULL first chunk must not break the write of
    a later chunk containing real values (see repository.extract_table
    docstring for the mechanism). chunk_size=2 forces two separate
    write_table calls so the chunk boundary is actually exercised.
    """
    sqlite_path = tmp_path / "chembl.db"
    _make_sqlite_with_sparse_first_chunk(sqlite_path)
    dest = tmp_path / "compound_properties.parquet"

    schema = pa.schema([("molregno", pa.int64()), ("alogp", pa.float64())])

    row_count = repository.extract_table(
        sqlite_path, "compound_properties", schema, dest, chunk_size=2
    )

    assert row_count == 4
    table = pq.read_table(dest)
    assert table.num_rows == 4
    assert table.column("alogp").to_pylist() == [None, None, 1.5, 2.7]


@pytest.mark.skipif(
    not os.environ.get("DWH_DSN"),
    reason="integration test — set DWH_DSN to a real Postgres to run",
)
def test_truncate_and_copy_loads_parquet_including_nulls(tmp_path):
    """Round-trips a parquet with a NULL and an empty string through
    TRUNCATE + COPY into a real Postgres table, asserting the two stay
    distinct (empty string preserved, NULL preserved). Also asserts the
    TRUNCATE half: a pre-existing row is gone after the load.
    """
    import psycopg2

    dsn = os.environ["DWH_DSN"]
    columns = ["molregno", "canonical_smiles", "standard_inchi_key"]

    # A dedicated throwaway table so we never touch real staging data.
    table = "compound_structures_pytest"
    setup = f"""
        CREATE SCHEMA IF NOT EXISTS staging;
        DROP TABLE IF EXISTS staging.{table};
        CREATE TABLE staging.{table} (
            molregno BIGINT, canonical_smiles TEXT, standard_inchi_key TEXT
        );
        INSERT INTO staging.{table} VALUES (999, 'STALE', 'STALE');
    """
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(setup)

        pdata = pa.table(
            {
                "molregno": [1, 2],
                "canonical_smiles": ["CCO", ""],  # real value / empty string
                "standard_inchi_key": ["KEY1", None],  # real value / NULL
            }
        )
        parquet_path = tmp_path / "compound_structures.parquet"
        pq.write_table(pdata, parquet_path)

        rows = repository.truncate_and_copy(dsn, table, parquet_path, columns)
        assert rows == 2

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT molregno, canonical_smiles, standard_inchi_key "
                f"FROM staging.{table} ORDER BY molregno"
            )
            result = cur.fetchall()

        # STALE row gone (TRUNCATE worked); empty string != NULL preserved
        assert result == [(1, "CCO", "KEY1"), (2, "", None)]
    finally:
        with conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS staging.{table}")
        conn.close()
