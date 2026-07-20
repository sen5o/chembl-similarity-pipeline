"""Regression tests for gen.repository — real sqlite and real pyarrow, no
mocks. Complements test_service.py, which mocks repository entirely to test
orchestration.
"""

from __future__ import annotations

import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
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
