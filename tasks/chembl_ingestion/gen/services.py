"""Orchestration for the chembl_ingestion task container.

acquire_chembl()  — Bronze acquisition. Dump -> 4 projected tables -> S3
                     parquet. Idempotent via the `_SUCCESS` marker (see
                     README, "acquire caches on completion, not bare prefix
                     presence"). This is what the Airflow task calls.

inspect_schema()  — read-only PRAGMA introspection. Run this once per
                     ChEMBL release, before trusting TABLE_COLUMNS below,
                     to confirm the real column names in the dump.
"""

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from . import repository, utils

log = logging.getLogger("chembl_ingestion")

# Tables ingested per the brief (step 1): exactly these four, projected to
# the columns we actually consume downstream (README ADR 5 — projection,
# not selection: all rows, only the columns we use).
#
# RELEASE CHOICE: the brief (6a) requires cx_logp and molecular_species in
# dim_molecule. Per ChEMBL docs these ChemAxon-calculated properties existed
# only from ChEMBL 26 through ChEMBL 35 and were removed afterwards (verified
# empirically: absent from the entire release-37 dump). We therefore pin to
# ChEMBL 35 — the most recent release that still contains both columns — via
# CHEMBL_RELEASE=35. See README ADR 6.
#
# ⚠ Column names/types below are confirmed against release 37 for the shared
# tables; cx_logp and molecular_species must be re-confirmed against a
# release-35 dump (run `python run.py inspect` with CHEMBL_RELEASE=35).
TABLE_COLUMNS: dict[str, list[str]] = {
    # SMILES structures — the input to fingerprint generation.
    "compound_structures": [
        "molregno",
        "canonical_smiles",
        "standard_inchi_key",
    ],
    # Identity + molecule_type (one of the 10 dim_molecule fields, brief 6a).
    # pref_name is the field the name resolver matches against.
    "molecule_dictionary": [
        "molregno",
        "chembl_id",
        "pref_name",
        "molecule_type",
    ],
    # 9 of the 10 dim_molecule fields (brief 6a) — column order follows the
    # brief. mw_freebase and full_mwt are also load-bearing for the
    # resolver's MW cross-check (parent vs. salt form).
    "compound_properties": [
        "molregno",
        "mw_freebase",
        "alogp",
        "psa",
        "cx_logp",
        "molecular_species",
        "full_mwt",
        "aromatic_rings",
        "heavy_atoms",
    ],
    # status feeds the OBSOLETE filter applied later in silver/corpus
    # (that filter is a Silver/selection decision, not a Bronze one).
    # entity_id lets us join lookup rows back to molregno if ever needed.
    "chembl_id_lookup": [
        "chembl_id",
        "entity_type",
        "entity_id",
        "status",
    ],
}

# Explicit pyarrow schema per table, column order matching TABLE_COLUMNS.
#
# This exists to fix a real bug: without an explicit schema, pyarrow infers
# each chunk's types independently from the Python values it sees. ChEMBL's
# property columns are sparse (many molecules have no cx_logp/alogp/etc.),
# so a chunk that happens to be all-NULL for some column infers as
# pa.null() — and ParquetWriter then rejects the next chunk, which infers
# a real type, with "Table schema does not match schema used to create
# file". Passing an explicit schema to every chunk avoids the mismatch
# entirely (verified locally before applying this fix).
TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "compound_structures": pa.schema(
        [
            ("molregno", pa.int64()),
            ("canonical_smiles", pa.string()),
            ("standard_inchi_key", pa.string()),
        ]
    ),
    "molecule_dictionary": pa.schema(
        [
            ("molregno", pa.int64()),
            ("chembl_id", pa.string()),
            ("pref_name", pa.string()),
            ("molecule_type", pa.string()),
        ]
    ),
    "compound_properties": pa.schema(
        [
            ("molregno", pa.int64()),
            ("mw_freebase", pa.float64()),
            ("alogp", pa.float64()),
            ("psa", pa.float64()),
            ("cx_logp", pa.float64()),
            ("molecular_species", pa.string()),
            ("full_mwt", pa.float64()),
            ("aromatic_rings", pa.int64()),
            ("heavy_atoms", pa.int64()),
        ]
    ),
    "chembl_id_lookup": pa.schema(
        [
            ("chembl_id", pa.string()),
            ("entity_type", pa.string()),
            ("entity_id", pa.int64()),
            ("status", pa.string()),
        ]
    ),
}


def inspect_schema() -> dict[str, list[str]]:
    """Print each target table's real columns from the release dump.

    Read-only: does not touch S3. Run once per release to lock down
    TABLE_COLUMNS against reality.
    """
    release = utils.get_release()
    sqlite_path = repository.download_dump(release)

    found: dict[str, list[str]] = {}
    for table in TABLE_COLUMNS:
        cols = repository.table_columns(sqlite_path, table)
        found[table] = cols
        log.info("%-22s %s", table, cols)
    return found


def acquire_chembl() -> str:
    """Bronze acquisition: dump -> 4 projected tables -> parquet in S3.

    Returns the S3 prefix the data was (or already had been) written to.
    """
    release = utils.get_release()
    bucket = utils.s3_bucket()
    prefix = utils.bronze_chembl_prefix(release)

    if repository.cache_hit(bucket, prefix):
        return prefix

    log.info("No cache for release %s at %s — acquiring", release, prefix)
    sqlite_path = repository.download_dump(release)

    with TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        manifest: dict = {"release": release, "tables": {}}

        for table, columns in TABLE_COLUMNS.items():
            dest = tmp_dir / f"{table}.parquet"
            schema = TABLE_SCHEMAS[table]
            row_count = repository.extract_table(sqlite_path, table, schema, dest)

            key = f"{prefix}/{table}/part-000.parquet"
            repository.upload_file(dest, bucket, key)

            manifest["tables"][table] = {"rows": row_count, "columns": columns}

        # Written last, and only after every table succeeded — this is what
        # makes the marker a trustworthy completion signal (see repository
        # .write_success_marker docstring).
        repository.write_success_marker(bucket, prefix, manifest)

    log.info("acquire_chembl complete for release %s", release)
    return prefix
