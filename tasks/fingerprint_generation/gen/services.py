"""Orchestration for fingerprint_generation.

generate_partition(part) does one partition end-to-end: read corpus slice ->
parse SMILES -> Morgan fingerprint -> stream to parquet -> upload -> mark
_SUCCESS. It streams throughout (never holds a whole partition in memory) and
reports a reconciliation metric: how many structures came in vs how many
fingerprints went out, so unparseable SMILES are a visible number, not a
silent loss.

Parallelism is across partitions via Airflow dynamic task mapping, bounded by
memory (not cores) — see README. No multiprocessing inside a partition: on a
single host it adds no cores and costs memory we don't have.
"""

from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from . import fingerprints, molecule_parser, repository, utils

log = logging.getLogger("fingerprint_generation")

_WRITE_BATCH = 20_000


def generate_partition(part: int) -> dict:
    """Compute fingerprints for one corpus partition. Returns metrics."""
    release = utils.get_release()
    bucket = utils.s3_bucket()
    dsn = utils.dwh_dsn()
    partitions = utils.num_partitions()
    prefix = utils.fingerprints_prefix(release)

    success_key = utils.partition_success_key(prefix, part)
    if repository.partition_cached(bucket, success_key):
        log.info("Partition %s already complete — skipping", part)
        return {"partition": part, "skipped": True}

    part_key = utils.partition_key(prefix, part)
    structures_in = 0
    fingerprints_out = 0

    with TemporaryDirectory() as tmp:
        dest = Path(tmp) / f"part-{part:03d}.parquet"
        writer = pq.ParquetWriter(dest, repository.FINGERPRINT_SCHEMA)
        try:
            batch: list[dict] = []
            for chembl_id, molregno, smiles in repository.read_corpus_partition(
                dsn, part, partitions
            ):
                structures_in += 1
                mol = molecule_parser.parse_smiles(smiles)
                if mol is None:
                    continue  # counted via the in/out gap, not silently dropped
                fp = fingerprints.to_binary(fingerprints.compute_fingerprint(mol))
                batch.append({"chembl_id": chembl_id, "molregno": molregno, "fingerprint": fp})
                fingerprints_out += 1
                if len(batch) >= _WRITE_BATCH:
                    writer.write_table(
                        pa.Table.from_pylist(batch, schema=repository.FINGERPRINT_SCHEMA)
                    )
                    batch.clear()
            if batch:
                writer.write_table(
                    pa.Table.from_pylist(batch, schema=repository.FINGERPRINT_SCHEMA)
                )
        finally:
            writer.close()

        repository.upload_file(dest, bucket, part_key)

    failed = structures_in - fingerprints_out
    manifest = {
        "partition": part,
        "partitions": partitions,
        "release": release,
        "structures_in": structures_in,
        "fingerprints_out": fingerprints_out,
        "unparseable_smiles": failed,
    }
    repository.write_success_marker(bucket, success_key, manifest)
    log.info(
        "Partition %s done: in=%s out=%s unparseable=%s",
        part,
        structures_in,
        fingerprints_out,
        failed,
    )
    return manifest
