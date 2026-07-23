"""Orchestration for dwh_load: Silver (S3) -> Gold (Postgres star schema).

  1. read top10.parquet (the ~560 facts)
  2. collect the molecules those facts reference (sources + targets)
  3. fetch their 10 brief-required attributes from staging
  4. load dimension + facts in one transaction (full refresh per release)

Full refresh, not incremental: the mart describes one pinned ChEMBL release and
is rebuilt from Silver in seconds, so merge logic would be complexity without a
use case (same reasoning as the staging load).
"""

from __future__ import annotations

import logging

from . import repository, utils

log = logging.getLogger("dwh_load")


def _referenced_molecules(fact_rows: list[dict]) -> list[str]:
    """Every chembl_id appearing in the facts, as a source or as a target.

    The dimension covers exactly these — a molecule that never appears in a
    top-10 has no fact to describe, so it does not belong in the mart.
    """
    ids: set[str] = set()
    for row in fact_rows:
        ids.add(row["source_chembl_id"])
        ids.add(row["target_chembl_id"])
    return sorted(ids)


def run() -> dict:
    release = utils.get_release()
    bucket = utils.s3_bucket()
    dsn = utils.dwh_dsn()

    fact_rows = repository.read_top10(bucket, utils.top_similar_key(release))
    if not fact_rows:
        raise RuntimeError("top10.parquet is empty — run similarity_computation first.")

    molecule_ids = _referenced_molecules(fact_rows)
    dim_rows = repository.fetch_molecule_properties(dsn, molecule_ids)

    # Every molecule referenced by a fact must exist in the dimension, or the
    # fact insert would fail on a missing key. Fail loudly here with the actual
    # ids rather than surfacing a KeyError deep inside the load.
    found = {row["chembl_id"] for row in dim_rows}
    absent = [m for m in molecule_ids if m not in found]
    if absent:
        raise RuntimeError(
            f"{len(absent)} molecule(s) referenced by facts are missing from "
            f"staging.molecule_dictionary, e.g. {absent[:5]}"
        )

    dim_count, fact_count = repository.load_star_schema(dsn, dim_rows, fact_rows)

    manifest = {
        "release": release,
        "facts_in": len(fact_rows),
        "facts_loaded": fact_count,
        "dimension_rows": dim_count,
        "sources": len({r["source_chembl_id"] for r in fact_rows}),
    }
    log.info("dwh_load complete: %s", manifest)
    return manifest
