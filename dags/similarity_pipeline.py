"""ChEMBL molecular similarity pipeline.

Iteration 1: skeleton only. Each task is a stub that logs and returns; real
logic lands in its own feature iteration. The task graph already reflects the
final dependency structure.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.sdk import dag, task
from callbacks import notify_teams_on_failure

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "on_failure_callback": notify_teams_on_failure,
    "retries": 1,
}


@dag(
    dag_id="similarity_pipeline",
    schedule=None,  # manual trigger; cadence separation is handled by cache-skip
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chembl", "similarity", "capstone"],
)
def similarity_pipeline():
    @task
    def ingest_chembl() -> str:
        """chembl_ingestion: dump -> staging -> silver/corpus (cache by release)."""
        log.info("STUB ingest_chembl")
        return "corpus_ready"

    @task
    def resolve_sources() -> str:
        """source_resolution: parse + DQ + name->chembl_id."""
        log.info("STUB resolve_sources")
        return "sources_resolved"

    @task
    def generate_fingerprints() -> str:
        """fingerprint_generation: Morgan(2, 2048) over full corpus -> S3."""
        log.info("STUB generate_fingerprints")
        return "fingerprints_ready"

    @task
    def compute_similarity() -> str:
        """similarity_computation: Tanimoto -> per-source parquet -> S3."""
        log.info("STUB compute_similarity")
        return "similarity_ready"

    @task
    def rank_top10() -> str:
        """similarity_computation: top-10 + has_duplicates_of_last_largest_score."""
        log.info("STUB rank_top10")
        return "ranked"

    @task
    def load_dwh() -> str:
        """dwh_load: parquet -> core.dim_molecule + core.fact_similarity."""
        log.info("STUB load_dwh")
        return "dwh_loaded"

    corpus = ingest_chembl()
    sources = resolve_sources()
    fps = generate_fingerprints()

    # ingestion feeds both resolution and fingerprinting; they are independent
    corpus >> [sources, fps]

    similarity = compute_similarity()
    [sources, fps] >> similarity

    similarity >> rank_top10() >> load_dwh()


similarity_pipeline()
