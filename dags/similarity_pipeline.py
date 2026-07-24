"""ChEMBL molecular similarity pipeline.

Each stage runs in its own container (see tasks/*/Dockerfile) via
DockerOperator — the containers are the unit of dependency isolation, so the
DAG orchestrates them rather than importing their code. Airflow only needs the
docker provider; RDKit, chembl_downloader and psycopg2 stay inside the images
that actually use them.

The graph mirrors the real stages. There is deliberately no separate ranking
task: similarity_computation produces the full per-source tables and the top-10
with the tie flag in one pass over the in-memory corpus, so splitting them would
mean loading the 1.1 GB corpus twice.

Every stage is idempotent and cache-aware, so re-triggering the DAG after a
successful run skips the completed work instead of recomputing it.
"""

from __future__ import annotations

import os

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from callbacks import notify_teams_on_failure
from docker.types import Mount

# --- configuration ---------------------------------------------------------

CORPUS_PARTITIONS = int(os.environ.get("CORPUS_PARTITIONS", 16))

# Task containers run as siblings on the compose network, not inside the
# scheduler, so they reach Postgres by service name rather than localhost.
TASK_ENV = {
    "CHEMBL_RELEASE": os.environ.get("CHEMBL_RELEASE", "35"),
    "DE_SCHOOL_S3_BUCKET": os.environ.get("DE_SCHOOL_S3_BUCKET", ""),
    "S3_ROOT_PREFIX": os.environ.get("S3_ROOT_PREFIX", ""),
    "INPUT_S3_PREFIX": os.environ.get("INPUT_S3_PREFIX", ""),
    "AWS_PROFILE": os.environ.get("AWS_PROFILE", ""),
    "AWS_REGION": os.environ.get("AWS_REGION", "eu-central-1"),
    "DWH_DSN": os.environ.get("TASK_DWH_DSN", "postgresql://airflow:airflow@postgres:5432/dwh"),
    "CORPUS_PARTITIONS": str(CORPUS_PARTITIONS),
}

# The compose network task containers join so they can resolve `postgres`.
DOCKER_NETWORK = os.environ.get("TASK_DOCKER_NETWORK", "local_deployment_default")

# Host paths mounted into every task container:
#   ~/.aws   - SSO credentials, read-only; tasks authenticate as the operator does
#   ~/.data  - chembl_downloader's cache, so a cold `acquire` doesn't re-download
#              the multi-GB release dump on every container start
HOST_HOME = os.environ.get("HOST_HOME", os.path.expanduser("~"))

COMMON_MOUNTS = [
    # Read-write: botocore writes refreshed SSO tokens back into ~/.aws/sso/cache,
    # so a read-only mount fails the moment the token needs renewing.
    Mount(source=f"{HOST_HOME}/.aws", target="/root/.aws", type="bind"),
    Mount(source=f"{HOST_HOME}/.data", target="/root/.data", type="bind"),
]

# Runtime wiring shared by every task container. Kept in one dict so a regular
# operator and a mapped one (.partial) cannot drift apart.
CONTAINER_KWARGS = {
    "environment": TASK_ENV,
    "mounts": COMMON_MOUNTS,
    "network_mode": DOCKER_NETWORK,
    "docker_url": "unix://var/run/docker.sock",
    "auto_remove": "success",
    # Docker Desktop cannot bind the operator's default temp mount
    "mount_tmp_dir": False,
}

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
    doc_md=__doc__,
)
def similarity_pipeline():
    # --- Bronze ------------------------------------------------------------
    acquire_chembl = DockerOperator(
        task_id="acquire_chembl",
        image="chembl-ingestion:latest",
        command="acquire",
        doc_md="Download the pinned ChEMBL release; project 4 tables to S3 parquet.",
        **CONTAINER_KWARGS,
    )

    load_staging = DockerOperator(
        task_id="load_staging",
        image="chembl-ingestion:latest",
        command="load",
        doc_md="S3 parquet -> staging.* via TRUNCATE + COPY (full refresh).",
        **CONTAINER_KWARGS,
    )

    # --- Silver ------------------------------------------------------------
    resolve_sources = DockerOperator(
        task_id="resolve_sources",
        image="chembl-source-resolution:latest",
        command="resolve",
        doc_md="Parse input CSVs, apply DQ rules, resolve names -> chembl_id.",
        **CONTAINER_KWARGS,
    )

    # One mapped task per corpus partition: independent units of work, so a
    # failure retries only its own slice rather than the whole corpus.
    generate_fingerprints = DockerOperator.partial(
        task_id="generate_fingerprints",
        image="chembl-fingerprint-generation:latest",
        doc_md="Morgan(2, 2048) over one corpus partition -> silver/fingerprints.",
        **CONTAINER_KWARGS,
    ).expand(
        command=[f"generate {i}" for i in range(CORPUS_PARTITIONS)],
    )

    compute_similarity = DockerOperator(
        task_id="compute_similarity",
        image="chembl-similarity-computation:latest",
        command="compute",
        doc_md="Tanimoto vs the full corpus; per-source tables + top-10 with tie flag.",
        **CONTAINER_KWARGS,
    )

    # --- Gold --------------------------------------------------------------
    load_dwh = DockerOperator(
        task_id="load_dwh",
        image="chembl-dwh-load:latest",
        command="load",
        doc_md="Silver top-10 -> core.dim_molecule + core.fact_similarity.",
        **CONTAINER_KWARGS,
    )

    # --- dependencies ------------------------------------------------------
    # staging feeds both branches: the resolver looks molecules up by name and
    # the fingerprint stage reads the corpus. The two are independent.
    acquire_chembl >> load_staging >> [resolve_sources, generate_fingerprints]

    # similarity needs both the resolved sources and the complete fingerprint set
    [resolve_sources, generate_fingerprints] >> compute_similarity >> load_dwh


similarity_pipeline()
