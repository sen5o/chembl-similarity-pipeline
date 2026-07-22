"""Orchestration for similarity_computation (brief steps 3-5).

Single task, by design: the corpus (~1.1 GB) is loaded into memory once and
reused for all ~56 sources. Mapping per-source was rejected — each mapped task
would reload the whole corpus (N x 1.1 GB and N re-reads from S3). See README.

Per source:
  1. BulkTanimotoSimilarity(source_fp, corpus)          -> step 3
  2. stream the full (target, score) table to S3         -> step 4
  3. rank: exclude self, top-10, tie flag                -> step 5
Then all sources' top-10 rows are written as one small table.

Caching (variant B, two layers), because a full run is IO-bound and ~8.5 min:
  - GLOBAL fast-path: if a previous run finished (_SUCCESS marker present),
    return its manifest immediately — nothing recomputed. This is the demo /
    re-run path.
  - PER-SOURCE: on a partial run (crash/retry, no global marker), a source
    whose full table already exists in S3 is not recomputed; its table is read
    back only to re-rank its top-10 into the combined output. Only missing
    sources are computed. So a retry does the remaining work, not all of it.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

from . import ranking, repository, similarity, utils

log = logging.getLogger("similarity_computation")


def _full_rows(chembl_ids, molregnos, scores):
    """Zip corpus ids/molregnos with computed scores into table rows."""
    yield from zip(chembl_ids, molregnos, scores, strict=True)


def _collect_top10(ranked, src: str, sink: list[dict]) -> None:
    for r in ranked:
        row = asdict(r)
        row["source_chembl_id"] = src
        sink.append(row)


def run() -> dict:
    release = utils.get_release()
    bucket = utils.s3_bucket()
    success_key = utils.success_key(release)

    # GLOBAL fast-path: a completed run's marker means everything is already in
    # S3 — return its manifest without touching the corpus or S3 objects again.
    if repository.object_exists(bucket, success_key):
        manifest = repository.read_manifest(bucket, success_key)
        log.info("Similarity already complete (cache hit) — skipping: %s", manifest)
        return manifest

    chembl_ids, molregnos, corpus_fps = repository.load_corpus(
        bucket, utils.fingerprints_prefix(release)
    )
    # index by chembl_id so we can pull each source's own fingerprint out of the
    # already-loaded corpus (guarantees source_fp == corpus_fp for that molecule,
    # so self-similarity is exactly 1.0)
    index = {cid: i for i, cid in enumerate(chembl_ids)}

    source_ids = repository.load_source_chembl_ids(bucket, utils.source_set_key())
    log.info("Sources: %s", len(source_ids))

    top10_rows: list[dict] = []
    computed = 0
    reused = 0
    missing: list[str] = []

    with TemporaryDirectory() as tmp:
        for src in source_ids:
            i = index.get(src)
            if i is None:
                # resolved source not present in the corpus (e.g. no structure)
                missing.append(src)
                continue

            table_key = utils.source_similarity_key(release, src)

            # PER-SOURCE cache: if this source's full table is already in S3,
            # don't recompute — read it back just to rank its top-10.
            if repository.object_exists(bucket, table_key):
                ranked = ranking.rank_top_n(
                    repository.read_full_table(bucket, table_key), source_chembl_id=src
                )
                _collect_top10(ranked, src, top10_rows)
                reused += 1
                continue

            scores = similarity.bulk_tanimoto(corpus_fps[i], corpus_fps)

            # step 4: full table -> parquet -> S3 (streamed, not materialised)
            dest = Path(tmp) / f"{src}.parquet"
            repository.write_full_table_streaming(_full_rows(chembl_ids, molregnos, scores), dest)
            repository.upload_file(dest, bucket, table_key)
            dest.unlink()  # free disk before the next source

            # step 5: rank over the full table. ranking consumes an iterator and
            # keeps only the top-10 + a cut-off count, so nothing 2.47M-sized is
            # held sorted beside the resident corpus.
            ranked = ranking.rank_top_n(
                zip(chembl_ids, molregnos, scores, strict=True), source_chembl_id=src
            )
            _collect_top10(ranked, src, top10_rows)

            computed += 1
            if (computed + reused) % 10 == 0:
                log.info("Progress %s/%s sources", computed + reused, len(source_ids))

    repository.write_top10(top10_rows, bucket, utils.top_similar_key(release))

    manifest = {
        "release": release,
        "sources_total": len(source_ids),
        "sources_computed": computed,
        "sources_reused_from_cache": reused,
        "sources_missing_from_corpus": missing,
        "top10_rows": len(top10_rows),
        "corpus_size": len(corpus_fps),
    }
    repository.write_success_marker(bucket, success_key, manifest)
    log.info("Similarity done: %s", manifest)
    return manifest
