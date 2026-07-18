# ChEMBL Molecular Similarity Pipeline

An end-to-end data engineering pipeline that, for a set of input compounds, finds the
**top-10 most structurally similar molecules** across the entire ChEMBL database, and
serves the results through a dimensional data mart.

Built with **Airflow 3**, **PostgreSQL**, **AWS S3**, and **RDKit**, deployed locally via
`docker-compose`.

---

## 1. What it does

1. Ingests four ChEMBL tables (the full compound universe, ~2.4M molecules with structures).
2. Resolves the input compound names → ChEMBL IDs, applying data-quality rules.
3. Computes **Morgan fingerprints** (radius 2, 2048 bits) for every ChEMBL structure.
4. Computes **Tanimoto similarity** of each input compound against the full corpus.
5. Selects the top-10 neighbours per compound, flagging boundary ties.
6. Loads a **star schema** (`dim_molecule` + `fact_similarity`) and exposes analytical views.

The input set is **all valid, unique, resolved compounds** from `input/surname_name/`
(confirmed with the mentor — the "100 molecules" figure in the brief is illustrative).

---

## 2. Architecture

The pipeline follows a **medallion** approach. The grading brief requires justifying the
layer choice, so the reasoning is made explicit below.

```
        INPUT                         BRONZE                    SILVER (S3 lake)                GOLD
  ┌───────────────┐          ┌──────────────────────┐   ┌──────────────────────────┐   ┌────────────────┐
  │ ChEMBL dump   │────────▶ │ staging.* (4 tables) │──▶│ corpus  (curated smiles) │──▶│ core           │
  │ (EBI FTP)     │          │  native types        │   │ fingerprints             │   │  dim_molecule  │
  ├───────────────┤          ├──────────────────────┤   │ similarity (per source)  │──▶│  fact_similarity│
  │ batch CSVs    │────────▶ │ input_raw (TEXT)     │──▶│ source_set               │   │  + views        │
  └───────────────┘          └──────────────────────┘   │  {resolved,quarantine}   │   └────────────────┘
                                                         └──────────────────────────┘
```

### Layers

| Tier | Where it lives | Contents | Why a separate layer |
|------|----------------|----------|----------------------|
| **Bronze** | Postgres `staging` schema + `s3://…/bronze/` | Raw ChEMBL tables + raw batch CSVs | Isolates raw data; ingestion is repeatable without re-download |
| **Silver** | `s3://…/silver/` (Parquet) | Curated `corpus`, resolved `source_set`, `fingerprints`, `similarity` | Heavy compute artifacts kept out of the database; reusable and versioned |
| **Gold** | Postgres `core` schema | Star schema + analytical views | Business-ready; contains only molecules referenced by facts |

**Justification of the "at least 2 layers" requirement.**
The two mandatory DWH layers are the Postgres schemas `staging` (Bronze) and `core` (Gold).
The Silver tier is an S3 **data-lake tier** holding intermediate compute artifacts
(fingerprints, per-molecule similarity tables). We deliberately do **not** call this a
"three-schema warehouse" — the Silver tier is an artifact store, not a set of DWH tables.
This is the honest description of the design.

### A note on staging types

Bronze uses **two different strategies by origin, not one blanket rule**:

- **Batch CSVs** land as `TEXT`/`VARCHAR` — they are external and dirty, so typing at the
  boundary would turn a bad row into a task failure instead of a quarantined record.
- **ChEMBL tables** keep their **native types** — ChEMBL is a curated relational source with
  its own schema; forcing TEXT onto it would be pointless.

### Bronze = all rows, needed columns only

Projection (dropping unused columns) is a storage decision and is applied in Bronze.
Selection (dropping rows, e.g. `entity_type='COMPOUND'`, non-`OBSOLETE`) is a **semantic**
decision and is deferred to Silver's `corpus` build. Bronze stays a faithful copy.

---

## 3. Project structure

```
.
├── README.md                          # launch/usage + example results (brief step 9)
├── dags/
│   ├── similarity_pipeline.py         # DAG: orchestrates task-containers
│   └── callbacks.py                   # on_failure → Teams (Power Automate Adaptive Card)
├── tasks/
│   ├── chembl_ingestion/              # step 1: 4 ChEMBL tables → STAGING/S3 → silver/corpus
│   │   ├── Dockerfile
│   │   ├── gen/{__init__,repository,services,utils}.py
│   │   ├── pyproject.toml · requirements.txt · run.py
│   │   └── tests/{fixtures/, test_service.py}
│   │
│   ├── source_resolution/             # DQ + name→chembl_id resolution
│   │   ├── gen/
│   │   │   ├── file_parser.py
│   │   │   ├── parser_factory.py      # routes valid vs invalid rows
│   │   │   ├── validators.py          # empty name, ic50<=0, schema drift
│   │   │   ├── resolver.py            # ChEMBL lookup + molecular-weight cross-check
│   │   │   ├── repository.py · services.py · utils.py
│   │   └── tests/
│   │       ├── fixtures/{valid_batch_001.csv, batch_004.csv}
│   │       └── {test_parsers, test_validators, test_resolver}.py
│   │
│   ├── fingerprint_generation/        # step 2: Morgan(2,2048) of all structures → S3
│   │   ├── gen/{fingerprints,molecule_parser,repository,services,utils}.py
│   │   └── tests/test_fingerprints.py
│   │
│   ├── similarity_computation/        # steps 3-5: Tanimoto → parquet → S3 → top-10 + flag
│   │   ├── gen/
│   │   │   ├── similarity.py          # BulkTanimotoSimilarity
│   │   │   ├── ranking.py             # top-10 + has_duplicates_of_last_largest_score
│   │   │   ├── repository.py · services.py · utils.py
│   │   └── tests/{test_similarity, test_ranking}.py
│   │
│   └── dwh_load/                      # step 6: parquet → STAGING → CORE (dim/fact)
│       ├── gen/{repository,services,utils}.py
│       └── tests/
│
└── local_deployment/
    ├── docker-compose.yml             # Postgres (DWH + Airflow meta) + web/scheduler/worker
    ├── migrations/                    # DDL as migrations
    │   ├── 01_staging.sql             # chembl staging (native) + input staging (TEXT)
    │   ├── 02_core_star_schema.sql    # dim_molecule, fact_similarity
    │   └── 03_views.sql               # 7a, 7b, 8a, 8b, 8c (applied at deploy)
    └── fixtures/                      # e2e mini-corpus + real batch CSVs
```

Each `tasks/*` folder is an independently containerised unit with its own dependencies,
invoked by the DAG. Views are created by migration `03_views.sql` at deploy time; the DAG
does not build them.

---

## 4. Data model (star schema)

**`dim_molecule`** — one row per molecule referenced by any fact (both sources and targets).

| Column | Source |
|--------|--------|
| `molecule_key` (surrogate PK) | generated |
| `chembl_id` (business key) | ChEMBL |
| `molecule_type` | `molecule_dictionary` |
| `mw_freebase`, `alogp`, `psa`, `cx_logp`, `molecular_species`, `full_mwt`, `aromatic_rings`, `heavy_atoms` | `compound_properties` |

Loaded as **SCD Type 1** — molecular properties are static reference data, so history is not
tracked. A surrogate key is used because ChEMBL merges molecules and marks IDs `OBSOLETE`
(`chembl_id_lookup.status`), so `chembl_id` is not guaranteed immutable.

**`fact_similarity`** — grain: one row per (source, target-in-top-10) pair.

| Column | Meaning |
|--------|---------|
| `source_molecule_key` (FK) | the query molecule |
| `target_molecule_key` (FK) | a similar molecule |
| `tanimoto_score` | similarity |
| `has_duplicates_of_last_largest_score` | boundary-tie flag |

Invariant used in tests: `count(fact_similarity) == N_sources × 10`.

---

## 5. Pipeline stages

| Stage | Task container | Brief step |
|-------|----------------|-----------|
| Acquire ChEMBL dump → staging → `silver/corpus` | `chembl_ingestion` | 1 |
| Resolve names → chembl_id, apply DQ | `source_resolution` | — |
| Morgan fingerprints for full corpus → S3 | `fingerprint_generation` | 2 |
| Tanimoto vs corpus → full table per source → S3 | `similarity_computation` | 3, 4 |
| Top-10 + tie flag | `similarity_computation` | 5 |
| Load `dim_molecule` + `fact_similarity` | `dwh_load` | 6 |
| Views | migration `03_views.sql` | 7, 8 |

`source_resolution` and `fingerprint_generation` depend only on ingestion and are independent
of each other. `similarity_computation` depends on both.

### Similarity design notes

- **Fingerprints** are the only real cost (~2.4M molecules). Parallelised via task mapping
  (for retry granularity) plus an in-process `multiprocessing.Pool` (for actual cores).
- **Similarity** is a single streaming task: the ~58 source fingerprints fit in memory; the
  corpus is streamed in chunks in a single pass. `BulkTanimotoSimilarity` is fast (~seconds),
  so distributing it would cost more in orchestration overhead than it saves.
- **Ranking reads the full similarity table** per source. The tie flag needs the complete
  count of molecules at the 10th-place score; a distributed partial top-K would silently
  undercount ties.
- The full per-molecule similarity table (required by step 4) is retained as the **evidence
  base** that makes the tie flag verifiable.

---

## 6. Data quality

**Principle: reject only on fields we consume.** A source row exists to yield a `chembl_id`
that has a structure. IC50 is never used downstream, so a corrupt IC50 cannot justify a reject.

| Outcome | Condition | Destination |
|---------|-----------|-------------|
| REJECT | empty / missing name | `quarantine.parquet` |
| REJECT | name unresolved (all tiers) | `quarantine.parquet` |
| REJECT | ambiguous, MW cannot disambiguate | `quarantine.parquet` |
| REJECT | no `canonical_smiles` in ChEMBL | `quarantine.parquet` |
| REJECT | MW mismatch (gross) | `quarantine.parquet` |
| WARN | `ic50 <= 0` / non-numeric / empty | `resolved.parquet` + `dq_flags` |
| WARN | MW mismatch (within tolerance) | `resolved.parquet` + `dq_flags` |
| WARN | resolved via a lower-confidence tier | `resolved.parquet` + `dq_flags` |
| WARN | duplicate name across batches | collapsed + `dq_flags` |

Per the mentor's guidance, a corrupt IC50 on an otherwise real compound (e.g. Haloperidol)
is **kept and flagged**, not dropped. DQ flags live in the Silver `source_set` (grain = input
row), not in the warehouse (grain = molecule). Deduplication is by `chembl_id`, **after**
resolution, merging the `dq_flags` of collapsed rows.

### Name resolution (strategy chain)

| Tier | Method | Cost |
|------|--------|------|
| 1 | exact match on normalised `pref_name` (local) | none |
| 2 | strip salt/ester suffix → parent → validate against `mw_freebase` | none |
| 3 | ChEMBL REST point-lookup on the remainder, cached to Silver | ~5–10 calls |
| 4 | quarantine with an explicit reason | — |

The REST API is used only as a control plane (release/status) and for a handful of point
lookups — never as the bulk data plane. Bulk ingestion is done from the release dump.

---

## 7. Prerequisites

- Docker + docker-compose
- AWS credentials with access to the course S3 bucket (`De-School-students` SSO profile, `eu-central-1`)
- **Cold run only:** ~8–25 GB free disk for the one-time ChEMBL dump extraction
  (afterwards the extract is cached in S3 and the download is skipped entirely)
- An MS Teams incoming webhook (Power Automate) for failure notifications

---

## 8. Setup & launch

```bash
# 1. Start the stack (Postgres + Airflow); migrations create staging/core/views
cd local_deployment
docker-compose up -d

# 2. Configure Airflow Variable and connections
#    CHEMBL_RELEASE=37   (current release, May 2026)
#    plus S3 + Postgres + Teams connections via airflow_settings.yaml

# 3. Trigger the pipeline
#    Airflow UI → similarity_pipeline → Trigger DAG
```

**Cold path** (first run ever): downloads and extracts the ChEMBL dump — several GB and the
full fingerprint generation. Runs once; the results are cached in S3.

**Warm path** (every subsequent run): `acquire` and `generate_fingerprints` skip on cache
hit, so a full run re-does only the cheap analytics stages. This is the path used for the demo.

---

## 9. Testing

```bash
# Per-task unit tests
cd tasks/<task> && pytest

# End-to-end on the mini-corpus (local_deployment/fixtures)
# runs the whole pipeline on ~500 molecules + the real batch CSVs in seconds
```

Coverage highlights: parser routing (valid vs invalid), validators (`ic50<=0` → **valid +
flag**, empty name → quarantine), resolver (name → chembl_id, MW cross-check, ambiguous /
unresolved), fingerprints (known SMILES → known on-bits, unparseable → skip), similarity
(reference Tanimoto pair, self-match excluded), ranking (boundary ties). The e2e test runs
twice to assert idempotency.

---

## 10. Example results

_Populated after the first full run (see execution plan, Phase 12)._

The five views delivered on top of the data mart:

- **7a — average similarity per source molecule.**
- **7b — average deviation of a neighbour's `alogp` from the source molecule.**
- **8a — pivot:** 10 random source molecules as columns, target `chembl_id` as rows, similarity in the cells.
- **8b — window view:** each (source, target, score) row plus the next-ranked target for that
  source and that source's 2nd-ranked target. _(Exact ranking scope pending
  confirmation — see Open questions.)_
- **8c — grouped averages** via `GROUPING SETS` (per source; per source's aromatic_rings +
  heavy_atoms; per source's heavy_atoms; whole dataset), with aggregation nulls shown as `TOTAL`.

> Placeholder — sample rows and a screenshot of each view's output will be added here.

---

## 11. Key design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| Ingestion | Release dump, not REST API for bulk | Quarterly releases → full refresh per release; offset pagination over 5.4M rows is an OLTP interface abused for a bulk task |
| Layers | 2 DWH schemas + S3 lake tier | Honest description; Silver is an artifact store, not DWH tables |
| Bronze | All rows, needed columns only | Projection = storage; selection = semantics (deferred to Silver) |
| Versioning | Everything keyed by `CHEMBL_RELEASE` | Deterministic keys → idempotency for free; proves the pipeline is not a one-off |
| Dimension | SCD Type 1 + surrogate key | Static reference data; `chembl_id` not immutable (OBSOLETE merges) |
| Similarity | Single streaming task | Compute is seconds; orchestration overhead would dominate |
| Ranking | Over the full table | Tie flag needs the complete 10th-place count |
| Isolation | READ COMMITTED (default) | Contention removed by design: parallel writes go to distinct S3 keys; the mart is rebuilt in one transaction |
| Orchestration | One DAG, cadence split via cache skip | A second DAG would add a component for a separation the cache already provides |
| Not done | SCD2, incremental load, Spark, sensors | Scope discipline for a quarterly, single-host, static-input pipeline |

---

## 12. Known limitations

- The full per-molecule similarity table (~1 GB for a few hundred output rows) is written to
  satisfy step 4 and to make the tie flag auditable. This is compliance + evidence, not an
  engineering optimum, and is stated as such.
- The cold path requires several GB of local disk for a one-time dump extraction. The graded
  reviewer pays zero for this — the extract is cached in S3.
- The DAG is intentionally near-linear with no sensors: the input is static, so a sensor would
  be theatre. Parallelism exists only where there are real minutes to save (fingerprints).

---

## Open questions

- **View 8b** — whether the "next / second most similar target" ranking stays entirely within
  the source molecule's scope or shifts to target→target chaining. Implemented as
  source-scoped (Interpretation 1) and isolated in a single file, pending confirmation.
