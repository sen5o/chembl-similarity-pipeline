# ChEMBL Molecular Similarity Pipeline

An end-to-end data engineering pipeline that, for a set of input compounds, finds the
**top-10 most structurally similar molecules** across the ChEMBL database, and serves the
results through a dimensional data mart.

Built with **Airflow 3**, **PostgreSQL**, **AWS S3**, and **RDKit**, deployed locally via
`docker-compose`.

> **Status.** Complete and verified end-to-end on real ChEMBL 35 data: ingestion, source
> resolution, fingerprints, similarity, DWH load, analytical views, and the Airflow DAG running
> every stage in its own container. See §14 for the full status table.

---

## 1. What it does

1. Ingests four ChEMBL tables (the full compound universe: ~2.5M molecules, of which
   ~2.47M carry a 2-D structure).
2. Resolves the input compound names → ChEMBL IDs, applying data-quality rules.
3. Computes **Morgan fingerprints** (radius 2, 2048 bits) for every ChEMBL structure.
4. Computes **Tanimoto similarity** of each input compound against the full corpus.
5. Selects the top-10 neighbours per compound, flagging boundary ties.
6. Loads a **star schema** (`dim_molecule` + `fact_similarity`) and exposes analytical views.

The source set is **all valid, unique, resolved compounds** from the input folder — confirmed
with the mentor. The brief's "100 chosen molecules" is illustrative; nothing hardcodes a
molecule count.

---

## 2. ChEMBL release: pinned to 35

The pipeline is pinned to **ChEMBL 35** via `CHEMBL_RELEASE`, not the latest release. The brief
(6a) requires `cx_logp` and `molecular_species` in the dimension table. These ChemAxon-computed
properties existed only through ChEMBL 35 and were removed afterwards. This was verified against
real dumps: both columns are absent from the entire release-37 dump (checked across all tables)
and present in release 35 — where `cx_logp` is populated for ~97% of molecules (2,409,279 of
2,478,212). Release 35 is the most recent release that still contains both. Ingestion is
release-keyed end to end (`.../release=35/…`), so switching releases is a single variable change
and re-runs are idempotent.

---

## 3. Architecture

A **medallion** design. The two mandatory DWH layers are the Postgres schemas `staging` (Bronze)
and `core` (Gold); the Silver tier is an S3 data-lake tier holding intermediate artifacts. This
is the honest description — Silver is an artifact store, not warehouse tables.

```
   INPUT                BRONZE                    SILVER (S3)                   GOLD
ChEMBL dump (rel 35) -> staging.* (native types) -> source_set {resolved, quarantine}
batch CSVs           -> input_raw  (TEXT)        -> fingerprints/ (2.47M)
                                                 -> similarity/   (56 full tables)
                                                 -> top_similar/  (560 rows)
                                                                    -> core.dim_molecule
                                                                    -> core.fact_similarity
                                                                    -> analytical views
```

| Tier | Where it lives | Contents | Why a separate layer |
|------|----------------|----------|----------------------|
| **Bronze** | Postgres `staging` + `s3://…/bronze/` | Raw ChEMBL tables + raw batch CSVs | Isolates raw data; ingestion repeatable without re-download |
| **Silver** | `s3://…/silver/` (Parquet) | Resolved `source_set`, fingerprints, similarity | Heavy compute kept out of the DB; reusable, versioned |
| **Gold** | Postgres `core` | Star schema + views | Business-ready; only molecules referenced by facts |

### Staging types — by origin, not one rule

- **Batch CSVs** land as `TEXT` — external, dirty data must be quarantined, not fail the load.
- **ChEMBL tables** keep **native types** — ChEMBL is a curated relational source.

Bronze keeps **all rows, needed columns only**: projection is a storage decision; row filtering
(`entity_type`, `status`, has-structure) is semantic and applied downstream.

### The corpus is a filtered read, not a materialised layer

The "corpus" (the compound universe searched for neighbours) is defined once, in
`fingerprint_generation`'s `read_corpus_partition`: `entity_type = 'COMPOUND'`,
`status = 'ACTIVE'`, has a structure, partitioned by `molregno % N`.

It is **not** materialised as a separate Silver artifact. On ChEMBL 35 those filters remove
nothing — measured: all 2,474,590 structured molecules are ACTIVE COMPOUND, and none has a NULL
or empty SMILES — so writing 2.47M rows back to S3 would duplicate data without cleaning it. The
filters stay because they encode the rule explicitly and defend against a dirtier future release.
That single function is also the seam where a materialised corpus would slot in if one were ever
needed.

### S3 layout (under `final_task/hovhannes_karapetian/`)

```
bronze/chembl/release=35/{compound_structures,molecule_dictionary,
                          compound_properties,chembl_id_lookup}/part-000.parquet + _SUCCESS
silver/source_set/{resolved,quarantine}.parquet
silver/fingerprints/release=35/part-000..015.parquet + per-partition _SUCCESS
silver/similarity/release=35/source_chembl_id=<id>/part-000.parquet     (56 full tables)
silver/top_similar/release=35/{top10.parquet,_SUCCESS}
```

Input CSVs live separately under `input/hovhannes-karapetian/` (input path uses a hyphen; the
output prefix uses an underscore).

---

## 4. Data model (star schema)

**`dim_molecule`** — one row per molecule referenced by any fact (sources and targets).

| Column | Source |
|--------|--------|
| `molecule_key` (surrogate PK) | generated |
| `chembl_id` (business key) | ChEMBL |
| `molecule_type` | `molecule_dictionary` |
| `mw_freebase, alogp, psa, cx_logp, molecular_species, full_mwt, aromatic_rings, heavy_atoms` | `compound_properties` |

**SCD Type 1** — molecular properties are static reference data for a pinned release. A
surrogate key is used because ChEMBL merges molecules and retires IDs, so `chembl_id` is not
immutable. On a single pinned release that is mostly about modelling the pattern correctly
rather than solving an observed problem — stated plainly rather than overclaimed.

**`fact_similarity`** — grain: one row per (source, target-in-top-10):
`source_molecule_key`, `target_molecule_key`, `tanimoto_score`,
`has_duplicates_of_last_largest_score`. Invariant: `count = N_sources × 10`.

The tie flag marks **only the boundary rows inside the top-10** — those whose score equals the
10th-place score, and only when further molecules outside the top-10 share it. It does not mark
the whole top-10, and the spilled-over molecules are not stored separately (they remain in the
full per-source table). Confirmed with the mentor.

Schema-level guarantees rather than code-only ones: `CHECK` constraints keep `tanimoto_score`
within [0,1] and forbid self-similarity rows; the PK forbids a target appearing twice in one
source's top-10.

**The full per-source similarity tables are deliberately not loaded into Postgres.** They are
2.47M rows each (138M total) and exist in S3 as the brief's step-4 evidence. No view queries
them, so materialising them in the warehouse would be dead weight; the mart carries what the
analytics actually read.

---

## 5. Data quality & name resolution

**Principle: reject only on fields we consume.** A source row exists to yield a `chembl_id` that
has a structure. IC50 is never used downstream, so a corrupt IC50 is a WARN (kept), not a
reject — per the mentor's ruling.

| Outcome | Condition | Destination |
|---------|-----------|-------------|
| REJECT | empty/missing name, or file with no name column | `quarantine.parquet` |
| REJECT | name unresolved / ambiguous / no structure / gross MW mismatch | `quarantine.parquet` |
| WARN | `ic50 <= 0` or non-numeric; MW unavailable; low-confidence resolution | `resolved.parquet` + `dq_flags` |

The parser maps columns to canonical fields by name (case-insensitive, synonym-aware), not by
position or count, and tolerates BOM, whitespace, quoting, ragged rows, and `N/A` without
crashing. DQ flags live in the Silver `source_set` (grain = input row), not the warehouse
(grain = molecule). Dedup is by `chembl_id`, after resolution, merging the `dq_flags` of
collapsed rows.

### Resolver — strategy chain

| Tier | Method | Status |
|------|--------|--------|
| 1 | exact match on normalised `pref_name` | **active** |
| 2 | salt/ester suffix → parent, MW-verified | deferred (extension seam in `resolver.py`) |
| 3 | ChEMBL API synonym lookup (e.g. Paracetamol→Acetaminophen) | deferred (extension seam) |
| 4 | quarantine with an explicit reason | active |

Molecular weight is a **verifier, never a selector**: name leads, MW confirms or disambiguates.
A candidate is accepted only if it has a structure and its `mw_freebase`/`full_mwt` is within
`max(0.5 Da, 1%)` of the input weight. Multiple name matches are disambiguated by MW; if MW
can't pick one, the row is quarantined as `ambiguous` rather than guessed. Tiers 2 and 3 are
deferred on measured grounds: the current input contains no salts, and the single synonym miss
is Paracetamol. Both slot into the chain later without rework.

### DQ gate

Metrics are logged on every run. The task fails only on a **systemic** collapse: `resolved == 0`
or a resolution rate below 50% (computed over rows that reached the resolver, so parse-level
dirtiness isn't counted against resolution). Below 90% logs a warning but proceeds.

---

## 6. Results (measured on ChEMBL 35)

### Ingestion

Row counts: `compound_structures` 2,474,590 · `molecule_dictionary` 2,496,335 ·
`compound_properties` 2,478,212 · `chembl_id_lookup` 4,806,457. Postgres counts match the S3
`_SUCCESS` manifest exactly for all four tables. Values were verified beyond counts: no
NULL/empty-string confusion in structures, sane numeric ranges (`avg mw_freebase` 433.1,
`cx_logp` ∈ [−20.95, 24.88]), and `molregno` non-null and unique across the dictionary — so
downstream joins are safe. The ~21.7k dictionary rows without a structure are records that
legitimately have no 2-D structure and are excluded at the fingerprint stage.

### Source resolution

Current input (5 CSVs, 58 rows):

| Metric | Value |
|--------|-------|
| Parsed rows | 58 |
| DQ-rejected (parse-level) | 1 — empty name (`CPD-043`) |
| Reached resolver | 57 |
| Resolved to `chembl_id` | 56 |
| Resolution-quarantined | 1 — `Paracetamol` (unresolved synonym → Tier 3) |
| **Resolution rate** | **98.25%** |

### Fingerprints

2,474,576 fingerprints across 16 partitions, ~25 s per partition (~7 min for the full corpus,
sequential). **14 SMILES out of 2.47M (0.0006%) could not be parsed by RDKit** and are excluded;
they are counted in each partition's reconciliation manifest (`structures_in` vs
`fingerprints_out`) rather than silently dropped.

### Similarity

56 sources × 2,474,576 corpus = ~138M Tanimoto comparisons; 560 top-10 rows (56 × 10), no source
missing from the corpus. The corpus loads into memory as 2.47M `ExplicitBitVect` objects in
**1,110 MB** (measured), which is what made the single-task, load-once design viable.

Run times, all measured: cold run **8:33**; warm run reusing per-source tables **5:25**;
fully cached run **2.3 s**.

### Data mart

615 dimension rows, 560 facts, 56 distinct sources, 607/615 molecules carrying `cx_logp`. The
615 is one fewer than the 616 upper bound (56 sources + 560 unique targets) because exactly one
molecule appears both as a source and as another source's neighbour.

### Finding: 71 pairs score exactly 1.0

71 of 560 facts (12.7%) have `tanimoto_score = 1.0` despite self-matches being excluded. These
are **stereoisomer pairs**. Morgan fingerprints are built on connectivity and do not encode
stereochemistry (`includeChirality` defaults to false), so enantiomers are bit-identical.
Verified on real structures:

```
CHEMBL521  IBUPROFEN     CC(C)Cc1ccc( C(C)C(=O)O )cc1
CHEMBL175  DEXIBUPROFEN  CC(C)Cc1ccc([C@H](C)C(=O)O)cc1
```

Identical connectivity; the only difference is the `[C@H]` stereo marker. Same for
citalopram / escitalopram. This is expected behaviour for structural similarity search, not a
defect — and it is precisely why self-matches are excluded **by `chembl_id`, not by
`score = 1.0`**: the latter would have discarded 71 legitimate nearest neighbours.

### Worked example: Aspirin

The clearest evidence that the results are chemically meaningful, not merely self-consistent.
Searching 2,474,576 molecules for the neighbours of **CHEMBL25 (Aspirin)** returns:

| Score | ChEMBL ID | Name | Structure |
|-------|-----------|------|-----------|
| 0.8889 | CHEMBL3833404 | Carbaspirin | two aspirin units co-crystallised with urea |
| 0.8571 | CHEMBL350343 | Diplosalsalate | aspirin esterified with salicylic acid |
| 0.7407 | CHEMBL5282669 | — | the carboxyl replaced by a ketone |
| 0.7037 | CHEMBL4515737 | — | carbonate in place of the acetate |
| 0.7000 | CHEMBL1451173 | Dipyrocetyl | the same scaffold bearing two acetate groups |
| 0.6774 | CHEMBL163612 | Phenylaspirinate | phenyl ester of aspirin |
| 0.6667 | CHEMBL1530334 | — | methyl carbonate of salicylic acid |
| 0.6667 | CHEMBL163148 | — | methyl salicylate with an acetate |
| 0.6552 | CHEMBL173216 | — | carbamate in place of the acetate |
| 0.6452 | CHEMBL433917 | — | dimethyl carbamate |

Every one of the ten is an aspirin derivative, ordered the way a chemist would expect: structures
containing aspirin whole rank above single functional-group substitutions on its scaffold. Ranks
7 and 8 share a score — a tie *inside* the top-10 — while the 10th score is unique, so nothing
spills past the cut-off and the tie flag is correctly false throughout.

---

## 7. Project structure

```
README.md
dags/{similarity_pipeline.py, callbacks.py}
tasks/
  chembl_ingestion/       acquire dump -> S3 parquet; load parquet -> staging.*
  source_resolution/      parse + DQ + name->chembl_id -> silver/source_set
  fingerprint_generation/ Morgan(2,2048) over the corpus -> silver/fingerprints
  similarity_computation/ Tanimoto + full tables + top-10 with tie flag
  dwh_load/               silver top-10 -> core star schema
local_deployment/
  docker-compose.yml
  Dockerfile.airflow          # stock Airflow + the docker provider
  migrations/{01_staging.sql, 02_core_star_schema.sql, 03_views.sql}
  init-dwh-db.sql
e2e_verify.sh                 # cross-layer consistency check
```

Each `tasks/*` folder is an independently containerised unit (`gen/{repository,services,
utils}.py` + `run.py`) with its own dependencies. Views are created by migration
`03_views.sql`; the DAG does not build them.

---

## 8. Orchestration

The DAG (`dags/similarity_pipeline.py`) runs every stage as its own container via
`DockerOperator`, so the containers described in §7 are the real unit of isolation rather than a
claim: RDKit, `chembl_downloader` and `psycopg2` live only in the images that use them, and
Airflow itself needs nothing but the docker provider.

```
acquire_chembl -> load_staging -> resolve_sources ---------> compute_similarity -> load_dwh
                              \-> generate_fingerprints[16] /
```

Two things about the graph are deliberate:

- **No separate ranking task.** `compute_similarity` produces the full per-source tables and the
  top-10 with the tie flag in a single pass over the in-memory corpus. Splitting them would mean
  loading 1.1 GB twice for no gain.
- **Fingerprints are mapped, not looped.** One mapped task per corpus partition, so a failure
  retries its own slice instead of the whole corpus, and progress is visible per partition.

Because every stage is cache-aware, re-triggering a completed DAG skips finished work: a full
warm run takes about two minutes, almost all of it in `load_staging` (the only stage without a
cache, by design — it is a full refresh).

### Runtime wiring, and two traps in it

- **The docker provider is baked into a small custom image** (`Dockerfile.airflow`) instead of
  being installed at container start. Installing it at runtime works only while the container
  runs as the `airflow` user; the moment it runs as root — which reaching the docker socket may
  require — pip refuses and the scheduler exits immediately.
- **`~/.aws` is mounted read-write, not read-only.** Read-only looks safer and fails: botocore
  writes refreshed SSO tokens back into `~/.aws/sso/cache`, so the first token renewal inside a
  task container dies on a read-only filesystem.
- Bind-mount sources are resolved by the **host** docker daemon, so `HOST_HOME` must be the host
  path (e.g. `/Users/you`); expanding `~` inside the scheduler would yield `/home/airflow`.
- Task containers join the compose network and reach Postgres as `postgres:5432`, not localhost.

Failures post an Adaptive Card to Teams (`dags/callbacks.py`). The handler swallows its own
errors: a broken webhook must never replace the exception that actually caused the failure.

---

## 9. Prerequisites

- Docker + docker-compose
- AWS credentials for the course S3 bucket (`De-School-students` SSO profile, `eu-central-1`)
- Cold run only: several GB of local disk for the one-time ChEMBL dump (cached in S3 after)
- An MS Teams incoming webhook for failure notifications (optional for local runs)

---

## 10. Setup & launch

```bash
# 1. build the five task images the DAG runs, plus the Airflow image.
# Names must match the DAG exactly — a missing image makes DockerOperator try to
# pull it from a registry, which fails with a confusing "repository does not exist".
docker build -t chembl-ingestion:latest              tasks/chembl_ingestion/
docker build -t chembl-source-resolution:latest      tasks/source_resolution/
docker build -t chembl-fingerprint-generation:latest tasks/fingerprint_generation/
docker build -t chembl-similarity-computation:latest tasks/similarity_computation/
docker build -t chembl-dwh-load:latest               tasks/dwh_load/
docker build -f local_deployment/Dockerfile.airflow -t chembl-airflow:3.2.0 local_deployment/

# 2. start the stack
cd local_deployment
cp .env.example .env          # set HOST_HOME to your host home directory
docker-compose up -d          # migrations create staging + core schemas and views
```

Airflow UI: `http://localhost:8080`, user `admin`, password auto-generated (Airflow 3):

```bash
docker-compose logs airflow-apiserver | grep -i password
```

Migrations run only on a **first** boot with an empty volume. To apply them to an existing
database:

```bash
docker-compose exec -T postgres psql -U airflow -d dwh < migrations/02_core_star_schema.sql
docker-compose exec -T postgres psql -U airflow -d dwh < migrations/03_views.sql
```

### Airflow 3 setup notes

Airflow 3 differs from 2.x in three ways this custom compose accounts for (each caused a startup
failure until fixed):

- **Auth**: default auth manager is `SimpleAuthManager`; `airflow users create` (a FAB command)
  no longer exists — the admin password is auto-generated.
- **Task execution API**: tasks run via an execution API whose default URL is
  `http://localhost:8080`, the wrong host inside the scheduler container — so
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must point at the `airflow-apiserver` service.
- **JWT**: scheduler and api-server must share `AIRFLOW__API_AUTH__JWT_SECRET`, or task tokens
  fail signature verification.

---

## 11. Running the stages manually

```bash
export CHEMBL_RELEASE=35 DE_SCHOOL_S3_BUCKET=de-school-educational-data
export S3_ROOT_PREFIX=final_task/hovhannes_karapetian AWS_PROFILE=De-School-students
export DWH_DSN=postgresql://airflow:airflow@localhost:5432/dwh

cd tasks/chembl_ingestion
python run.py inspect        # print the release's real column names
python run.py acquire        # dump -> 4 projected tables -> S3 (cached on _SUCCESS)
python run.py load           # S3 parquet -> staging.* via COPY

cd ../source_resolution
export INPUT_S3_PREFIX=input/hovhannes-karapetian/
python run.py resolve        # parse + DQ + resolve -> silver/source_set

cd ../fingerprint_generation
export CORPUS_PARTITIONS=16
for i in $(seq 0 15); do python run.py generate $i; done

cd ../similarity_computation
python run.py compute        # Tanimoto -> full tables + top-10

cd ../dwh_load
python run.py load           # top-10 -> core.dim_molecule + core.fact_similarity
```

Every stage is idempotent: re-running a completed stage is a cache hit, not repeated work.

---

## 12. Testing

```bash
cd tasks/<task> && pytest
```

Unit tests mock IO to test orchestration; a few tests run the real library on small synthetic
data (a 4-row SQLite dump, a 4-molecule corpus) to test correctness rather than wiring.
`ruff`, `mypy`, and Conventional-Commit enforcement run via `pre-commit` on every commit; `mypy`
runs per task, because each task has its own `gen` package and analysing them as one tree
collides on the shared name.

Coverage highlights: parser robustness (schema drift, case-insensitive columns, `N/A`, empty
name); validators (`ic50<=0` → valid+flag, empty name → quarantine); resolver (exact match, MW
verification, disambiguation, ambiguous/unresolved/no-structure); a regression test for a real
bug where an all-NULL first chunk broke the parquet writer; COPY round-trip (NULL vs empty
string); fingerprint reference values (aspirin = 24 on-bits) and binary round-trip; ranking
tie-flag boundary cases (no tie / tie spilling past the cut-off / tie contained inside the
top-10); cache paths; and the DWH referential-integrity guard.

---

## 13. Key design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| Ingestion | Release dump, not REST API for bulk | Quarterly releases → full refresh; offset pagination over 5.4M rows abuses an OLTP interface |
| Dump format | SQLite, not `pg_restore -t` | Parquet in S3 is required anyway (Bronze + brief step 3), so SQLite → parquet → COPY is one linear path instead of two |
| Release | Pinned to ChEMBL 35 | `cx_logp` / `molecular_species` (brief 6a) exist only through release 35 |
| Layers | 2 DWH schemas + S3 lake tier | Honest: Silver is an artifact store, not warehouse tables |
| Corpus | Filtered read, not a materialised Silver artifact | Measured: the filters remove nothing on release 35; materialising would duplicate 2.47M rows without cleaning them |
| Versioning | Everything keyed by `CHEMBL_RELEASE` | Deterministic keys → idempotency; proves the pipeline is repeatable |
| Cache | `_SUCCESS` marker, not bare prefix | A partial upload isn't mistaken for a complete one |
| Staging types | TEXT for CSVs, native for ChEMBL | Different origin → different rules |
| Fingerprint storage | RDKit binary in a parquet `BYTES` column | Native round-trip for `BulkTanimotoSimilarity`; RDKit compresses sparse vectors (aspirin ≈ 41 B, not 256) |
| Resolver MW | Verifier, never selector | Mass is shared by many molecules; name leads, MW confirms |
| Resolver tiers | Tier 1 active; Tiers 2/3 deferred with seams | Measured: no salts in the input; one synonym miss |
| Similarity task | One task, corpus loaded once | Measured 1,110 MB resident; per-source mapping would reload it N times |
| Top-10 | `heapq.nlargest`, not a full sort | Measured: 0.12 s vs 6 s per source — 5.6 min saved across 56 sources |
| Self-match | Excluded by `chembl_id`, not by `score = 1.0` | 71 legitimate stereoisomer pairs score exactly 1.0 |
| Similarity cache | Two layers: global marker + per-source | A cold run is 8.5 min and IO-bound; a retry resumes instead of recomputing |
| Fact grain | Top-10 pairs only | Full 2.47M-row tables are step-4 evidence in S3; no view reads them |
| Dimension | SCD Type 1 + surrogate key, facts-only scope | Static reference data; `chembl_id` not immutable; a dimension describes facts |
| Pivot (8a) | Generic column slots + legend view | A view's columns are fixed at creation; hardcoding IDs breaks on new input, dynamic DDL moves schema into the pipeline |
| Orchestration | DockerOperator per stage | Makes container-per-task real rather than documented; Airflow needs only the docker provider |
| Airflow image | Provider baked into a custom image | Runtime install breaks outright when the container runs as root, and repeats on every boot |
| Isolation | READ COMMITTED (default) | Contention removed by design: parallel writes to distinct S3 keys; mart rebuilt in one transaction |
| Not done | SCD2, incremental load, Spark, streaming, nested multiprocessing | Scope discipline; several were prototyped in design and dropped once measurement showed they bought nothing |

---

## 14. Pipeline status

| Stage | Task | Status |
|-------|------|--------|
| Acquire ChEMBL → S3 (Bronze) | `chembl_ingestion` | built, verified on real data |
| Load parquet → `staging.*` | `chembl_ingestion` | built, verified |
| Resolve names → `chembl_id` | `source_resolution` | built, verified (98.25%) |
| Morgan fingerprints | `fingerprint_generation` | built, verified (2,474,576) |
| Tanimoto + top-10 + tie flag | `similarity_computation` | built, verified (560 rows) |
| Load star schema | `dwh_load` | built, verified (615 dim / 560 facts) |
| Analytical views (7a–8c) | `03_views.sql` | built, verified |
| DAG wiring + Teams payload | `dags/` | built, verified from the UI |

---

## 15. Views

All six live in `core` and are created by migration.

- **`v_avg_similarity_per_source`** (7a) — mean/min/max Tanimoto per source.
- **`v_avg_alogp_deviation`** (7b) — mean **absolute** deviation,
  mean |alogp(neighbour) − alogp(source)|, which is the interpretation the mentor prefers: the
  question is how far neighbours sit from the source, not in which direction. `alogp` is not
  universally populated, so a `comparable_neighbours` count reports how many pairs actually
  contributed instead of leaving the gap implicit.
- **`v_pivot_source_legend`** + **`v_top10_pivot`** (8a) — the similarity matrix with ten
  sources as columns. A view's column names are fixed at creation, so the columns are generic
  slots and the legend maps slot → `chembl_id`; "random" is implemented as deterministic
  pseudo-random (ordering by `md5(chembl_id)`) so the pivot is stable across queries.
- **`v_neighbour_ranking_context`** (8b) — each (source, target) row plus the next-ranked and
  the 2nd-ranked target **within that source's own ranking**; confirmed source-scoped with the
  mentor, with no target→target chaining. The `nth_value` window carries an explicit
  `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame: the default frame ends at
  the current row, so rank 1 would silently return NULL (verified).
- **`v_similarity_grouping_sets`** (8c) — four groupings in one pass, no `UNION`: per source;
  per source's (`aromatic_rings`, `heavy_atoms`); per source's `heavy_atoms`; and the whole
  dataset. `GROUPING()` distinguishes a rolled-up key from a genuinely NULL value — both would
  otherwise print as NULL — and rolled-up keys render as `TOTAL`. Grand total: 560 pairs,
  average Tanimoto 0.8148.
