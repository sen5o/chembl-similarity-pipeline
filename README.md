# ChEMBL Molecular Similarity Pipeline

An end-to-end data engineering pipeline that, for a set of input compounds, finds the
**top-10 most structurally similar molecules** across the ChEMBL database, and serves the
results through a dimensional data mart.

Built with **Airflow 3**, **PostgreSQL**, **AWS S3**, and **RDKit**, deployed locally via
`docker-compose`.

> **Status.** Built and verified end-to-end on real data: ChEMBL ingestion (Bronze) and source
> resolution (Silver). Pending: fingerprint generation, similarity computation, DWH load, and
> analytical views. See §13 for the full status table.

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
        INPUT                    BRONZE                     SILVER (S3)                  GOLD
  input: ChEMBL dump (rel 35) + batch CSVs
    -> staging.* (native types) + input (TEXT)
      -> source_set {resolved, quarantine}  [built]
         fingerprints / similarity          [pending]
           -> core: dim_molecule + fact_similarity + views  [pending]
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
(`entity_type`, non-`OBSOLETE`, has-structure) is semantic and applied downstream, at the
fingerprint stage.

### S3 layout (under `final_task/hovhannes_karapetian/`)

```
bronze/chembl/release=35/{compound_structures,molecule_dictionary,
                          compound_properties,chembl_id_lookup}/*.parquet + _SUCCESS
silver/source_set/{resolved,quarantine}.parquet
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

**SCD Type 1** — molecular properties are static reference data. A surrogate key is used because
ChEMBL merges molecules and marks IDs `OBSOLETE`, so `chembl_id` is not immutable.

**`fact_similarity`** — grain: one row per (source, target-in-top-10) pair:
`source_molecule_key`, `target_molecule_key`, `tanimoto_score`,
`has_duplicates_of_last_largest_score`. Test invariant: `count = N_sources × 10`.

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
crashing — robust to arbitrary input. DQ flags live in the Silver `source_set` (grain = input
row), not the warehouse (grain = molecule). Dedup is by `chembl_id`, after resolution, merging
the `dq_flags` of collapsed rows.

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
can't pick one, the row is quarantined as `ambiguous` rather than guessed. Tier 2 and Tier 3 are
deferred (current input has no salts, and the one synonym miss is Paracetamol); both slot into
the chain later without rework, and any unresolved salt/synonym quarantines cleanly.

### DQ gate

Metrics are logged on every run. The task fails only on a **systemic** collapse: `resolved == 0`
or a resolution rate below 50% (computed over rows that reached the resolver, so parse-level
dirtiness isn't counted against resolution). Below 90% logs a warning but proceeds.

---

## 6. Results

**Source resolution** — current input (5 CSVs, 58 rows) against ChEMBL 35:

| Metric | Value |
|--------|-------|
| Parsed rows | 58 |
| DQ-rejected (parse-level) | 1 — empty name (`CPD-043`) |
| Reached resolver | 57 |
| Resolved to `chembl_id` | 56 |
| Resolution-quarantined | 1 — `Paracetamol` (unresolved synonym → Tier 3) |
| **Resolution rate** | **98.25%** |

**Ingestion** row counts (ChEMBL 35): `compound_structures` 2,474,590 · `molecule_dictionary`
2,496,335 · `compound_properties` 2,478,212 · `chembl_id_lookup` 4,806,457. Postgres row counts
match the S3 `_SUCCESS` manifest exactly for all four tables. Values were verified beyond counts:
no NULL/empty-string confusion in structures, numeric ranges sane (`avg mw_freebase` 433.1,
`cx_logp` ∈ [−20.95, 24.88]), and `molregno` non-null and unique across the dictionary — so
downstream joins are safe. The ~21.7k dictionary rows without a structure are records that
legitimately have no 2-D structure and are excluded at the fingerprint stage.

_Fingerprint / similarity / view results to follow as those stages are built._

---

## 7. Project structure

```
README.md
dags/{similarity_pipeline.py, callbacks.py}
tasks/
  chembl_ingestion/       [built]  acquire dump -> staging -> S3; load parquet -> Postgres
  source_resolution/      [built]  parse + DQ + name->chembl_id -> silver/source_set
  fingerprint_generation/ (pending)
  similarity_computation/ (pending)
  dwh_load/               (pending)
local_deployment/
  docker-compose.yml
  migrations/{01_staging.sql, 02_core_star_schema.sql, 03_views.sql}
  init-dwh-db.sql
```

Each `tasks/*` folder is an independently containerised unit (`gen/{repository,services,
utils}.py` + `run.py`) with its own dependencies. Views are created by migration
`03_views.sql`; the DAG does not build them.

---

## 8. Prerequisites

- Docker + docker-compose
- AWS credentials for the course S3 bucket (`De-School-students` SSO profile, `eu-central-1`)
- Cold run only: several GB of local disk for the one-time ChEMBL dump (cached in S3 after)
- An MS Teams incoming webhook for failure notifications (optional for local runs)

---

## 9. Setup & launch

```bash
cd local_deployment
cp .env.example .env          # set CHEMBL_RELEASE=35 and the Airflow image tag
docker-compose up -d          # migrations create staging + core schemas
```

Airflow UI: `http://localhost:8080`, user `admin`, password auto-generated (Airflow 3):

```bash
docker-compose logs airflow-apiserver | grep -i password
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

## 10. Running the stages manually

```bash
# ChEMBL ingestion (Bronze)
cd tasks/chembl_ingestion
export CHEMBL_RELEASE=35 DE_SCHOOL_S3_BUCKET=de-school-educational-data
export S3_ROOT_PREFIX=final_task/hovhannes_karapetian
export DWH_DSN=postgresql://airflow:airflow@localhost:5432/dwh AWS_PROFILE=De-School-students
python run.py acquire      # dump -> 4 projected tables -> S3 (cached on _SUCCESS)
python run.py load         # S3 parquet -> staging.* via COPY

# Source resolution (Silver)
cd ../source_resolution
export INPUT_S3_PREFIX=input/hovhannes-karapetian/    # plus the vars above
python run.py resolve      # parse + DQ + resolve -> silver/source_set
```

---

## 11. Testing

```bash
cd tasks/<task> && pytest
```

Unit tests mock IO to test orchestration; integration tests (e.g. `truncate_and_copy` against a
real Postgres, gated on `DWH_DSN`) exercise the real path. `ruff`, `mypy`, and Conventional-Commit
enforcement run via `pre-commit` on every commit.

Coverage so far: parser robustness (schema drift, case-insensitive columns, `N/A`, empty name),
validators (`ic50<=0` -> valid+flag, empty name -> quarantine), resolver (exact match, MW
verification, disambiguation, ambiguous/unresolved/no-structure), COPY round-trip (NULL vs empty
string), and orchestration (dedup with flag union, DQ gate thresholds).

---

## 12. Key design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| Ingestion | Release dump, not REST API for bulk | Quarterly releases -> full refresh; offset pagination over 5.4M rows abuses an OLTP interface |
| Release | Pinned to ChEMBL 35 | `cx_logp` / `molecular_species` (brief 6a) exist only through release 35 |
| Layers | 2 DWH schemas + S3 lake tier | Honest: Silver is an artifact store, not warehouse tables |
| Versioning | Everything keyed by `CHEMBL_RELEASE` | Deterministic keys -> idempotency; proves the pipeline is repeatable |
| Cache | `_SUCCESS` marker, not bare prefix | A partial upload isn't mistaken for a complete one |
| Staging types | TEXT for CSVs, native for ChEMBL | Different origin -> different rules |
| Resolver MW | Verifier, never selector | Mass is shared by many molecules; name leads, MW confirms |
| Resolver tiers | Tier 1 active; Tier 2/3 deferred with seams | YAGNI, measured: no salts in input; one synonym miss |
| Dimension | SCD Type 1 + surrogate key | Static reference data; `chembl_id` not immutable |
| Isolation | READ COMMITTED (default) | Contention removed by design: parallel writes to distinct S3 keys; mart rebuilt in one transaction |
| Not done | SCD2, incremental load, Spark, streaming in the resolver | Scope discipline for a quarterly, single-host, small-source-set pipeline |

---

## 13. Pipeline status

| Stage | Task | Status |
|-------|------|--------|
| Acquire ChEMBL -> S3 (Bronze) | `chembl_ingestion` | built, verified on real data |
| Load parquet -> `staging.*` | `chembl_ingestion` | built, verified |
| Resolve names -> `chembl_id` | `source_resolution` | built, verified (98.25%) |
| Morgan fingerprints | `fingerprint_generation` | pending |
| Tanimoto + top-10 + tie flag | `similarity_computation` | pending |
| Load star schema | `dwh_load` | pending |
| Analytical views (7a-8c) | `03_views.sql` | pending |
| DAG wiring + Teams + demo | `dags/` | pending |

---

## 14. Views (planned)

- **7a** — average similarity per source molecule.
- **7b** — average deviation of a neighbour's `alogp` from the source molecule.
- **8a** — pivot: 10 random source molecules as columns, target `chembl_id` as rows, similarity in the cells.
- **8b** — each (source, target, score) row plus, **within the source's own ranking**, the
  next-ranked target and the source's 2nd-ranked target. Confirmed source-scoped by the mentor
  ("go with the first interpretation") — no target->target chaining.
- **8c** — grouped averages via `GROUPING SETS` (per source; per source's aromatic_rings +
  heavy_atoms; per source's heavy_atoms; whole dataset), aggregation nulls shown as `TOTAL`,
  no `UNION`.
