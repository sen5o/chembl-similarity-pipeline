-- 01_staging.sql — Bronze layer
-- Applied automatically on first DWH container boot (in filename order).

CREATE SCHEMA IF NOT EXISTS staging;

-- Input batch CSVs land as TEXT: external, dirty data must be quarantined,
-- not fail the load on a type mismatch. (See README §2, "A note on staging types".)
CREATE TABLE IF NOT EXISTS staging.input_raw (
    source_file   TEXT,
    row_number    INTEGER,
    compound_name TEXT,
    ic50_nm       TEXT,
    molecular_weight TEXT,
    raw_line      TEXT,
    ingested_at   TIMESTAMPTZ DEFAULT now()
);

-- ChEMBL staging tables — native types (README ADR 7: ChEMBL is a curated
-- relational source, so it keeps native types; only the dirty input CSVs use
-- TEXT). Column set and types mirror chembl_ingestion's TABLE_SCHEMAS exactly:
-- pyarrow int64 -> BIGINT, float64 -> DOUBLE PRECISION, string -> TEXT.
-- No PK/FK constraints here: staging is a Bronze landing area, loaded by
-- TRUNCATE + COPY (full refresh per release), not a modelled layer.

CREATE TABLE IF NOT EXISTS staging.compound_structures (
    molregno           BIGINT,
    canonical_smiles   TEXT,
    standard_inchi_key TEXT
);

CREATE TABLE IF NOT EXISTS staging.molecule_dictionary (
    molregno      BIGINT,
    chembl_id     TEXT,
    pref_name     TEXT,
    molecule_type TEXT
);

CREATE TABLE IF NOT EXISTS staging.compound_properties (
    molregno          BIGINT,
    mw_freebase       DOUBLE PRECISION,
    alogp             DOUBLE PRECISION,
    psa               DOUBLE PRECISION,
    cx_logp           DOUBLE PRECISION,
    molecular_species TEXT,
    full_mwt          DOUBLE PRECISION,
    aromatic_rings    BIGINT,
    heavy_atoms       BIGINT
);

CREATE TABLE IF NOT EXISTS staging.chembl_id_lookup (
    chembl_id   TEXT,
    entity_type TEXT,
    entity_id   BIGINT,
    status      TEXT
);
