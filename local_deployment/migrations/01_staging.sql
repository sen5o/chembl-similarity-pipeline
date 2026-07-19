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

-- ChEMBL staging tables (native types) are created in the chembl_ingestion iteration.
-- TODO(feature/chembl-ingestion): staging.chembl_id_lookup, molecule_dictionary,
--                                 compound_properties, compound_structures
