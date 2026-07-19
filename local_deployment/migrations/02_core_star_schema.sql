-- 02_core_star_schema.sql — Gold layer (star schema)

CREATE SCHEMA IF NOT EXISTS core;

-- TODO(feature/dwh-ddl):
--   core.dim_molecule    (molecule_key PK, chembl_id, 10 properties; SCD Type 1)
--   core.fact_similarity (source_molecule_key FK, target_molecule_key FK,
--                         tanimoto_score, has_duplicates_of_last_largest_score)
