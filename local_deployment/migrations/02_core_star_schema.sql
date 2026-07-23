-- 02_core_star_schema.sql — Gold layer (star schema)
--
-- Grain of the mart: one fact row per (source molecule, target molecule that
-- made that source's top-10). ~56 sources x 10 = ~560 rows.
--
-- The FULL per-source similarity tables (2.47M rows each, brief step 4) are
-- deliberately NOT loaded here: they live in S3 as Silver evidence. No view in
-- 03_views.sql queries them, so materialising 138M rows in Postgres would be
-- dead weight. The mart carries what the analytics actually read.

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- Dimension
-- ---------------------------------------------------------------------------
-- One row per molecule referenced by any fact (sources and their top-10
-- targets) — a few hundred rows, not the 2.47M corpus. A dimension exists to
-- describe facts; molecules that never appear in a top-10 have nothing to
-- describe.
--
-- SCD Type 1 (overwrite): molecular properties are static reference data for a
-- given ChEMBL release; there is no history to preserve.
--
-- Surrogate key: chembl_id is the business key but is not immutable — ChEMBL
-- merges molecules and retires ids (status OBS). The surrogate insulates facts
-- from that. On a single pinned release this is mostly about modelling the
-- pattern correctly rather than solving an observed problem; stated plainly
-- rather than overclaimed.
--
-- The 10 attributes are exactly those the brief (6a) requires.
CREATE TABLE IF NOT EXISTS core.dim_molecule (
    molecule_key      BIGSERIAL PRIMARY KEY,
    chembl_id         TEXT NOT NULL UNIQUE,          -- business key
    molecule_type     TEXT,
    mw_freebase       DOUBLE PRECISION,
    alogp             DOUBLE PRECISION,
    psa               DOUBLE PRECISION,
    cx_logp           DOUBLE PRECISION,
    molecular_species TEXT,
    full_mwt          DOUBLE PRECISION,
    aromatic_rings    BIGINT,
    heavy_atoms       BIGINT,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.dim_molecule IS
    'SCD1 dimension: one row per molecule appearing in fact_similarity.';

-- ---------------------------------------------------------------------------
-- Fact
-- ---------------------------------------------------------------------------
-- Grain: (source, target-in-top-10). Both keys reference the same dimension —
-- a molecule can be a source in one row and a target in another.
CREATE TABLE IF NOT EXISTS core.fact_similarity (
    source_molecule_key BIGINT NOT NULL REFERENCES core.dim_molecule (molecule_key),
    target_molecule_key BIGINT NOT NULL REFERENCES core.dim_molecule (molecule_key),
    tanimoto_score      DOUBLE PRECISION NOT NULL,
    has_duplicates_of_last_largest_score BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- one row per (source, target) pair: a target cannot appear twice in the
    -- same source's top-10
    PRIMARY KEY (source_molecule_key, target_molecule_key),

    -- Tanimoto is a bounded coefficient; anything outside [0,1] is a bug
    CONSTRAINT tanimoto_score_in_range
        CHECK (tanimoto_score >= 0 AND tanimoto_score <= 1),

    -- a molecule is not its own neighbour: self-matches (score 1.0) are excluded
    -- during ranking, and this keeps that guarantee in the schema, not only in code
    CONSTRAINT no_self_similarity
        CHECK (source_molecule_key <> target_molecule_key)
);

COMMENT ON TABLE core.fact_similarity IS
    'Top-10 structurally similar molecules per source; grain = (source, target).';
COMMENT ON COLUMN core.fact_similarity.has_duplicates_of_last_largest_score IS
    'True when targets outside the top-10 share the 10th-place score (contended cut-off).';

-- Views read source-first, which the PK's leading column already covers. This
-- index serves the reverse lookup: "which sources is this molecule a neighbour of".
CREATE INDEX IF NOT EXISTS ix_fact_similarity_target
    ON core.fact_similarity (target_molecule_key);
