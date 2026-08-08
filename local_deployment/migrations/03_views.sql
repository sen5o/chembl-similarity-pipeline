-- 03_views.sql — analytical views on the data mart (brief steps 7 & 8)
--
-- Views are declarative and created by migration; the DAG does not build them.
-- They read core.fact_similarity (grain: source x top-10 target) joined to
-- core.dim_molecule twice — once for the source's attributes, once for the
-- target's — since both roles point at the same dimension.
--
-- Note on rounding: tanimoto_score is DOUBLE PRECISION, and PostgreSQL has no
-- round(double precision, integer) — only round(numeric, integer). Hence the
-- explicit ::numeric casts below.

-- ===========================================================================
-- 7a — average similarity per source molecule
-- ===========================================================================
CREATE OR REPLACE VIEW core.v_avg_similarity_per_source AS
SELECT
    ds.chembl_id                              AS source_chembl_id,
    count(*)                                  AS neighbours,
    round(avg(f.tanimoto_score)::numeric, 4)  AS avg_tanimoto,
    round(min(f.tanimoto_score)::numeric, 4)  AS min_tanimoto,
    round(max(f.tanimoto_score)::numeric, 4)  AS max_tanimoto
FROM core.fact_similarity f
JOIN core.dim_molecule ds ON ds.molecule_key = f.source_molecule_key
GROUP BY ds.chembl_id;

COMMENT ON VIEW core.v_avg_similarity_per_source IS
    'Brief 7a: mean Tanimoto of a source molecule against its top-10 neighbours.';

-- ===========================================================================
-- 7b — average deviation of a neighbour's alogp from the source's alogp
-- ===========================================================================
-- alogp is not populated for every molecule. avg() ignores NULL operands, so
-- incomparable pairs drop out silently; `comparable_neighbours` reports how
-- many pairs actually contributed, making the gap visible instead of implied.
CREATE OR REPLACE VIEW core.v_avg_alogp_deviation AS
SELECT
    ds.chembl_id AS source_chembl_id,
    ds.alogp     AS source_alogp,
    count(*) FILTER (
        WHERE ds.alogp IS NOT NULL AND dt.alogp IS NOT NULL
    ) AS comparable_neighbours,
    round(avg(abs(dt.alogp - ds.alogp))::numeric, 4) AS avg_abs_alogp_deviation
FROM core.fact_similarity f
JOIN core.dim_molecule ds ON ds.molecule_key = f.source_molecule_key
JOIN core.dim_molecule dt ON dt.molecule_key = f.target_molecule_key
GROUP BY ds.chembl_id, ds.alogp;

COMMENT ON VIEW core.v_avg_alogp_deviation IS
    'Brief 7b: mean |alogp(neighbour) - alogp(source)| across a source''s top-10.';

-- ===========================================================================
-- 8a — pivot: 10 source molecules as columns, targets as rows
-- ===========================================================================
-- A view's column names are fixed at creation time, so the ten sources cannot
-- become literal column headers without either hardcoding today's chembl_ids
-- (breaks when the input set changes) or rebuilding the view dynamically after
-- every load (moves DDL into the pipeline). Instead the columns are generic
-- slots and this legend maps slot -> chembl_id.
--
-- "Random" is implemented as deterministic-pseudo-random: ordering by md5 of
-- the chembl_id gives an arbitrary but reproducible pick, so the pivot is
-- stable across runs rather than shuffling on every query.
CREATE OR REPLACE VIEW core.v_pivot_source_legend AS
SELECT chembl_id AS source_chembl_id, slot
FROM (
    SELECT
        d.chembl_id,
        row_number() OVER (ORDER BY md5(d.chembl_id)) AS slot
    FROM core.dim_molecule d
    WHERE EXISTS (
        SELECT 1
        FROM core.fact_similarity f
        WHERE f.source_molecule_key = d.molecule_key
    )
) ranked
WHERE slot <= 10;

COMMENT ON VIEW core.v_pivot_source_legend IS
    'Brief 8a companion: which source molecule occupies each pivot column slot.';

CREATE OR REPLACE VIEW core.v_top10_pivot AS
SELECT
    dt.chembl_id AS target_chembl_id,
    max(CASE WHEN l.slot = 1  THEN f.tanimoto_score END) AS source_01,
    max(CASE WHEN l.slot = 2  THEN f.tanimoto_score END) AS source_02,
    max(CASE WHEN l.slot = 3  THEN f.tanimoto_score END) AS source_03,
    max(CASE WHEN l.slot = 4  THEN f.tanimoto_score END) AS source_04,
    max(CASE WHEN l.slot = 5  THEN f.tanimoto_score END) AS source_05,
    max(CASE WHEN l.slot = 6  THEN f.tanimoto_score END) AS source_06,
    max(CASE WHEN l.slot = 7  THEN f.tanimoto_score END) AS source_07,
    max(CASE WHEN l.slot = 8  THEN f.tanimoto_score END) AS source_08,
    max(CASE WHEN l.slot = 9  THEN f.tanimoto_score END) AS source_09,
    max(CASE WHEN l.slot = 10 THEN f.tanimoto_score END) AS source_10
FROM core.fact_similarity f
JOIN core.dim_molecule ds            ON ds.molecule_key = f.source_molecule_key
JOIN core.v_pivot_source_legend l    ON l.source_chembl_id = ds.chembl_id
JOIN core.dim_molecule dt            ON dt.molecule_key = f.target_molecule_key
GROUP BY dt.chembl_id;

COMMENT ON VIEW core.v_top10_pivot IS
    'Brief 8a: similarity matrix, 10 sources as columns (see v_pivot_source_legend), targets as rows.';

-- ===========================================================================
-- 8b — each pair plus its neighbours within the SAME source's ranking
-- ===========================================================================
-- Confirmed with the mentor as source-scoped: the "next" and "second" targets
-- are positions inside this source's own top-10, not a chain through the
-- target molecule's own neighbours.
--
-- The explicit frame on nth_value is required, not decorative: the default
-- frame ends at the current row, so rank 1 would see no second-ranked value
-- and silently return NULL. UNBOUNDED FOLLOWING makes every row see the whole
-- partition (verified: without it, row 1 returns NULL).
CREATE OR REPLACE VIEW core.v_neighbour_ranking_context AS
SELECT
    ds.chembl_id       AS source_chembl_id,
    dt.chembl_id       AS target_chembl_id,
    f.tanimoto_score,
    f.has_duplicates_of_last_largest_score,
    row_number() OVER w                AS rank_in_source,
    lead(dt.chembl_id) OVER w          AS next_ranked_target,
    nth_value(dt.chembl_id, 2) OVER (
        PARTITION BY f.source_molecule_key
        ORDER BY f.tanimoto_score DESC, dt.chembl_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                  AS second_ranked_target
FROM core.fact_similarity f
JOIN core.dim_molecule ds ON ds.molecule_key = f.source_molecule_key
JOIN core.dim_molecule dt ON dt.molecule_key = f.target_molecule_key
WINDOW w AS (
    PARTITION BY f.source_molecule_key
    ORDER BY f.tanimoto_score DESC, dt.chembl_id
);

COMMENT ON VIEW core.v_neighbour_ranking_context IS
    'Brief 8b: each (source, target) row plus the next-ranked and 2nd-ranked target within that source''s ranking.';

-- ===========================================================================
-- 8c — grouped averages via GROUPING SETS, aggregation nulls shown as TOTAL
-- ===========================================================================
-- Four groupings in one pass, no UNION:
--   per source; per source's (aromatic_rings, heavy_atoms); per source's
--   heavy_atoms; and the whole dataset.
--
-- GROUPING() distinguishes "this column is rolled up" from "this column is
-- genuinely NULL in the data" — both would otherwise print as NULL and be
-- indistinguishable. Rolled-up columns render as 'TOTAL'.
CREATE OR REPLACE VIEW core.v_similarity_grouping_sets AS
SELECT
    CASE WHEN grouping(ds.chembl_id)      = 1 THEN 'TOTAL'
         ELSE ds.chembl_id END                       AS source_chembl_id,
    CASE WHEN grouping(ds.aromatic_rings) = 1 THEN 'TOTAL'
         ELSE ds.aromatic_rings::text END            AS aromatic_rings,
    CASE WHEN grouping(ds.heavy_atoms)    = 1 THEN 'TOTAL'
         ELSE ds.heavy_atoms::text END               AS heavy_atoms,
    count(*)                                         AS pairs,
    round(avg(f.tanimoto_score)::numeric, 4)         AS avg_tanimoto
FROM core.fact_similarity f
JOIN core.dim_molecule ds ON ds.molecule_key = f.source_molecule_key
GROUP BY GROUPING SETS (
    (ds.chembl_id),
    (ds.aromatic_rings, ds.heavy_atoms),
    (ds.heavy_atoms),
    ()
);

COMMENT ON VIEW core.v_similarity_grouping_sets IS
    'Brief 8c: average similarity across four grouping sets in one pass; rolled-up keys shown as TOTAL.';
