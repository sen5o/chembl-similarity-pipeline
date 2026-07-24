#!/usr/bin/env bash
# e2e_verify.sh — cross-layer consistency check for the ChEMBL similarity pipeline.
#
# Verifies that every layer agrees with the next, without recomputing anything.
# Run from the repository root with the usual environment exported.
#
#   export CHEMBL_RELEASE=35 DE_SCHOOL_S3_BUCKET=de-school-educational-data
#   export S3_ROOT_PREFIX=final_task/hovhannes_karapetian AWS_PROFILE=De-School-students
#   export DWH_DSN=postgresql://airflow:airflow@localhost:5432/dwh
#   bash e2e_verify.sh

set -uo pipefail

BUCKET="${DE_SCHOOL_S3_BUCKET}"
ROOT="${S3_ROOT_PREFIX}"
REL="${CHEMBL_RELEASE}"
PROFILE="${AWS_PROFILE}"
COMPOSE="${COMPOSE_CMD:-docker-compose -f local_deployment/docker-compose.yml}"
PSQL="$COMPOSE exec -T postgres psql -U airflow -d dwh -tAc"

pass=0; fail=0
check() { # name expected actual
    if [ -z "$3" ]; then
        printf '  \033[31mFAIL\033[0m %-46s no value returned (expected %s)\n' "$1" "$2"; fail=$((fail+1)); return
    fi
    if [ "$2" == "$3" ]; then
        printf '  \033[32mOK\033[0m   %-46s %s\n' "$1" "$3"; pass=$((pass+1))
    else
        printf '  \033[31mFAIL\033[0m %-46s got %s, expected %s\n' "$1" "$3" "$2"; fail=$((fail+1))
    fi
}

echo "=== 1. BRONZE: S3 artifacts ==="
n=$(aws s3 ls "s3://$BUCKET/$ROOT/bronze/chembl/release=$REL/" --profile "$PROFILE" | grep -c PRE)
check "bronze tables in S3" 4 "$n"
aws s3 ls "s3://$BUCKET/$ROOT/bronze/chembl/release=$REL/_SUCCESS" --profile "$PROFILE" >/dev/null 2>&1 \
    && check "bronze _SUCCESS marker" present present \
    || check "bronze _SUCCESS marker" present missing

echo
echo "=== 2. BRONZE -> STAGING: row counts match the manifest ==="
manifest=$(aws s3 cp "s3://$BUCKET/$ROOT/bronze/chembl/release=$REL/_SUCCESS" - --profile "$PROFILE" 2>/dev/null)
for t in compound_structures molecule_dictionary compound_properties chembl_id_lookup; do
    expected=$(echo "$manifest" | python3 -c "import sys,json; print(json.load(sys.stdin)['tables']['$t']['rows'])")
    actual=$($PSQL "SELECT count(*) FROM staging.$t;" | tr -d '[:space:]')
    check "staging.$t" "$expected" "$actual"
done

echo
echo "=== 3. SILVER: source resolution ==="
aws s3 ls "s3://$BUCKET/$ROOT/silver/source_set/resolved.parquet" --profile "$PROFILE" >/dev/null 2>&1 \
    && check "resolved.parquet exists" present present \
    || check "resolved.parquet exists" present missing

echo
echo "=== 4. SILVER: fingerprints reconcile with the corpus ==="
n=$(aws s3 ls "s3://$BUCKET/$ROOT/silver/fingerprints/release=$REL/" --profile "$PROFILE" | grep -c '\.parquet$')
check "fingerprint partitions" "${CORPUS_PARTITIONS:-16}" "$n"

fp_total=0; unparseable=0
for i in $(seq -w 0 $(( ${CORPUS_PARTITIONS:-16} - 1 ))); do
    m=$(aws s3 cp "s3://$BUCKET/$ROOT/silver/fingerprints/release=$REL/part-0$i._SUCCESS" - --profile "$PROFILE" 2>/dev/null)
    out=$(echo "$m" | python3 -c "import sys,json; print(json.load(sys.stdin)['fingerprints_out'])" 2>/dev/null || echo 0)
    bad=$(echo "$m" | python3 -c "import sys,json; print(json.load(sys.stdin)['unparseable_smiles'])" 2>/dev/null || echo 0)
    fp_total=$((fp_total + out)); unparseable=$((unparseable + bad))
done
corpus=$($PSQL "SELECT count(*) FROM staging.compound_structures cs
                JOIN staging.chembl_id_lookup cil
                  ON cil.entity_id = cs.molregno AND cil.entity_type='COMPOUND'
                WHERE cil.status='ACTIVE';" | tr -d '[:space:]')
echo "  ..   corpus (filtered read from staging)     $corpus"
echo "  ..   unparseable SMILES (excluded)           $unparseable"
check "fingerprints + unparseable = corpus" "$corpus" "$((fp_total + unparseable))"

echo
echo "=== 5. SILVER: similarity ==="
sim_dirs=$(aws s3 ls "s3://$BUCKET/$ROOT/silver/similarity/release=$REL/" --profile "$PROFILE" | grep -c "source_chembl_id=")
sim_manifest=$(aws s3 cp "s3://$BUCKET/$ROOT/silver/top_similar/release=$REL/_SUCCESS" - --profile "$PROFILE" 2>/dev/null)
sources=$(echo "$sim_manifest" | python3 -c "import sys,json; print(json.load(sys.stdin)['sources_total'])")
top10=$(echo "$sim_manifest"  | python3 -c "import sys,json; print(json.load(sys.stdin)['top10_rows'])")
csize=$(echo "$sim_manifest"  | python3 -c "import sys,json; print(json.load(sys.stdin)['corpus_size'])")
check "full similarity tables (one per source)" "$sources" "$sim_dirs"
check "top-10 rows = sources x 10" "$((sources * 10))" "$top10"
check "similarity corpus = fingerprint count" "$fp_total" "$csize"

echo
echo "=== 6. GOLD: star schema ==="
facts=$($PSQL "SELECT count(*) FROM core.fact_similarity;" | tr -d '[:space:]')
dim=$($PSQL   "SELECT count(*) FROM core.dim_molecule;" | tr -d '[:space:]')
fsrc=$($PSQL  "SELECT count(DISTINCT source_molecule_key) FROM core.fact_similarity;" | tr -d '[:space:]')
orphans=$($PSQL "SELECT count(*) FROM core.fact_similarity f
                 WHERE NOT EXISTS (SELECT 1 FROM core.dim_molecule d WHERE d.molecule_key=f.source_molecule_key)
                    OR NOT EXISTS (SELECT 1 FROM core.dim_molecule d WHERE d.molecule_key=f.target_molecule_key);" | tr -d '[:space:]')
selfm=$($PSQL "SELECT count(*) FROM core.fact_similarity WHERE source_molecule_key=target_molecule_key;" | tr -d '[:space:]')
oob=$($PSQL   "SELECT count(*) FROM core.fact_similarity WHERE tanimoto_score < 0 OR tanimoto_score > 1;" | tr -d '[:space:]')
check "facts loaded = silver top-10" "$top10" "$facts"
check "distinct sources in facts" "$sources" "$fsrc"
check "facts with no dimension row (orphans)" 0 "$orphans"
check "self-similarity rows" 0 "$selfm"
check "scores outside [0,1]" 0 "$oob"
echo "  ..   dimension rows                          $dim"

echo
echo "=== 7. VIEWS ==="
nviews=$($PSQL "SELECT count(*) FROM pg_views WHERE schemaname='core';" | tr -d '[:space:]')
check "views in core" 6 "$nviews"
v7a=$($PSQL "SELECT count(*) FROM core.v_avg_similarity_per_source;" | tr -d '[:space:]')
check "7a rows = sources" "$sources" "$v7a"
v8b=$($PSQL "SELECT count(*) FROM core.v_neighbour_ranking_context;" | tr -d '[:space:]')
check "8b rows = facts" "$facts" "$v8b"
v8c=$($PSQL "SELECT pairs FROM core.v_similarity_grouping_sets
             WHERE source_chembl_id='TOTAL' AND aromatic_rings='TOTAL' AND heavy_atoms='TOTAL';" | tr -d '[:space:]')
check "8c grand total pairs = facts" "$facts" "$v8c"
legend=$($PSQL "SELECT count(*) FROM core.v_pivot_source_legend;" | tr -d '[:space:]')
check "8a legend slots" 10 "$legend"

echo
echo "======================================================================"
printf '  passed: %s   failed: %s\n' "$pass" "$fail"
[ "$fail" -eq 0 ] && echo "  END-TO-END CONSISTENT" || echo "  INCONSISTENCIES FOUND — see FAIL lines above"
echo "======================================================================"
exit $([ "$fail" -eq 0 ] && echo 0 || echo 1)
