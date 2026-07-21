"""IO boundary for source resolution: the ChEMBL candidate lookup.

One narrow query resolves the whole batch: given the normalised input names,
return every matching molecule_dictionary row joined to its properties (MW)
and a flag for whether it has a structure. Returning *all* matches per name
(not LIMIT 1) is deliberate — the resolver needs to see multiplicity to
detect ambiguity. Everything the resolver decides is computed in Python from
this result; this module only reads.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger("source_resolution")


@dataclass(frozen=True)
class Candidate:
    """A ChEMBL molecule that matched an input name by pref_name."""

    normalised_name: str
    chembl_id: str
    mw_freebase: float | None
    full_mwt: float | None
    has_structure: bool


def fetch_candidates(dsn: str, normalised_names: list[str]) -> list[Candidate]:
    """Return all Tier-1 candidates for the given normalised names.

    Matches upper(trim(pref_name)) against the names (mirroring
    utils.normalise_name). Joins compound_properties for MW and left-joins
    compound_structures to flag structure availability. One round trip.
    """
    if not normalised_names:
        return []

    import psycopg2

    query = """
        SELECT
            upper(btrim(md.pref_name))          AS normalised_name,
            md.chembl_id,
            cp.mw_freebase,
            cp.full_mwt,
            (cs.molregno IS NOT NULL)           AS has_structure
        FROM staging.molecule_dictionary md
        LEFT JOIN staging.compound_properties cp ON cp.molregno = md.molregno
        LEFT JOIN staging.compound_structures cs
               ON cs.molregno = md.molregno
              AND cs.canonical_smiles IS NOT NULL
        WHERE upper(btrim(md.pref_name)) = ANY(%s)
    """

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (normalised_names,))
            rows = cur.fetchall()
    finally:
        conn.close()

    candidates = [
        Candidate(
            normalised_name=r[0],
            chembl_id=r[1],
            mw_freebase=r[2],
            full_mwt=r[3],
            has_structure=r[4],
        )
        for r in rows
    ]
    log.info(
        "Fetched %s candidate rows for %s names",
        len(candidates),
        len(normalised_names),
    )
    return candidates
