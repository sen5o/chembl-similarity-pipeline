"""Name -> chembl_id resolution logic (pure; no IO).

Given a parsed input row and the Tier-1 candidates fetched for its name,
decide the outcome. The chain is deliberately structured as ordered tiers so
that deferred tiers can be slotted in later without touching this control
flow (see the extension seam before quarantine):

    Tier 1  exact match on normalised pref_name  (active)
    [Tier 2 salt/ester -> parent]                (deferred — seam below)
    [Tier 3 ChEMBL API synonym lookup]           (deferred — seam below)
    quarantine with an explicit reason

Molecular weight is used only to *verify* or *disambiguate* candidates found
by name — never to select a molecule on its own (that would be a false-match
hazard, since many distinct molecules share a mass). Name leads; MW confirms.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import utils
from .file_parser import SourceRow
from .repository import Candidate


@dataclass
class Resolution:
    """Outcome of resolving one SourceRow."""

    chembl_id: str | None
    method: str | None  # e.g. "tier1_exact"; None when quarantined
    reject_reason: str | None  # set when quarantined
    dq_flags: list[str]


def _verify_mw(row: SourceRow, candidate: Candidate) -> tuple[bool, str | None]:
    """Check a single candidate's MW against the input row.

    Returns (accept, flag). If the row has no usable input MW, we cannot
    verify and accept on the name match alone (flagged). A ChEMBL weight is
    taken from mw_freebase, falling back to full_mwt (salt form). A gross
    mismatch on both rejects; anything within tolerance accepts.
    """
    if row.molecular_weight is None:
        return True, "mw_unverified"

    chembl_weights = [w for w in (candidate.mw_freebase, candidate.full_mwt) if w is not None]
    if not chembl_weights:
        return True, "mw_unverified"

    if any(utils.mw_within_tolerance(row.molecular_weight, w) for w in chembl_weights):
        return True, None
    return False, "mw_mismatch"


def resolve(row: SourceRow, candidates: list[Candidate]) -> Resolution:
    """Resolve one row to a chembl_id, or quarantine it with a reason.

    `candidates` are the Tier-1 matches for this row's normalised name
    (already filtered to that name by the caller).
    """
    flags = list(row.dq_flags)

    # Candidates must have a structure to be usable for fingerprinting.
    usable = [c for c in candidates if c.has_structure]

    # --- Tier 1: exact pref_name match -----------------------------------
    if len(usable) == 1:
        candidate = usable[0]
        accept, mw_flag = _verify_mw(row, candidate)
        if mw_flag:
            flags.append(mw_flag)
        if not accept:
            return Resolution(None, None, "mw_mismatch", flags)
        return Resolution(candidate.chembl_id, "tier1_exact", None, flags)

    if len(usable) > 1:
        # Ambiguous name: use MW to pick a single candidate.
        if row.molecular_weight is not None:
            confirmed = [
                c
                for c in usable
                if any(
                    utils.mw_within_tolerance(row.molecular_weight, w)
                    for w in (c.mw_freebase, c.full_mwt)
                    if w is not None
                )
            ]
            if len(confirmed) == 1:
                flags.append("disambiguated_by_mw")
                return Resolution(confirmed[0].chembl_id, "tier1_mw_disambig", None, flags)
        return Resolution(None, None, "ambiguous", flags)

    # No usable Tier-1 candidate. Distinguish "matched a name but it had no
    # structure" from "no name match at all" for a clearer quarantine reason.
    if candidates:  # matched by name, but none had a structure
        return Resolution(None, None, "no_structure", flags)

    # --- Extension seam: deferred tiers go here, before quarantine -------
    # TODO(tier-2): salt/ester suffix -> parent-name lookup, MW-verified.
    # TODO(tier-3): ChEMBL API synonym lookup (e.g. Paracetamol->Acetaminophen).
    # Both would attempt resolution here and return a Resolution on success;
    # only if they also fail do we fall through to quarantine below.

    return Resolution(None, None, "unresolved", flags)
