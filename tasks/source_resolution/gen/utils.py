"""Configuration, name normalisation, and MW tolerance for source resolution.

normalise_name() is the single source of truth for how a compound name is
canonicalised before matching. The SQL side must apply the *same*
transformation (upper + trim) to pref_name, or Python and Postgres would
disagree on what "matches". Keeping it here, in one place, is what prevents
that drift.
"""

from __future__ import annotations

import logging
import os


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("source_resolution")


def get_release() -> str:
    release = os.environ.get("CHEMBL_RELEASE")
    if not release:
        raise RuntimeError("CHEMBL_RELEASE is not set.")
    return release


def dwh_dsn() -> str:
    dsn = os.environ.get("DWH_DSN")
    if not dsn:
        raise RuntimeError(
            "DWH_DSN is not set, e.g. postgresql://airflow:airflow@localhost:5432/dwh"
        )
    return dsn


def normalise_name(name: str) -> str:
    """Canonical form used for matching: trimmed, collapsed inner whitespace,
    uppercased. Postgres must mirror this (upper(trim(pref_name))).
    """
    return " ".join(name.split()).upper()


# MW verification tolerance (README ADR / resolver design, B-1): a candidate's
# ChEMBL weight must be within the *larger* of an absolute and a relative
# bound of the input weight. Absolute handles tiny molecules where 1% is a
# hair; relative handles large molecules where 0.5 Da is a hair.
MW_ABS_TOLERANCE = 0.5  # daltons
MW_REL_TOLERANCE = 0.01  # 1%


def mw_within_tolerance(input_mw: float, candidate_mw: float) -> bool:
    """True if candidate_mw is close enough to input_mw to confirm identity."""
    allowed = max(MW_ABS_TOLERANCE, MW_REL_TOLERANCE * input_mw)
    return abs(input_mw - candidate_mw) <= allowed
