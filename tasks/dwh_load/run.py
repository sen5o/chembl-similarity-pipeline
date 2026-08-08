"""CLI entrypoint for the dwh_load task container.

Usage:
    python run.py load    # silver top-10 -> core.dim_molecule + core.fact_similarity
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 2 or sys.argv[1] != "load":
        print(__doc__)
        return 1
    services.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
