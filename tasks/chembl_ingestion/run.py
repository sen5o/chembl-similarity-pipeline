"""CLI entrypoint for the chembl_ingestion task container.

Usage:
    python run.py acquire   # Bronze acquisition (idempotent, cached on _SUCCESS)
    python run.py inspect   # print real column names per table (schema check)
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 2 or sys.argv[1] not in {"acquire", "inspect"}:
        print(__doc__)
        return 1

    if sys.argv[1] == "acquire":
        services.acquire_chembl()
    else:
        services.inspect_schema()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
