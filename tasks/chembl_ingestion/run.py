"""CLI entrypoint for the chembl_ingestion task container.

Usage:
    python run.py acquire   # Bronze acquisition (idempotent, cached on _SUCCESS)
    python run.py load      # load Bronze parquet from S3 into staging.* (Postgres)
    python run.py inspect   # print real column names per table (schema check)
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 2 or sys.argv[1] not in {"acquire", "load", "inspect"}:
        print(__doc__)
        return 1

    command = sys.argv[1]
    if command == "acquire":
        services.acquire_chembl()
    elif command == "load":
        services.load_staging()
    else:
        services.inspect_schema()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
