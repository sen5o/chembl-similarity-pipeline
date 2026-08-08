"""CLI entrypoint for the source_resolution task container.

Usage:
    python run.py resolve   # parse+validate+resolve input -> silver/source_set

Required env: DE_SCHOOL_S3_BUCKET, INPUT_S3_PREFIX, S3_ROOT_PREFIX, DWH_DSN
(and AWS credentials for S3 access).
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 2 or sys.argv[1] != "resolve":
        print(__doc__)
        return 1
    services.resolve_sources()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
