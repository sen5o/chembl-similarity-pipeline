"""CLI entrypoint for similarity_computation (brief steps 3-5).

Usage:
    python run.py compute   # Tanimoto of all sources vs corpus, full tables + top-10

Single invocation: loads the corpus once and processes all sources. There is
no per-source partitioning (see services.run for why).
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 2 or sys.argv[1] != "compute":
        print(__doc__)
        return 1
    services.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
