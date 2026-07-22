"""CLI entrypoint for the fingerprint_generation task container.

Usage:
    python run.py generate <partition_index>   # fingerprint one corpus partition

Partitions are fingerprinted independently and in parallel via Airflow dynamic
task mapping; this entrypoint handles exactly one. CORPUS_PARTITIONS controls
how many there are (default 16).
"""

from __future__ import annotations

import sys

from gen import services, utils


def main() -> int:
    utils.configure_logging()
    if len(sys.argv) != 3 or sys.argv[1] != "generate":
        print(__doc__)
        return 1
    try:
        part = int(sys.argv[2])
    except ValueError:
        print(f"partition index must be an integer, got {sys.argv[2]!r}")
        return 1

    services.generate_partition(part)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
