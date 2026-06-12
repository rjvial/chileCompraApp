"""Thin wrapper — the strawman generator lives in chilecompra_er.strawman.

Equivalent CLI:  chilecompra-er generate-schemas [--only id] [--samples 50]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection
from chilecompra_er.strawman import (  # noqa: F401  (re-exported for older scripts)
    FAMILIES,
    RESPONSE_SCHEMA,
    SYSTEM,
    curate,
    dry_run,
    fetch_samples,
    generate,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default=None)
    parser.add_argument("--samples", type=int, default=50)
    args = parser.parse_args()

    conn = get_connection()
    try:
        generate(conn, only=args.only, samples=args.samples)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
