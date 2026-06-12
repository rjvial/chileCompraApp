"""M0 profiling against the live graph: head-noun groups ranked by awarded
spend, the ranking that picks which categories to build (design §3, §10).

    python examples/profile_m0.py [--top 30] [--csv data/profiling_m0.csv]
"""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection
from chilecompra_er.profiling import fetch_item_spend, profile


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--segment", type=int, default=None,
                        help="UNSPSC segment scope, e.g. 42 = medical supplies")
    args = parser.parse_args()

    conn = get_connection()
    try:
        rows = fetch_item_spend(conn, unspsc_segment=args.segment)
    finally:
        conn.close()
    print(f"tender items profiled: {len(rows)}")

    stats = profile(rows)

    header = f"{'group':<22}{'records':>9}{'distinct':>10}{'spend CLP':>18}{'share':>8}{'cum':>8}"
    print(header)
    print("-" * len(header))
    for s in stats[: args.top]:
        print(f"{s.group:<22}{s.records:>9}{s.distinct_texts:>10}"
              f"{s.spend_clp:>18,.0f}{s.spend_share:>8.1%}{s.cum_share:>8.1%}")

    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["group", "records", "distinct_texts", "spend_clp",
                        "spend_share", "cum_share"])
            for s in stats:
                w.writerow([s.group, s.records, s.distinct_texts,
                            f"{s.spend_clp:.0f}", f"{s.spend_share:.6f}",
                            f"{s.cum_share:.6f}"])
        print(f"\nfull ranking written to {args.csv}")


if __name__ == "__main__":
    main()
