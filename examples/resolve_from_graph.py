"""Retrieve source records from the Mercado Público graph and resolve them
into the catalog — the live end-to-end process.

    python examples/resolve_from_graph.py --contains foley --limit 200
    python examples/resolve_from_graph.py --contains foley --kind offer --dry
    python examples/resolve_from_graph.py --wipe-category sondas_foley

--dry resolves without writing anything (in-memory catalog, no SourceRecords).
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.ingest.export import export_csv

from chilecompra_er.graphdb import get_connection
from chilecompra_er.ingest import (
    fetch_oc_items,
    fetch_offers,
    fetch_tender_items,
    resolve_items,
)
from chilecompra_er.resolve import InMemoryCatalog, Neo4jCatalog, Resolver

FETCHERS = {
    "tender": fetch_tender_items,
    "offer": fetch_offers,
    "oc": fetch_oc_items,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contains", default=None, help="filter on buyer text")
    parser.add_argument("--kind", choices=FETCHERS, default="tender")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--dry", action="store_true", help="no writes at all")
    parser.add_argument("--show", type=int, default=10, help="sample reports to print")
    parser.add_argument("--out", type=Path, default=None,
                        help="CSV output prefix, e.g. data\\foley")
    parser.add_argument("--wipe-category", default=None,
                        help="delete a category's catalog nodes + smoke SourceRecords, then exit")
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.wipe_category:
            conn.query(
                """
                MATCH (g:GenericProduct {category_id: $cid})
                OPTIONAL MATCH (g)<-[:RESOLVED_TO]-(s:SourceRecord)
                DETACH DELETE g, s
                """,
                parameters={"cid": args.wipe_category},
            )
            conn.query("MATCH (c:Category {category_id: $cid}) DETACH DELETE c",
                       parameters={"cid": args.wipe_category})
            print(f"wiped category {args.wipe_category}")
            return

        items = FETCHERS[args.kind](conn, contains=args.contains, limit=args.limit)
        catalog = InMemoryCatalog() if args.dry else Neo4jCatalog(conn)
        resolver = Resolver(catalog)

        stats, reports = resolve_items(resolver, items, persist=not args.dry,
                                       collect_reports=True)
        print(stats.summary())

        if args.out:
            for path in export_csv(args.out, reports, catalog):
                print(f"written: {path}")

        shown = 0
        print("\nsample resolutions:")
        for r in reports:
            if r.status == "unresolved" or shown >= args.show:
                continue
            print(f"  {r.raw_text[:70]!r}")
            print(f"    -> {r.node_id}  attrs={r.extraction.values}  "
                  f"basis={r.price_basis.basis}")
            shown += 1
    finally:
        conn.close()


if __name__ == "__main__":
    main()
