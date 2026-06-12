"""End-to-end smoke test against the live Neo4j catalog.

Resolves a handful of descriptions, runs the design note's forward and
reverse traceability queries (§7), prints the results, and deletes the smoke
data unless --keep is passed.

    python examples/smoke_neo4j.py [--keep]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection
from chilecompra_er.resolve import Neo4jCatalog, Resolver, SourceRef

SMOKE_SOURCE = "smoke_test"

EXAMPLES = [
    "SONDA FOLEY CH16 SILICONA 2 VIAS",
    "Cateter foley n°16 100% silicona dos vías",  # must land on the same node
    "SONDA FOLEY 16",                              # partial parent
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="keep smoke data in the graph")
    args = parser.parse_args()

    conn = get_connection()
    catalog = Neo4jCatalog(conn)
    resolver = Resolver(catalog)

    try:
        node_ids = set()
        for i, raw in enumerate(EXAMPLES, start=1):
            src = SourceRef(SMOKE_SOURCE, tender_id="SMOKE-1", line_no=str(i), raw_text=raw)
            r = resolver.resolve(raw, source=src)
            node_ids.add(r.node_id)
            print(f"[{i}] {raw!r} -> {r.node_id} (created={r.created}, parent={r.parent_id})")

        assert len(node_ids) == 2, f"expected dedup to 2 nodes, got {len(node_ids)}"

        print("\nforward trace (where did line 1 land?):")
        for rec in conn.query(
            """
            MATCH (s:SourceRecord {source: $src, tender_id: 'SMOKE-1', line_no: '1'})
                  -[r:RESOLVED_TO {current: true}]->(node)
            RETURN node.id AS id, r.extractor_version AS extractor, r.schema_version AS schema
            """,
            parameters={"src": SMOKE_SOURCE},
        ):
            print(f"  {rec['id']}  extractor={rec['extractor']}  schema={rec['schema']}")

        print("\nreverse trace / rollup (which records evidence the 16Fr parent + descendants?):")
        for rec in conn.query(
            """
            MATCH (g:GenericProduct {category_id: 'sondas_foley', specificity: 1})
                  -[:PARENT_OF*0..]->(d)<-[r:RESOLVED_TO {current: true}]-(s)
            RETURN d.id AS node, s.tender_id AS tender, s.line_no AS line
            ORDER BY line
            """,
        ):
            print(f"  {rec['node']}  <- {rec['tender']} line {rec['line']}")

    finally:
        if args.keep:
            print("\nsmoke data kept (--keep)")
        else:
            conn.query(
                "MATCH (s:SourceRecord {source: $src}) DETACH DELETE s",
                parameters={"src": SMOKE_SOURCE},
            )
            conn.query(
                "MATCH (g:GenericProduct {category_id: 'sondas_foley'}) DETACH DELETE g"
            )
            conn.query("MATCH (c:Category {category_id: 'sondas_foley'}) DETACH DELETE c")
            print("\nsmoke data cleaned up")
        conn.close()


if __name__ == "__main__":
    main()
