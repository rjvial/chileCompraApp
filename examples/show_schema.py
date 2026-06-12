"""Print the live Neo4j schema: labels, relationship types, constraints, indexes."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection


def main() -> None:
    conn = get_connection()
    try:
        print("--- node labels + counts ---")
        for r in conn.query("MATCH (n) UNWIND labels(n) AS l RETURN l AS label, count(*) AS c ORDER BY c DESC"):
            print(f"  {r['label']}: {r['c']}")

        print("--- relationship types + counts ---")
        for r in conn.query("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC"):
            print(f"  {r['t']}: {r['c']}")

        print("--- properties per label (sampled) ---")
        for r in conn.query(
            "CALL db.schema.nodeTypeProperties() "
            "YIELD nodeLabels, propertyName, propertyTypes "
            "RETURN nodeLabels, collect(propertyName + ': ' + reduce(s='', t IN propertyTypes | s + t)) AS props"
        ):
            print(f"  {r['nodeLabels']}: {r['props']}")

        print("--- constraints ---")
        for r in conn.query("SHOW CONSTRAINTS"):
            print(f"  {r['name']}: {r['type']} on {r['labelsOrTypes']} {r['properties']}")

        print("--- indexes ---")
        for r in conn.query("SHOW INDEXES"):
            print(f"  {r['name']}: {r['type']} on {r['labelsOrTypes']} {r['properties']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
