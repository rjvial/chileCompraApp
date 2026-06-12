"""Probe: can supplier OFFER text recover the product for rubric-only tender
lines? (M3 motivation — 52% of tender lines are UNSPSC paths with no product
info; their offers often contain the real description.)

Read-only; resolves offer descriptions against an in-memory catalog.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection
from chilecompra_er.resolve import InMemoryCatalog, Resolver

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 1500

conn = get_connection()
try:
    records = conn.query(
        """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND size(split(i.descripcion_comprador, ' / ')) >= 3
        RETURN o.descripcion_proveedor AS text
        LIMIT $limit
        """,
        parameters={"limit": LIMIT},
    )
finally:
    conn.close()

resolver = Resolver(InMemoryCatalog())
classified = with_attrs = boilerplate = 0
for rec in records:
    r = resolver.resolve(rec["text"])
    if r.unresolved_reason == "boilerplate_rubric":
        boilerplate += 1
    elif r.status == "resolved_generic":
        classified += 1
        if r.extraction and r.extraction.values:
            with_attrs += 1

n = len(records)
print(f"offers examined (their tender line is rubric-only): {n}")
print(f"offer text itself also rubric/boilerplate          : {boilerplate} ({boilerplate/n:.0%})")
print(f"offer text classifies into a launched category     : {classified} ({classified/n:.0%})")
print(f"  ...with extracted identity attributes            : {with_attrs} ({with_attrs/n:.0%})")
print()
print("Reading: every percent in the last line is product information that is")
print("INVISIBLE from the tender line alone — recoverable via the M3 offer path.")
