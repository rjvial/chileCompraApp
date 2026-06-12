"""First product-level price series from the persisted catalog.

Joins the catalog's traceability edges back to awarded offers: GenericProduct
<- RESOLVED_TO - SourceRecord -> ItemLicitacion <- PARA_ITEM - Oferta
(awarded). Prices are per the published unit (basis normalization pending —
unknown basis is reported, never assumed per-unit; design §6).

    python examples/price_series.py bandas_molares
"""

import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.graphdb import get_connection

category = sys.argv[1] if len(sys.argv) > 1 else "bandas_molares"

conn = get_connection()
try:
    records = conn.query(
        """
        MATCH (g:GenericProduct {category_id: $cat})<-[r:RESOLVED_TO {current: true}]-(s:SourceRecord)
        WHERE s.source = 'mp_item_licitacion'
        MATCH (i:ItemLicitacion {id_licitacion: s.tender_id})
        WHERE toString(i.id_item) = s.line_no
        MATCH (o:Oferta {es_adjudicada: true})-[:PARA_ITEM]->(i)
        OPTIONAL MATCH (l:Licitacion)-[:TIENE_ITEM]->(i)
        RETURN g.id AS product, g{.*} AS props, s.tender_id AS tender,
               o.precio_unitario_clean AS unit_price, o.moneda AS currency,
               coalesce(l.fecha_publicacion, o.fecha) AS date
        ORDER BY product, date
        """,
        parameters={"cat": category},
    )
finally:
    conn.close()

out = Path(f"data/price_series_{category}.csv")
series = defaultdict(list)
with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["product", "attributes", "date", "tender", "unit_price", "currency"])
    for rec in records:
        props = {k: v for k, v in rec["props"].items()
                 if k not in ("id", "category_id", "identity_key", "specificity",
                              "is_complete", "created_at")}
        w.writerow([rec["product"], json.dumps(props, ensure_ascii=False),
                    (rec["date"] or "")[:10], rec["tender"],
                    rec["unit_price"], rec["currency"]])
        if rec["unit_price"]:
            series[(rec["product"], json.dumps(props, ensure_ascii=False))].append(
                float(rec["unit_price"]))

print(f"{len(records)} price observations across "
      f"{len(series)} generic products -> {out}")
print("\nproducts with the deepest price history:")
ranked = sorted(series.items(), key=lambda kv: -len(kv[1]))
for (product, attrs), prices in ranked[:8]:
    print(f"  {product}  n={len(prices):>3}  median={statistics.median(prices):>12,.0f} CLP"
          f"  range=[{min(prices):,.0f} .. {max(prices):,.0f}]")
    print(f"    {attrs}")
