"""Product-level price series from the persisted catalog.

Joins the catalog's traceability edges back to awarded offers:
GenericProduct <- RESOLVED_TO - SourceRecord -> ItemLicitacion <- PARA_ITEM -
Oferta (awarded). Prices are per the published unit — basis normalization is
pending (design §6: unknown basis is reported, never assumed per-unit).
"""

from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

_BOOKKEEPING = {"id", "category_id", "identity_key", "specificity",
                "is_complete", "created_at"}


def build_series(conn, category_id: str) -> list[dict]:
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
        parameters={"cat": category_id},
    )
    rows = []
    for rec in records:
        attrs = {k: v for k, v in rec["props"].items() if k not in _BOOKKEEPING}
        rows.append({
            "product": rec["product"],
            "attributes": json.dumps(attrs, ensure_ascii=False),
            "date": (rec["date"] or "")[:10],
            "tender": rec["tender"],
            "unit_price": rec["unit_price"],
            "currency": rec["currency"],
        })
    return rows


def write_series_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["product", "attributes", "date",
                                          "tender", "unit_price", "currency"])
        w.writeheader()
        w.writerows(rows)
    return path


def summarize(rows: list[dict], top: int = 8) -> list[str]:
    series: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        if r["unit_price"]:
            series[(r["product"], r["attributes"])].append(float(r["unit_price"]))
    lines = []
    for (product, attrs), prices in sorted(series.items(), key=lambda kv: -len(kv[1]))[:top]:
        lines.append(f"  {product}  n={len(prices):>3}  "
                     f"median={statistics.median(prices):>12,.0f} CLP  "
                     f"range=[{min(prices):,.0f} .. {max(prices):,.0f}]")
        lines.append(f"    {attrs}")
    return lines
