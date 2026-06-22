"""Product-level price series from the persisted catalog.

Reads prices off the explicit (:Oferta)-[:OFFERS]->(:Product) edge — the price
lives on the edge (the Product itself is the price-free Brand × GenericProduct
pairing). Each row carries the offer's brand (via (:Product)-[:OF_BRAND]->(:Brand))
so prices can be sliced by brand. Awarded offers are the realized price; pass
awarded_only=False to include the full bid spread.

Each row carries the raw `unit_price` AND a `normalized_unit_price` (per base unit)
with its `basis` — produced by basis.normalize_unit_prices per product: a per-pack
quote is divided by its stated pack size only when that lands it in the product's
price cluster, and otherwise the basis is `unknown` and the point is flagged out of
the comparison (design §6: flag-don't-guess). `summarize` reports on the normalized
prices and the flagged count.
"""

from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

from .basis import BASIS_UNKNOWN, normalize_unit_prices, pack_size_from_text

_BOOKKEEPING = {"id", "category_id", "identity_key", "specificity",
                "is_complete", "created_at"}


def build_series(conn, category_id: str, awarded_only: bool = True) -> list[dict]:
    records = conn.query(
        f"""
        MATCH (o:Oferta)-[r:OFFERS]->(p:Product)-[:VARIANT_OF]->(g:GenericProduct {{category_id: $cat}})
        WHERE r.unit_price IS NOT NULL
          {'AND r.awarded = true' if awarded_only else ''}
        OPTIONAL MATCH (p)-[:OF_BRAND]->(b:Brand)
        RETURN g.id AS product, g{{.*}} AS props, b.name AS brand,
               o.id_licitacion AS tender, r.unit_price AS unit_price,
               r.currency AS currency, r.date AS date, r.supplier_text AS supplier_text
        ORDER BY product, date
        """,
        parameters={"cat": category_id},
    )
    # Group by product so each one's price cluster normalizes against its own peers.
    by_product: dict[str, list] = defaultdict(list)
    for rec in records:
        by_product[rec["product"]].append(rec)

    rows = []
    for product, recs in by_product.items():
        prices = [float(r["unit_price"]) if r["unit_price"] is not None else None for r in recs]
        packs = [pack_size_from_text(r["supplier_text"]) for r in recs]
        norm = normalize_unit_prices(prices, packs)
        for rec, (nprice, basis), pack in zip(recs, norm, packs):
            attrs = {k: v for k, v in rec["props"].items() if k not in _BOOKKEEPING}
            rows.append({
                "product": product,
                "brand": rec["brand"],
                "attributes": json.dumps(attrs, ensure_ascii=False),
                "date": (rec["date"] or "")[:10],
                "tender": rec["tender"],
                "unit_price": rec["unit_price"],
                "normalized_unit_price": round(nprice, 2) if nprice is not None else None,
                "basis": basis,
                "pack_size": pack,
                "currency": rec["currency"],
            })
    return rows


_CSV_FIELDS = ["product", "brand", "attributes", "date", "tender", "unit_price",
              "normalized_unit_price", "basis", "pack_size", "currency"]


def write_series_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    return path


def summarize(rows: list[dict], top: int = 8) -> list[str]:
    """Per-product summary over the NORMALIZED (per-base-unit) prices, with the
    count of points flagged out for an undeterminable basis."""
    series: dict[tuple[str, str], list[float]] = defaultdict(list)
    flagged: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        key = (r["product"], r["attributes"])
        nprice = r.get("normalized_unit_price")
        if nprice and r.get("basis") != BASIS_UNKNOWN:
            series[key].append(float(nprice))
        else:
            flagged[key] += 1
    lines = []
    for (product, attrs), prices in sorted(series.items(), key=lambda kv: -len(kv[1]))[:top]:
        flg = flagged.get((product, attrs), 0)
        lines.append(f"  {product}  n={len(prices):>3} (+{flg} flagged)  "
                     f"median={statistics.median(prices):>12,.0f} CLP/base-unit  "
                     f"range=[{min(prices):,.0f} .. {max(prices):,.0f}]")
        lines.append(f"    {attrs}")
    return lines
