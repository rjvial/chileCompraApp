"""Price series over product clusters (design: the redesign, "price-clusters").

Reads prices two hops out:
`(:Oferta)-[:COTIZA {precio_normalizado, precio_unitario, rut, fecha}]->(:Producto)-[:VARIANTE_DE]->(:ProductoCanonico)`.
Normalization already happened at persist time (price per base unit is on the
edge), so this just reads and aggregates. Each row carries the offer's `brand`
(from its Producto), so the series can also be sliced by brand.

A cluster IS the substitutable-product comparison unit, so its series answers
both goals directly: price **over time** (rows ordered by date) and **across
competition** (distinct `rut` suppliers / brands and the price spread among them).
"""
from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path


def build_cluster_series(conn, category: str | None = None,
                         signature: str | None = None) -> list[dict]:
    records = conn.query(
        """
        MATCH (o:Oferta)-[e:COTIZA]->(p:Producto)-[:VARIANTE_DE]->(c:ProductoCanonico)
        WHERE ($cat IS NULL OR c.category = $cat)
          AND ($sig IS NULL OR c.signature = $sig)
          AND e.precio_normalizado IS NOT NULL
        RETURN c.signature AS cluster, c.category AS category, p.brand AS brand,
               e.precio_normalizado AS price, e.precio_unitario AS unit_price,
               e.rut AS rut, e.fecha AS date, e.moneda AS currency
        ORDER BY cluster, date
        """,
        parameters={"cat": category, "sig": signature},
    )
    return [{
        "cluster": r["cluster"], "category": r["category"], "brand": r["brand"],
        "date": (r["date"] or "")[:10], "rut": r["rut"],
        "unit_price": r["unit_price"],
        "normalized_price": round(float(r["price"]), 2) if r["price"] is not None else None,
        "currency": r["currency"],
    } for r in records]


_CSV_FIELDS = ["cluster", "category", "brand", "date", "rut", "unit_price",
               "normalized_price", "currency"]


def write_cluster_series_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    return path


def summarize(rows: list[dict], top: int = 10) -> list[str]:
    """Per-cluster summary: bid count, distinct competitors (rut), median + range
    of the per-base-unit price, and the date span. The competition + time view."""
    by_cluster: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("normalized_price") is not None:
            by_cluster[r["cluster"]].append(r)

    lines = []
    for cluster, recs in sorted(by_cluster.items(), key=lambda kv: -len(kv[1]))[:top]:
        prices = [r["normalized_price"] for r in recs]
        competitors = len({r["rut"] for r in recs if r["rut"]})
        brands = len({r["brand"] for r in recs if r.get("brand")})
        dates = sorted(d for d in (r["date"] for r in recs) if d)
        span = f"{dates[0]}..{dates[-1]}" if dates else "—"
        lines.append(
            f"  {cluster}\n"
            f"    n={len(prices):>4}  competitors={competitors:>3}  brands={brands:>3}  "
            f"median={statistics.median(prices):>12,.0f}  "
            f"range=[{min(prices):,.0f} .. {max(prices):,.0f}]  {span}")
    return lines
