"""Unit test for the pure price-clusters cluster-price summary (price/cluster_series.py)."""
from __future__ import annotations

from chilecompra_er.price.cluster_series import summarize


def _row(cluster, price, rut, date="2026-01-01", brand="bbraun"):
    return {"cluster": cluster, "category": "foley", "brand": brand, "date": date,
            "rut": rut, "unit_price": price, "normalized_price": price, "currency": "CLP"}


def test_summarize_counts_competitors_and_price_stats():
    rows = [
        _row("foley|calibre=16fr", 100, "A", "2026-01-01", brand="bbraun"),
        _row("foley|calibre=16fr", 300, "A", "2026-03-01", brand="medline"),
        _row("foley|calibre=16fr", 200, "B", "2026-02-01", brand="bbraun"),
        _row("foley|calibre=18fr", 500, "C"),
        # excluded: no normalized price
        {"cluster": "foley|calibre=16fr", "category": "foley", "date": "",
         "rut": "Z", "unit_price": None, "normalized_price": None, "currency": "CLP"},
    ]
    out = "\n".join(summarize(rows))
    assert "foley|calibre=16fr" in out
    assert "n=   3" in out                 # the None-price row is excluded
    assert "competitors=  2" in out        # ruts A, B (Z excluded with the None row)
    assert "brands=  2" in out             # bbraun, medline
    assert "median=         200" in out
    assert "range=[100 .. 300]" in out
    assert "2026-01-01..2026-03-01" in out  # date span
    # most-observed cluster first
    assert out.index("16fr") < out.index("18fr")
