"""Unit tests for the pure match-persistence record-builders (ingest/clusters.py).
No graph — the impure writers are exercised by `match --persist` against Neo4j."""
from __future__ import annotations

from chilecompra_er.ingest.clusters import (
    build_records,
    cluster_id,
    normalized_price,
    product_id,
)
from chilecompra_er.resolve.matcher import cluster
from chilecompra_er.resolve.profile import IdentityAttr, Packaging, Profile


def mk(cat, attrs=(), model=None, pack=None, base_unit="unidad", brand=None):
    return Profile(True, cat, tuple(IdentityAttr(n, v, v) for n, v in attrs),
                   brand, model, Packaging(pack, "caja" if pack else None, None),
                   base_unit, "high", ())


def test_cluster_id_is_deterministic_and_prefixed():
    a = cluster_id("foley|calibre=16fr")
    assert a == cluster_id("foley|calibre=16fr") and a.startswith("pc_")
    assert a != cluster_id("foley|calibre=18fr")


def test_normalized_price():
    assert normalized_price(100, 2) == 50.0       # per-base-unit
    assert normalized_price(100, None) == 100.0   # no pack → unchanged
    assert normalized_price(100, 0) == 100.0      # zero pack → unchanged
    assert normalized_price(None, 2) is None      # unparseable
    assert normalized_price("x", 2) is None


def test_build_records_maps_offers_to_brand_specific_products():
    items = [("h0", mk("foley", [("calibre", "16fr")], pack=1, brand="bbraun")),
             ("h1", mk("foley", [("calibre", "16fr"), ("material", "latex")], pack=2,
                       brand="bbraun"))]
    res = cluster([p for _h, p in items])
    cluster_rows, refines_rows, product_rows, hash_to_product, pack_by_hash = \
        build_records(res, items)

    assert len(cluster_rows) == 2
    coarse, fine = cluster_id("foley|calibre=16fr"), cluster_id("foley|calibre=16fr|material=latex")
    pid0 = product_id(coarse, "bbraun", "1|caja")
    pid1 = product_id(fine, "bbraun", "2|caja")
    assert hash_to_product == {"h0": pid0, "h1": pid1}
    assert pack_by_hash == {"h0": 1, "h1": 2}
    # one Product per (cluster, brand, packaging); each VARIANT_OF its cluster
    assert {p["id"]: p["cluster_id"] for p in product_rows} == {pid0: coarse, pid1: fine}
    assert all(p["brand"] == "bbraun" for p in product_rows)
    # REFINES is between clusters, unchanged
    assert refines_rows == [{"finer": fine, "coarser": coarse}]


def test_distinct_brands_same_cluster_are_distinct_products():
    items = [("h0", mk("foley", [("calibre", "16fr")], brand="bbraun")),
             ("h1", mk("foley", [("calibre", "16fr")], brand="medline"))]
    res = cluster([p for _h, p in items])
    cluster_rows, _ref, product_rows, hash_to_product, _pack = build_records(res, items)
    assert len(cluster_rows) == 1                       # one brand-independent cluster
    assert len(product_rows) == 2                       # two brand-specific products
    assert hash_to_product["h0"] != hash_to_product["h1"]
    assert all(p["cluster_id"] == cluster_rows[0]["id"] for p in product_rows)


def test_same_brand_spec_pack_dedup_to_one_product():
    items = [("h0", mk("dialisis", [("volumen", "5000ml")], model="da4", brand="fres")),
             ("h1", mk("dialisis", [], model="da4", brand="fres"))]
    res = cluster([p for _h, p in items])
    cluster_rows, _ref, product_rows, hash_to_product, _pack = build_records(res, items)
    assert len(cluster_rows) == 1                       # merged via model_token
    assert len(product_rows) == 1                       # same brand + cluster + pack → one product
    assert hash_to_product["h0"] == hash_to_product["h1"]
    assert product_rows[0]["brand"] == "fres"
