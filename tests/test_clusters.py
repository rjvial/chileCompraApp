"""Unit tests for the pure L2-persistence record-builders (ingest/clusters.py).
No graph — the impure writers are exercised by `match --persist` against Neo4j."""
from __future__ import annotations

from chilecompra_er.ingest.clusters import (
    build_records,
    cluster_id,
    normalized_price,
)
from chilecompra_er.resolve.matcher import cluster
from chilecompra_er.resolve.profile import IdentityAttr, Packaging, Profile


def mk(cat, attrs=(), model=None, pack=None, base_unit="unidad"):
    return Profile(True, cat, tuple(IdentityAttr(n, v, v) for n, v in attrs),
                   None, model, Packaging(pack, "caja" if pack else None, None),
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


def test_build_records_maps_members_and_packaging():
    items = [("h0", mk("foley", [("calibre", "16fr")], pack=1)),
             ("h1", mk("foley", [("calibre", "16fr"), ("material", "latex")], pack=2))]
    res = cluster([p for _h, p in items])
    node_rows, refines_rows, hash_to_cluster, pack_by_hash = build_records(res, items)

    assert len(node_rows) == 2
    coarse = cluster_id("foley|calibre=16fr")
    fine = cluster_id("foley|calibre=16fr|material=latex")
    assert hash_to_cluster == {"h0": coarse, "h1": fine}
    assert pack_by_hash == {"h0": 1, "h1": 2}
    # REFINES: finer → coarser, referencing real cluster ids
    assert refines_rows == [{"finer": fine, "coarser": coarse}]
    # node rows carry base_unit + counts
    assert all(r["base_unit"] == "unidad" and r["n_profiles"] == 1 for r in node_rows)


def test_build_records_model_token_merge_maps_both_hashes_to_one_cluster():
    items = [("h0", mk("dialisis", [("volumen", "5000ml")], model="da4")),
             ("h1", mk("dialisis", [], model="da4"))]
    res = cluster([p for _h, p in items])
    node_rows, _refines, hash_to_cluster, _pack = build_records(res, items)
    assert len(node_rows) == 1                       # merged via model_token
    assert hash_to_cluster["h0"] == hash_to_cluster["h1"]
    assert node_rows[0]["signature"] == "dialisis|volumen=5000ml"
