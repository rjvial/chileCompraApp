"""Persist the L2 match result to the graph (design: redesign L2, persistence).

Writes a three-tier catalog:
    (:ProductCluster {id, signature, category, ...})              -- brand-INDEPENDENT product
    (:Product {id, brand, category, signature, pack_size, ...})   -- brand-SPECIFIC offering
    (:Oferta)-[:OFFERS {normalized_price, unit_price, rut, date}]->(:Product)
    (:Product)-[:VARIANT_OF]->(:ProductCluster)
    (finer:ProductCluster)-[:REFINES]->(coarser:ProductCluster)

A `Product` is the offer's resolved, brand-specific form: deduped by
(cluster, brand, packaging), so every bid of the same brand+spec+pack shares one
node. Price is per-bid and lives on the `OFFERS` edge (a deduped node can't carry a
single price); the cluster is the brand-independent rollup. Offers join to their
Product by text-hash: each cluster member is an index into the profile list, which
is parallel to the store's (text_hash, profile) items; an offer's normalized
description hashes to that key.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ..resolve.matcher import MatchResult

CLUSTER_PREFIX = "pc_"
PRODUCT_PREFIX = "pr_"
SIN_MARCA = "sin_marca"          # sentinel brand for bids with no recognizable brand


def cluster_id(signature: str) -> str:
    """Deterministic, signature-derived id (so a re-run reproduces ids exactly)."""
    return CLUSTER_PREFIX + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]


def _pack_key(packaging) -> str:
    """Canonical packaging key for the Product identity. `granel` (loose) when there
    is no pack evidence, else size|unit."""
    size = getattr(packaging, "pack_size", None)
    unit = getattr(packaging, "pack_unit", None)
    if size is None and unit is None:
        return "granel"
    return f"{size if size is not None else ''}|{unit or ''}"


def product_id(cid: str, brand: str, pack_key: str) -> str:
    """Deterministic id for a brand-specific Product within a cluster — keyed by
    (cluster, brand, packaging)."""
    return PRODUCT_PREFIX + hashlib.sha1(
        f"{cid}|{brand}|{pack_key}".encode("utf-8")).hexdigest()[:12]


def normalized_price(unit_price, pack_size) -> float | None:
    """Per-base-unit price: divide a pack quote by its size. None when the price
    can't be parsed. Packaging is normalization, never identity (design Step 1)."""
    try:
        up = float(unit_price)
    except (TypeError, ValueError):
        return None
    if pack_size and pack_size > 0:
        return up / pack_size
    return up


def build_records(result: MatchResult, items: list[tuple[str, object]]):
    """Pure: turn a MatchResult + the store's (text_hash, profile) items into
    (cluster_rows, refines_rows, product_rows, hash_to_product, pack_by_hash).

    `items` MUST be the same list (same order) whose profiles were passed to
    cluster() — Cluster.members are indices into it. Each member maps to a Product
    keyed by (its cluster, its brand, its packaging); Products carry the cluster's
    signature and VARIANT_OF that cluster.
    """
    sig_to_id = {c.signature: cluster_id(c.signature) for c in result.clusters}

    cluster_rows = []
    products: dict[str, dict] = {}              # product_id -> node row (deduped)
    hash_to_product: dict[str, str] = {}
    pack_by_hash: dict[str, object] = {}
    for c in result.clusters:
        cid = sig_to_id[c.signature]
        base_unit = None
        for idx in c.members:
            h, prof = items[idx]
            brand = (getattr(prof, "brand", None) or SIN_MARCA)
            pack = getattr(prof, "packaging", None)
            pid = product_id(cid, brand, _pack_key(pack))
            hash_to_product[h] = pid
            pack_by_hash[h] = getattr(pack, "pack_size", None)
            products.setdefault(pid, {
                "id": pid, "brand": brand, "category": c.category,
                "signature": c.signature, "cluster_id": cid,
                "pack_size": getattr(pack, "pack_size", None),
                "pack_unit": getattr(pack, "pack_unit", None),
            })
            if base_unit is None:
                base_unit = getattr(prof, "base_unit", None)
        cluster_rows.append({
            "id": cid, "signature": c.signature, "category": c.category,
            "model_tokens": sorted(c.model_tokens), "flags": sorted(c.flags),
            "n_profiles": len(c.members), "base_unit": base_unit,
        })

    refines_rows = [
        {"finer": sig_to_id[f], "coarser": sig_to_id[co]}
        for f, co in result.refines
        if f in sig_to_id and co in sig_to_id
    ]
    return cluster_rows, refines_rows, list(products.values()), hash_to_product, pack_by_hash


# --- impure batched writers ---------------------------------------------------

_CHUNK = 5_000


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def write_clusters(conn, cluster_rows, refines_rows, *, chunk=_CHUNK, log=lambda _m: None):
    """MERGE the ProductCluster nodes, then the REFINES edges (edges need both
    endpoints present, so nodes go first)."""
    for c in _chunks(cluster_rows, chunk):
        conn.query(
            """
            UNWIND $rows AS r
            MERGE (c:ProductCluster {id: r.id})
            SET c.signature = r.signature, c.category = r.category,
                c.model_tokens = r.model_tokens, c.flags = r.flags,
                c.n_profiles = r.n_profiles, c.base_unit = r.base_unit
            """,
            parameters={"rows": c})
    log(f"wrote {len(cluster_rows):,} ProductCluster nodes")
    for c in _chunks(refines_rows, chunk):
        conn.query(
            """
            UNWIND $rows AS r
            MATCH (f:ProductCluster {id: r.finer})
            MATCH (co:ProductCluster {id: r.coarser})
            MERGE (f)-[:REFINES]->(co)
            """,
            parameters={"rows": c})
    log(f"wrote {len(refines_rows):,} REFINES edges")


def write_products(conn, product_rows, *, chunk=_CHUNK, log=lambda _m: None):
    """MERGE the brand-specific Product nodes and link each VARIANT_OF its cluster
    (clusters must already be written)."""
    for c in _chunks(product_rows, chunk):
        conn.query(
            """
            UNWIND $rows AS r
            MERGE (p:Product {id: r.id})
            SET p.brand = r.brand, p.category = r.category, p.signature = r.signature,
                p.pack_size = r.pack_size, p.pack_unit = r.pack_unit
            WITH p, r
            MATCH (c:ProductCluster {id: r.cluster_id})
            MERGE (p)-[:VARIANT_OF]->(c)
            """,
            parameters={"rows": c})
    log(f"wrote {len(product_rows):,} Product nodes (+ VARIANT_OF)")


def read_offers_checkpoint(path) -> int:
    """Offers already consumed from the stream (the resume offset). 0 if absent."""
    try:
        return int(json.loads(Path(path).read_text(encoding="utf-8"))["processed"])
    except (OSError, ValueError, KeyError):
        return 0


def _write_offers_checkpoint(path, processed: int) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps({"processed": processed}), encoding="utf-8")


def write_offers(conn, offer_rows, hash_to_product, pack_by_hash, normalizer,
                 *, chunk=_CHUNK, start=0, checkpoint_path=None,
                 log=lambda _m: None):
    """Stream offers, route each to its Product by normalized-text-hash, and MERGE
    the `OFFERS` edge with the per-bid price (per-base-unit + raw). Offers whose
    text didn't canonicalize to a clustered profile are skipped (counted).

    Resumable: `offer_rows` must already be fetched with `skip=start` (the stream
    order is stable). The checkpoint records the absolute stream position at every
    flush; a killed run re-reads it, re-fetches from there, and continues. MERGE
    makes every write idempotent, so even a re-run without a checkpoint is safe."""
    from ..resolve.profile import text_hash

    written = skipped = 0
    processed = start
    buf: list[dict] = []

    def flush():
        nonlocal written
        if not buf:
            return
        conn.query(
            """
            UNWIND $rows AS r
            MATCH (o:Oferta {id_licitacion: r.lic, id_item: r.item, id_oferta: r.oferta})
            MATCH (p:Product {id: r.product_id})
            MERGE (o)-[e:OFFERS]->(p)
            SET e.normalized_price = r.np, e.unit_price = r.up,
                e.currency = r.cur, e.rut = r.rut, e.date = r.date
            """,
            parameters={"rows": buf})
        written += len(buf)
        buf.clear()
        if checkpoint_path:
            _write_offers_checkpoint(checkpoint_path, processed)

    for o in offer_rows:
        processed += 1
        text = o.get("text")
        h = text_hash(normalizer(text)) if text else None
        pid = hash_to_product.get(h) if h else None
        if pid is None:
            skipped += 1
        else:
            pack = pack_by_hash.get(h)
            buf.append({"lic": o.get("lic"), "item": o.get("item"),
                        "oferta": o.get("oferta"), "product_id": pid,
                        "np": normalized_price(o.get("unit_price"), pack),
                        "up": o.get("unit_price"), "cur": o.get("currency"),
                        "rut": o.get("rut"), "date": o.get("date")})
        if len(buf) >= chunk:
            flush()
            log(f"  OFFERS written {written:,} (skipped {skipped:,}, pos {processed:,})")
    flush()
    log(f"OFFERS done: {written:,} edges, {skipped:,} offers unplaced")
    return written, skipped
