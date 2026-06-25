"""Persist the L2 match result to the graph (design: redesign L2, persistence).

Writes the new, shadow catalog:
    (:ProductCluster {id, signature, category, ...})
    (:Oferta)-[:PRICED_IN {normalized_price, unit_price, rut, date, pack_size}]->(:ProductCluster)
    (finer:ProductCluster)-[:REFINES]->(coarser:ProductCluster)

The pure record-builders (build_records, normalized_price, cluster_id) are split
from the impure batched writers so the cluster->offer mapping is unit-testable
without a graph. PRICED_IN joins offers to clusters by text-hash: each cluster
member is an index into the profile list, which is parallel to the store's
(text_hash, profile) items; an offer's normalized description hashes to that key.
"""
from __future__ import annotations

import hashlib

from ..resolve.matcher import MatchResult

CLUSTER_PREFIX = "pc_"


def cluster_id(signature: str) -> str:
    """Deterministic, signature-derived id (so a re-run reproduces ids exactly)."""
    return CLUSTER_PREFIX + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12]


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
    (node_rows, refines_rows, hash_to_cluster, pack_by_hash).

    `items` MUST be the same list (same order) whose profiles were passed to
    cluster() — Cluster.members are indices into it.
    """
    sig_to_id = {c.signature: cluster_id(c.signature) for c in result.clusters}

    node_rows = []
    hash_to_cluster: dict[str, str] = {}
    for c in result.clusters:
        cid = sig_to_id[c.signature]
        base_unit = None
        for idx in c.members:
            h, prof = items[idx]
            hash_to_cluster[h] = cid
            if base_unit is None:
                base_unit = getattr(prof, "base_unit", None)
        node_rows.append({
            "id": cid, "signature": c.signature, "category": c.category,
            "model_tokens": sorted(c.model_tokens), "flags": sorted(c.flags),
            "n_profiles": len(c.members), "base_unit": base_unit,
        })

    # REFINES edges; drop any endpoint that was absorbed (not in sig_to_id).
    refines_rows = [
        {"finer": sig_to_id[f], "coarser": sig_to_id[co]}
        for f, co in result.refines
        if f in sig_to_id and co in sig_to_id
    ]

    pack_by_hash = {
        h: getattr(getattr(prof, "packaging", None), "pack_size", None)
        for h, prof in items
    }
    return node_rows, refines_rows, hash_to_cluster, pack_by_hash


# --- impure batched writers ---------------------------------------------------

_CHUNK = 5_000


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def write_clusters(conn, node_rows, refines_rows, *, chunk=_CHUNK, log=lambda _m: None):
    """MERGE the ProductCluster nodes, then the REFINES edges (edges need both
    endpoints present, so nodes go first)."""
    for c in _chunks(node_rows, chunk):
        conn.query(
            """
            UNWIND $rows AS r
            MERGE (c:ProductCluster {id: r.id})
            SET c.signature = r.signature, c.category = r.category,
                c.model_tokens = r.model_tokens, c.flags = r.flags,
                c.n_profiles = r.n_profiles, c.base_unit = r.base_unit
            """,
            parameters={"rows": c})
    log(f"wrote {len(node_rows):,} ProductCluster nodes")
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


def write_priced_in(conn, offer_rows, hash_to_cluster, pack_by_hash, normalizer,
                    *, chunk=_CHUNK, log=lambda _m: None):
    """Stream offers, route each to its cluster by normalized-text-hash, and MERGE
    the PRICED_IN edge with the per-base-unit price. Offers whose text didn't
    canonicalize to a clustered profile are skipped (counted)."""
    from ..resolve.profile import text_hash

    written = skipped = 0
    buf: list[dict] = []

    def flush():
        nonlocal written
        if not buf:
            return
        conn.query(
            """
            UNWIND $rows AS r
            MATCH (o:Oferta {id_oferta: r.id})
            MATCH (c:ProductCluster {id: r.cluster_id})
            MERGE (o)-[e:PRICED_IN]->(c)
            SET e.normalized_price = r.np, e.unit_price = r.up,
                e.currency = r.cur, e.rut = r.rut, e.date = r.date,
                e.pack_size = r.pack
            """,
            parameters={"rows": buf})
        written += len(buf)
        buf.clear()

    for o in offer_rows:
        text = o.get("text")
        h = text_hash(normalizer(text)) if text else None
        cid = hash_to_cluster.get(h) if h else None
        if cid is None:
            skipped += 1
            continue
        pack = pack_by_hash.get(h)
        buf.append({"id": o.get("id"), "cluster_id": cid,
                    "np": normalized_price(o.get("unit_price"), pack),
                    "up": o.get("unit_price"), "cur": o.get("currency"),
                    "rut": o.get("rut"), "date": o.get("date"), "pack": pack})
        if len(buf) >= chunk:
            flush()
            log(f"  PRICED_IN written {written:,} (skipped {skipped:,})")
    flush()
    log(f"PRICED_IN done: {written:,} edges, {skipped:,} offers unplaced")
    return written, skipped
