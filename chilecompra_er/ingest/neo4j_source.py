"""Retrieval of source records from the existing Mercado Público graph.

The transactional layer already lives in Neo4j (Licitacion / ItemLicitacion /
Oferta / OrdenCompra / ItemOC, loaded by the ingestion pipeline with run_id /
record_hash provenance). For the entity-resolution process that graph IS the
source store (design §2): payloads stay on those nodes; the redesign reads
offer descriptions + prices from it to build the cluster catalog.

Offer text matters beyond price: supplier descriptions state attributes the
buyer text omits (brand/model), so they are the canonicalization corpus.
"""

from __future__ import annotations

from collections.abc import Iterator

# Cypher fragment: true when descripcion_comprador is a real description, not
# a UNSPSC rubric path ("Equipamiento ... / ... / Vendas..."). Mirrors the
# rubric rule (>= 2 " / " separators = >= 3 split parts). Used by PROFILING and
# SAMPLING so rankings and strawman corpora aren't contaminated.
NOT_RUBRIC = "size(split(i.descripcion_comprador, ' / ')) < 3"

# Tender-level context. The line item description (descripcion_comprador) is the
# authoritative product text; the parent tender's title supplements it when the
# line is terse or boilerplate. `l.titulo` is the concise tender headline
# (confirmed against the live graph, e.g. "MEDICAMENTOS USO VETERINARIO");
# `l.descripcion` is deliberately NOT used — it is bases-administrativas
# boilerplate ("Las presentes Bases de Licitacion contemplan..."). Change
# TENDER_NAME alone to switch fields; a missing property reads as null in Cypher
# (coalesced away), degrading to item-only.
TENDER_NAME = "l.titulo"


def combined_text(item_expr: str, tender_expr: str = TENDER_NAME) -> str:
    """Cypher expression: the item text with the tender title appended as
    context (' · ' join — never ' / ', so the combined text can't trip the
    rubric filter). A null/empty tender leaves the item text unchanged."""
    return (f"CASE WHEN coalesce({tender_expr}, '') = '' THEN {item_expr} "
            f"ELSE {item_expr} + ' · ' + coalesce({tender_expr}, '') END")


# Driver fetch size (Bolt PULL batch) for the single streamed fetches.
# Bigger = fewer network round trips on a high-latency link, more rows buffered
# per pull. 10k rows per pull is a fine trade-off.
_BATCH = 10_000

# LIMIT sentinel for "all remaining rows" (Neo4j wants a concrete Long). Must be
# far above any real row count yet small enough that SKIP+LIMIT — which Neo4j adds
# internally — can't overflow Long. 1e12 ≫ the corpus (~1.3M) and ≪ 2^63.
_NO_LIMIT = 1_000_000_000_000


def segment_bounds(segment: int | None) -> tuple[int | None, int | None]:
    """A UNSPSC prefix -> a half-open numeric range [low, high) over the
    8-digit code, so a RANGE index on codigo_unspsc_producto serves it
    directly (toString(...) STARTS WITH could not). Works for any prefix
    width — 42 -> [42000000, 43000000), 4214 -> [42140000, 42150000), a full
    8-digit code -> exact match. None means "all segments" (no filter)."""
    if segment is None:
        return None, None
    factor = 10 ** (8 - len(str(segment)))
    return segment * factor, (segment + 1) * factor


def fetch_offer_descriptions(conn, unspsc_segment: int | None = None,
                             limit: int | None = None, skip: int = 0
                             ) -> Iterator[tuple[str, int | None]]:
    """Stream every offer's (descripcion_proveedor, item UNSPSC) for
    canonicalization (the redesign's dedup source). Yields raw rows in the source's
    stable scan order; dedup happens downstream in resolve/canonicalize (by
    normalized text-hash), so this is a plain streamed scan — no server-side
    DISTINCT/grouping that would strain the heap. Scope with `unspsc_segment`
    (numeric range, index-backed) to bound a run to one commodity segment."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        RETURN o.descripcion_proveedor AS text, i.codigo_unspsc_producto AS unspsc
        SKIP $skip LIMIT $limit
    """
    params = {"skip": skip, "limit": limit if limit is not None else _NO_LIMIT,
              "seg_low": seg_low, "seg_high": seg_high}
    for rec in conn.stream(cypher, parameters=params, fetch_size=_BATCH):
        yield rec["text"], rec["unspsc"]


def fetch_offer_prices(conn, unspsc_segment: int | None = None,
                       limit: int | None = None, skip: int = 0) -> Iterator[dict]:
    """Stream the fields the match COTIZA edge needs: the offer id (match key),
    its description (-> text-hash -> cluster), unit price, currency, supplier RUT
    (the competitor), and date. One streamed scan; segment-scopable to match the
    canonicalize run that produced the profiles.

    The time axis is the parent tender's `fecha_publicacion` (the offer node carries
    no date of its own); OPTIONAL so an offer is never dropped if its tender link is
    missing — its date just stays null."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        OPTIONAL MATCH (i)<-[:TIENE_ITEM]-(l:Licitacion)
        RETURN o.id_licitacion AS lic, o.id_item AS item, o.id_oferta AS oferta,
               o.descripcion_proveedor AS text,
               coalesce(o.precio_unitario_clean, o.precio_unitario) AS unit_price,
               o.moneda AS currency, o.rut_proveedor AS rut,
               l.fecha_publicacion AS date
        SKIP $skip LIMIT $limit
    """
    params = {"skip": skip, "limit": limit if limit is not None else _NO_LIMIT,
              "seg_low": seg_low, "seg_high": seg_high}
    for rec in conn.stream(cypher, parameters=params, fetch_size=_BATCH):
        yield dict(rec)
