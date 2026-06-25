"""Retrieval of source records from the existing Mercado Público graph.

The transactional layer already lives in Neo4j (Licitacion / ItemLicitacion /
Oferta / OrdenCompra / ItemOC, loaded by the ingestion pipeline with run_id /
record_hash provenance). For the entity-resolution process that graph IS the
source store (design §2): payloads stay on those nodes; resolution only adds the
catalog layer + a direct (:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct) edge
keyed by the stable ids retrieved here.

Three record kinds, each with a stable record key:
  tender_item  mp_item_licitacion | id_licitacion | id_item
  offer        mp_oferta          | id_licitacion | id_item:id_oferta
  oc_item      mp_item_oc         | id_oc         | id_item_oc

Offer text matters beyond price: supplier descriptions state attributes the
tender text omits (brand/model — the M3 branded path, and schema-mining
corpus before that).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

from ..resolve.assignment import SourceRef

SOURCE_TENDER_ITEM = "mp_item_licitacion"
SOURCE_OFFER = "mp_oferta"
SOURCE_OC_ITEM = "mp_item_oc"

# Cypher fragment: true when descripcion_comprador is a real description, not
# a UNSPSC rubric path ("Equipamiento ... / ... / Vendas..."). Mirrors the
# resolver's boilerplate rule (>= 2 " / " separators = >= 3 split parts).
# Used by PROFILING and SAMPLING so rankings and strawman corpora aren't
# contaminated; record RETRIEVAL stays unfiltered — rubric lines must still
# be ingested and explicitly resolved as boilerplate (total & explicit).
NOT_RUBRIC = "size(split(i.descripcion_comprador, ' / ')) < 3"

# Tender-level context. The line item description (descripcion_comprador) is the
# authoritative product text; the parent tender's title supplements it when the
# line is terse or boilerplate — the same "item wins, tender is fallback" rule
# the resolver applies (resolve_joint). `l.titulo` is the concise tender
# headline (confirmed against the live graph, e.g. "MEDICAMENTOS USO
# VETERINARIO"); `l.descripcion` is deliberately NOT used — it is
# bases-administrativas boilerplate ("Las presentes Bases de Licitacion
# contemplan..."). Change TENDER_NAME alone to switch fields; a missing
# property reads as null in Cypher (coalesced away), degrading to item-only.
TENDER_NAME = "l.titulo"


def combined_text(item_expr: str, tender_expr: str = TENDER_NAME) -> str:
    """Cypher expression: the item text with the tender title appended as
    context (' · ' join — never ' / ', so the combined text can't trip the
    rubric filter). A null/empty tender leaves the item text unchanged."""
    return (f"CASE WHEN coalesce({tender_expr}, '') = '' THEN {item_expr} "
            f"ELSE {item_expr} + ' · ' + coalesce({tender_expr}, '') END")


# Driver fetch size (Bolt PULL batch) for the single streamed fetch in _paged.
# Bigger = fewer network round trips on a high-latency link, more rows buffered
# per pull. 10k items+offers per pull is a fine trade-off.
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


def count_resolve_items(conn, contains: str | None = None,
                        unspsc_segment: int | None = None) -> int:
    """The deterministic loop size of a resolve run: how many ItemLicitacion
    rows fetch_items/fetch_tender_items will stream for this scope. The MATCH +
    WHERE mirror those fetchers exactly (same node, same buyer-text + segment
    filters, same contains lowercasing as _paged), so the result equals the
    number of records resolve will iterate — the denominator for progress %,
    ETA, and resume. Stable for a fixed scope: resolution only adds catalog
    nodes, it never adds or mutates ItemLicitacion, so re-counting mid-run
    returns the same number."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = """
        MATCH (l:Licitacion)-[:TIENE_ITEM]->(i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        RETURN count(i) AS n
    """
    rows = conn.query(cypher, parameters={
        "contains": contains.lower() if contains else None,
        "seg_low": seg_low, "seg_high": seg_high})
    return int(rows[0]["n"]) if rows else 0


@dataclass(frozen=True)
class SourceItem:
    """One retrievable source record: reference + text + price fields."""

    ref: SourceRef
    kind: str  # tender_item | offer | oc_item
    raw_text: str
    unspsc: int | None = None
    quantity: float | None = None
    unit_price: float | None = None
    total: float | None = None
    currency: str | None = None
    date: str | None = None
    extra: dict = field(default_factory=dict)


def _paged(conn, cypher: str, build, contains: str | None, limit: int | None,
           skip: int = 0, extra_params: dict | None = None) -> Iterator[SourceItem]:
    """Stream the whole result in ONE query, pulled lazily by the driver in
    `_BATCH`-sized batches. The query keeps a single SKIP (the resume offset,
    applied once by the server) and an effectively unbounded LIMIT, so the source
    is scanned ONCE — instead of the old SKIP/LIMIT paging that re-scanned every
    prior row on each page (O(n²) total, which made a deep resolve slow down as
    it advanced). This holds a flat rate to the end; memory stays bounded because
    the driver yields lazily rather than buffering the full result."""
    params = {"contains": contains.lower() if contains else None,
              "skip": skip, "limit": limit if limit is not None else _NO_LIMIT}
    if extra_params:
        params.update(extra_params)
    for rec in conn.stream(cypher, parameters=params, fetch_size=_BATCH):
        yield build(rec)


def fetch_tender_items(conn, contains: str | None = None,
                       limit: int | None = None, skip: int = 0,
                       unspsc_segment: int | None = None) -> Iterator[SourceItem]:
    """Buyer-side tender lines: the primary resolution corpus.

    The segment filter is a numeric range on codigo_unspsc_producto (index-
    backed); pass any prefix, or None for all segments. No global ORDER BY:
    skip/limit page in the source's stable scan order — resolution only adds
    catalog nodes, never mutates Item/Oferta, so the order is repeatable
    across resumed runs (and persisted writes are idempotent on record_key)."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = f"""
        MATCH (l:Licitacion)-[:TIENE_ITEM]->(i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        RETURN i.id_licitacion AS tender_id, i.id_item AS item_id,
               i.descripcion_comprador AS text, i.codigo_unspsc_producto AS unspsc,
               i.cantidad AS quantity, i.moneda_item AS currency,
               i.unidad_medida AS uom, i.nombre_producto_fuente AS source_name,
               coalesce({TENDER_NAME}, '') AS tender_text,
               l.fecha_publicacion AS date
        SKIP $skip LIMIT $limit
    """

    def build(rec) -> SourceItem:
        return SourceItem(
            ref=SourceRef(SOURCE_TENDER_ITEM, str(rec["tender_id"]),
                          str(rec["item_id"]), rec["text"]),
            kind="tender_item",
            raw_text=rec["text"],
            unspsc=rec["unspsc"],
            quantity=rec["quantity"],
            currency=rec["currency"],
            date=rec["date"],
            extra={"uom": rec["uom"], "source_name": rec["source_name"],
                   "tender_text": rec["tender_text"]},
        )

    return _paged(conn, cypher, build, contains, limit, skip=skip,
                  extra_params={"seg_low": seg_low, "seg_high": seg_high})


def fetch_items(conn, contains: str | None = None,
                limit: int | None = None, skip: int = 0,
                unspsc_segment: int | None = None) -> Iterator[SourceItem]:
    """Item-centric corpus: one record per ItemLicitacion, carrying its buyer
    line, parent tender title, UNSPSC code, AND all its supplier offers
    (extra['offers']) so resolve_item can resolve the item once from the pooled
    signals and bind every offer to that single GenericProduct (design: the
    item is the anchor; offers are bids for the same product).

    Offers are gathered with a pattern comprehension (yields [] when an item has
    none, never a list of nulls). Same stable scan order / segment filter as
    fetch_tender_items."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = f"""
        MATCH (l:Licitacion)-[:TIENE_ITEM]->(i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        WITH l, i,
             [(i)<-[:PARA_ITEM]-(o:Oferta) WHERE o.descripcion_proveedor IS NOT NULL |
                {{offer_id: o.id_oferta, text: o.descripcion_proveedor,
                  awarded: o.es_adjudicada,
                  unit_price: coalesce(o.precio_unitario_clean, o.precio_unitario),
                  quantity: coalesce(o.cantidad_clean, o.cantidad_ofertada),
                  total_clp: o.precio_total_clp,
                  currency: o.moneda, date: o.fecha}}] AS offers
        RETURN i.id_licitacion AS tender_id, i.id_item AS item_id,
               i.descripcion_comprador AS text, i.codigo_unspsc_producto AS unspsc,
               i.cantidad AS quantity, i.moneda_item AS currency,
               coalesce({TENDER_NAME}, '') AS tender_text,
               l.fecha_publicacion AS date, offers
        SKIP $skip LIMIT $limit
    """

    def build(rec) -> SourceItem:
        return SourceItem(
            ref=SourceRef(SOURCE_TENDER_ITEM, str(rec["tender_id"]),
                          str(rec["item_id"]), rec["text"]),
            kind="item",
            raw_text=rec["text"],
            unspsc=rec["unspsc"],
            quantity=rec["quantity"],
            currency=rec["currency"],
            date=rec["date"],
            extra={"tender_text": rec["tender_text"], "offers": rec["offers"]},
        )

    return _paged(conn, cypher, build, contains, limit, skip=skip,
                  extra_params={"seg_low": seg_low, "seg_high": seg_high})


def fetch_offers(conn, contains: str | None = None, awarded_only: bool = False,
                 limit: int | None = None, skip: int = 0,
                 unspsc_segment: int | None = None) -> Iterator[SourceItem]:
    """Supplier offers for tender items, each carrying the linked buyer text
    (extra['buyer_text']) so the joint path can resolve both together.
    `contains` filters on the BUYER text; `unspsc_segment` scopes by the
    item's UNSPSC (numeric range, index-backed; None = all segments). No
    global ORDER BY — paging uses the read-only source's stable scan order,
    see fetch_tender_items."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = f"""
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          {'AND o.es_adjudicada = true' if awarded_only else ''}
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        RETURN o.id_licitacion AS tender_id, o.id_item AS item_id,
               o.id_oferta AS offer_id, o.descripcion_proveedor AS text,
               i.descripcion_comprador AS buyer_text,
               coalesce(o.cantidad_clean, o.cantidad_ofertada) AS quantity,
               coalesce(o.precio_unitario_clean, o.precio_unitario) AS unit_price,
               coalesce(o.precio_total_clean, o.precio_total) AS total,
               o.precio_total_clp AS total_clp, o.moneda AS currency,
               o.es_adjudicada AS awarded, o.fecha AS date,
               i.codigo_unspsc_producto AS unspsc
        SKIP $skip LIMIT $limit
    """

    def build(rec) -> SourceItem:
        return SourceItem(
            ref=SourceRef(SOURCE_OFFER, str(rec["tender_id"]),
                          f"{rec['item_id']}:{rec['offer_id']}", rec["text"]),
            kind="offer",
            raw_text=rec["text"],
            unspsc=rec["unspsc"],
            quantity=rec["quantity"],
            unit_price=rec["unit_price"],
            total=rec["total"],
            currency=rec["currency"],
            date=rec["date"],
            extra={"buyer_text": rec["buyer_text"], "awarded": rec["awarded"],
                   "total_clp": rec["total_clp"]},
        )

    return _paged(conn, cypher, build, contains, limit, skip=skip,
                  extra_params={"seg_low": seg_low, "seg_high": seg_high})


def fetch_oc_items(conn, contains: str | None = None,
                   limit: int | None = None, skip: int = 0) -> Iterator[SourceItem]:
    """Purchase-order lines: award-side records with firm prices. No global
    ORDER BY — like the other fetchers, paging rides the read-only source's
    stable scan order, so the single streamed scan never sorts/materializes the
    whole result before the first row."""
    cypher = """
        MATCH (oc:OrdenCompra)-[:CONTIENE_ITEM_OC]->(it:ItemOC)
        WHERE it.especificacion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(it.especificacion_comprador) CONTAINS $contains)
        RETURN it.id_oc AS oc_id, it.id_item_oc AS item_id,
               it.especificacion_comprador AS text,
               it.especificacion_proveedor AS supplier_text,
               it.codigo_unspsc_producto AS unspsc, it.cantidad AS quantity,
               it.precio_neto AS unit_price, it.total_linea_neto AS total,
               it.moneda_item AS currency, it.unidad_medida AS uom,
               oc.fecha_envio AS date
        SKIP $skip LIMIT $limit
    """

    def build(rec) -> SourceItem:
        return SourceItem(
            ref=SourceRef(SOURCE_OC_ITEM, str(rec["oc_id"]),
                          str(rec["item_id"]), rec["text"]),
            kind="oc_item",
            raw_text=rec["text"],
            unspsc=rec["unspsc"],
            quantity=rec["quantity"],
            unit_price=rec["unit_price"],
            total=rec["total"],
            currency=rec["currency"],
            date=rec["date"],
            extra={"supplier_text": rec["supplier_text"], "uom": rec["uom"]},
        )

    return _paged(conn, cypher, build, contains, limit, skip=skip)


def fetch_offer_descriptions(conn, unspsc_segment: int | None = None,
                             limit: int | None = None, skip: int = 0
                             ) -> Iterator[tuple[str, int | None]]:
    """Stream every offer's (descripcion_proveedor, item UNSPSC) for L1
    canonicalization (the redesign's L0 source). Yields raw rows in the source's
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
    """Stream the fields the L2 PRICED_IN edge needs: the offer id (match key),
    its description (-> text-hash -> cluster), unit price, currency, supplier RUT
    (the competitor), and date. One streamed scan; segment-scopable to match the
    canonicalize run that produced the profiles."""
    seg_low, seg_high = segment_bounds(unspsc_segment)
    cypher = """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        RETURN o.id_licitacion AS lic, o.id_item AS item, o.id_oferta AS oferta,
               o.descripcion_proveedor AS text,
               coalesce(o.precio_unitario_clean, o.precio_unitario) AS unit_price,
               o.moneda AS currency, o.rut_proveedor AS rut, o.fecha AS date
        SKIP $skip LIMIT $limit
    """
    params = {"skip": skip, "limit": limit if limit is not None else _NO_LIMIT,
              "seg_low": seg_low, "seg_high": seg_high}
    for rec in conn.stream(cypher, parameters=params, fetch_size=_BATCH):
        yield dict(rec)


def count_tender_items(conn, contains: str | None = None) -> int:
    rec = conn.query(
        """
        MATCH (i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
        RETURN count(i) AS c
        """,
        parameters={"contains": contains.lower() if contains else None},
    )
    return rec[0]["c"]


def count_offers(conn, contains: str | None = None) -> int:
    rec = conn.query(
        """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
        RETURN count(o) AS c
        """,
        parameters={"contains": contains.lower() if contains else None},
    )
    return rec[0]["c"]
