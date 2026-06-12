"""Retrieval of source records from the existing Mercado Público graph.

The transactional layer already lives in Neo4j (Licitacion / ItemLicitacion /
Oferta / OrdenCompra / ItemOC, loaded by the ingestion pipeline with run_id /
record_hash provenance). For the entity-resolution process that graph IS the
source store (design §2): payloads stay on those nodes; resolution only adds
thin :SourceRecord reference nodes + :RESOLVED_TO edges keyed by the stable
ids retrieved here.

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

_BATCH = 1000


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


def _paged(conn, cypher: str, build, contains: str | None, limit: int | None) -> Iterator[SourceItem]:
    """Stream results in SKIP/LIMIT pages so large pulls don't buffer fully."""
    fetched = 0
    skip = 0
    while True:
        page = min(_BATCH, limit - fetched) if limit is not None else _BATCH
        if page <= 0:
            return
        records = conn.query(
            cypher,
            parameters={"contains": contains.lower() if contains else None,
                        "skip": skip, "limit": page},
        )
        for rec in records:
            yield build(rec)
            fetched += 1
        if len(records) < page:
            return
        skip += page


def fetch_tender_items(conn, contains: str | None = None,
                       limit: int | None = None) -> Iterator[SourceItem]:
    """Buyer-side tender lines: the primary resolution corpus."""
    cypher = """
        MATCH (l:Licitacion)-[:TIENE_ITEM]->(i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
        RETURN i.id_licitacion AS tender_id, i.id_item AS item_id,
               i.descripcion_comprador AS text, i.codigo_unspsc_producto AS unspsc,
               i.cantidad AS quantity, i.moneda_item AS currency,
               i.unidad_medida AS uom, i.nombre_producto_fuente AS source_name,
               l.fecha_publicacion AS date
        ORDER BY tender_id, item_id
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
            extra={"uom": rec["uom"], "source_name": rec["source_name"]},
        )

    return _paged(conn, cypher, build, contains, limit)


def fetch_offers(conn, contains: str | None = None, awarded_only: bool = False,
                 limit: int | None = None) -> Iterator[SourceItem]:
    """Supplier offers for tender items. `contains` filters on the BUYER text
    of the linked item, so offers retrieve alongside their tender lines."""
    cypher = f"""
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          {'AND o.es_adjudicada = true' if awarded_only else ''}
          AND ($contains IS NULL OR toLower(i.descripcion_comprador) CONTAINS $contains)
        RETURN o.id_licitacion AS tender_id, o.id_item AS item_id,
               o.id_oferta AS offer_id, o.descripcion_proveedor AS text,
               i.descripcion_comprador AS buyer_text,
               coalesce(o.cantidad_clean, o.cantidad_ofertada) AS quantity,
               coalesce(o.precio_unitario_clean, o.precio_unitario) AS unit_price,
               coalesce(o.precio_total_clean, o.precio_total) AS total,
               o.precio_total_clp AS total_clp, o.moneda AS currency,
               o.es_adjudicada AS awarded, o.fecha AS date,
               i.codigo_unspsc_producto AS unspsc
        ORDER BY tender_id, item_id, offer_id
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

    return _paged(conn, cypher, build, contains, limit)


def fetch_oc_items(conn, contains: str | None = None,
                   limit: int | None = None) -> Iterator[SourceItem]:
    """Purchase-order lines: award-side records with firm prices."""
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
        ORDER BY oc_id, item_id
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

    return _paged(conn, cypher, build, contains, limit)


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
