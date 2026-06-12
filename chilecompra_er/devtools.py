"""Diagnostic commands: offline demo, live smoke test, M3 offer probe.

These are dev/ops tools, not pipeline stages — promoted from examples/ so
the CLI is the single operational surface.
"""

from __future__ import annotations

from .resolve import InMemoryCatalog, Neo4jCatalog, Resolver, SourceRef

DEMO_EXAMPLES = [
    # sondas_foley — the original vertical slice
    "SONDA FOLEY CH16 SILICONA 2 VIAS BALON 30 CC",
    "SONDA FOLEY CH16 SILICONA 2 VIAS",
    "SONDA FOLEY 16",
    "SONDA FOLEY 16 LATEX 2 VIAS",
    "Cateter foley n°16 100% silicona dos vías",     # same node as #2
    "SONDA FOLEY 16 LATEX 2 VIAS CAJA X 10",         # pack evidence -> per_pack
    # a spread of other categories
    "Jeringa hipodérmica 10 ml luer slip con aguja 21G",
    "GUANTE NITRILO TALLA M NO ESTERIL CAJA X 100",
    "SUTURA NYLON 3/0 AGUJA CURVA",
    "BANDA PRIMER MOLAR SUPERIOR TUBO DOBLE MBT N36",
    "Equipamiento y suministros médicos / Productos para el cuidado de heridas / Vendas",  # rubric
    "TERMOMETRO DIGITAL AXILAR",                     # outside the register
]


def run_demo(log=print) -> None:
    """Resolve the canonical examples against an in-memory catalog — no
    graph, no LLM; shows every pipeline behavior in one screen."""
    catalog = InMemoryCatalog()
    resolver = Resolver(catalog)

    for i, raw in enumerate(DEMO_EXAMPLES, start=1):
        source = SourceRef("demo", tender_id="T-001", line_no=str(i), raw_text=raw)
        r = resolver.resolve(raw, source=source)
        log(f"\n[{i}] {raw!r}")
        log(f"    normalized : {r.normalized}")
        if r.status == "unresolved":
            log(f"    -> UNRESOLVED ({r.unresolved_reason}) — visible debt, never guessed")
            continue
        log(f"    category   : {r.classification.category_id}")
        log(f"    attributes : {r.extraction.values}")
        log(f"    node       : {r.node_id}  (created={r.created}, parent={r.parent_id})")
        if r.price_basis.basis != "unknown":
            log(f"    price basis: {r.price_basis.basis} (pack={r.price_basis.pack_size})")

    log("\n--- catalog hierarchy (PARENT_OF) ---")
    nodes, specs = catalog.nodes, catalog.specs

    def walk(nid: str, depth: int) -> None:
        spec = specs[nid]
        flag = "complete" if spec.is_complete else "partial"
        log("    " * depth + f"{nid} {spec.properties} [{flag}]")
        for child, view in nodes.items():
            if view.parent_id == nid:
                walk(child, depth + 1)

    for root in sorted(nid for nid, n in nodes.items() if n.parent_id is None):
        walk(root, 0)


def run_smoke(conn, keep: bool = False, log=print) -> bool:
    """Live round-trip: resolve three descriptions into the graph, verify the
    forward and reverse traceability queries (design §7), then delete the
    smoke data unless keep=True. Returns True on success."""
    SMOKE = "smoke_test"
    examples = [
        "SONDA FOLEY CH16 SILICONA 2 VIAS",
        "Cateter foley n°16 100% silicona dos vías",  # must land on the same node
        "SONDA FOLEY 16",                              # partial parent
    ]
    catalog = Neo4jCatalog(conn)
    resolver = Resolver(catalog)
    ok = True
    try:
        node_ids = set()
        for i, raw in enumerate(examples, start=1):
            src = SourceRef(SMOKE, tender_id="SMOKE-1", line_no=str(i), raw_text=raw)
            r = resolver.resolve(raw, source=src)
            node_ids.add(r.node_id)
            log(f"[{i}] {raw!r} -> {r.node_id} (created={r.created})")
        if len(node_ids) != 2:
            log(f"FAIL: expected dedup to 2 nodes, got {len(node_ids)}")
            ok = False

        forward = conn.query(
            """
            MATCH (s:SourceRecord {source: $src, tender_id: 'SMOKE-1', line_no: '1'})
                  -[r:RESOLVED_TO {current: true}]->(node)
            RETURN node.id AS id, r.extractor_version AS extractor
            """,
            parameters={"src": SMOKE},
        )
        log(f"forward trace: {forward[0]['id']} (extractor {forward[0]['extractor']})"
            if forward else "FAIL: forward trace empty")
        ok = ok and bool(forward)

        reverse = conn.query(
            """
            MATCH (g:GenericProduct {category_id: 'sondas_foley', specificity: 1})
                  -[:PARENT_OF*0..]->(d)<-[r:RESOLVED_TO {current: true}]-(s)
            RETURN count(s) AS n
            """
        )
        log(f"reverse trace/rollup: {reverse[0]['n']} records under the 16Fr parent")
        ok = ok and reverse[0]["n"] >= 3
    finally:
        if keep:
            log("smoke data kept (--keep)")
        else:
            conn.query("MATCH (s:SourceRecord {source: $src}) DETACH DELETE s",
                       parameters={"src": SMOKE})
            conn.query("MATCH (g:GenericProduct {category_id: 'sondas_foley'}) "
                       "DETACH DELETE g")
            conn.query("MATCH (c:Category {category_id: 'sondas_foley'}) DETACH DELETE c")
            log("smoke data cleaned up")
    return ok


def probe_offers(conn, limit: int = 1500, log=print) -> dict:
    """M3 feasibility: for rubric-only tender lines, how often does the
    supplier OFFER text recover a category / attributes? Read-only."""
    records = conn.query(
        """
        MATCH (o:Oferta)-[:PARA_ITEM]->(i:ItemLicitacion)
        WHERE o.descripcion_proveedor IS NOT NULL
          AND size(split(i.descripcion_comprador, ' / ')) >= 3
        RETURN o.descripcion_proveedor AS text
        LIMIT $limit
        """,
        parameters={"limit": limit},
    )
    resolver = Resolver(InMemoryCatalog())
    classified = with_attrs = boilerplate = 0
    for rec in records:
        r = resolver.resolve(rec["text"])
        if r.unresolved_reason == "boilerplate_rubric":
            boilerplate += 1
        elif r.status == "resolved_generic":
            classified += 1
            if r.extraction and r.extraction.values:
                with_attrs += 1
    n = max(len(records), 1)
    log(f"offers examined (their tender line is rubric-only): {len(records)}")
    log(f"offer text itself also rubric/boilerplate          : {boilerplate} ({boilerplate/n:.0%})")
    log(f"offer text classifies into a launched category     : {classified} ({classified/n:.0%})")
    log(f"  ...with extracted identity attributes            : {with_attrs} ({with_attrs/n:.0%})")
    log("")
    log("Every percent in the last line is product information invisible from")
    log("the tender line alone — recoverable via the M3 offer path.")
    return {"examined": len(records), "boilerplate": boilerplate,
            "classified": classified, "with_attrs": with_attrs}
