"""Offline demo: resolve the design note's example descriptions end to end
against an in-memory catalog (no Neo4j needed).

    python examples/resolve_demo.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.resolve import InMemoryCatalog, Resolver, SourceRef

EXAMPLES = [
    # sondas_foley — the original vertical slice
    "SONDA FOLEY CH16 SILICONA 2 VIAS BALON 30 CC",
    "SONDA FOLEY CH16 SILICONA 2 VIAS",
    "SONDA FOLEY 16",
    "SONDA FOLEY 16 LATEX 2 VIAS",
    "Cateter foley n°16 100% silicona dos vías",     # same node as #2
    "SONDA FOLEY 16 LATEX 2 VIAS CAJA X 10",         # pack evidence -> per_pack
    # the nine categories widened from the segment-42 spend profiling
    "Jeringa hipodérmica 10 ml luer slip con aguja 21G",
    "JERINGA 5ML LUER SLIP",
    "GUANTE NITRILO TALLA M NO ESTERIL CAJA X 100",
    "Guante de látex estéril talla 7.5",
    "GASA ESTERIL 10X10 CM SOBRE INDIVIDUAL",
    "AGUJA HIPODERMICA 21G X 1 1/2 DESECHABLE",
    "SUTURA NYLON 3/0 AGUJA CURVA",
    "VENDA ELASTICA 10 CM X 5 M",
    "CIRCUITO RESPIRATORIO ADULTO DESECHABLE",
    "SONDA NASOGASTRICA 16 FR",
    "SONDA DE ASPIRACION 14 FR",
    # outside the launched register -> explicit unresolved, never guessed
    "TERMOMETRO DIGITAL AXILAR",
]


def main() -> None:
    catalog = InMemoryCatalog()
    resolver = Resolver(catalog)

    for i, raw in enumerate(EXAMPLES, start=1):
        source = SourceRef("demo", tender_id="T-001", line_no=str(i), raw_text=raw)
        r = resolver.resolve(raw, source=source)
        print(f"\n[{i}] {raw!r}")
        print(f"    normalized : {r.normalized}")
        if r.status == "unresolved":
            print(f"    -> UNRESOLVED ({r.unresolved_reason}) — visible debt, never guessed")
            continue
        print(f"    category   : {r.classification.category_id}")
        print(f"    attributes : {r.extraction.values}")
        print(f"    node       : {r.node_id}  (created={r.created}, parent={r.parent_id})")
        if r.price_basis.basis != "unknown":
            print(f"    price basis: {r.price_basis.basis} (pack={r.price_basis.pack_size})")

    print("\n--- catalog hierarchy (PARENT_OF) ---")
    nodes = catalog.nodes
    specs = catalog.specs

    def label(nid: str) -> str:
        s = specs[nid]
        attrs = {k: v for k, v in s.properties.items()}
        flag = "complete" if s.is_complete else "partial"
        return f"{nid} {attrs} [{flag}]"

    roots = [nid for nid, n in nodes.items() if n.parent_id is None]

    def walk(nid: str, depth: int) -> None:
        print("    " * depth + label(nid))
        for child, view in nodes.items():
            if view.parent_id == nid:
                walk(child, depth + 1)

    for root in sorted(roots):
        walk(root, 0)

    print("\n--- resolutions (versioned, total & explicit) ---")
    for r in catalog.resolutions:
        print(f"    {r['record_key']}: {r['status']} -> {r['target_id']} "
              f"(v{r['version']}, current={r['current']})")


if __name__ == "__main__":
    main()
