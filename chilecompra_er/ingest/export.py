"""CSV export of resolution results, shared by the CLI and example scripts."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from ..resolve.assignment import InMemoryCatalog


def export_csv(prefix: Path, reports, catalog) -> list[Path]:
    """Write resolution reports and (for dry runs) the in-memory catalog to
    <prefix>_resoluciones.csv / <prefix>_productos_genericos.csv.
    UTF-8 with BOM so Excel renders Spanish accents correctly."""
    prefix.parent.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    res_path = prefix.with_name(prefix.name + "_resoluciones.csv")
    with open(res_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["raw_text", "normalized", "status", "unresolved_reason",
                    "category", "node_id", "created", "parent_id",
                    "attributes", "illegal_values", "price_basis", "pack_size"])
        for r in reports:
            w.writerow([
                r.raw_text, r.normalized, r.status, r.unresolved_reason or "",
                r.classification.category_id or "",
                r.node_id or "", r.created, r.parent_id or "",
                json.dumps(r.extraction.values, ensure_ascii=False) if r.extraction else "",
                json.dumps(r.extraction.illegal, ensure_ascii=False)
                if r.extraction and r.extraction.illegal else "",
                r.price_basis.basis if r.price_basis else "",
                r.price_basis.pack_size if r.price_basis and r.price_basis.pack_size else "",
            ])
    written.append(res_path)

    if isinstance(catalog, InMemoryCatalog):
        # Always written, headers-only when the run produced no products —
        # every --out run yields exactly two files.
        prod_path = prefix.with_name(prefix.name + "_productos_genericos.csv")
        with open(prod_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["node_id", "category_id", "identity_key", "attributes",
                        "specificity", "is_complete", "parent_id"])
            for nid, spec in catalog.specs.items():
                w.writerow([
                    spec.id, spec.category_id, spec.identity_key,
                    json.dumps(spec.properties, ensure_ascii=False),
                    spec.specificity, spec.is_complete,
                    catalog.nodes[nid].parent_id or "",
                ])
        written.append(prod_path)

    return written
