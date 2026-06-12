"""One-off curation 2026-06-13, from the corpus_dry_39cat benchmark audit:

1. Tighten greedy Tier-1 includes: token + \\w* captured morphological false
   friends (fresado -> fresas_dentales, canulado -> canulas, agujero -> agujas).
2. filtros schema: add diametro (mm) — 100% of filtro records anchored on the
   category root because the draft had no size attribute.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chilecompra_er.categories.schema import _bump_minor

REGISTER = "chilecompra_er/categories/register.json"
FILTROS = "chilecompra_er/categories/schemas/filtros.json"

FIX = {
    "fresas_dentales": ["\\bfresas?\\b"],
    "canulas": ["\\bcanulas?\\b"],
    "agujas": ["\\bagujas?\\b"],
}

reg = json.load(open(REGISTER, encoding="utf-8"))
for cat in reg["categories"]:
    if cat["category_id"] in FIX:
        cat["include"] = FIX[cat["category_id"]]
reg["register_version"] = _bump_minor(reg["register_version"])
reg["notes"] += (" v-fix: includes tightened to exact singular/plural for "
                 "fresas/canulas/agujas (suffix wildcard captured fresado/"
                 "canulado/agujero - false categories).")
json.dump(reg, open(REGISTER, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("register", reg["register_version"], "- include patterns tightened")

s = json.load(open(FILTROS, encoding="utf-8"))
if not any(a["name"] == "diametro" for a in s["attribute_defs"]):
    s["attribute_defs"].append({
        "name": "diametro", "role": "identity", "unit": "mm",
        "domain": ["15mm", "19mm", "22mm", "25mm", "30mm", "37mm", "40mm",
                   "50mm", "60mm", "90mm", "100mm", "150mm", "190mm", "200mm",
                   "210mm", "240mm", "280mm", "300mm"],
        "rules": [{"kind": "regex", "pattern": "\\b(\\d{2,3})\\s*mm\\b",
                   "template": "{1}mm"}],
        "curation_notes": "Curacion 2026-06-13: el corpus es mayormente "
                          "'FILTRO ... NNN MM' y el borrador no capturaba nada "
                          "(100% raiz en benchmark); diametro como identity.",
    })
    s["curation_log"].append("Curated 2026-06-13: added diametro (mm) attribute "
                             "after benchmark showed 100% root anchoring.")
    json.dump(s, open(FILTROS, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("filtros schema: diametro attribute added")
