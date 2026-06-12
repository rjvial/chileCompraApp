"""M1 curation pass for suturas + bandas_molares (from full-corpus dry runs
data/m1_suturas_*.csv / data/m1_bandas_*.csv). Every call recorded."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

SUTURAS = "chilecompra_er/categories/schemas/suturas.json"
BANDAS = "chilecompra_er/categories/schemas/bandas_molares.json"

s = json.load(open(SUTURAS, encoding="utf-8"))
for attr in s["attribute_defs"]:
    if attr["name"] == "calibre":
        # "0/0" is alternate notation for USP 0 — canonicalize, don't add a value
        attr["rules"].insert(0, {"kind": "keyword", "pattern": "\\b0 0\\b", "value": "0"})
    if attr["name"] == "material":
        if "seda" not in attr["domain"]:
            attr["domain"].insert(0, "seda")
            attr["rules"].insert(0, {"kind": "keyword", "pattern": "\\bseda\\b",
                                     "value": "seda"})
    if attr["name"] == "longitud_hebra":
        for v in ["22cm", "35cm", "45cm", "100cm", "120cm", "150cm"]:
            if v not in attr["domain"]:
                attr["domain"].append(v)
s["schema_version"] = "0.2.0-draft"
s["curation_log"].append(
    "Curation 2026-06-13 (full-corpus dry run, 139 resolved / 6 illegal): "
    "seda added to material (classic suture silk was missing); 0/0 notation "
    "canonicalized to calibre 0; longitud_hebra domain widened with observed "
    "lengths. Illegal source 150cm came from a surgical-pack item (pack noise, "
    "not schema gap).")

json.dump(s, open(SUTURAS, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("suturas curated -> 0.2.0-draft")

b = json.load(open(BANDAS, encoding="utf-8"))
b["schema_version"] = "0.2.0-draft"
b["curation_log"].append(
    "Curation 2026-06-13 (full-corpus dry run, 123 resolved): zero illegal "
    "values, zero root-anchored records, all six attributes extracting — no "
    "edits required. Cleanest category in the catalog; chosen for first "
    "persisted vertical slice.")
json.dump(b, open(BANDAS, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("bandas_molares curated -> 0.2.0-draft")
