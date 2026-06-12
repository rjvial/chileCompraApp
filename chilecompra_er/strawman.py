"""LLM schema strawman (design §3 step 5), promoted from examples/ for CLI use.

For each candidate category: fetch real tender descriptions from the graph
(read-only), normalize them, ask the model for a draft schema in our
attribute_defs dialect, mechanically validate every rule (regexes must
compile, keyword values must be in-domain), run the §3-step-8 dry-run metrics
over the samples, and write the draft JSON to categories/schemas/.

Output is a STRAWMAN: cheap and disposable, never authoritative — every file
is marked draft and carries the dry-run numbers for expert curation.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from .categories.schema import SCHEMAS_DIR, load_register, load_schema
from .llm import complete_json
from .normalize import Normalizer
from .resolve.extractor import extract


def families(register: dict | None = None) -> list[dict]:
    """Sampling targets for the strawman, straight from the register: every
    category that declares a corpus_regex. Adding a category via
    `chilecompra-er add-category` automatically makes it generatable."""
    register = register or load_register()
    return [
        {"category_id": c["category_id"], "name": c["name"],
         "corpus_regex": c["corpus_regex"]}
        for c in register["categories"]
        if c.get("corpus_regex")
    ]


# Backward-compatible module-level snapshot (examples/ scripts import this).
FAMILIES = families()

RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["base_unit", "attribute_defs"],
    "properties": {
        "base_unit": {"type": "string"},
        "attribute_defs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "role", "unit", "domain", "rules", "curation_notes"],
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string", "enum": ["identity", "descriptive"]},
                    "unit": {"type": "string"},
                    "domain": {"type": "array", "items": {"type": "string"}},
                    "rules": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["kind", "pattern", "template", "value"],
                            "properties": {
                                "kind": {"type": "string", "enum": ["regex", "keyword"]},
                                "pattern": {"type": "string"},
                                "template": {"type": "string"},
                                "value": {"type": "string"},
                            },
                        },
                    },
                    "curation_notes": {"type": "string"},
                },
            },
        },
    },
}

SYSTEM = """Eres el asistente de catalogación de un pipeline de resolución de entidades \
para dispositivos médicos de ChileCompra. Tu tarea: a partir de descripciones reales \
NORMALIZADAS de una categoría, proponer el borrador del esquema de atributos.

Reglas del esquema (no negociables):
- role "identity": atributo clínico que fija la identidad del producto (calibre, material, \
talla, capacidad, vías, estéril SOLO si discrimina dentro de la categoría). role \
"descriptive": se normaliza pero nunca participa en la igualdad. Máximo 6 atributos; \
prefiere pocos atributos identity bien fundados.
- domain: conjunto CERRADO de valores canónicos observados o clínicamente estándar \
(sé generoso con rangos de tallas/medidas estándar). Formato de valor canónico: ASCII, \
minúsculas, sin espacios: "10ml", "21g", "16fr", "talla_m", "latex", "esteril", "2_vias".
- NUNCA propongas atributos de empaque (caja x N) — el empaque es price_basis, no identidad.
- rules: se aplican sobre texto NORMALIZADO: minúsculas, sin acentos, límites \
dígito/letra separados con espacio ("10ml" aparece como "10 ml", "ch16" como "ch 16"), \
palabras numéricas ya convertidas a dígitos, comas decimales convertidas a punto. Dos tipos:
  * kind "regex": pattern es una regex de Python con UN grupo de captura; template \
construye el valor canónico con {1}, p.ej. pattern "\\\\b(\\\\d{1,2})\\\\s*ml\\\\b" + \
template "{1}ml". Deja value como cadena vacía.
  * kind "keyword": pattern es una regex SIN grupos cuya presencia implica el valor fijo \
de value (que DEBE estar en domain). Deja template como cadena vacía.
  Ordena las reglas de más específica a más general (la primera que produce un valor \
dentro del dominio gana). Para atributos sin unidad deja unit como cadena vacía.
- name de cada atributo: español, snake_case, ASCII sin acentos (p.ej. "capacidad",
"conexion", "calibre_aguja", "material", "talla", "esteril") — coherente con las
categorías existentes.
- curation_notes: una línea por atributo justificando rol y dominio.
- base_unit: la unidad clínica base de precio (p.ej. "unidad (1 jeringa)") — nunca caja.

Responde SOLO el JSON pedido."""


def fetch_samples(conn, corpus_regex: str, limit: int) -> list[str]:
    records = conn.query(
        """
        MATCH (i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND i.descripcion_comprador =~ $rx
        RETURN DISTINCT i.descripcion_comprador AS text
        LIMIT $limit
        """,
        parameters={"rx": corpus_regex, "limit": limit},
    )
    return [r["text"] for r in records]


def curate(draft: dict, category_id: str, category_name: str) -> tuple[dict, list[str]]:
    """Mechanical validation of the strawman: compile every regex, check rule
    shape and keyword domains; drop what fails, log every drop."""
    notes: list[str] = []
    attrs = []
    for attr in draft.get("attribute_defs", [])[:6]:
        attr_name = attr.get("name", "").strip()
        role = attr.get("role", "")
        if not attr_name or role not in ("identity", "descriptive"):
            notes.append(f"dropped malformed attribute entry {attr!r:.80}")
            continue
        domain = list(dict.fromkeys(v.strip() for v in attr.get("domain", []) if v.strip()))
        rules = []
        for rule in attr.get("rules", []):
            try:
                compiled = re.compile(rule.get("pattern", ""))
            except re.error as exc:
                notes.append(f"{attr_name}: dropped rule (regex error: {exc})")
                continue
            if rule.get("kind") == "regex":
                if compiled.groups < 1 or "{1}" not in rule.get("template", ""):
                    notes.append(f"{attr_name}: dropped regex rule without group/template")
                    continue
                rules.append({"kind": "regex", "pattern": rule["pattern"],
                              "template": rule["template"]})
            elif rule.get("kind") == "keyword":
                if rule.get("value") not in domain:
                    notes.append(f"{attr_name}: dropped keyword rule with out-of-domain "
                                 f"value {rule.get('value')!r}")
                    continue
                rules.append({"kind": "keyword", "pattern": rule["pattern"],
                              "value": rule["value"]})
        if not domain:
            notes.append(f"{attr_name}: dropped attribute (empty domain)")
            continue
        entry = {
            "name": attr_name,
            "role": role,
            "domain": domain,
            "rules": rules,
            "curation_notes": attr.get("curation_notes", ""),
        }
        if attr.get("unit", "").strip():
            entry["unit"] = attr["unit"].strip()
        attrs.append(entry)
    doc = {
        "category_id": category_id,
        "name": category_name,
        "schema_version": "0.1.0-draft",
        "status": "draft",
        "base_unit": draft.get("base_unit", "unidad"),
        "curation_log": [
            "DRAFT generated by the LLM strawman (design §3 step 5) from real corpus "
            "samples — cheap and disposable, never authoritative. Requires the full M1 "
            "procedure (mining passes, price-discrimination test, expert curation, "
            "reference validation) before launch.",
        ],
        "attribute_defs": attrs,
    }
    return doc, notes


def dry_run(doc_path: Path, normalized_samples: list[str]) -> dict:
    """§3 step 8 metrics over the sample corpus."""
    schema = load_schema(doc_path)
    coverage = {a.name: 0 for a in schema.attribute_defs}
    illegal = 0
    for text in normalized_samples:
        ex = extract(text, schema)
        for attr_name in ex.values:
            coverage[attr_name] += 1
        illegal += len(ex.illegal)
    n = max(len(normalized_samples), 1)
    return {
        "samples": len(normalized_samples),
        "coverage": {k: round(v / n, 3) for k, v in coverage.items()},
        "illegal": illegal,
    }


def generate(conn, only: str | None = None, samples: int = 50, log=print) -> list[Path]:
    """Generate draft schemas for FAMILIES; returns the written paths."""
    norm = Normalizer()
    written: list[Path] = []
    for fam in families():
        if only and fam["category_id"] != only:
            continue
        raw = fetch_samples(conn, fam["corpus_regex"], samples)
        if len(raw) < 5:
            log(f"== {fam['category_id']}: only {len(raw)} samples — SKIPPED")
            continue
        normalized = [norm(t) for t in raw]
        log(f"== {fam['category_id']}: {len(raw)} samples; e.g. {raw[0][:70]!r}")

        prompt = (
            f"Categoría: {fam['name']} (id: {fam['category_id']}).\n"
            f"Descripciones normalizadas reales ({len(normalized)}):\n"
            + "\n".join(f"- {t}" for t in normalized)
        )
        draft = complete_json(prompt, RESPONSE_SCHEMA, system=SYSTEM)
        doc, notes = curate(draft, fam["category_id"], fam["name"])
        for note in notes:
            log(f"   curate: {note}")

        path = SCHEMAS_DIR / f"{fam['category_id']}.json"
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

        stats = dry_run(path, normalized)
        doc["curation_log"].append(f"Dry run on {stats['samples']} corpus samples: "
                                   f"coverage {stats['coverage']}, "
                                   f"illegal extractions {stats['illegal']}.")
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"   wrote {path.name}: attrs={[a['name'] for a in doc['attribute_defs']]}")
        log(f"   dry run: coverage={stats['coverage']} illegal={stats['illegal']}")
        written.append(path)
    return written
