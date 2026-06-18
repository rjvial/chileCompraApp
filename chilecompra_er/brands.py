"""LLM brand-lexicon builder (improvement loop, step 3a).

For each curated category, sample real descriptions from the corpus and ask the
model for the brand / trade-name tokens that identify products of that category
(the "relyx", "cavit", "panamax" words a head-noun regex can never match). The
LLM proposes; deterministic code disposes — every brand must:

  * normalize to exactly ONE token (the lexicon matches single tokens),
  * be >= 3 chars and not a pure number,
  * not be generic filler (profiling._NOT_FAMILY),
  * actually appear in the sampled descriptions (no hallucinated brands),
  * not already be classified by Tier-1 (keep the lexicon to what regex misses).

A token proposed for more than one category is ambiguous and dropped. Survivors
merge into categories/brand_lexicon.json (existing curated entries win).
"""

from __future__ import annotations

import json
from pathlib import Path

from .categories.schema import load_register
from .llm import complete_json
from .normalize import Normalizer
from .profiling import _NOT_FAMILY
from .resolve.brand_lexicon import BRAND_LEXICON_PATH, _WORD, load_brand_map
from .resolve.classifier import CLASSIFIED, Tier1Classifier
from .strawman import families, fetch_samples

DEFAULT_FORMAT = ("<brand token, normalized: lowercase, unaccented> -> <existing "
                  "category_id>. Consulted as a classification tier after Tier-1 "
                  "regex. Curator-maintained; entries whose category_id is absent "
                  "from the register are ignored.")

BRAND_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["brands"],
    "properties": {"brands": {"type": "array", "items": {"type": "string"}}},
}

BRAND_SYSTEM = """Eres el curador de marcas de un catálogo de compras públicas \
(ChileCompra, productos de cualquier rubro). Recibes descripciones reales de UNA \
categoría y extraes los NOMBRES DE MARCA o NOMBRES COMERCIALES que identifican \
productos de esa categoría: p.ej. "relyx", "cavit", "vitrebond", "alveogyl", \
"panamax", "midwest".

NO incluyas: el nombre genérico de la familia (p.ej. "jeringa", "guante"), \
materiales ("latex", "silicona"), medidas/tallas, ni palabras de relleno. Devuelve \
SOLO marcas que aparezcan literalmente en las descripciones, como tokens de UNA sola \
palabra en minúscula ASCII sin acentos. Si no hay marcas claras, devuelve lista vacía. \
Responde SOLO el JSON pedido."""


def valid_brand_tokens(raw_brands: list[str], normalized_samples: list[str], clf,
                       normalizer: Normalizer | None = None) -> list[str]:
    """Pure validation of one category's LLM brand proposals against its samples."""
    normalizer = normalizer or Normalizer()
    sample_tokens = {t for s in normalized_samples for t in _WORD.findall(s)}
    out: list[str] = []
    for brand in raw_brands:
        toks = _WORD.findall(normalizer(brand))
        if len(toks) != 1:
            continue
        tok = toks[0]
        if len(tok) < 3 or tok.isdigit():
            continue
        if tok in _NOT_FAMILY:
            continue
        if tok not in sample_tokens:
            continue  # not in the real corpus -> likely hallucinated
        if clf.classify(tok).status == CLASSIFIED:
            continue  # Tier-1 already covers it -> redundant
        if tok not in out:
            out.append(tok)
    return out


def merge_brand_maps(existing: dict[str, str], generated: dict[str, str],
                     overwrite: bool = False) -> tuple[dict[str, str], int, int]:
    """Merge generated brands into the existing map. Returns (merged, added,
    conflicts). Without overwrite, existing (curated) entries always win."""
    if overwrite:
        return dict(generated), len(generated), 0
    merged = dict(existing)
    added = conflicts = 0
    for brand, cat in generated.items():
        if brand in merged:
            if merged[brand] != cat:
                conflicts += 1  # existing curated entry kept
        else:
            merged[brand] = cat
            added += 1
    return merged, added, conflicts


def save_brand_map(mapping: dict[str, str], path: Path = BRAND_LEXICON_PATH,
                   fmt: str = DEFAULT_FORMAT) -> None:
    data = {"_format": fmt, "brands": dict(sorted(mapping.items()))}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")


def build(conn, only: str | None = None, samples: int = 50,
          max_per_category: int = 15, log=print,
          register: dict | None = None) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Returns (generated, dropped): generated is {brand: category_id} for brands
    claimed by exactly one category; dropped is {brand: [categories]} for the
    ambiguous ones (claimed by more than one)."""
    register = register or load_register()
    norm = Normalizer()
    clf = Tier1Classifier(register)
    proposals: dict[str, set[str]] = {}

    for fam in families(register):
        if only and fam["category_id"] != only:
            continue
        raw = fetch_samples(conn, fam["corpus_regex"], samples)
        if len(raw) < 5:
            log(f"== {fam['category_id']}: only {len(raw)} samples — SKIPPED")
            continue
        normalized = [norm(t) for t in raw]
        resp = complete_json(
            f"Categoría: {fam['name']} (id: {fam['category_id']}).\n"
            f"Descripciones normalizadas reales ({len(normalized)}):\n"
            + "\n".join(f"- {t}" for t in normalized),
            BRAND_SCHEMA, system=BRAND_SYSTEM)
        tokens = valid_brand_tokens(resp.get("brands", [])[:max_per_category],
                                    normalized, clf, norm)
        for tok in tokens:
            proposals.setdefault(tok, set()).add(fam["category_id"])
        log(f"== {fam['category_id']}: {len(tokens)} valid brand(s): {tokens}")

    generated = {b: next(iter(cats)) for b, cats in proposals.items() if len(cats) == 1}
    dropped = {b: sorted(cats) for b, cats in proposals.items() if len(cats) > 1}
    return generated, dropped
