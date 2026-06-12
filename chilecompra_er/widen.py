"""Widen the register by N categories from the spend ranking — the M4 loop
as one command.

Pipeline: profile (head-noun x spend) -> drop residue/covered/thin groups ->
LLM vet of the shortlist (design §8: Tier 3 bootstraps new categories — it
judges whether a group is a coherent product family and proposes the Tier-1
definition) -> mechanical validation of every proposed pattern -> preview or
apply (add_category + strawman schema per category).

The LLM proposes; deterministic code disposes: every regex must compile, the
canonical example must classify to its own category against the real
register, and ids must be fresh — anything failing is downgraded to
not-viable with a reason, never silently fixed into the register.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .categories.schema import CATEGORIES_DIR, add_category, load_register
from .llm import complete_json
from .normalize import Normalizer
from .profiling import RESIDUE, fetch_item_spend, profile
from .resolve.classifier import AMBIGUOUS, CLASSIFIED, Tier1Classifier


@dataclass
class Candidate:
    token: str
    records: int
    spend_share: float
    samples: list[str] = field(default_factory=list)
    viable: bool = False
    category_id: str = ""
    name: str = ""
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    corpus_regex: str = ""
    canonical_example: str = ""
    reason: str = ""
    rejected_by: str = ""  # "vet" (junk verdict, cacheable) | "validation" (state-dependent)


# Persisted vet verdicts: tokens the LLM judged junk stay rejected across runs
# (re-vetting "bases"/"meses" every invocation is pure waste). Mechanical
# validation rejections are NOT cached — they depend on register state.
VET_REJECTIONS_PATH = CATEGORIES_DIR / "vet_rejections.json"


def load_vet_rejections(path: Path = VET_REJECTIONS_PATH) -> dict[str, str]:
    if not Path(path).exists():
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_vet_rejections(rejections: dict[str, str],
                        path: Path = VET_REJECTIONS_PATH) -> None:
    Path(path).write_text(json.dumps(rejections, ensure_ascii=False, indent=2,
                                     sort_keys=True) + "\n", encoding="utf-8")


VET_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["candidates"],
    "properties": {
        "candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["token", "viable", "category_id", "name", "include",
                             "exclude", "corpus_regex", "canonical_example", "reason"],
                "properties": {
                    "token": {"type": "string"},
                    "viable": {"type": "boolean"},
                    "category_id": {"type": "string"},
                    "name": {"type": "string"},
                    "include": {"type": "array", "items": {"type": "string"}},
                    "exclude": {"type": "array", "items": {"type": "string"}},
                    "corpus_regex": {"type": "string"},
                    "canonical_example": {"type": "string"},
                    "reason": {"type": "string"},
                },
            },
        },
    },
}

VET_SYSTEM = """Eres el curador de categorías de un catálogo de dispositivos médicos de \
ChileCompra. Recibes grupos candidatos (token frecuente + gasto + descripciones reales) \
y decides cuáles son FAMILIAS DE PRODUCTO coherentes (una categoría = un conjunto de \
ítems que comparte un mismo esquema de atributos de identidad).

NO son familias: palabras de relleno ("bases", "meses", "tecnica", "compatible"), \
números de línea de licitación, servicios, grupos que mezclan productos clínicamente \
distintos sin atributo común. Si el token agrupa varias familias distintas, elige LA \
dominante en las muestras y di en reason qué quedó fuera.

Para cada candidato viable propone:
- category_id: español snake_case ASCII plural (p.ej. "termometros").
- name: nombre legible.
- include: 1-3 regex de Python sobre texto NORMALIZADO (minúsculas, sin acentos, \
dígito/letra separados) que DEFINEN la pertenencia, p.ej. "\\\\btermometro\\\\w*". \
Precisas antes que amplias.
- exclude: regex que vetan capturas falsas vistas en las muestras (accesorios, kits, \
otras familias).
- corpus_regex: regex case-insensitive sobre texto CRUDO para muestrear el corpus, \
p.ej. "(?i).*termometro.*".
- canonical_example: COPIA EXACTA de una de las descripciones de muestra que mejor \
representa la categoría (se usará como ejemplo dorado en tests).
- reason: una línea.

Para candidatos no viables: viable=false, reason explica por qué; deja los demás \
campos como cadenas/listas vacías. Responde SOLO el JSON pedido."""


def _is_covered(token: str, clf: Tier1Classifier) -> bool:
    return clf.classify(token).status in (CLASSIFIED, AMBIGUOUS)


def _fetch_token_samples(conn, token: str, limit: int) -> list[str]:
    from .ingest.neo4j_source import NOT_RUBRIC

    records = conn.query(
        f"""
        MATCH (i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND {NOT_RUBRIC}
          AND i.descripcion_comprador =~ $rx
        RETURN DISTINCT i.descripcion_comprador AS text
        LIMIT $limit
        """,
        parameters={"rx": f"(?i).*{re.escape(token)}.*", "limit": limit},
    )
    return [r["text"] for r in records]


def finalize_candidates(shortlist: list[Candidate], raw: dict,
                        register: dict) -> list[Candidate]:
    """Merge the LLM verdicts into the shortlist and validate mechanically.
    Pure function — unit-testable without LLM or graph."""
    by_token = {c["token"]: c for c in raw.get("candidates", [])}
    # Working copy: candidates accepted earlier in this call join the
    # categories later candidates are validated against, so the same family
    # surfacing under two tokens (e.g. bandas + tubo -> bandas molares) is
    # caught as a collision instead of registered twice.
    categories = list(register["categories"])
    existing_ids = {c["category_id"] for c in categories}
    norm = Normalizer()

    for cand in shortlist:
        verdict = by_token.get(cand.token)
        if verdict is None or not verdict.get("viable"):
            cand.viable = False
            cand.reason = (verdict or {}).get("reason", "no verdict from vet")
            cand.rejected_by = "vet" if verdict is not None else "validation"
            continue
        cand.rejected_by = "validation"  # default for any gate below

        cand.category_id = verdict.get("category_id", "").strip()
        cand.name = verdict.get("name", "").strip() or cand.token.capitalize()
        cand.include = [p for p in verdict.get("include", []) if p.strip()]
        cand.exclude = [p for p in verdict.get("exclude", []) if p.strip()]
        cand.corpus_regex = verdict.get("corpus_regex", "").strip()
        cand.canonical_example = verdict.get("canonical_example", "").strip()
        cand.reason = verdict.get("reason", "")

        if not re.fullmatch(r"[a-z0-9_]+", cand.category_id) or \
                cand.category_id in existing_ids:
            cand.viable = False
            cand.reason = f"invalid or duplicate category_id {cand.category_id!r}"
            continue
        try:
            for pattern in [*cand.include, *cand.exclude,
                            *([cand.corpus_regex] if cand.corpus_regex else [])]:
                re.compile(pattern)
        except re.error as exc:
            cand.viable = False
            cand.reason = f"proposed pattern does not compile: {exc}"
            continue
        if not cand.include:
            cand.include = [f"\\b{re.escape(cand.token)}\\w*"]
        if not cand.corpus_regex:
            cand.corpus_regex = f"(?i).*{re.escape(cand.token)}.*"

        # The canonical example must classify to this category against the
        # working register plus this entry — same check the tests will run.
        entry = {"category_id": cand.category_id, "name": cand.name,
                 "status": "candidate", "include": cand.include,
                 "exclude": cand.exclude,
                 "canonical_example": cand.canonical_example}
        trial = {"register_version": "trial", "categories": [*categories, entry]}
        clf = Tier1Classifier(trial)

        def classifies(text: str) -> bool:
            c = clf.classify(norm(text))
            return c.status == CLASSIFIED and c.category_id == cand.category_id

        if not cand.canonical_example or not classifies(cand.canonical_example):
            replacement = next((s for s in cand.samples if classifies(s)), None)
            if replacement is None:
                cand.viable = False
                cand.reason = ("no sample classifies unambiguously to the proposed "
                               "category (overlap with existing rules?)")
                continue
            cand.canonical_example = replacement

        # Regression gate: the new rules must not make any already-accepted
        # category's golden example ambiguous (e.g. a broad "sondas" would
        # swallow "SONDA FOLEY ..." away from sondas_foley).
        broken = [
            existing["category_id"] for existing in categories
            if existing.get("canonical_example")
            and clf.classify(norm(existing["canonical_example"])).category_id
            != existing["category_id"]
        ]
        if broken:
            cand.viable = False
            cand.reason = (f"include patterns would make existing categories "
                           f"ambiguous: {', '.join(broken[:4])}")
            continue

        cand.viable = True
        existing_ids.add(cand.category_id)
        categories.append(entry)

        # An entry the working register itself now mis-classifies would fail
        # the test suite — re-update the canonical after acceptance.
        entry["canonical_example"] = cand.canonical_example
    return shortlist


def _llm_vet(batch: list[Candidate]) -> dict:
    prompt_parts = []
    for c in batch:
        sample_lines = "\n".join(f"    - {s[:140]}" for s in c.samples[:8])
        prompt_parts.append(f"token: {c.token} (registros: {c.records}, "
                            f"gasto: {c.spend_share:.1%})\n{sample_lines}")
    return complete_json("Grupos candidatos:\n\n" + "\n\n".join(prompt_parts),
                         VET_SCHEMA, system=VET_SYSTEM)


def propose(conn, count: int = 10, segment: int | None = 42, min_samples: int = 15,
            min_spend_share: float = 0.0005, revisit: bool = False,
            vet=None, batch_size: int = 12,
            log=print) -> tuple[list[Candidate], list[Candidate]]:
    """Returns (chosen, rejected). Writes only the vet-rejection cache.

    Walks the WHOLE spend ranking in vet batches until `count` viable
    categories are found, the spend floor is reached, or the ranking is
    exhausted. Tokens the vet previously judged junk are skipped (cache at
    categories/vet_rejections.json; revisit=True re-evaluates them).
    """
    register = load_register()
    clf = Tier1Classifier(register)
    vet = vet or _llm_vet
    rejections = {} if revisit else load_vet_rejections()
    skipped_cached = 0

    log("profiling corpus...")
    stats = profile(fetch_item_spend(conn, unspsc_segment=segment))

    # Working register: accepted candidates are appended so every later batch
    # validates against them, not just against what was on disk.
    working = {"register_version": register["register_version"],
               "categories": list(register["categories"])}

    chosen: list[Candidate] = []
    rejected: list[Candidate] = []
    batch: list[Candidate] = []
    chosen_ids: set[str] = set()

    def flush_batch() -> None:
        nonlocal batch
        if not batch or len(chosen) >= count:
            batch = []
            return
        log(f"  vetting batch of {len(batch)}: {[c.token for c in batch]}")
        vetted = finalize_candidates(batch, vet(batch), working)
        for c in vetted:
            if c.viable and c.category_id not in chosen_ids and len(chosen) < count:
                chosen.append(c)
                chosen_ids.add(c.category_id)
                working["categories"].append({
                    "category_id": c.category_id, "name": c.name,
                    "status": "candidate", "include": c.include,
                    "exclude": c.exclude, "canonical_example": c.canonical_example,
                })
                log(f"    + {c.category_id} ({c.spend_share:.1%} spend)")
            else:
                if c.viable:  # duplicate id from an earlier batch, or over count
                    c.viable, c.reason = False, c.reason or "superseded in this run"
                rejected.append(c)
        batch = []

    for s in stats:
        if len(chosen) >= count:
            break
        if s.spend_share < min_spend_share:
            log(f"  spend floor reached at {s.group!r} "
                f"({s.spend_share:.2%} < {min_spend_share:.2%}) — stopping scan")
            break
        if s.group == RESIDUE or _is_covered(s.group, clf):
            continue
        if s.group in rejections:
            skipped_cached += 1
            continue
        samples = _fetch_token_samples(conn, s.group, 30)
        if len(samples) < min_samples:
            log(f"  skip {s.group}: only {len(samples)} distinct samples")
            continue
        batch.append(Candidate(token=s.group, records=s.records,
                               spend_share=s.spend_share, samples=samples))
        if len(batch) >= batch_size:
            flush_batch()
    flush_batch()

    if skipped_cached:
        log(f"  skipped {skipped_cached} tokens previously vetted as junk "
            f"(--revisit to re-evaluate)")
    newly_junk = {c.token: c.reason for c in rejected if c.rejected_by == "vet"}
    if newly_junk:
        cache = load_vet_rejections()
        cache.update(newly_junk)
        save_vet_rejections(cache)
        log(f"  cached {len(newly_junk)} junk verdicts for future runs")

    if len(chosen) < count:
        log(f"  scan ended: {len(chosen)}/{count} viable categories found")
    return chosen, rejected


def apply(conn, candidates: list[Candidate], schema_samples: int = 50, log=print) -> None:
    from .strawman import fetch_samples, generate

    registerable = []
    for cand in candidates:
        # The vet judged the family from token-group samples; its proposed
        # corpus_regex can be narrower. A category whose corpus can't feed the
        # strawman (>=5 samples) must not enter the register at all.
        yield_count = len(fetch_samples(conn, cand.corpus_regex, 10))
        if yield_count < 5:
            log(f"NOT registered {cand.category_id}: corpus_regex matches only "
                f"{yield_count} descriptions — too thin for a schema")
            continue
        registerable.append(cand)

    for cand in registerable:
        entry = add_category(
            category_id=cand.category_id, name=cand.name, include=cand.include,
            exclude=cand.exclude, corpus_regex=cand.corpus_regex,
            canonical_example=cand.canonical_example,
        )
        log(f"registered {entry['category_id']}")
    for cand in registerable:
        generate(conn, only=cand.category_id, samples=schema_samples, log=log)
