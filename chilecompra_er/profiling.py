"""M0 profiling: head-noun groups ranked by spend (design §3, steps 1-3).

Head noun: after normalization, the first content token — in Spanish
procurement text almost always the product family. Groups merge through a
small curated synonym map; spend uses awarded-offer CLP totals (ItemLicitacion
itself carries no price). Descriptions matching no family noun fall into the
"_residue" bucket for manual scan — usually new families or junk, and always
visible, never silently dropped.

The output ranking is what picks the categories to build: cut where
cumulative spend reaches ~80% or build capacity runs out.
"""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from .normalize import Normalizer

RESIDUE = "_residue"

# Columns of the cached ranking file — the profiling -> register handoff.
RANKING_HEADER = ["group", "records", "distinct_texts", "spend_clp",
                  "spend_share", "cum_share"]

# Curated synonym map (design §3 step 2) — grows during M0 curation.
DEFAULT_SYNONYMS = {
    "cateter": "sonda",
    "aposito": "gasa",
    "vendaje": "venda",
}

# Tokens that can never be a family noun: qualifiers, units, generic filler.
# Curated against the all-segments profile, where these otherwise topped the
# ranking or swelled the residue bucket. A head noun is the first token NOT in
# this set, so anything here is skipped over to the real product word.
_NOT_FAMILY = frozenset({
    # articles, prepositions, conjunctions
    "de", "del", "la", "el", "los", "las", "un", "una", "y", "o", "a", "al",
    "en", "con", "sin", "para", "por", "segun", "conforme", "mediante",
    # qualifiers / dimensions (never the family itself)
    "tipo", "uso", "marca", "modelo", "desechable", "esteril", "adulto",
    "pediatrico", "neonatal", "adulta", "alta", "baja", "largo", "ancho",
    "alto", "corto", "grande", "pequeno", "mediano", "mayor", "menor",
    "nuevo", "nueva", "generico", "generica", "original", "similar",
    # procurement / tender boilerplate that leaks in as a leading token
    "adquisicion", "compra", "servicio", "suministro", "insumo", "insumos",
    "varios", "otros", "bases", "base", "linea", "lineas", "item", "items",
    "mes", "meses", "ofertar", "oferta", "ofertas", "acuerdo", "total",
    "subtotal", "que", "debe", "deben", "valor", "valores", "codigo", "cod",
    "referencia", "ref", "cantidad", "cantidades", "cada", "requiere",
    "requerimiento", "solicita", "solicitud", "adjunto", "adjunta", "adjuntos",
    "producto", "productos", "articulo", "articulos", "aprox", "rec",
    # units / measure tokens (length >= 3; shorter ones never tokenize)
    "ml", "cc", "mm", "cm", "fr", "ch", "gr", "kg", "mg", "lt", "mcg", "mcl",
    "mts", "grs", "mgs", "kgs", "lts", "nro", "sol",
    # packaging / display nouns
    "uds", "unidad", "unidades", "caja", "bolsa", "pack", "set", "kit", "rollo",
    "talla", "medida", "medidas", "color", "ver", "detalle", "detalles",
    "especificaciones", "tecnicas", "adjuntas", "anexo", "x",
})

_TOKEN = re.compile(r"[a-z]{3,}")


@dataclass
class GroupStat:
    group: str
    records: int
    distinct_texts: int
    spend_clp: float
    spend_share: float = 0.0
    cum_share: float = 0.0


def candidate_family_nouns(normalized_texts: list[str], top_n: int | None = None,
                           min_count: int = 5,
                           synonyms: dict[str, str] = DEFAULT_SYNONYMS) -> list[str]:
    """Corpus tokens that recur in >= min_count distinct descriptions and are
    not stoplisted — the candidate family-noun vocabulary. top_n=None (default)
    keeps the WHOLE vocabulary: capping it (the old top-150) is what dumped most
    of an all-segments corpus into residue, because a description leading with a
    word outside the cap had no family to land on. min_count still excludes
    singletons (typos / one-off phrasings) — those are residue, not families."""
    counts: Counter[str] = Counter()
    for text in normalized_texts:
        seen = set()
        for tok in _TOKEN.findall(text):
            tok = synonyms.get(tok, tok)
            if tok not in _NOT_FAMILY and tok not in seen:
                counts[tok] += 1
                seen.add(tok)
    ranked = counts.most_common(top_n)  # most_common(None) returns all
    return [tok for tok, c in ranked if c >= min_count]


def head_noun(normalized_text: str, families: set[str],
              synonyms: dict[str, str] = DEFAULT_SYNONYMS) -> str:
    """First family noun appearing in the description, synonym-merged;
    RESIDUE when none matches."""
    for tok in _TOKEN.findall(normalized_text):
        tok = synonyms.get(tok, tok)
        if tok in families:
            return tok
    return RESIDUE


def profile(rows: list[dict], normalizer: Normalizer | None = None,
            families: list[str] | None = None,
            synonyms: dict[str, str] = DEFAULT_SYNONYMS,
            top_families: int | None = None, min_count: int = 5) -> list[GroupStat]:
    """rows: [{"text": ..., "context": ..., "spend_clp": ...}] -> spend-ranked
    head-noun groups. "context" (the parent tender's title) is optional.

    The item text is authoritative for the head noun; the tender title is a
    fallback used only when the item itself yields no family noun (a terse line
    like "ITEM 5 / segun anexo" inherits its tender's family). Item-first, not
    a merge: one tender title fans out over many different-product lines, so
    blending it in would mis-group lines that already name their own family.

    Families default to the corpus-derived candidate list, mined from item AND
    tender texts so a family word that appears only in tenders can still anchor
    the fallback (synonym targets are always included so merged groups keep
    their canonical name).
    """
    normalizer = normalizer or Normalizer()
    normalized = [(normalizer(r["text"]),
                   normalizer(r["context"]) if r.get("context") else "",
                   float(r.get("spend_clp") or 0)) for r in rows]

    if families is None:
        corpus = [t for t, _, _ in normalized] + [c for _, c, _ in normalized if c]
        families = candidate_family_nouns(corpus, top_families, min_count, synonyms)
    family_set = set(families) | set(synonyms.values())

    records: Counter[str] = Counter()
    spend: defaultdict[str, float] = defaultdict(float)
    texts: defaultdict[str, set] = defaultdict(set)
    for text, context, clp in normalized:
        group = head_noun(text, family_set, synonyms)
        if group == RESIDUE and context:
            group = head_noun(context, family_set, synonyms)
        records[group] += 1
        spend[group] += clp
        texts[group].add(text)

    total_spend = sum(spend.values()) or 1.0
    stats = [
        GroupStat(group=g, records=records[g], distinct_texts=len(texts[g]),
                  spend_clp=spend[g], spend_share=spend[g] / total_spend)
        for g in records
    ]
    stats.sort(key=lambda s: s.spend_clp, reverse=True)
    cum = 0.0
    for s in stats:
        cum += s.spend_share
        s.cum_share = cum
    return stats


def fetch_item_spend(conn, unspsc_segment: int | None = None,
                     limit: int | None = None, progress=None,
                     progress_every: int = 20000) -> list[dict]:
    """Tender items with awarded-offer spend in CLP (the spend signal for the
    ranking; items without awards profile at zero, still counted).

    unspsc_segment scopes the profile (e.g. 42 = medical equipment and
    supplies) — UNSPSC used only as a coarse profiling scope, never as the
    grouping key (design §3 step 2). Grouping is per item (id keys), so
    repeated descriptions count as separate records.

    Streams the corpus row by row so a whole-marketplace profile can report
    progress instead of blocking on one query. Each item's awarded spend is a
    per-row pattern comprehension (not a global GROUP BY) — the latter is an
    eager operator that finishes the full scan before emitting any row, which
    would defeat streaming. `progress(n)` is called every `progress_every`
    rows.
    """
    from .ingest.neo4j_source import NOT_RUBRIC, TENDER_NAME, segment_bounds

    seg_low, seg_high = segment_bounds(unspsc_segment)
    limit_clause = "LIMIT $limit" if limit is not None else ""
    # OPTIONAL so items whose tender link is missing still profile (item-only,
    # as before) instead of being silently dropped from the ranking.
    cypher = f"""
        MATCH (i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND {NOT_RUBRIC}
          AND ($seg_low IS NULL OR
               (i.codigo_unspsc_producto >= $seg_low AND i.codigo_unspsc_producto < $seg_high))
        OPTIONAL MATCH (l:Licitacion)-[:TIENE_ITEM]->(i)
        RETURN i.descripcion_comprador AS text,
               coalesce({TENDER_NAME}, '') AS context,
               reduce(t = 0.0,
                      p IN [(i)<-[:PARA_ITEM]-(o:Oferta) WHERE o.es_adjudicada = true
                            -- toFloat() so a dirty non-numeric precio_total_clp
                            -- nulls out to 0.0 instead of crashing the sum (same
                            -- hardening as fallback.fetch_fallback_items)
                            | coalesce(toFloat(o.precio_total_clp), 0.0)]
                      | t + p) AS spend_clp
        {limit_clause}
    """
    params = {"seg_low": seg_low, "seg_high": seg_high, "limit": limit}
    rows: list[dict] = []
    for rec in conn.stream(cypher, parameters=params):
        rows.append({"text": rec["text"], "context": rec["context"],
                     "spend_clp": rec["spend_clp"]})
        if progress is not None and len(rows) % progress_every == 0:
            progress(len(rows))
    if progress is not None and rows:
        progress(len(rows))
    return rows


def write_ranking(stats: list[GroupStat], path: Path | str) -> None:
    """Persist the spend ranking — the cached file the `register` command reuses."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(RANKING_HEADER)
        for s in stats:
            w.writerow([s.group, s.records, s.distinct_texts, f"{s.spend_clp:.0f}",
                        f"{s.spend_share:.6f}", f"{s.cum_share:.6f}"])


def load_ranking(path: Path | str) -> list[GroupStat]:
    """Read a ranking written by `write_ranking` back into GroupStats, so the
    `register` command reuses the cached profile instead of re-streaming."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        return [
            GroupStat(group=r["group"], records=int(r["records"]),
                      distinct_texts=int(r["distinct_texts"]),
                      spend_clp=float(r["spend_clp"]),
                      spend_share=float(r["spend_share"]),
                      cum_share=float(r["cum_share"]))
            for r in csv.DictReader(f)
        ]
