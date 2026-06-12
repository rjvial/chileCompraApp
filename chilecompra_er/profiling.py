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

import re
from collections import Counter, defaultdict
from dataclasses import dataclass

from .normalize import Normalizer

RESIDUE = "_residue"

# Curated synonym map (design §3 step 2) — grows during M0 curation.
DEFAULT_SYNONYMS = {
    "cateter": "sonda",
    "aposito": "gasa",
    "vendaje": "venda",
}

# Tokens that can never be a family noun: qualifiers, units, generic filler.
_NOT_FAMILY = frozenset({
    "de", "del", "la", "el", "los", "las", "un", "una", "y", "o", "a", "al",
    "en", "con", "sin", "para", "por", "segun", "tipo", "uso", "marca",
    "desechable", "esteril", "adulto", "pediatrico", "neonatal", "adquisicion",
    "compra", "servicio", "suministro", "insumo", "insumos", "varios", "otros",
    "ml", "cc", "mm", "cm", "fr", "ch", "gr", "kg", "mg", "lt", "un", "uds",
    "unidad", "unidades", "caja", "bolsa", "pack", "set", "kit", "rollo",
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


def candidate_family_nouns(normalized_texts: list[str], top_n: int = 150,
                           min_count: int = 5,
                           synonyms: dict[str, str] = DEFAULT_SYNONYMS) -> list[str]:
    """Rank corpus tokens by description frequency; the top of the ranking is
    the candidate family-noun list (hand-confirmed during M0 curation).
    Tokens below min_count descriptions never qualify — singletons are
    residue, not families. Synonyms count toward their canonical form."""
    counts: Counter[str] = Counter()
    for text in normalized_texts:
        seen = set()
        for tok in _TOKEN.findall(text):
            tok = synonyms.get(tok, tok)
            if tok not in _NOT_FAMILY and tok not in seen:
                counts[tok] += 1
                seen.add(tok)
    return [tok for tok, c in counts.most_common(top_n) if c >= min_count]


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
            top_families: int = 150, min_count: int = 5) -> list[GroupStat]:
    """rows: [{"text": ..., "spend_clp": ...}] -> spend-ranked head-noun groups.

    Families default to the corpus-derived candidate list (synonym targets are
    always included so merged groups keep their canonical name).
    """
    normalizer = normalizer or Normalizer()
    normalized = [(normalizer(r["text"]), float(r.get("spend_clp") or 0)) for r in rows]

    if families is None:
        families = candidate_family_nouns([t for t, _ in normalized], top_families,
                                          min_count, synonyms)
    family_set = set(families) | set(synonyms.values())

    records: Counter[str] = Counter()
    spend: defaultdict[str, float] = defaultdict(float)
    texts: defaultdict[str, set] = defaultdict(set)
    for text, clp in normalized:
        group = head_noun(text, family_set, synonyms)
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


def fetch_item_spend(conn, unspsc_segment: int | None = None) -> list[dict]:
    """Tender items with awarded-offer spend in CLP (the spend signal for the
    ranking; items without awards profile at zero, still counted).

    unspsc_segment scopes the profile (e.g. 42 = medical equipment and
    supplies) — UNSPSC used only as a coarse profiling scope, never as the
    grouping key (design §3 step 2). Grouping is per item (id keys), so
    repeated descriptions count as separate records.
    """
    records = conn.query(
        """
        MATCH (i:ItemLicitacion)
        WHERE i.descripcion_comprador IS NOT NULL
          AND ($seg IS NULL OR
               toString(i.codigo_unspsc_producto) STARTS WITH toString($seg))
        OPTIONAL MATCH (o:Oferta {es_adjudicada: true})-[:PARA_ITEM]->(i)
        RETURN i.id_licitacion AS tender_id, i.id_item AS item_id,
               i.descripcion_comprador AS text,
               sum(coalesce(o.precio_total_clp, 0)) AS spend_clp
        """,
        parameters={"seg": unspsc_segment},
    )
    return [{"text": r["text"], "spend_clp": r["spend_clp"]} for r in records]
