"""Fallback-residue analysis — the category-coverage backlog (improvement loop, step 1).

A `--kind item` run links every item, but items that no curated family matched
land on a coarse UNSPSC fallback node (`category_id = "unspsc_<code>"`, see
resolve.resolver). This module reads those nodes back from the graph and turns
them into a prioritized backlog:

  * `bucket_ranking`  — which UNSPSC commodity codes carry the most fallback
    *items* (the metric we want to cut), with their awarded spend, the rubric
    -only share, and the top head-noun families inside each bucket;
  * `residue_ranking` — the head-noun families that recur across the WHOLE
    residue, spend-ranked. This is the actionable part: each is a category worth
    registering, and the ranking feeds `register --from-fallback` (step 2).

Everything is derived from the real persisted resolution, so it doubles as the
baseline metric for "how much fallback is left".
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from .normalize import Normalizer
from .profiling import (
    DEFAULT_SYNONYMS,
    RESIDUE,
    GroupStat,
    candidate_family_nouns,
    head_noun,
    profile,
)
# GenericProduct.category_id prefix for UNSPSC fallback buckets (unspsc_<code>).
UNSPSC_PREFIX = "unspsc_"


def is_rubric(text: str | None) -> bool:
    """True for a UNSPSC rubric path ("Equipamiento ... / ... / Vendas..."), a
    taxonomy string rather than a real product description — flagged by >= 2
    ' / ' separators. Single source of this rule on the Python side (mirrored in
    Cypher by ingest.neo4j_source.NOT_RUBRIC)."""
    return bool(text) and text.count(" / ") >= 2


# GenericProduct.category_id prefix the fallback resolution uses for UNSPSC nodes.
FALLBACK_PREFIX = UNSPSC_PREFIX


@dataclass
class BucketStat:
    code: str                 # e.g. "unspsc_42151601"
    items: int                # items that fell back into this commodity code
    spend_clp: float          # awarded-offer CLP under the bucket
    spend_share: float
    rubric_items: int         # of those items, how many are rubric-only buyer lines
    top_families: list[tuple[str, int]] = field(default_factory=list)  # head-noun -> count


_is_rubric = is_rubric  # legacy alias kept for existing callers/tests


def fetch_fallback_items(conn) -> list[dict]:
    """One row per item that fell back, as profiling rows
    [{"text", "spend_clp", "code"}].

    Items resolve (RESOLVED_TO) to the fallback GenericProduct; their offers bind
    as branded Products (VARIANT_OF the same node) with the price on the
    (:Oferta)-[:OFFERS]->(:Product) edge, so the bucket's awarded spend is the sum
    over those edges. Per-item spend is the bucket spend attributed uniformly across
    its items — enough signal to rank families without an item<->offer join.
    """
    items = conn.query(
        "MATCH (i:ItemLicitacion)-[:RESOLVED_TO]->(g:GenericProduct) "
        "WHERE g.category_id STARTS WITH $p "
        "RETURN g.category_id AS code, i.descripcion_comprador AS text",
        parameters={"p": FALLBACK_PREFIX})
    spend = conn.query(
        "MATCH (:Oferta)-[r:OFFERS]->(p:Product)-[:VARIANT_OF]->(g:GenericProduct) "
        "WHERE g.category_id STARTS WITH $p AND r.awarded = true "
        # toFloat() coerces numeric strings and nulls out non-numeric junk, so
        # SUM never trips over a dirty source price (precio_total_clp can arrive
        # as a string); IS NOT NULL alone let those through and crashed the query.
        "RETURN g.category_id AS code, sum(toFloat(r.total_clp)) AS spend",
        parameters={"p": FALLBACK_PREFIX})
    bucket_spend = {r["code"]: float(r["spend"] or 0) for r in spend}
    counts: Counter[str] = Counter(r["code"] for r in items)
    rows = []
    for r in items:
        code = r["code"]
        per = bucket_spend.get(code, 0.0) / counts[code] if counts[code] else 0.0
        rows.append({"text": r["text"] or "", "spend_clp": per, "code": code})
    return rows


def residue_ranking(rows: list[dict], normalizer: Normalizer | None = None,
                    min_count: int = 5) -> list[GroupStat]:
    """Head-noun families across the fallback residue, ranked by ITEM COUNT — the
    candidate categories to register. Thin wrapper over the M0 profiler so the
    output is the same GroupStat ranking `register`/propose already consume.

    Ranked by count, NOT spend: `fetch_fallback_items` can only attribute a
    bucket's awarded spend uniformly across its items (no item<->offer join), so
    residue "spend" is a smeared average that floats tiny families sharing a
    high-value bucket to the top (it surfaced services/pharma). Item count is the
    honest priority signal here. We overload each GroupStat's `spend_share` with
    its RECORD share and sort by records, so propose's descending-order walk and
    `min_spend_share` floor act on counts unchanged (the floor becomes a
    min-record-share floor).

    Rubric-only buyer lines are dropped first: their head noun is always the
    leading rubric word ("equipamiento"), which would otherwise swamp the
    ranking with a non-family. They are genuinely uninformative — fallback is
    the right answer for them, not a new category."""
    informative = [r for r in rows if not _is_rubric(r["text"])]
    stats = profile(informative, normalizer=normalizer, min_count=min_count)
    total_records = sum(s.records for s in stats) or 1
    for s in stats:
        s.spend_share = s.records / total_records
    stats.sort(key=lambda s: s.records, reverse=True)
    cum = 0.0
    for s in stats:
        cum += s.spend_share
        s.cum_share = cum
    return stats


def bucket_ranking(rows: list[dict], normalizer: Normalizer | None = None,
                   min_count: int = 5, top_families: int = 4) -> list[BucketStat]:
    """Per-UNSPSC-code fallback stats, ranked by item volume (the metric to cut).
    `rows` are fetch_fallback_items() rows (must carry "code")."""
    normalizer = normalizer or Normalizer()
    prepared = [(r["code"], normalizer(r["text"]), float(r.get("spend_clp") or 0),
                 r["text"]) for r in rows]
    families = (set(candidate_family_nouns(
                    [t for _, t, _, raw in prepared if not _is_rubric(raw)],
                    min_count=min_count))
                | set(DEFAULT_SYNONYMS.values()))

    items: Counter[str] = Counter()
    spend: defaultdict[str, float] = defaultdict(float)
    rubric: Counter[str] = Counter()
    fams: defaultdict[str, Counter] = defaultdict(Counter)
    for code, ntext, clp, raw in prepared:
        items[code] += 1
        spend[code] += clp
        if _is_rubric(raw):
            rubric[code] += 1
            continue  # rubric lines have no product family — don't pollute top_families
        fams[code][head_noun(ntext, families)] += 1

    total = sum(spend.values()) or 1.0
    stats = [
        BucketStat(
            code=code, items=items[code], spend_clp=spend[code],
            spend_share=spend[code] / total, rubric_items=rubric[code],
            top_families=[(f, n) for f, n in fams[code].most_common()
                          if f != RESIDUE][:top_families])
        for code in items
    ]
    stats.sort(key=lambda b: b.items, reverse=True)
    return stats
