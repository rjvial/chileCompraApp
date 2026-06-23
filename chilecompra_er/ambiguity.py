"""Ambiguity report — the register-overlap backlog (coverage loop, hygiene).

Counterpart to fallback.py. Where fallback.py ranks the UNCOVERED families (items
no category matched), this ranks the OVERLAPPING ones. An item lands on fallback
as *ambiguous* when more than one Tier-1 category claims it — the register's
category boundaries overlap on that text. Today that cost is invisible: ambiguous
items drop silently to the UNSPSC bucket. This re-classifies the fallback residue,
keeps the ambiguous items, and ranks the colliding category SETS so overlaps are a
visible, trackable metric.

Each overlap splits into two populations, because the fix differs:
  * spurious overlap — ONE product that two category regexes both match
    ("aguja de sutura" -> agujas + suturas). Fixable: add an exclude so the right
    family wins. This is the actionable backlog.
  * genuine bundle — a line that really lists SEVERAL products ("mascarillas,
    canulas, gasas"). Ambiguity is the correct answer; forcing a pick would
    miscategorize. Flagged by list separators and left alone.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from .normalize import Normalizer
from .resolve.classifier import AMBIGUOUS, Tier1Classifier
from .resolve.resolver import is_rubric

# Separators that join several product phrases. A line carrying a few of these is
# enumerating products (a bundle), not describing one.
_SEP = re.compile(r"[,;/+]| y | e ")


def looks_like_bundle(normalized: str) -> bool:
    """True for a line that enumerates several products (>= 2 list separators) —
    a multi-product bundle, where ambiguity is the right answer — as opposed to a
    single product that two category regexes happen to both claim."""
    return len(_SEP.findall(normalized)) >= 2


@dataclass
class OverlapStat:
    pair: tuple[str, ...]     # the colliding category ids, sorted
    items: int                # ambiguous items this set claims
    bundle_items: int         # of those, lines that look like multi-product bundles
    samples: list[str] = field(default_factory=list)  # spurious-overlap examples

    @property
    def spurious_items(self) -> int:
        """Single-product collisions — the fixable kind (add an exclude)."""
        return self.items - self.bundle_items


def ambiguity_ranking(rows: list[dict], classifier=None, normalizer=None,
                      min_count: int = 3, max_samples: int = 4) -> list[OverlapStat]:
    """Re-classify residue rows ({'text': ...}) and rank colliding category SETS by
    spurious (fixable) ambiguous-item count. Rubric-only lines are skipped — their
    head noun is the taxonomy's, so fallback is the right answer, not a new exclude."""
    classifier = classifier or Tier1Classifier()
    normalizer = normalizer or Normalizer()
    groups: dict[tuple, dict] = defaultdict(
        lambda: {"items": 0, "bundle": 0, "samples": []})
    for r in rows:
        text = r.get("text") or ""
        if is_rubric(text):
            continue
        norm = normalizer(text)
        cls = classifier.classify(norm)
        if cls.status != AMBIGUOUS:
            continue
        pair = tuple(sorted(cls.matched))
        g = groups[pair]
        g["items"] += 1
        if looks_like_bundle(norm):
            g["bundle"] += 1
        elif len(g["samples"]) < max_samples:  # sample the spurious (fixable) lines
            g["samples"].append(text[:100])
    stats = [OverlapStat(pair=p, items=g["items"], bundle_items=g["bundle"],
                         samples=g["samples"])
             for p, g in groups.items() if g["items"] >= min_count]
    stats.sort(key=lambda s: (s.spurious_items, s.items), reverse=True)
    return stats
