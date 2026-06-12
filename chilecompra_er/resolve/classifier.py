"""Tier-1 category classification (design note §8).

Deterministic include/exclude regexes from the category register — fast,
auditable, and the label source for the future Tier-2 statistical classifier.
"None of the above" is a feature: anything not matching exactly one category
stays unclassified (visible debt, reported as unresolved spend), never
silently miscategorized. Tier 2 (TF-IDF + linear) and Tier 3 (LLM residue)
plug in behind the same Classification result type.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..categories.schema import load_register

CLASSIFIED = "classified"
UNCLASSIFIED = "unclassified"
AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class Classification:
    category_id: str | None
    status: str  # classified | unclassified | ambiguous
    matched: tuple[str, ...] = field(default_factory=tuple)  # rule provenance
    tier: str = "tier1"


class Tier1Classifier:
    def __init__(self, register: dict | None = None):
        register = register or load_register()
        self.register_version = register["register_version"]
        self._cats: list[tuple[str, list[re.Pattern], list[re.Pattern]]] = []
        for cat in register["categories"]:
            if cat["status"] == "out_of_scope":
                continue
            self._cats.append((
                cat["category_id"],
                [re.compile(p) for p in cat.get("include", [])],
                [re.compile(p) for p in cat.get("exclude", [])],
            ))

    def classify(self, normalized_text: str) -> Classification:
        hits: list[tuple[str, tuple[str, ...]]] = []
        for category_id, include, exclude in self._cats:
            matched = tuple(p.pattern for p in include if p.search(normalized_text))
            if matched and not any(p.search(normalized_text) for p in exclude):
                hits.append((category_id, matched))
        if len(hits) == 1:
            return Classification(hits[0][0], CLASSIFIED, hits[0][1])
        if not hits:
            return Classification(None, UNCLASSIFIED)
        # Two categories claiming one item means the register's boundaries
        # overlap — route to review, never pick one silently.
        return Classification(None, AMBIGUOUS, tuple(h[0] for h in hits))
