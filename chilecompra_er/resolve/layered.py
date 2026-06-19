"""Layered classifier (improvement loop, step 3): Tier-1 -> brand -> Tier-2.

Composes the tiers behind the one .classify(normalized) -> Classification
interface the resolver already uses, so it drops in via Resolver(classifier=...).

Order and rules:
  1. Tier-1 regex (deterministic, precise). A CLASSIFIED or AMBIGUOUS verdict
     stops here — ambiguity is a register-overlap problem to fix at the source,
     not something lower tiers should silently paper over.
  2. Brand lexicon — catches brand-only lines with no head noun.
  3. Tier-2 statistical — catches wording the regex misses, above its threshold.
Lower tiers only ever turn UNCLASSIFIED into CLASSIFIED; they never override an
upstream verdict. If all abstain, the item stays UNCLASSIFIED (-> UNSPSC fallback).
"""

from __future__ import annotations

from .classifier import CLASSIFIED, UNCLASSIFIED, Classification


class LayeredClassifier:
    def __init__(self, tier1, brand=None, tier2=None):
        self.tier1 = tier1
        self.brand = brand
        self.tier2 = tier2
        self.register_version = tier1.register_version

    def prime(self, normalized_texts) -> None:
        """Batch-prime the statistical tier (Tier-2) for a chunk of texts so its
        per-item classify() calls become cache lookups (~150x cheaper than one
        predict_proba per item). No-op when there's no Tier-2 tier."""
        if self.tier2 is not None and hasattr(self.tier2, "prime"):
            self.tier2.prime(normalized_texts)

    def classify(self, normalized_text: str) -> Classification:
        primary = self.tier1.classify(normalized_text)
        if primary.status != UNCLASSIFIED:
            return primary  # CLASSIFIED or AMBIGUOUS both short-circuit
        for tier in (self.brand, self.tier2):
            if tier is None:
                continue
            verdict = tier.classify(normalized_text)
            if verdict.status == CLASSIFIED:
                return verdict
        return Classification(None, UNCLASSIFIED, tier="layered")
