"""Brand / trade-name lexicon (improvement loop, step 3a).

Many fallback items are brand names with no head noun — "relyx", "cavit",
"vitrebond", "alveogyl" — that no head-noun regex (Tier-1) and no classifier
trained on head-noun labels (Tier-2) can ever match, because the family word
simply isn't in the text. This maps a brand token to the category it belongs
to, consulted as its own tier: a brand hit classifies the line.

The map lives at categories/brand_lexicon.json:

    {"_format": "<brand token (normalized)> -> <category_id>; curator-maintained",
     "brands": {"relyx": "cementos_dentales", "cavit": "materiales_temporales_dentales"}}

Brand tokens are matched against the NORMALIZED text (lowercase, unaccented),
so keys must be normalized too. Only entries whose category_id exists in the
register are honoured — a stale brand is dropped, never a crash. Two brands
pointing at different categories in one line -> ambiguous (never guess).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ..categories.schema import CATEGORIES_DIR, load_register
from .classifier import AMBIGUOUS, CLASSIFIED, UNCLASSIFIED, Classification

BRAND_LEXICON_PATH = CATEGORIES_DIR / "brand_lexicon.json"
_WORD = re.compile(r"[a-z0-9]+")


def load_brand_map(path: Path = BRAND_LEXICON_PATH) -> dict[str, str]:
    """Read the {brand: category_id} map from disk ({} if absent/empty)."""
    if not Path(path).exists():
        return {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return dict(data.get("brands", {}))


class BrandLexicon:
    """Tier interface twin of Tier1Classifier: .classify(normalized) -> Classification."""

    def __init__(self, mapping: dict[str, str], register: dict | None = None):
        register = register or load_register()
        self.register_version = register["register_version"]
        valid = {c["category_id"] for c in register["categories"]}
        # Keep only brands whose target category actually exists.
        self.brands = {b.lower(): c for b, c in mapping.items() if c in valid}

    @classmethod
    def load(cls, register: dict | None = None,
             path: Path = BRAND_LEXICON_PATH) -> "BrandLexicon":
        return cls(load_brand_map(path), register=register)

    def classify(self, normalized_text: str) -> Classification:
        if not self.brands:
            return Classification(None, UNCLASSIFIED, tier="brand")
        hits: dict[str, str] = {}  # category_id -> brand token that matched
        for tok in _WORD.findall(normalized_text):
            cat = self.brands.get(tok)
            if cat is not None:
                hits.setdefault(cat, tok)
        if len(hits) == 1:
            cat, tok = next(iter(hits.items()))
            return Classification(cat, CLASSIFIED, matched=(tok,), tier="brand")
        if not hits:
            return Classification(None, UNCLASSIFIED, tier="brand")
        return Classification(None, AMBIGUOUS, tuple(sorted(hits)), tier="brand")
