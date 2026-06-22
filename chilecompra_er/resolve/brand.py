"""Brand extraction for the branded-Product model.

A `:Product` is the pairing `Brand × GenericProduct`. To build it we need the
*brand* a given offer names. Two signals, in priority order:

  1. The supplier's **explicit label** — offers routinely carry "MARCA: HEALFLEX",
     "Marca 3M", "marca medline". A regex over the raw text captures the token
     right after "marca". This is the dominant, most reliable signal.
  2. A **known brand token** from the curated brand lexicon (the same map
     `BrandLexicon` consults for classification, reused here only to *recognise* a
     brand token anywhere in the text — e.g. "relyx", "cavit").

Neither -> the `(sin marca)` sentinel, so every offer still pairs with a Brand and
`Product = Brand × GenericProduct` holds universally.

Unlike classification, brand extraction runs on the **raw** offer text, not the
digit-split normalized form: normalization turns "3M" into "3 m", which would
shred exactly the alphanumeric brand tokens we care about.
"""

from __future__ import annotations

import re
import unicodedata

# Token right after an explicit "marca" label. Case-insensitive over RAW text so
# "3M", "B-Braun", "Med-Plus" survive intact.
_MARCA = re.compile(r"\bmarca\b\s*[:\-]?\s*([A-Za-z0-9][\w\-]*)", re.IGNORECASE)
_TOKEN = re.compile(r"[A-Za-z0-9]+")

# Words that follow "marca" but name no brand — "marca registrada", "marca propia".
_STOP = {
    "registrada", "registrado", "propia", "propio", "comercial", "generica",
    "generico", "unica", "unico", "sin", "no", "del", "de", "la", "el", "y",
    "modelo", "ref", "referencia",
}

SIN_MARCA = "(sin marca)"


def _norm_token(s: str) -> str:
    """Lowercase + strip accents — the Brand node key form (matches lexicon keys)."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.lower()


def extract_brand(raw_text: str | None,
                  brand_map: dict[str, str] | None = None) -> tuple[str, str, str]:
    """(brand_id, brand_name, source) for an offer.

    `brand_id` is the normalized token used as the `:Brand` node key; `brand_name`
    keeps the original casing for display. `source` is one of "marca" / "lexicon" /
    "none". `brand_map` is the {token: category_id} brand lexicon map (optional).
    """
    raw = raw_text or ""

    m = _MARCA.search(raw)
    if m:
        cap = m.group(1)
        bid = _norm_token(cap)
        # a real brand has >=2 chars, at least one letter, and isn't a filler word
        if len(bid) >= 2 and any(c.isalpha() for c in bid) and bid not in _STOP:
            return bid, cap, "marca"

    if brand_map:
        for cap in _TOKEN.findall(raw):
            bid = _norm_token(cap)
            if bid in brand_map:
                return bid, cap, "lexicon"

    return SIN_MARCA, SIN_MARCA, "none"
