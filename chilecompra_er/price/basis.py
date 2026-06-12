"""Price-basis normalization (design note §6).

Every price observation carries an explicit basis — per_base_unit, per_pack,
or unknown — with provenance. Flag-don't-guess: absence of pack evidence is
never evidence of unit pricing; an unresolvable basis is excluded and flagged,
never assumed per-unit (a missing point is recoverable, a 100x point is
poison). The published UoM field is a feature, never the decision.

Patterns run over NORMALIZED text (lowercase, accents stripped, digit/letter
boundaries spaced), so "CAJA X100" arrives as "caja x 100".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

BASIS_PER_BASE_UNIT = "per_base_unit"
BASIS_PER_PACK = "per_pack"
BASIS_UNKNOWN = "unknown"

_PACK_PATTERNS = [
    re.compile(r"\b(?:caja|bolsa|pack|cj|display|estuche|sobre)\s*(?:de|x)?\s*(\d{1,5})\b"),
    re.compile(r"\bx\s*(\d{1,5})\s*(?:un|u|uds?|unid\w*)\b"),
    re.compile(r"\b(\d{1,5})\s*(?:un|uds?|unid\w*)\s*(?:por|x)\s*(?:caja|bolsa|pack|envase)\b"),
]


@dataclass
class PriceBasis:
    basis: str
    pack_size: int | None = None
    evidence: list[dict] = field(default_factory=list)


def infer_basis(normalized_text: str) -> PriceBasis:
    """Pack evidence in the text -> per_pack with size; otherwise unknown."""
    for pattern in _PACK_PATTERNS:
        m = pattern.search(normalized_text)
        if m:
            return PriceBasis(
                basis=BASIS_PER_PACK,
                pack_size=int(m.group(1)),
                evidence=[{"pattern": pattern.pattern, "matched": m.group(0)}],
            )
    return PriceBasis(basis=BASIS_UNKNOWN)


def cross_check(total: float, quantity: float, unit_price: float,
                pack_size: int | None = None, tolerance: float = 0.015) -> str | None:
    """Where total, quantity and unit price coexist, test the basis hypotheses
    against total ~= quantity x price (design §6). Returns a promoted basis or
    None — promotion only on positive arithmetic evidence.

    With a known pack size:
      total ~= qty x price             -> price is per the sell unit = per_pack
      total ~= qty x pack_size x price -> price is per base unit
    Without pack evidence, arithmetic consistency cannot distinguish "per
    unit" from "per (unknown) pack", so nothing is promoted.
    """
    if not total or not quantity or not unit_price:
        return None

    def close(a: float, b: float) -> bool:
        return abs(a - b) <= tolerance * max(abs(a), abs(b))

    if pack_size:
        if close(total, quantity * pack_size * unit_price):
            return BASIS_PER_BASE_UNIT
        if close(total, quantity * unit_price):
            return BASIS_PER_PACK
    return None
