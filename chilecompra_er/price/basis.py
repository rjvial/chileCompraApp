"""Price-basis normalization (design note §6).

Every price observation carries an explicit basis — per_base_unit, per_pack,
or unknown — with provenance. Flag-don't-guess: absence of pack evidence is
never evidence of unit pricing; an unresolvable basis is excluded and flagged,
never assumed per-unit (a missing point is recoverable, a 100x point is
poison). The published UoM field is a feature, never the decision.

Patterns run over NORMALIZED text (lowercase, accents stripped, digit/letter
boundaries spaced), so "CAJA X100" arrives as "caja x 100".

In Mercado Público `total = quantity x unit_price` is an accounting IDENTITY, so
the arithmetic cross_check (below) carries no signal on real data — a supplier's
`unit_price` may be per base unit OR per pack and the totals look identical. The
working signal is price MAGNITUDE: per-unit prices for one GenericProduct cluster,
and a per-pack quote sits ~pack_size above the cluster. `normalize_unit_prices`
disambiguates per product on POSITIVE evidence — it divides a price by its stated
pack size only when that lands it in the peer cluster — and flags the rest.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field

BASIS_PER_BASE_UNIT = "per_base_unit"
BASIS_PER_PACK = "per_pack"
BASIS_UNKNOWN = "unknown"

_PACK_PATTERNS = [
    re.compile(r"\b(?:caja|bolsa|pack|cj|display|estuche|set|kit|envase|blister)"
               r"\s*(?:de|x|por|con)?\s*(\d{1,5})\b"),
    re.compile(r"\bx\s*(\d{1,5})\s*(?:un|u|uds?|unid\w*|rollos?|jeringas?|piezas?|pares?)\b"),
    re.compile(r"\b(\d{1,5})\s*(?:un|uds?|unid\w*|rollos?|jeringas?|piezas?|pares?)"
               r"\s*(?:por|x|/)\s*(?:caja|bolsa|pack|envase|set|kit)\b"),
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


def pack_size_from_text(text: str | None) -> int | None:
    """The pack/sell-unit count stated in an offer's NORMALIZED text ("caja x 12"
    -> 12, "set de 2 jeringas" -> 2), or None if none is stated. Permissive on
    purpose: a spurious size is harmless because normalize_unit_prices only ever
    divides by it when the result lands in the product's price cluster."""
    if not text:
        return None
    for pattern in _PACK_PATTERNS:
        m = pattern.search(text.lower())
        if m:
            n = int(m.group(1))
            if n >= 2:        # a "pack of 1" is not a pack
                return n
    return None


def _classify(prices: list, pack_sizes: list, center: float, band: float):
    lo, hi = center / band, center * band
    out = []
    for price, pack in zip(prices, pack_sizes):
        if not price or price <= 0:
            out.append((None, BASIS_UNKNOWN))
        elif lo <= price <= hi:                       # already in the cluster
            out.append((float(price), BASIS_PER_BASE_UNIT))
        elif pack and pack > 1 and lo <= price / pack <= hi:  # /pack lands in it
            out.append((price / pack, BASIS_PER_PACK))
        else:                                         # fits neither -> flag
            out.append((None, BASIS_UNKNOWN))
    return out


def normalize_unit_prices(prices: list, pack_sizes: list, *,
                          min_n: int = 5, band: float = 3.0) -> list[tuple]:
    """Distribution-aware per-unit normalization for ONE GenericProduct's awarded
    offers. Returns [(normalized_price | None, basis)] aligned with the inputs.

    The per-unit prices form a cluster around the robust center (median); a per-pack
    quote sits ~pack_size above it. An offer is normalized to per base unit only on
    positive evidence — its raw price already sits in the cluster (per_base_unit),
    or dividing by its stated pack size lands it there (per_pack). Offers that fit
    neither, and products too sparse (< min_n offers) to define a cluster, are
    'unknown' (None) — flagged, never guessed (design §6). `band` is the
    multiplicative half-width of the cluster (center/band .. center*band).
    """
    n = len(prices)
    valid = [float(p) for p in prices if p and p > 0]
    if len(valid) < min_n:
        return [(None, BASIS_UNKNOWN) for _ in range(n)]
    center = statistics.median(valid)
    if center <= 0:
        return [(None, BASIS_UNKNOWN) for _ in range(n)]
    res = _classify(prices, pack_sizes, center, band)
    # Refine the center on the prices that looked per-unit, so a large minority of
    # per-pack outliers can't drag it — then re-classify against the tighter cluster.
    base = [p for p, b in res if b == BASIS_PER_BASE_UNIT]
    if len(base) >= min_n:
        refined = statistics.median(base)
        if refined > 0:
            res = _classify(prices, pack_sizes, refined, band)
    return res
