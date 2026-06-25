"""L1 canonical profile — the structured representation Claude produces for ONE
product description (design: the L0→L3 redesign, "L1 canonicalize").

A profile decides whether two bids are THE SAME SUBSTITUTABLE PRODUCT, brand- and
packaging-independent. Its cardinal rule: every identity attribute must quote the
substring that ANCHORS it (`evidence`) — a bare number with no concept word can
never become identity. That single rule makes the pr_fd4522a53b7e false-merge
(cable + calcium read as "dextrose 2.5%") structurally impossible, and gives
auditability for free.

This module is pure: the dataclass, the JSON schema (for structured outputs), the
system prompt (the cached prefix), and the helpers to build a request and parse a
result. The batch driver lives in resolve/canonicalize.py; the matcher in
resolve/matcher.py.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

# --- the profile dataclass ----------------------------------------------------

@dataclass(frozen=True)
class IdentityAttr:
    name: str            # canonical snake_case Spanish: calibre, material, volumen…
    value: str           # canonical normalized value WITH unit: 16fr, 5000ml, 2.5pct
    evidence: str        # exact substring of the source that NAMES this attribute


@dataclass(frozen=True)
class Packaging:
    pack_size: int | None = None
    pack_unit: str | None = None     # caja, pack, blister, bolsa…
    evidence: str | None = None


@dataclass(frozen=True)
class Profile:
    is_product: bool
    category: str                    # normalized family / KNOWN_FAMILIES id (blocking key)
    identity_attributes: tuple[IdentityAttr, ...] = ()
    brand: str | None = None         # NOT identity — recorded for slicing
    model_token: str | None = None   # manufacturer model/SKU; a shared one is a strong match
    packaging: Packaging = field(default_factory=Packaging)
    base_unit: str | None = None     # unit ONE item is measured in, for price normalization
    confidence: str = "low"          # low | medium | high
    flags: tuple[str, ...] = ()

    def signature(self) -> str:
        """The canonical identity signature used by the L2 matcher: category plus
        the sorted identity name=value pairs. Brand and packaging are excluded by
        construction. Empty-attribute profiles signature to the bare category."""
        attrs = "|".join(f"{a.name}={a.value}"
                         for a in sorted(self.identity_attributes,
                                         key=lambda a: (a.name, a.value)))
        return f"{self.category}|{attrs}" if attrs else self.category


# --- structured-outputs JSON schema (the L1 output contract) ------------------

FLAGS = ["ambiguous_category", "below_min_info", "multi_product",
         "non_medical", "conflicting_attributes"]

PROFILE_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["is_product", "category", "identity_attributes", "brand",
                 "model_token", "packaging", "base_unit", "confidence", "flags"],
    "properties": {
        "is_product": {"type": "boolean"},
        "category": {"type": "string"},
        "identity_attributes": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "value", "evidence"],
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "string"},
                    "evidence": {"type": "string"},
                },
            },
        },
        "brand": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "model_token": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "packaging": {
            "type": "object",
            "additionalProperties": False,
            "required": ["pack_size", "pack_unit", "evidence"],
            "properties": {
                "pack_size": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                "pack_unit": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "evidence": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            },
        },
        "base_unit": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "flags": {"type": "array", "items": {"type": "string", "enum": FLAGS}},
    },
}


# --- the system prompt (the cached prefix) ------------------------------------

_SYSTEM_TEMPLATE = """\
You canonicalize ONE product description from Chile's public-procurement
marketplace (ChileCompra, mostly medical/dental/lab supplies, Chilean Spanish)
into a structured profile. The profile decides whether two bids are THE SAME
SUBSTITUTABLE PRODUCT — independent of brand and packaging.

WHAT GOES WHERE
- identity_attributes = the functional spec: what the item IS and does
  (size, material, concentration, volume-per-unit, type). This decides sameness.
- brand = manufacturer/trade name. NOT identity — different brands of the same
  spec are the same product. Record it; never let it drive identity.
- model_token = a manufacturer model/reference (DA4, Renalyte 326, 350P).
- packaging = how many base units are bundled ("caja x 2"). This is price
  normalization, NOT identity.
- base_unit = the unit ONE item is measured in (unidad, ml, m, par, rollo).

THE CARDINAL RULE — EVIDENCE OR NOTHING
Every identity_attribute MUST quote, in `evidence`, the exact substring that
NAMES that attribute. If the text has a number with no word tying it to a
concept, DO NOT emit an attribute for it.
  "DEXTROSA 2.5%"   -> concentracion=2.5pct  (anchored by "DEXTROSA ... %")
  "Ca 2,5 mEq/L"    -> calcio=2.5meq_l        (anchored by "Ca ... mEq/L") NOT dextrose
  "cordon 3x2,5 mm" -> seccion=2.5mm          (anchored by "mm")           NOT a concentration
  "lote 2,5"        -> emit NOTHING for "2,5"  (no concept word)
A bare number is never sufficient for identity. When in doubt, omit and lower confidence.

CANONICALIZATION
- lowercase; strip accents; decimal comma to point ("2,5" -> "2.5").
- canonical units: fr (french), g (gauge), mm, cm->mm, ml, l->ml, mg, pct (%).
- normalize leading zeros ("08fr" -> "8fr").
- category: a normalized singular Spanish head-noun. If it clearly matches one of
  the KNOWN_FAMILIES below, use that exact id; else emit a concise normalized
  phrase and set flag "ambiguous_category" if unsure.

NOT A PRODUCT (is_product=false): services, rubric-only lines echoing a UNSPSC
category, pure boilerplate ("segun bases tecnicas").

CONSERVATISM
- Spec-less but real ("Desfibrilador externo") -> is_product=true, no attributes,
  confidence=low, flag "below_min_info".
- A line enumerating several distinct products -> flag "multi_product".
- Conflicting values for one attribute -> flag "conflicting_attributes".

KNOWN_FAMILIES (snap to these ids when applicable):
{known_families}
"""


def known_families(register: dict) -> list[str]:
    return [c["category_id"] for c in register.get("categories", [])]


def system_prompt(register: dict) -> str:
    fams = ", ".join(known_families(register))
    return _SYSTEM_TEMPLATE.format(known_families=fams)


# --- per-call request + result helpers ----------------------------------------

def text_hash(normalized_text: str) -> str:
    """Stable cache key — canonicalize ONCE per distinct normalized text, persist,
    reuse. Makes the LLM step a cached pure function (determinism)."""
    return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()


def build_user_message(description: str, *, unspsc: int | str | None = None,
                       buyer_line: str | None = None) -> str:
    """The volatile per-call content (everything after the cached prefix)."""
    parts = []
    if unspsc is not None:
        parts.append(f"UNSPSC: {unspsc}")
    if buyer_line:
        parts.append("BUYER_LINE (context only — describe the OFFER, use this only "
                     f"to disambiguate):\n  {buyer_line!r}")
    parts.append(f"DESCRIPTION:\n  {description!r}")
    return "\n".join(parts)


def parse_profile(d: dict) -> Profile:
    """Build a Profile from the validated structured-outputs dict."""
    pk = d.get("packaging") or {}
    return Profile(
        is_product=bool(d["is_product"]),
        category=d["category"],
        identity_attributes=tuple(
            IdentityAttr(name=a["name"], value=a["value"], evidence=a["evidence"])
            for a in d.get("identity_attributes", [])),
        brand=d.get("brand"),
        model_token=d.get("model_token"),
        packaging=Packaging(pack_size=pk.get("pack_size"),
                            pack_unit=pk.get("pack_unit"),
                            evidence=pk.get("evidence")),
        base_unit=d.get("base_unit"),
        confidence=d.get("confidence", "low"),
        flags=tuple(d.get("flags", [])),
    )


def profile_to_dict(p: Profile) -> dict:
    """Round-trip for persistence (JSONL profile store keyed by text_hash)."""
    return {
        "is_product": p.is_product, "category": p.category,
        "identity_attributes": [{"name": a.name, "value": a.value,
                                 "evidence": a.evidence} for a in p.identity_attributes],
        "brand": p.brand, "model_token": p.model_token,
        "packaging": {"pack_size": p.packaging.pack_size,
                      "pack_unit": p.packaging.pack_unit,
                      "evidence": p.packaging.evidence},
        "base_unit": p.base_unit, "confidence": p.confidence,
        "flags": list(p.flags),
    }
