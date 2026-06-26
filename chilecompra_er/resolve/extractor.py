"""Layer-1 attribute extraction (design note §7, "How to build extraction").

Per-attribute regexes/keywords over normalized text — high precision, fully
auditable, fills most attributes. Missing stays absent, never imputed; an
out-of-domain yield is dropped and recorded (the illegal-value rate is a
schema dry-run metric). Layer 2 (constrained-output LLM for the residue)
plugs in later behind the same Extraction type; it fills gaps but never
decides identity.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from ..categories.schema import CategorySchema

LAYER_1 = "L1_regex"

# --- unit canonicalization (Fix D part 1) ---------------------------------
# Rule templates already normalize the unit *label* (e.g. "cc" -> "ml"), but
# cannot convert *magnitude* (no arithmetic in a template), so "8cm" and "80mm"
# stay distinct strings and one of them is dropped as out-of-domain. We convert
# a matched value to the attribute's unit BEFORE the domain check, within one
# physical family. This collapses unit-fragmentation (2.5cm == 25mm) and stops
# representation-only drops. fr/gauge are NOT convertible (gauge is inverse, Fr
# is diameter x3) and are deliberately left to the enum-completion pass.
_LENGTH = {"mm": 1.0, "milimetro": 1.0, "milimetros": 1.0,
           "cm": 10.0, "centimetro": 10.0, "centimetros": 10.0,
           "m": 1000.0, "metro": 1000.0, "metros": 1000.0,
           "in": 25.4, "inch": 25.4, "pulg": 25.4, "pulgada": 25.4, '"': 25.4}
_VOLUME = {"ml": 1.0, "cc": 1.0, "cm3": 1.0, "cl": 10.0, "dl": 100.0,
           "l": 1000.0, "lt": 1000.0, "litro": 1000.0, "litros": 1000.0}
_FAMILIES = [_LENGTH, _VOLUME]
_NUM_UNIT = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*([a-z\"]*)\s*$")


def _family_for(unit: str | None):
    """The conversion table whose base unit `unit` belongs to, or None."""
    if not unit:
        return None
    u = unit.lower()
    for fam in _FAMILIES:
        if u in fam:
            return fam
    return None


def _fmt(num: float) -> str:
    """Clean numeric string: integer when whole, else up to 2 decimals, no
    trailing zeros (80.0 -> '80', 25.4 -> '25.4', 2.50 -> '2.5')."""
    if num == int(num):
        return str(int(num))
    return f"{num:.2f}".rstrip("0").rstrip(".")


def _target_unit(attr) -> str | None:
    """The unit to canonicalize an attribute's values to: its declared `unit`
    if convertible, else the majority unit of its (numeric) domain."""
    if _family_for(getattr(attr, "unit", None)):
        return attr.unit.lower()
    units = []
    for d in attr.domain:
        m = _NUM_UNIT.match(d.lower())
        if m and m.group(2):
            units.append(m.group(2))
    if not units:
        return None
    top = Counter(units).most_common(1)[0][0]
    return top if _family_for(top) else None


def canonicalize_measure(value: str, target_unit: str | None) -> str:
    """Convert `value` to `target_unit` within a length/volume family. Returns
    the value unchanged when there's no convertible target, the value can't be
    parsed as number+unit, or its unit is outside the target's family (so a
    genuinely foreign unit still drops as illegal, as before)."""
    fam = _family_for(target_unit)
    if fam is None:
        return value
    m = _NUM_UNIT.match(value.lower())
    if not m:
        return value
    num = float(m.group(1).replace(",", "."))
    unit = m.group(2) or target_unit          # bare number assumed already in target
    if unit not in fam:
        return value
    base = num * fam[unit]
    return _fmt(base / fam[target_unit.lower()]) + target_unit.lower()


def normalize_value(value: str, target_unit: str | None) -> str:
    """Canonicalize an extracted value before the domain check. For a
    length/volume attribute, convert magnitude to `target_unit`. For a
    non-convertible numeric attribute (Fr, gauge), just clean the numeric part
    so leading-zero spellings collapse ('08fr' -> '8fr', '06fr' -> '6fr').
    Non-numeric values (e.g. 'latex', '2_vias') pass through untouched."""
    if _family_for(target_unit):
        return canonicalize_measure(value, target_unit)
    m = _NUM_UNIT.match(value)
    if not m:
        return value
    num = float(m.group(1).replace(",", "."))
    return _fmt(num) + m.group(2)


@dataclass
class Extraction:
    values: dict[str, str] = field(default_factory=dict)      # attr -> canonical value
    provenance: dict[str, dict] = field(default_factory=dict)  # attr -> evidence
    illegal: list[dict] = field(default_factory=list)          # dropped out-of-domain yields

    @property
    def identity_values(self) -> dict[str, str]:
        # populated by extract(); identity filtering happens there via schema
        return {k: v for k, v in self.values.items() if self.provenance[k]["role"] == "identity"}


def extract(normalized_text: str, schema: CategorySchema) -> Extraction:
    out = Extraction()
    for attr in schema.attribute_defs:
        target = _target_unit(attr)
        for rule in attr.rules:
            hit = rule.apply(normalized_text)
            if hit is None:
                continue
            value, match = hit
            value = normalize_value(value, target)  # unit-convert + strip leading zeros
            if value in attr.domain:
                out.values[attr.name] = value
                out.provenance[attr.name] = {
                    "layer": LAYER_1,
                    "role": attr.role,
                    "pattern": rule.pattern.pattern,
                    "matched": match.group(0),
                    "confidence": 1.0,
                }
                break  # first in-domain yield wins; rules are ordered by specificity
            out.illegal.append({
                "attribute": attr.name,
                "value": value,
                "pattern": rule.pattern.pattern,
                "matched": match.group(0),
            })
    return out
