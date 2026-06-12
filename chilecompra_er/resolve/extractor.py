"""Layer-1 attribute extraction (design note §7, "How to build extraction").

Per-attribute regexes/keywords over normalized text — high precision, fully
auditable, fills most attributes. Missing stays absent, never imputed; an
out-of-domain yield is dropped and recorded (the illegal-value rate is a
schema dry-run metric). Layer 2 (constrained-output LLM for the residue)
plugs in later behind the same Extraction type; it fills gaps but never
decides identity.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..categories.schema import CategorySchema

LAYER_1 = "L1_regex"


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
        for rule in attr.rules:
            hit = rule.apply(normalized_text)
            if hit is None:
                continue
            value, match = hit
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
