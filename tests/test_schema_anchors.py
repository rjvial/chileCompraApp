"""Phase 0 coherence guard: no registered schema may carry an anchorless-numeric
identity rule (one that can fire on a bare number with no concept word), unless
that rule has a `requires` context guard.

This is the regression test for the false-merge class found in pr_fd4522a53b7e
("Ca 2,5 mEq" and "cable 3x2,5 mm" wrongly stamped dextrose 2.5%). Add a
`requires` guard naming the attribute's concept to fix an offender.
"""
from __future__ import annotations

import pytest

from chilecompra_er.categories.schema import (
    CATEGORIES_DIR,
    anchorless_identity_rules,
    load_register,
    load_schema,
)


def _registered_schemas():
    register = load_register()
    for cat in register["categories"]:
        path = CATEGORIES_DIR / cat["schema_file"]
        if path.exists():
            yield cat["category_id"], load_schema(path)


def test_no_anchorless_identity_rules():
    offenders: dict[str, list[tuple[str, str]]] = {}
    for category_id, schema in _registered_schemas():
        bad = anchorless_identity_rules(schema)
        if bad:
            offenders[category_id] = bad
    assert not offenders, (
        "anchorless identity rules (add a `requires` guard naming the concept):\n"
        + "\n".join(f"  {cid}: {rules}" for cid, rules in sorted(offenders.items()))
    )


def test_requires_guard_makes_rule_inert_without_context():
    """A guarded numeric rule fires only when its concept word is present."""
    from chilecompra_er.categories.schema import Rule
    import re

    rule = Rule(kind="keyword", pattern=re.compile(r"\b2\.5\b"), value="2.5pct",
                requires=re.compile(r"dextrosa|glucosa"))
    hit = rule.apply("solucion dextrosa 2.5 bolsa")
    assert hit is not None and hit[0] == "2.5pct"             # concept present → fires
    assert rule.apply("ca 2.5 meq de calcio") is None         # no concept word
    assert rule.apply("cable 3 x 2.5 mm") is None             # wrong concept
