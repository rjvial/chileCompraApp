"""Register integrity — data-driven from the register itself.

Each register entry carries its own hand-authored canonical_example (written
when the category is added, by a human or accepted from the register vet). The
tests assert the example exists and classifies unambiguously to its own
category — golden fixtures stored next to the rules they check, so adding a
category never requires editing this file. Cross-category boundary decisions
stay hand-written below: they encode curation calls, not per-category facts.
"""

import pytest

from chilecompra_er.categories import load_register, schema_for
from chilecompra_er.normalize import Normalizer
from chilecompra_er.resolve import Tier1Classifier
from chilecompra_er.resolve.classifier import CLASSIFIED

register = load_register()
norm = Normalizer()
clf = Tier1Classifier(register)

CATEGORY_IDS = [c["category_id"] for c in register["categories"]]


def test_every_category_has_a_canonical_example():
    missing = [c["category_id"] for c in register["categories"]
               if not c.get("canonical_example")]
    assert missing == [], f"categories without golden example: {missing}"


@pytest.mark.parametrize("category_id", CATEGORY_IDS)
def test_schema_loads_with_identity_attrs(category_id):
    schema = schema_for(category_id, register)
    assert schema.category_id == category_id
    assert len(schema.identity_names) >= 1


@pytest.mark.parametrize("category", register["categories"],
                         ids=[c["category_id"] for c in register["categories"]])
def test_canonical_example_classifies_unambiguously(category):
    text = category.get("canonical_example", "")
    c = clf.classify(norm(text))
    assert c.status == CLASSIFIED, f"{text!r} -> {c.status} {c.matched}"
    assert c.category_id == category["category_id"]


def test_sibling_overlap_resolved_by_exclusions():
    # aguja inside a jeringa/sutura description is an attribute, not a category
    assert clf.classify(norm("JERINGA 5ML CON AGUJA 23G")).category_id == "jeringas"
    assert clf.classify(norm("SUTURA SEDA 2/0 CON AGUJA RECTA")).category_id == "suturas"
    # venda de gasa is a venda
    assert clf.classify(norm("VENDA DE GASA ORILLADA 10 CM")).category_id == "vendas"
    # foley accessories and kits are no longer captured by sondas_foley
    assert clf.classify(norm("SUJETADOR ADHESIVO DE SONDA FOLEY")).category_id is None
    assert clf.classify(norm("SET NEFROSTOMIA CON BALON FOLEY")).category_id is None
    # bolsa de drenaje belongs to bolsas_recolectoras, not drenajes
    assert clf.classify(norm("BOLSA DE DRENAJE 2000 ML")).category_id == "bolsas_recolectoras"
