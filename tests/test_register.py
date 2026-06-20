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
    """An exclude on the broader family resolves a sibling overlap: 'aguja' inside
    a jeringa or sutura description is an attribute, not the product, so the agujas
    family excludes those contexts and the line resolves to its host. Tested on a
    controlled fixture register (not the live catalog) so it exercises the
    exclusion MECHANISM and stays stable as the catalog evolves."""
    reg = {"register_version": "1.0.0", "categories": [
        {"category_id": "jeringas", "name": "Jeringas", "status": "candidate",
         "include": ["\\bjeringa\\w*"], "exclude": [],
         "schema_file": "schemas/jeringas.json", "corpus_regex": "(?i).*jeringa.*"},
        {"category_id": "suturas", "name": "Suturas", "status": "candidate",
         "include": ["\\bsutura\\w*"], "exclude": [],
         "schema_file": "schemas/suturas.json", "corpus_regex": "(?i).*sutura.*"},
        {"category_id": "agujas", "name": "Agujas", "status": "candidate",
         "include": ["\\baguja\\w*"], "exclude": ["\\bjeringa\\w*", "\\bsutura\\w*"],
         "schema_file": "schemas/agujas.json", "corpus_regex": "(?i).*aguja.*"},
    ]}
    c = Tier1Classifier(reg)
    # aguja vetoed by the jeringa/sutura context -> resolves to the host family
    assert c.classify(norm("JERINGA 5ML CON AGUJA 23G")).category_id == "jeringas"
    assert c.classify(norm("SUTURA SEDA 2/0 CON AGUJA RECTA")).category_id == "suturas"
    # a plain aguja line (no host context) still resolves to agujas
    assert c.classify(norm("AGUJA HIPODERMICA 23G DESECHABLE")).category_id == "agujas"
