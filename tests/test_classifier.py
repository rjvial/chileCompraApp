from chilecompra_er.resolve import Tier1Classifier
from chilecompra_er.resolve.classifier import AMBIGUOUS, CLASSIFIED, UNCLASSIFIED

clf = Tier1Classifier()


def test_foley_classified():
    c = clf.classify("sonda foley 16 silicona")
    assert c.status == CLASSIFIED and c.category_id == "sondas"


def test_outside_launched_categories_stays_unclassified():
    # a nonsense token no family covers (stable as the catalog grows)
    c = clf.classify("widgetron digital axilar")
    assert c.status == UNCLASSIFIED and c.category_id is None


def test_exclusion_pattern_blocks():
    # each sibling family excludes the other's term, so a line carrying both
    # ("foley" AND "nelaton") makes both abstain -> left UNCLASSIFIED for review.
    reg = {"register_version": "test", "categories": [
        {"category_id": "sondas_foley", "status": "candidate",
         "include": ["\\bfoley\\b"], "exclude": ["\\bnelaton\\b"]},
        {"category_id": "sondas_nelaton", "status": "candidate",
         "include": ["\\bnelaton\\b"], "exclude": ["\\bfoley\\b"]},
    ]}
    c = Tier1Classifier(reg).classify("sonda foley nelaton 16")
    assert c.status == UNCLASSIFIED


def test_ambiguity_routed_not_guessed():
    register = {
        "register_version": "test",
        "categories": [
            {"category_id": "a", "status": "candidate", "include": ["foo"], "exclude": []},
            {"category_id": "b", "status": "candidate", "include": ["foo"], "exclude": []},
        ],
    }
    c = Tier1Classifier(register).classify("foo bar")
    assert c.status == AMBIGUOUS and c.category_id is None
