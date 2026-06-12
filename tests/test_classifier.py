from chilecompra_er.resolve import Tier1Classifier
from chilecompra_er.resolve.classifier import AMBIGUOUS, CLASSIFIED, UNCLASSIFIED

clf = Tier1Classifier()


def test_foley_classified():
    c = clf.classify("sonda foley 16 silicona")
    assert c.status == CLASSIFIED and c.category_id == "sondas_foley"


def test_outside_launched_categories_stays_unclassified():
    c = clf.classify("termometro digital axilar")
    assert c.status == UNCLASSIFIED and c.category_id is None


def test_exclusion_pattern_blocks():
    c = clf.classify("sonda foley nelaton 16")  # contradictory text -> route to review
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
