from chilecompra_er.resolve.classifier import (
    AMBIGUOUS,
    CLASSIFIED,
    UNCLASSIFIED,
    Classification,
)
from chilecompra_er.resolve.layered import LayeredClassifier


class Stub:
    """Minimal tier: always returns a fixed Classification."""
    def __init__(self, result, register_version="t"):
        self.result = result
        self.register_version = register_version
    def classify(self, _text):
        return self.result


CLF = Classification("foo", CLASSIFIED, tier="x")
UNC = Classification(None, UNCLASSIFIED)
AMB = Classification(None, AMBIGUOUS, ("a", "b"))


def test_tier1_classified_short_circuits():
    layered = LayeredClassifier(Stub(CLF), brand=Stub(Classification("bar", CLASSIFIED)))
    assert layered.classify("x").category_id == "foo"  # brand never consulted


def test_tier1_ambiguous_short_circuits_no_lower_override():
    layered = LayeredClassifier(Stub(AMB), brand=Stub(Classification("bar", CLASSIFIED)))
    assert layered.classify("x").status == AMBIGUOUS


def test_falls_through_to_brand_then_tier2():
    layered = LayeredClassifier(
        Stub(UNC), brand=Stub(UNC),
        tier2=Stub(Classification("t2", CLASSIFIED, tier="tier2")))
    out = layered.classify("x")
    assert out.status == CLASSIFIED and out.category_id == "t2" and out.tier == "tier2"


def test_brand_wins_before_tier2():
    layered = LayeredClassifier(
        Stub(UNC),
        brand=Stub(Classification("b", CLASSIFIED, tier="brand")),
        tier2=Stub(Classification("t2", CLASSIFIED, tier="tier2")))
    assert layered.classify("x").category_id == "b"


def test_all_abstain_stays_unclassified():
    layered = LayeredClassifier(Stub(UNC), brand=Stub(UNC), tier2=Stub(UNC))
    assert layered.classify("x").status == UNCLASSIFIED
