from chilecompra_er.brands import merge_brand_maps, valid_brand_tokens
from chilecompra_er.resolve.classifier import CLASSIFIED, UNCLASSIFIED, Classification

SAMPLES = ["kit relyx temp a 3.5", "cemento relyx unicem", "guante nitrilo talla m"]


class StubClf:
    """Tier-1 stand-in: classifies only the tokens in `covered`."""
    def __init__(self, covered=()):
        self.covered = set(covered)

    def classify(self, tok):
        return (Classification("x", CLASSIFIED) if tok in self.covered
                else Classification(None, UNCLASSIFIED))


def test_keeps_real_single_token_brand_normalized_and_deduped():
    assert valid_brand_tokens(["RelyX", "Relyx"], SAMPLES, StubClf()) == ["relyx"]


def test_rejects_multitoken_absent_filler_and_short():
    clf = StubClf()
    assert valid_brand_tokens(["3M ESPE"], SAMPLES, clf) == []   # -> multiple tokens
    assert valid_brand_tokens(["vitrebond"], SAMPLES, clf) == []  # not in samples
    assert valid_brand_tokens(["kit"], SAMPLES, clf) == []        # generic filler (_NOT_FAMILY)
    assert valid_brand_tokens(["ab"], SAMPLES, clf) == []         # too short


def test_rejects_tokens_tier1_already_covers():
    assert valid_brand_tokens(["guante"], SAMPLES, StubClf(covered={"guante"})) == []
    assert valid_brand_tokens(["guante"], SAMPLES, StubClf()) == ["guante"]


def test_merge_keeps_existing_curated_on_conflict():
    merged, added, conflicts = merge_brand_maps(
        {"relyx": "cementos"}, {"relyx": "otra_cat", "cavit": "temporales"})
    assert merged == {"relyx": "cementos", "cavit": "temporales"}
    assert added == 1 and conflicts == 1


def test_merge_overwrite_replaces():
    merged, added, conflicts = merge_brand_maps({"a": "1"}, {"b": "2"}, overwrite=True)
    assert merged == {"b": "2"} and added == 1 and conflicts == 0
