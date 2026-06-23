from chilecompra_er.ambiguity import ambiguity_ranking, looks_like_bundle
from chilecompra_er.resolve.classifier import AMBIGUOUS, CLASSIFIED, Classification


class _FakeClassifier:
    """Returns AMBIGUOUS (with the colliding category ids) for known texts."""
    def __init__(self, ambiguous: dict[str, tuple]):
        self.ambiguous = ambiguous

    def classify(self, text: str) -> Classification:
        if text in self.ambiguous:
            return Classification(None, AMBIGUOUS, self.ambiguous[text])
        return Classification("ok", CLASSIFIED, ("inc",))


def _identity(t):  # passthrough normalizer so the fake-classifier keys match
    return t


def test_bundle_detection():
    assert looks_like_bundle("mascarillas, canulas, gasas")      # 2 commas
    assert looks_like_bundle("guante y mascarilla y gorro")      # 2 " y "
    assert not looks_like_bundle("aguja de sutura recta")        # one product


def test_ranks_colliding_sets_by_spurious_count():
    rows = [{"text": "aguja de sutura recta"}] * 5 + [{"text": "gasa torula"}] * 2
    amb = {"aguja de sutura recta": ("agujas", "suturas"),
           "gasa torula": ("gasas", "torulas")}
    stats = ambiguity_ranking(rows, classifier=_FakeClassifier(amb),
                              normalizer=_identity, min_count=1)
    assert stats[0].pair == ("agujas", "suturas") and stats[0].spurious_items == 5
    assert stats[1].pair == ("gasas", "torulas")
    assert stats[0].samples and "aguja" in stats[0].samples[0]


def test_bundles_excluded_from_spurious_and_unsampled():
    rows = [{"text": "mascarillas, canulas, gasas"}] * 3 + [{"text": "aguja sutura"}]
    amb = {"mascarillas, canulas, gasas": ("canulas", "gasas", "mascarillas"),
           "aguja sutura": ("agujas", "suturas")}
    stats = {s.pair: s for s in ambiguity_ranking(
        rows, classifier=_FakeClassifier(amb), normalizer=_identity, min_count=1)}
    bundle = stats[("canulas", "gasas", "mascarillas")]
    assert bundle.bundle_items == 3 and bundle.spurious_items == 0
    assert bundle.samples == []  # bundles are not offered as fixable examples
    assert stats[("agujas", "suturas")].spurious_items == 1


def test_min_count_filters_rare_collisions():
    rows = [{"text": "aguja sutura"}]
    amb = {"aguja sutura": ("agujas", "suturas")}
    stats = ambiguity_ranking(rows, classifier=_FakeClassifier(amb),
                              normalizer=_identity, min_count=3)
    assert stats == []


def test_rubric_lines_skipped():
    # rubric paths (>= 2 " / ") are taxonomy strings, not overlaps
    rows = [{"text": "equipamiento / insumos / agujas y suturas"}]
    amb = {"equipamiento / insumos / agujas y suturas": ("agujas", "suturas")}
    stats = ambiguity_ranking(rows, classifier=_FakeClassifier(amb),
                              normalizer=_identity, min_count=1)
    assert stats == []
