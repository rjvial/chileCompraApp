from chilecompra_er.resolve.classifier import CLASSIFIED, UNCLASSIFIED
from chilecompra_er.resolve.tier2 import Tier2Classifier, train

# normalized-style texts (lowercase, unaccented), two clear families
TEXTS = [
    "jeringa 10 ml desechable luer",
    "jeringa 5 ml desechable",
    "jeringa 20 ml luer lock",
    "jeringa insulina 1 ml",
    "guante nitrilo talla m",
    "guante latex talla l",
    "guante nitrilo examen talla s",
    "guante quirurgico esteril",
]
LABELS = ["jeringas", "jeringas", "jeringas", "jeringas",
          "guantes", "guantes", "guantes", "guantes"]


def test_classifies_in_family_text():
    clf = train(TEXTS, LABELS, threshold=0.4)
    assert clf.classify("jeringa 50 ml desechable").category_id == "jeringas"
    assert clf.classify("guante nitrilo talla xl").category_id == "guantes"
    assert clf.classify("jeringa 50 ml desechable").tier == "tier2"


def test_empty_text_abstains():
    clf = train(TEXTS, LABELS, threshold=0.4)
    assert clf.classify("   ").status == UNCLASSIFIED


def test_high_threshold_can_abstain():
    # an unreachable threshold forces abstention even on an in-family line
    clf = train(TEXTS, LABELS, threshold=1.01)
    assert clf.classify("jeringa 10 ml").status == UNCLASSIFIED


def test_save_load_round_trip(tmp_path):
    clf = train(TEXTS, LABELS, threshold=0.4)
    p = tmp_path / "m.joblib"
    clf.save(p)
    reloaded = Tier2Classifier.load(p)
    a = clf.classify("jeringa 10 ml desechable")
    b = reloaded.classify("jeringa 10 ml desechable")
    assert a.category_id == b.category_id == "jeringas"
    assert reloaded.threshold == clf.threshold
