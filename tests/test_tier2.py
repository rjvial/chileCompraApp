from types import SimpleNamespace

import pytest

from chilecompra_er.cli import cmd_train_tier2
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


# --- skip-if-exists: the pipeline trains only when there is no .joblib ---------

class _NoConnect(Exception):
    pass


def _args(out, skip_if_exists):
    return SimpleNamespace(out=out, skip_if_exists=skip_if_exists,
                           threshold=0.6, min_rows=500, eval=False)


def test_skip_if_exists_short_circuits_before_connecting(tmp_path, monkeypatch):
    # an existing model + skip_if_exists must return 0 WITHOUT touching the graph
    monkeypatch.setattr("chilecompra_er.graphdb.get_connection",
                        lambda: (_ for _ in ()).throw(_NoConnect()))
    p = tmp_path / "m.joblib"
    p.write_bytes(b"existing")
    assert cmd_train_tier2(_args(p, skip_if_exists=True)) == 0


def test_standalone_retrains_even_if_model_exists(tmp_path, monkeypatch):
    # default (standalone) path does NOT skip: it proceeds to connect/train,
    # so a stubbed get_connection that raises proves the skip did not fire
    monkeypatch.setattr("chilecompra_er.graphdb.get_connection",
                        lambda: (_ for _ in ()).throw(_NoConnect()))
    p = tmp_path / "m.joblib"
    p.write_bytes(b"existing")
    with pytest.raises(_NoConnect):
        cmd_train_tier2(_args(p, skip_if_exists=False))
