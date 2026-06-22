"""Tier-2 evaluation harness: augment helper, held-out curve, gold-set scoring."""

from chilecompra_er.resolve import tier2_eval
from chilecompra_er.resolve.tier2 import augment, train, unspsc_tokens


# --- UNSPSC feature encoding --------------------------------------------------

def test_unspsc_tokens_hierarchical():
    toks = unspsc_tokens(42181500)
    assert "unspscseg42" in toks and "unspscfam4218" in toks and "unspsccls421815" in toks


def test_unspsc_tokens_missing_is_empty():
    assert unspsc_tokens(None) == "" and unspsc_tokens("") == "" and unspsc_tokens("x") == ""


def test_augment_appends_only_when_code_present():
    assert augment("sonda foley", None) == "sonda foley"
    assert augment("sonda foley", 42142500).startswith("sonda foley ")
    assert "unspscseg42" in augment("sonda foley", 42142500)


# --- synthetic dataset (big enough for a 10% split + min_df=2) -----------------

def _dataset(n_each=30):
    texts, labels, codes = [], [], []
    families = [
        ("jeringas", [f"jeringa {v} ml desechable luer" for v in range(1, n_each + 1)], 42142500),
        ("guantes", [f"guante nitrilo examen talla {v}" for v in range(1, n_each + 1)], 42132200),
        ("sondas", [f"sonda foley ch{v} silicona dos vias" for v in range(1, n_each + 1)], 42142600),
    ]
    for label, lines, code in families:
        for ln in lines:
            texts.append(ln)
            labels.append(label)
            codes.append(code)
    return texts, labels, codes


# --- held-out curve -----------------------------------------------------------

def test_holdout_curve_shape_and_monotonic_coverage():
    texts, labels, _ = _dataset()
    res = tier2_eval.evaluate_holdout(texts, labels, None, test_size=0.2, seed=0)
    assert res["n_test"] > 0 and res["with_unspsc"] is False
    covs = [r["coverage"] for r in res["curve"]]
    # coverage never increases as the threshold rises
    assert all(a >= b - 1e-9 for a, b in zip(covs, covs[1:]))
    # precisions are valid fractions where defined
    for r in res["curve"]:
        assert r["precision"] is None or 0.0 <= r["precision"] <= 1.0


def test_holdout_with_unspsc_flag_and_runs():
    texts, labels, codes = _dataset()
    res = tier2_eval.evaluate_holdout(texts, labels, codes, test_size=0.2, seed=0)
    assert res["with_unspsc"] is True
    assert len(res["curve"]) == len(tier2_eval.DEFAULT_THRESHOLDS)
    assert "WITH unspsc" in tier2_eval.format_curve(res)


# --- gold-set scoring ---------------------------------------------------------

def test_evaluate_gold_overall_and_residue():
    texts, labels, _ = _dataset()
    clf = train(texts, labels, threshold=0.3)  # low threshold so it classifies the gold rows
    gold = [
        {"text": "jeringa 99 ml desechable luer", "true_category": "jeringas", "residue": True},
        {"text": "sonda foley ch99 silicona dos vias", "true_category": "sondas", "residue": False},
    ]
    out = tier2_eval.evaluate_gold(clf, gold)
    assert out["overall"]["n"] == 2
    assert out["overall"]["precision"] is None or 0.0 <= out["overall"]["precision"] <= 1.0
    assert "residue_only" in out and out["residue_only"]["n"] == 1
    assert "gold-set eval" in tier2_eval.format_gold(out)
