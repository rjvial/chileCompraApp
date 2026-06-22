"""Tier-2 evaluation harness (design: measure before tuning).

Raw "accuracy vs Tier-1 labels" overstates quality — it scores *agreement with the
rules*, not correctness on the residue Tier-2 exists to catch. This gives the two
things you actually tune on:

  - a held-out COVERAGE / PRECISION curve: precision vs the fraction of items
    classified, as the confidence threshold varies — so you can see where the 0.60
    cutoff really sits and pick an operating point;
  - the SAME curve WITH the UNSPSC commodity code folded in as a feature, so the
    lift from that feature is measurable here, in memory, before it is wired into
    the production resolver.

`evaluate_gold` scores a human-labeled CSV (true precision, with a residue-only
breakdown). Build that CSV with the `tier2-label-sample` CLI export.

Everything trains throwaway in-memory pipelines via tier2.train_pipeline; nothing
here touches the saved production model.
"""

from __future__ import annotations

from collections import Counter

DEFAULT_THRESHOLDS = (0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95)


def _curve(proba, y_true, classes, thresholds):
    """coverage + precision at each threshold from one predict_proba matrix."""
    import numpy as np

    y = np.asarray(y_true)
    conf = proba.max(axis=1)
    pred = np.asarray(classes)[proba.argmax(axis=1)]
    correct = pred == y
    rows = []
    for t in thresholds:
        m = conf >= t
        n = int(m.sum())
        rows.append({
            "threshold": float(t),
            "coverage": float(m.mean()) if len(m) else 0.0,
            "n_classified": n,
            "precision": float(correct[m].mean()) if n else None,
        })
    return rows


def _can_stratify(labels) -> bool:
    c = Counter(labels)
    return len(c) > 1 and min(c.values()) >= 2


def evaluate_holdout(texts, labels, unspsc=None, *, test_size=0.1, seed=0,
                     thresholds=DEFAULT_THRESHOLDS):
    """Train on a split, return the held-out coverage/precision curve. Pass `unspsc`
    (per-row codes, same length as texts) to fold the commodity code in as a feature
    — call once with and once without to read the feature's lift. Note: precision
    here is measured against the Tier-1 labels (an upper bound on true precision);
    use evaluate_gold for ground truth."""
    from sklearn.model_selection import train_test_split

    from .tier2 import augment, train_pipeline

    idx = list(range(len(texts)))
    strat = labels if _can_stratify(labels) else None
    tr, te = train_test_split(idx, test_size=test_size, random_state=seed, stratify=strat)

    def build(i):
        return augment(texts[i], unspsc[i] if unspsc is not None else None)

    xtr, ytr = [build(i) for i in tr], [labels[i] for i in tr]
    xte, yte = [build(i) for i in te], [labels[i] for i in te]
    pipe = train_pipeline(xtr, ytr)
    proba = pipe.predict_proba(xte)
    return {
        "n_train": len(tr), "n_test": len(te), "n_classes": len(set(ytr)),
        "with_unspsc": unspsc is not None,
        "curve": _curve(proba, yte, pipe.classes_, thresholds),
    }


def evaluate_gold(clf, rows, *, threshold=None):
    """Precision / coverage of a loaded Tier2Classifier on a human GOLD set.

    `rows`: dicts with 'text', 'true_category', and optional 'residue' (True for
    items Tier-1 missed — the ones Tier-2 is really judged on). Reports overall and,
    if any residue rows are flagged, a residue-only breakdown (the honest number)."""
    thr = clf.threshold if threshold is None else threshold

    def assess(subset):
        total = len(subset)
        classified = correct = 0
        for r in subset:
            v = clf.classify(r["text"])
            p = float(v.matched[0].split("=")[1]) if v.matched else 0.0
            if v.category_id is not None and p >= thr:
                classified += 1
                correct += int(v.category_id == r["true_category"])
        return {
            "n": total,
            "coverage": classified / total if total else None,
            "precision": correct / classified if classified else None,
        }

    out = {"threshold": thr, "overall": assess(rows)}
    residue = [r for r in rows if r.get("residue")]
    if residue:
        out["residue_only"] = assess(residue)
    return out


def format_curve(result) -> str:
    tag = "WITH unspsc" if result["with_unspsc"] else "text only"
    lines = [
        f"held-out eval ({tag}): train {result['n_train']:,} / test {result['n_test']:,} "
        f"across {result['n_classes']} categories",
        "  thresh  coverage  precision  n_classified",
        "  ------  --------  ---------  ------------",
    ]
    for r in result["curve"]:
        prec = " n/a " if r["precision"] is None else f"{r['precision']:.1%}"
        lines.append(f"   {r['threshold']:.2f}    {r['coverage']:6.1%}     {prec:>6}     "
                     f"{r['n_classified']:,}")
    return "\n".join(lines)


def format_gold(result) -> str:
    def line(name, d):
        cov = "n/a" if d["coverage"] is None else f"{d['coverage']:.1%}"
        prec = "n/a" if d["precision"] is None else f"{d['precision']:.1%}"
        return f"  {name:<14} n={d['n']:,}  coverage={cov}  precision={prec}"

    lines = [f"gold-set eval (threshold {result['threshold']:.2f}):", line("overall", result["overall"])]
    if "residue_only" in result:
        lines.append(line("residue-only", result["residue_only"]))
        lines.append("  (residue-only = items Tier-1 missed — Tier-2's true precision)")
    return "\n".join(lines)
