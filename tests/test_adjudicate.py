"""Unit tests for the adjudication scaffold (resolve/adjudicate.py) — pure
parts + dry-run; no LLM."""
from __future__ import annotations

from chilecompra_er.resolve.adjudicate import (
    VerdictStore,
    adjudicate,
    build_questions,
    signature_profiles,
)
from chilecompra_er.resolve.matcher import cluster
from chilecompra_er.resolve.profile import IdentityAttr, Packaging, Profile


def mk(cat, attrs=(), model=None):
    return Profile(True, cat, tuple(IdentityAttr(n, v, e) for n, v, e in attrs),
                   None, model, Packaging(), "unidad", "high", ())


def _residue_scenario():
    profs = [
        mk("dialisis", [("calcio", "2.5meq_l", "2.5")], model="da4"),
        mk("dialisis", [("calcio", "3.5meq_l", "3.5")], model="da4"),   # model-token conflict
        mk("foley", [("calibre", "16fr", "16")]),
        mk("foley", [("calibre", "16fr", "16"), ("material", "latex", "latex")]),
        mk("foley", [("calibre", "16fr", "16"), ("material", "silicona", "sil")]),  # ambiguous
    ]
    items = [(f"h{i}", p) for i, p in enumerate(profs)]
    return items, cluster(profs)


def test_build_questions_covers_both_residue_kinds():
    items, result = _residue_scenario()
    qs = build_questions(result, signature_profiles(items))
    kinds = {q.kind for q in qs}
    assert kinds == {"model_token_conflict", "ambiguous_partial"}
    mtc = next(q for q in qs if q.kind == "model_token_conflict")
    assert "da4" in mtc.prompt and "calcio=2.5meq_l" in mtc.prompt


def test_question_keys_are_stable():
    items, result = _residue_scenario()
    a = build_questions(result, signature_profiles(items))
    b = build_questions(result, signature_profiles(items))
    assert sorted(q.key for q in a) == sorted(q.key for q in b)


def test_adjudicate_dry_run_counts_and_cache(tmp_path):
    items, result = _residue_scenario()
    qs = build_questions(result, signature_profiles(items))
    store = VerdictStore(tmp_path / "verdicts.jsonl")
    stats = adjudicate(qs, store, dry_run=True)
    assert stats.questions == len(qs) and stats.cached == 0

    store.put_many({qs[0].key: {"decision": "different", "target": None,
                                "confidence": "high", "rationale": "x"}})
    stats2 = adjudicate(qs, store, dry_run=True)
    assert stats2.cached == 1


def test_verdict_store_round_trip(tmp_path):
    s = VerdictStore(tmp_path / "v.jsonl")
    s.put_many({"k1": {"decision": "same", "target": None}})
    assert s.has("k1") and s.get("k1")["decision"] == "same"
    assert VerdictStore(tmp_path / "v.jsonl").get("k1")["decision"] == "same"  # reloads
