from chilecompra_er.categories import load_register
from chilecompra_er.profiling import GroupStat
from chilecompra_er.resolve import Tier1Classifier
from chilecompra_er.register import (
    Candidate, _is_covered, finalize_candidates, propose,
)

register = load_register()
clf = Tier1Classifier(register)


def cand(token, samples=None):
    return Candidate(token=token, records=10, spend_share=0.01,
                     samples=samples or [f"{token} ejemplo uno", f"{token} ejemplo dos"])


def verdict(token, **over):
    base = {"token": token, "viable": True, "category_id": f"{token}s",
            "name": token.capitalize(), "include": [f"\\b{token}\\w*"],
            "exclude": [], "corpus_regex": f"(?i).*{token}.*",
            "canonical_example": f"{token} ejemplo uno", "reason": "familia coherente"}
    base.update(over)
    return base


def test_covered_tokens_are_detected():
    assert _is_covered("venda", clf)          # existing category
    assert _is_covered("foley", clf)
    assert not _is_covered("termometro", clf)  # not yet a category


def test_vet_rejection_is_respected():
    out = finalize_candidates([cand("bases")], {"candidates": [
        verdict("bases", viable=False, reason="palabra de relleno")]}, register)
    assert out[0].viable is False and "relleno" in out[0].reason


def test_duplicate_id_downgraded():
    out = finalize_candidates([cand("venda")], {"candidates": [
        verdict("venda", category_id="vendas")]}, register)
    assert out[0].viable is False and "duplicate" in out[0].reason


def test_bad_regex_downgraded_not_fixed():
    out = finalize_candidates([cand("termometro")], {"candidates": [
        verdict("termometro", include=["(unclosed"])]}, register)
    assert out[0].viable is False and "compile" in out[0].reason


def test_bad_canonical_example_replaced_from_samples():
    out = finalize_candidates([cand("termometro")], {"candidates": [
        verdict("termometro", canonical_example="texto inventado sin el token")]},
        register)
    assert out[0].viable is True
    assert out[0].canonical_example == "termometro ejemplo uno"


def test_proposal_breaking_existing_categories_rejected():
    # \bsonda\w* would make SONDA FOLEY / NASOGASTRICA / ASPIRACION examples
    # ambiguous — the regression gate must reject it even though its own
    # canonical example ("SONDA DE ALIMENTACION") classifies cleanly.
    out = finalize_candidates(
        [cand("sonda", samples=["SONDA DE ALIMENTACION"])],
        {"candidates": [verdict("sonda", category_id="sondas",
                                canonical_example="SONDA DE ALIMENTACION")]},
        register)
    assert out[0].viable is False
    assert "existing categories ambiguous" in out[0].reason


def test_overlapping_proposal_with_no_clean_example_rejected():
    # include pattern collides with an existing category on every sample
    out = finalize_candidates(
        [cand("gasa", samples=["gasa esteril 10 x 10"])],
        {"candidates": [verdict("gasa", category_id="gasas_nuevas")]}, register)
    assert out[0].viable is False


# --- propose(): count cap vs. unlimited -----------------------------------

# Four fresh, uncovered families that each validate cleanly against the real
# register (their includes don't collide with existing categories).
_UNCOVERED = ["termometro", "escalpelo", "torniquete"]


class _FakeConn:
    """_fetch_token_samples only needs query() to yield {'text': ...} rows."""
    def query(self, _cypher, parameters=None):
        return [{"text": f"muestra distinta {n}"} for n in range(8)]


def _accept_all_vet(batch):
    return {"candidates": [verdict(c.token, category_id=f"{c.token}_nueva")
                           for c in batch]}


def _ranking(tokens):
    return [GroupStat(group=t, records=100, distinct_texts=8, spend_clp=1.0,
                      spend_share=0.01, cum_share=0.01) for t in tokens]


def test_count_caps_the_number_chosen():
    chosen, _ = propose(_FakeConn(), _ranking(_UNCOVERED), count=2,
                        min_samples=5, revisit=True, vet=_accept_all_vet,
                        log=lambda *_: None)
    assert len(chosen) == 2


def test_count_none_lifts_the_cap():
    chosen, _ = propose(_FakeConn(), _ranking(_UNCOVERED), count=None,
                        min_samples=5, revisit=True, vet=_accept_all_vet,
                        log=lambda *_: None)
    assert {c.token for c in chosen} == set(_UNCOVERED)
