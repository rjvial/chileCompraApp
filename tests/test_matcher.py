"""Unit tests for the matcher (resolve/matcher.py) — pure/offline."""
from __future__ import annotations

from chilecompra_er.resolve.profile import IdentityAttr, Packaging, Profile
from chilecompra_er.resolve.matcher import Verdict, cluster, same_product


def mk(category, attrs=(), model=None, conf="high", flags=(), is_product=True):
    return Profile(
        is_product=is_product, category=category,
        identity_attributes=tuple(IdentityAttr(n, v, v) for n, v in attrs),
        brand=None, model_token=model, packaging=Packaging(),
        base_unit="unidad", confidence=conf, flags=tuple(flags))


# --- same_product predicate (truth table) ------------------------------------

def test_different_category_is_different():
    assert same_product(mk("a"), mk("b")) is Verdict.DIFFERENT


def test_model_token_shortcut():
    assert same_product(mk("x", model="da4"), mk("x", model="da4")) is Verdict.SAME
    assert same_product(mk("x", model="da4"), mk("x", model="da12")) is Verdict.DIFFERENT


def test_conflicting_attribute_is_hard_cut():
    a = mk("foley", [("calibre", "16fr")])
    b = mk("foley", [("calibre", "18fr")])
    assert same_product(a, b) is Verdict.DIFFERENT


def test_equal_signature_is_same():
    a = mk("foley", [("calibre", "16fr"), ("material", "latex")])
    b = mk("foley", [("material", "latex"), ("calibre", "16fr")])
    assert same_product(a, b) is Verdict.SAME


def test_subset_is_compatible():
    coarse = mk("foley", [("calibre", "16fr")])
    fine = mk("foley", [("calibre", "16fr"), ("material", "latex")])
    assert same_product(coarse, fine) is Verdict.COMPATIBLE


def test_thin_bare_category_is_unknown():
    a = mk("desfibriladores", [], conf="low", flags=["below_min_info"])
    b = mk("desfibriladores", [], conf="low")
    assert same_product(a, b) is Verdict.UNKNOWN


def test_the_original_bug_cannot_merge():
    # cable gauge vs calcium — both carry "2.5" but in different categories.
    cable = mk("cordon_electrico", [("seccion", "2.5mm")])
    acid = mk("concentrado_acido", [("calcio", "2.5meq_l")])
    assert same_product(cable, acid) is Verdict.DIFFERENT


# --- clustering --------------------------------------------------------------

def test_identical_signatures_collapse():
    res = cluster([mk("foley", [("calibre", "16fr")]),
                   mk("foley", [("calibre", "16fr")])])
    assert len(res.clusters) == 1
    assert sorted(res.clusters[0].members) == [0, 1]


def test_model_token_merges_partial_specs():
    # same product, one bid omits the volume — the model_token rescues it.
    res = cluster([mk("dialisis", [("volumen", "5000ml")], model="da4"),
                   mk("dialisis", [], model="da4")])
    assert len(res.clusters) == 1
    c = res.clusters[0]
    assert sorted(c.members) == [0, 1]
    assert c.signature == "dialisis|volumen=5000ml"      # finest signature kept


def test_conflict_stays_separate():
    res = cluster([mk("foley", [("calibre", "16fr")]),
                   mk("foley", [("calibre", "18fr")])])
    assert len(res.clusters) == 2
    assert res.refines == []


def test_subset_makes_refines_edge_not_merge():
    res = cluster([mk("foley", [("calibre", "16fr")]),
                   mk("foley", [("calibre", "16fr"), ("material", "latex")])])
    assert len(res.clusters) == 2                          # strict default: not merged
    assert ("foley|calibre=16fr|material=latex", "foley|calibre=16fr") in res.refines


def test_ambiguous_partial_flagged_not_bridged():
    # a bare "foley 16fr" compatible with two divergent children must not bridge them.
    res = cluster([mk("foley", [("calibre", "16fr")]),
                   mk("foley", [("calibre", "16fr"), ("material", "latex")]),
                   mk("foley", [("calibre", "16fr"), ("material", "silicona")])])
    assert len(res.clusters) == 3
    coarse = next(c for c in res.clusters if c.signature == "foley|calibre=16fr")
    assert "ambiguous_partial" in coarse.flags
    assert any(r["type"] == "ambiguous_partial" for r in res.residue)


def test_attach_partials_when_unique_child():
    res = cluster([mk("foley", [("calibre", "16fr")]),
                   mk("foley", [("calibre", "16fr"), ("material", "latex")])],
                  attach_partials=True)
    assert len(res.clusters) == 1                          # coarse folded into unique child
    assert sorted(res.clusters[0].members) == [0, 1]
