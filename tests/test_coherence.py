"""Unit tests for the coherence auditor offline tiers (chilecompra_er/coherence.py)."""
from __future__ import annotations

from chilecompra_er.coherence import audit_offline, check_clusters, check_profiles
from chilecompra_er.resolve.matcher import cluster
from chilecompra_er.resolve.profile import IdentityAttr, Packaging, Profile


def mk(cat, attrs=(), model=None):
    # attrs: (name, value, evidence)
    return Profile(True, cat,
                   tuple(IdentityAttr(n, v, e) for n, v, e in attrs),
                   None, model, Packaging(), "high", "high", ())


def _by_id(findings):
    return {f.id: f for f in findings}


# --- profile checks ----------------------------------------------------------

def test_clean_profiles_have_no_structural_breach():
    items = [("h0", mk("concentrado_acido",
                       [("calcio", "2.5meq_l", "2,5 mEqL de Ca")]))]
    f = _by_id(check_profiles(items))
    assert f["S1"].count == 0 and f["S2"].count == 0 and f["S8"].count == 0


def test_missing_evidence_trips_s1():
    items = [("h0", mk("foley", [("calibre", "16fr", "")]))]
    assert _by_id(check_profiles(items))["S1"].count == 1


def test_anchorless_evidence_trips_s2():
    # evidence is a bare number with no concept word — the regression guard.
    items = [("h0", mk("foley", [("concentracion", "2.5pct", "2,5")]))]
    f = _by_id(check_profiles(items))
    assert f["S2"].count == 1 and f["S2"].fail


def test_brand_in_identity_trips_s8():
    items = [("h0", mk("foley", [("marca", "bbraun", "bbraun")]))]
    assert _by_id(check_profiles(items))["S8"].count == 1


# --- cluster checks ----------------------------------------------------------

def test_clean_clusters_pass_structural():
    res = cluster([mk("foley", [("calibre", "16fr", "16 fr")]),
                   mk("foley", [("calibre", "16fr", "16 fr"), ("material", "latex", "latex")])])
    f = _by_id(check_clusters(res))
    assert f["S5"].count == 0 and f["S7"].count == 0


def test_single_bare_number_cluster_flagged_m1():
    res = cluster([mk("foley", [("calibre", "16", "16")])])   # value '16' is bare numeric
    assert _by_id(check_clusters(res))["M1"].count == 1


def test_ambiguous_partial_surfaced():
    res = cluster([mk("foley", [("calibre", "16fr", "x")]),
                   mk("foley", [("calibre", "16fr", "x"), ("material", "latex", "y")]),
                   mk("foley", [("calibre", "16fr", "x"), ("material", "silicona", "z")])])
    assert _by_id(check_clusters(res))["AMB"].count == 1


# --- end to end (offline) ----------------------------------------------------

def test_audit_offline_clean_set_has_zero_structural_failcount():
    items = [("h0", mk("foley", [("calibre", "16fr", "16 fr")])),
             ("h1", mk("agujas", [("calibre", "21g", "21 g")]))]
    findings = audit_offline(items)
    failcount = sum(f.count for f in findings if f.fail)
    assert failcount == 0
