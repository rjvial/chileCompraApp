"""Coherence auditor (design: the redesign, "coherence-check").

"You can't keep coherent what you can't detect drift in." Three tiers of named
invariants over the canonicalize profiles + clusters (+ the persisted graph):

- STRUCTURAL (must be 0; a breach is a bug → CI-gate exit code): the contract.
- SEMANTIC  (ranked review backlogs, expected nonzero): what structure can't catch.
- HEALTH    (distributions; trend, not pass/fail): catch a regression early.

Each invariant maps to a failure mode — imputation (S1/S2), false merge (S5/S6/
S8/M1/M4), false split (S7/M6). The offline tier runs over the profile store +
recomputed clusters (no graph, no LLM); the graph tier queries the persisted
:ProductCluster / :Product / :OFFERS catalog when it exists.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .categories.schema import _is_anchorless_pattern
from .resolve.matcher import MatchResult, cluster

# attribute names that must never carry identity (brand/packaging axes)
_NON_IDENTITY = {"brand", "marca", "packaging", "pack", "envase", "caja",
                 "unidad_empaque", "cantidad_empaque"}
_NUM_LEADING = re.compile(r"^\d")
# a value "carries a unit" if it has a letter, ratio slash, or unit symbol
_VALUE_ANCHOR = re.compile(r"[^\W\d_]|[/%°ºµμΩ]")


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def weak_evidence(value: str, evidence: str) -> bool:
    """True only for a genuinely ungrounded bare-number identity: the evidence is
    anchorless AND the value carries no unit/ratio (so it's not 70pct / 12fr / 6/0)
    AND the evidence digits don't even equal the value (so it isn't a self-grounding
    code like a bur size 018). Otherwise the identity is well-formed even if the
    quoted evidence span is narrow — that's a prompt nicety, not a structural breach."""
    if not _is_anchorless_pattern(evidence):
        return False
    if _VALUE_ANCHOR.search(value or ""):
        return False
    return _digits(evidence) != _digits(value)


@dataclass
class Finding:
    id: str                 # S1, S2, M1, …
    tier: str               # structural | semantic | health
    title: str
    count: int
    fail: bool              # structural gate: a nonzero count fails the run
    examples: list = field(default_factory=list)


# --- offline: over the canonicalize profiles ----------------------------------

def check_profiles(items: list[tuple[str, object]]) -> list[Finding]:
    s1, s2, s8 = [], [], []
    for h, p in items:
        for a in p.identity_attributes:
            ev = (a.evidence or "").strip()
            if not ev:
                s1.append({"hash": h[:12], "attr": a.name, "value": a.value})
            elif weak_evidence(a.value, ev):
                s2.append({"hash": h[:12], "attr": a.name, "value": a.value,
                           "evidence": a.evidence})
            if a.name in _NON_IDENTITY:
                s8.append({"hash": h[:12], "attr": a.name})
    return [
        Finding("S1", "structural", "identity attribute with no evidence",
                len(s1), True, s1[:10]),
        Finding("S2", "structural", "ungrounded bare-number identity (value has no "
                "unit and evidence doesn't ground it)", len(s2), True, s2[:10]),
        Finding("S8", "structural", "brand/packaging leaked into identity",
                len(s8), True, s8[:10]),
    ]


# --- offline: over the clusters -----------------------------------------------

def check_clusters(result: MatchResult) -> list[Finding]:
    # S5 signature uniqueness
    seen: set[str] = set()
    dups = [c.signature for c in result.clusters
            if c.signature in seen or seen.add(c.signature)]
    # S7 REFINES is a strict-subset DAG (coarser ⊂ finer; no self-loops)
    pairs_by_sig = {c.signature: c.pairs for c in result.clusters}
    bad_ref = []
    for finer, coarser in result.refines:
        pf, pc = pairs_by_sig.get(finer), pairs_by_sig.get(coarser)
        if finer == coarser or (pf is not None and pc is not None and not (pc < pf)):
            bad_ref.append({"finer": finer, "coarser": coarser})
    # M1 single weak (bare-number) attribute cluster
    m1 = [c.signature for c in result.clusters
          if len(c.pairs) == 1 and _NUM_LEADING.match(next(iter(c.pairs))[1])]
    # M4 / ambiguous from residue
    m4 = [r for r in result.residue if r["type"] == "model_token_conflict"]
    amb = [r for r in result.residue if r["type"] == "ambiguous_partial"]
    return [
        Finding("S5", "structural", "duplicate cluster signature", len(dups), True, dups[:10]),
        Finding("S7", "structural", "REFINES edge not strict-subset / self-loop",
                len(bad_ref), True, bad_ref[:10]),
        Finding("M1", "semantic", "single bare-number-attribute cluster (weak identity)",
                len(m1), False, m1[:10]),
        Finding("M4", "semantic", "model-token merge with attribute conflict",
                len(m4), False, m4[:10]),
        Finding("AMB", "semantic", "ambiguous partial (coarse spec, ≥2 divergent children)",
                len(amb), False, amb[:10]),
    ]


def health_metrics(items, result: MatchResult) -> list[Finding]:
    confidence: dict[str, int] = {}
    is_product = below = 0
    for _h, p in items:
        confidence[p.confidence] = confidence.get(p.confidence, 0) + 1
        is_product += int(p.is_product)
        below += int("below_min_info" in p.flags)
    members = sum(len(c.members) for c in result.clusters)
    return [
        Finding("H-confidence", "health", f"canonicalize confidence mix {confidence}", 0, False),
        Finding("H-product", "health",
                f"is_product {is_product}/{len(items)}, below_min_info {below}", 0, False),
        Finding("H-clusters", "health",
                f"{len(result.clusters):,} clusters over {members:,} clustered profiles "
                f"(of {len(items):,})", 0, False),
    ]


def audit_offline(items) -> list[Finding]:
    """All offline tiers: recompute clusters from the profiles, run every check."""
    profiles = [p for _h, p in items]
    result = cluster(profiles)
    return [*check_profiles(items), *check_clusters(result), *health_metrics(items, result)]


# --- graph tier (runs against the persisted :ProductCluster catalog) -----------

def check_graph(conn) -> list[Finding]:
    """Invariants only the persisted graph can answer. (S6 intra-cluster conflict
    is impossible by construction — one signature per cluster — so it is enforced,
    not checked.)"""
    def n(q):
        rows = conn.query(q)
        return int(rows[0]["n"]) if rows else 0

    s9 = n("MATCH (c:ProductCluster) WHERE NOT (c)<-[:VARIANT_OF]-(:Product) "
           "RETURN count(c) AS n")
    s4 = n("MATCH (o:Oferta)-[r:OFFERS]->(:Product) WITH o, count(r) AS k "
           "WHERE k > 1 RETURN count(o) AS n")
    placed = n("MATCH (:Oferta)-[:OFFERS]->(:Product) RETURN count(*) AS n")
    total = n("MATCH (o:Oferta) WHERE o.descripcion_proveedor IS NOT NULL RETURN count(o) AS n")
    unplaced = total - placed
    # M2 price incoherence: clusters whose normalized price has a high spread.
    m2 = conn.query(
        """
        MATCH (:Oferta)-[e:OFFERS]->(:Product)-[:VARIANT_OF]->(c:ProductCluster)
        WHERE e.normalized_price IS NOT NULL
        WITH c, count(e) AS n, avg(e.normalized_price) AS mean, stDev(e.normalized_price) AS sd
        WHERE n >= 5 AND mean > 0 AND sd / mean > 1.0
        RETURN c.signature AS signature, n, sd / mean AS cv
        ORDER BY cv * n DESC LIMIT 10
        """)
    return [
        Finding("S9", "structural", "orphan cluster (no Product)", s9, True),
        Finding("S4", "structural", "offer bound to more than one Product", s4, True),
        Finding("S10", "semantic", "unplaced offers (no cluster)", unplaced, False,
                [f"{unplaced:,}/{total:,}"]),
        Finding("M2", "semantic", "price-incoherent cluster (CV>1, n≥5)", len(m2), False,
                [dict(r) for r in m2]),
    ]
