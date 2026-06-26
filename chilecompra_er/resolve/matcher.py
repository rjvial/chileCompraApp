"""matcher — deterministic same-product decision + clustering over canonicalize profiles
(design: the redesign, "match").

The heart is the pairwise predicate `same_product(a, b)`. Clustering is built on
top of it: identical canonical signatures seed clusters, a shared model_token is
a sufficient merge (even cross-brand), a conflicting attribute is a hard cut, and
partial (subset) specs are linked by a REFINES hierarchy rather than greedily
merged — so a vague bid can never bridge two real products into one.

Pure and offline: no graph, no LLM. Persistence (ingest/clusters.py) and the
residual-pair adjudication (resolve/adjudicate.py = adjudicate) sit on top of this.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .profile import Profile


class Verdict(str, Enum):
    SAME = "same"               # definitely the same product
    DIFFERENT = "different"     # hard separation (conflict / different model / block)
    COMPATIBLE = "compatible"   # one spec ⊆ the other, no conflict — same branch, not a merge
    UNKNOWN = "unknown"         # both too thin to decide on text alone (defer to adjudicate / same-item)


def pairs(p: Profile) -> frozenset[tuple[str, str]]:
    """The identity name=value set (brand and packaging excluded by construction)."""
    return frozenset((a.name, a.value) for a in p.identity_attributes)


def attributes_conflict(a: Profile, b: Profile) -> bool:
    """True if a and b assign DIFFERENT values to a shared attribute name —
    the hard-cut signal (16fr vs 18fr). The guard against false merges."""
    av = {x.name: x.value for x in a.identity_attributes}
    bv = {x.name: x.value for x in b.identity_attributes}
    return any(av[k] != bv[k] for k in (av.keys() & bv.keys()))


def _thin(p: Profile) -> bool:
    return not p.identity_attributes and (p.confidence == "low"
                                          or "below_min_info" in p.flags)


def same_product(a: Profile, b: Profile) -> Verdict:
    """The pairwise decision (design match, Step C). Order matters: block, then the
    model-token shortcut, then the conflict hard-cut, then signature comparison."""
    if a.category != b.category:
        return Verdict.DIFFERENT
    if a.model_token and b.model_token:
        return Verdict.SAME if a.model_token == b.model_token else Verdict.DIFFERENT
    if attributes_conflict(a, b):
        return Verdict.DIFFERENT
    pa, pb = pairs(a), pairs(b)
    if pa == pb:
        if not pa and (_thin(a) or _thin(b)):
            return Verdict.UNKNOWN          # bare category + thin → can't merge on text
        return Verdict.SAME
    if pa < pb or pb < pa:
        return Verdict.COMPATIBLE           # one strictly finer; same branch
    return Verdict.COMPATIBLE               # disjoint axes, no conflict — route to adjudicate/attach


# --- clustering ---------------------------------------------------------------

@dataclass
class Cluster:
    signature: str                          # the cluster's (finest) canonical signature
    category: str
    pairs: frozenset[tuple[str, str]]
    members: list[int] = field(default_factory=list)   # indices into the input profiles
    model_tokens: set[str] = field(default_factory=set)
    flags: set[str] = field(default_factory=set)


@dataclass
class MatchResult:
    clusters: list[Cluster]
    refines: list[tuple[str, str]]          # (finer_signature, coarser_signature)
    residue: list[dict]                     # cases to route to adjudicate (adjudication)


class _UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        self.parent[self.find(a)] = self.find(b)


# Above this many distinct signatures in one category, skip the O(n^3) REFINES /
# ambiguous-partial computation for that block (the clusters themselves are still
# produced — only the auxiliary hierarchy is skipped) so a pathological category
# can't hang the run.
_MAX_HIERARCHY_BLOCK = 1500


def cluster(profiles: list[Profile], *, attach_partials: bool = False,
            log=lambda _m: None) -> MatchResult:
    """Group profiles into product clusters (the comparison units).

    `attach_partials` is the strictness dial (design match, Step D / open decision):
    when False (default, conservative) a coarse partial spec stays its own cluster
    linked by REFINES; when True it merges into a finer cluster *iff* that finer
    cluster is its unique immediate child. Tune it on same-item labels.
    """
    # 1. seed clusters by exact signature (within a category by construction).
    by_sig: dict[str, Cluster] = {}
    for i, p in enumerate(profiles):
        if not p.is_product:
            continue
        sig = p.signature()
        c = by_sig.get(sig)
        if c is None:
            c = by_sig[sig] = Cluster(signature=sig, category=p.category,
                                      pairs=pairs(p))
        c.members.append(i)
        if p.model_token:
            c.model_tokens.add(p.model_token)

    seeds = list(by_sig.values())
    index = {c.signature: k for k, c in enumerate(seeds)}
    uf = _UnionFind(len(seeds))
    residue: list[dict] = []

    # 2. model_token merge (a shared model is a sufficient match, even cross-brand).
    by_token: dict[tuple[str, str], list[int]] = {}
    for k, c in enumerate(seeds):
        for tok in c.model_tokens:
            by_token.setdefault((c.category, tok), []).append(k)
    for (cat, tok), ks in by_token.items():
        for k in ks[1:]:
            a, b = seeds[ks[0]], seeds[k]
            if attributes_conflict_pairs(a.pairs, b.pairs):
                residue.append({"type": "model_token_conflict", "category": cat,
                                "model_token": tok,
                                "signatures": [a.signature, b.signature]})
            uf.union(ks[0], k)

    # 3. collapse union-find groups into merged clusters; signature = finest seed.
    groups: dict[int, list[int]] = {}
    for k in range(len(seeds)):
        groups.setdefault(uf.find(k), []).append(k)
    merged: list[Cluster] = []
    for ks in groups.values():
        members = [m for k in ks for m in seeds[k].members]
        toks = set().union(*(seeds[k].model_tokens for k in ks))
        finest = max((seeds[k] for k in ks), key=lambda c: len(c.pairs))
        merged.append(Cluster(signature=finest.signature, category=finest.category,
                              pairs=finest.pairs, members=members, model_tokens=toks))

    # 4. REFINES hierarchy + partial handling, within each category block.
    refines: list[tuple[str, str]] = []
    by_cat: dict[str, list[Cluster]] = {}
    for c in merged:
        by_cat.setdefault(c.category, []).append(c)

    keep: list[Cluster] = []
    for cat, cs in by_cat.items():
        if len(cs) > _MAX_HIERARCHY_BLOCK:
            # oversized block: keep the clusters, skip the O(n^3) hierarchy pass.
            log(f"  hierarchy skipped for {cat}: {len(cs):,} signatures "
                f"(> {_MAX_HIERARCHY_BLOCK})")
            keep.extend(cs)
            continue
        # immediate finer children of each cluster (minimal proper supersets)
        children: dict[str, list[Cluster]] = {c.signature: [] for c in cs}
        for coarse in cs:
            supers = [f for f in cs if coarse.pairs < f.pairs]
            for f in supers:
                if not any(f.pairs > m.pairs > coarse.pairs for m in supers):
                    children[coarse.signature].append(f)
                    refines.append((f.signature, coarse.signature))
        absorbed: set[str] = set()
        for coarse in cs:
            kids = children[coarse.signature]
            if len(kids) >= 2:
                coarse.flags.add("ambiguous_partial")
                residue.append({"type": "ambiguous_partial",
                                "category": cat, "signature": coarse.signature,
                                "children": [k.signature for k in kids]})
            elif len(kids) == 1 and coarse.pairs and attach_partials:
                kids[0].members.extend(coarse.members)   # attach into the unique child
                absorbed.add(coarse.signature)
        keep.extend(c for c in cs if c.signature not in absorbed)

    return MatchResult(clusters=keep, refines=refines, residue=residue)


def attributes_conflict_pairs(a: frozenset[tuple[str, str]],
                              b: frozenset[tuple[str, str]]) -> bool:
    av, bv = dict(a), dict(b)
    return any(av[k] != bv[k] for k in (av.keys() & bv.keys()))
