"""Batch resolution of retrieved source items.

Streams SourceItems through a Resolver, accumulating the coverage numbers the
design treats as headline metrics (§8): resolved vs. unresolved counts and
the basis mix — unresolved is visible debt, never silently dropped.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from ..ambiguity import looks_like_bundle
from ..resolve.assignment import SourceRef, branded_product_id, plan_assignment
from ..resolve.brand import extract_brand
from ..resolve.classifier import CLASSIFIED
from ..resolve.extractor import extract
from ..resolve.resolver import UNSPSC_PREFIX, ResolutionReport, Resolver
from .neo4j_source import SourceItem


@dataclass
class ResolutionStats:
    total: int = 0
    by_status: Counter = field(default_factory=Counter)
    by_category: Counter = field(default_factory=Counter)
    by_basis: Counter = field(default_factory=Counter)
    by_unresolved_reason: Counter = field(default_factory=Counter)
    by_tier: Counter = field(default_factory=Counter)  # curated resolutions by
                                                        # winning tier (tier1/brand/tier2)
    resolved_without_attributes: int = 0  # anchored on a category root
    resolved_via_fallback: int = 0        # linked to a UNSPSC commodity bucket
    offers_bound: int = 0                 # offers tied via OFFERS edges to branded Products
    offer_routing: Counter = field(default_factory=Counter)  # offer-aware binding outcome
    nodes_created: int = 0
    illegal_values: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "by_status": dict(self.by_status),
            "by_category": dict(self.by_category),
            "by_basis": dict(self.by_basis),
            "by_unresolved_reason": dict(self.by_unresolved_reason),
            "by_tier": dict(self.by_tier),
            "resolved_without_attributes": self.resolved_without_attributes,
            "resolved_via_fallback": self.resolved_via_fallback,
            "offers_bound": self.offers_bound,
            "offer_routing": dict(self.offer_routing),
            "nodes_created": self.nodes_created,
            "illegal_values": self.illegal_values,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ResolutionStats":
        return cls(
            total=d.get("total", 0),
            by_status=Counter(d.get("by_status", {})),
            by_category=Counter(d.get("by_category", {})),
            by_basis=Counter(d.get("by_basis", {})),
            by_unresolved_reason=Counter(d.get("by_unresolved_reason", {})),
            by_tier=Counter(d.get("by_tier", {})),
            resolved_without_attributes=d.get("resolved_without_attributes", 0),
            resolved_via_fallback=d.get("resolved_via_fallback", 0),
            offers_bound=d.get("offers_bound", 0),
            offer_routing=Counter(d.get("offer_routing", {})),
            nodes_created=d.get("nodes_created", 0),
            illegal_values=d.get("illegal_values", 0),
        )

    def summary(self) -> str:
        resolved = self.by_status.get("resolved_generic", 0)
        curated = resolved - self.resolved_via_fallback
        linked_pct = f"{resolved / self.total:.1%}" if self.total else "n/a"
        lines = [
            f"records processed : {self.total}",
            f"items linked      : {resolved} ({linked_pct})  "
            f"= curated {curated} + UNSPSC-fallback {self.resolved_via_fallback}",
            f"by status         : {dict(self.by_status)}",
            f"unresolved reasons: {dict(self.by_unresolved_reason)}",
            f"by category       : {dict(self.by_category)}",
            f"curated by tier   : {dict(self.by_tier)} "
            "(which classifier rescued each curated item — tier2's marginal lift)",
            f"price basis mix   : {dict(self.by_basis)}",
            f"resolved w/o attrs: {self.resolved_without_attributes} "
            "(anchored on category roots — honest partials, no product info)",
            f"offers bound      : {self.offers_bound} (as OFFERS edges to branded Products)",
            f"offer routing     : {dict(self.offer_routing)} "
            "(same=item node, refined=finer same-family, recategorized=own family, "
            "conservative=kept on item)",
            f"nodes created     : {self.nodes_created}",
            f"illegal values    : {self.illegal_values} (dropped, counted — schema dry-run metric)",
        ]
        return "\n".join(lines)


_TIER2_BATCH = 2000  # chunk size for batched Tier-2 priming (see _prime_ahead)


def _item_texts(item):
    """Every raw text an item gets classified on, across all modes: the buyer/
    offer line (raw_text), the tender title (item mode), the buyer line (joint
    mode), and each offer description. Keys absent for a given mode yield None
    and are filtered by the caller — so priming covers whatever the resolver
    will classify, never leaving a cold cache."""
    yield item.raw_text
    yield item.extra.get("tender_text")
    yield item.extra.get("buyer_text")  # joint mode's secondary text
    for o in (item.extra.get("offers") or []):
        yield o.get("text")


def _prime_ahead(items, resolver, batch: int = _TIER2_BATCH):
    """Yield items unchanged, but in chunks: before releasing a chunk, batch-prime
    the classifier's Tier-2 cache on EVERY normalized text the chunk will classify
    (buyer + tender + offers — one predict_proba for the lot instead of one call
    per text). The consumer then resolves the chunk against a warm cache.
    Passthrough if the classifier has no prime() (Tier-1-only runs)."""
    primer = getattr(resolver.classifier, "prime", None)
    if primer is None:
        yield from items
        return
    norm = resolver.normalizer

    def prime(buf):
        primer([norm(t) for b in buf for t in _item_texts(b) if t])

    buf: list = []
    for item in items:
        buf.append(item)
        if len(buf) >= batch:
            prime(buf)
            yield from buf
            buf = []
    if buf:
        prime(buf)
        yield from buf


def resolve_items(resolver: Resolver, items: Iterable[SourceItem],
                  persist: bool = True,
                  collect_reports: bool = False,
                  progress: Callable[[ResolutionStats], None] | None = None,
                  progress_every: int = 200,
                  on_report: Callable[[ResolutionReport], None] | None = None,
                  stats: ResolutionStats | None = None,
                  joint: bool = False,
                  item_mode: bool = False,
                  fallback: str = "none") -> tuple[ResolutionStats, list[ResolutionReport]]:
    """Resolve a stream of source items. With persist=False nothing is
    written (no SourceRecord either) — profiling/dry-run mode.

    `on_report`, if given, is called with each report as it is produced (the
    streaming/partial-save hook). `progress`, if given, is called with the
    live stats every `progress_every` records (and once at the end), so long
    streamed runs can report incremental coverage instead of going silent
    until done. `stats` seeds the accumulator (resumed runs pass the
    checkpoint's cumulative stats so counts and progress stay cumulative)."""
    stats = stats if stats is not None else ResolutionStats()
    reports: list[ResolutionReport] = []

    # Batch-prime the statistical tier (Tier-2) per chunk: buffer items, predict
    # the chunk's texts in one call (~150x cheaper than per-item), then yield them
    # so the resolve loop's classify() calls hit the warm cache. No-op (passthrough)
    # when the classifier has no prime() — e.g. a Tier-1-only build run.
    items = _prime_ahead(items, resolver)

    for item in items:
        price_fields = {"total": item.total, "quantity": item.quantity,
                        "unit_price": item.unit_price}
        src = item.ref if persist else None
        if item_mode:
            report = resolver.resolve_item(
                buyer_text=item.raw_text,
                tender_text=item.extra.get("tender_text"),
                offers=item.extra.get("offers"),
                unspsc=item.unspsc,
                fallback=fallback,
                source=src,
                price_fields=price_fields,
            )
        elif joint:
            report = resolver.resolve_joint(
                offer_text=item.raw_text,
                buyer_text=item.extra.get("buyer_text"),
                source=src,
                price_fields=price_fields,
                awarded=item.extra.get("awarded"),
            )
        else:
            report = resolver.resolve(item.raw_text, source=src,
                                      price_fields=price_fields,
                                      context_text=item.extra.get("tender_text"))
        stats.total += 1
        stats.by_status[report.status] += 1
        if report.unresolved_reason:
            stats.by_unresolved_reason[report.unresolved_reason] += 1
        if report.status == "resolved_generic" and report.extraction is not None \
                and not report.extraction.values:
            stats.resolved_without_attributes += 1
        if report.classification.category_id:
            stats.by_category[report.classification.category_id] += 1
        if report.price_basis is not None:
            stats.by_basis[report.price_basis.basis] += 1
        if report.evidence.get("category_source") == "unspsc_fallback":
            stats.resolved_via_fallback += 1
        elif report.status == "resolved_generic":
            # curated (non-fallback) resolution: credit the winning classifier tier
            stats.by_tier[report.classification.tier] += 1
        # Bind the item's offers to its single resolved node (the intra-item
        # invariant). Counted always (coverage metric); written only when
        # persisting — as OFFERS edges to branded (Brand × generic) Products.
        if item_mode and report.status == "resolved_generic" and report.node_id:
            offers = item.extra.get("offers") or []
            stats.offers_bound += len(offers)
            if persist:
                _bind_offers(resolver, item.ref, offers, report, stats)
        if report.created:
            stats.nodes_created += 1
        if report.extraction is not None:
            stats.illegal_values += len(report.extraction.illegal)
        if on_report is not None:
            on_report(report)
        if collect_reports:
            reports.append(report)
        if progress is not None and stats.total % progress_every == 0:
            progress(stats)

    if progress is not None and stats.total and stats.total % progress_every != 0:
        progress(stats)  # final partial batch
    return stats, reports


def _descriptive_values(schema, ext) -> dict:
    """The DESCRIPTIVE (non-identity) subset of an extraction's values."""
    names = {a.name for a in schema.attribute_defs if not a.is_identity}
    return {k: v for k, v in ext.values.items() if k in names}


def _assign_generic(resolver, schema, values: dict) -> str:
    """Find-or-create the GenericProduct for `values` (the offer's own identity)
    and return its id. plan_assignment parents finer nodes under coarser ones via
    PARENT_OF, so a refinement of the item's generic lands as its child."""
    existing = resolver.catalog.load(schema.category_id, schema)
    plan = plan_assignment(schema, values, existing)
    resolver.catalog.apply(plan, schema)
    return plan.home_id


def _offer_target(resolver, text: str, item_gid: str, item_cat: str | None,
                  item_idv: dict, item_fallback: bool, item_schema):
    """Resolve an offer to the generic its OWN text supports (offer-aware
    binding). Returns (generic_id, conforming, descriptive, outcome):

      same          -> offer adds no new identity; inherit the item's node
      refined        -> offer is more specific (Case A); its finer node, PARENT_OF g
      recategorized  -> offer is a different family (Case B); its own node, conforming=False
      conservative   -> unclassified/ambiguous/bundle/low-confidence; keep on item node
    """
    norm = resolver.normalizer(text) if text else ""
    # No usable text, or a UNSPSC-fallback item (no schema identity to compare):
    # keep the offer on the item's node, as before.
    if not norm or item_fallback or item_schema is None:
        desc = _descriptive_values(item_schema, extract(norm, item_schema)) \
            if (item_schema is not None and norm) else {}
        return item_gid, True, desc, ("item_fallback" if item_fallback else "conservative")

    cls = resolver.classifier.classify(norm)
    # Unclassified / ambiguous / multi-product bundle: ambiguity is the right
    # answer — never invent a target, bind to the item's node.
    if cls.status != CLASSIFIED or looks_like_bundle(norm):
        desc = _descriptive_values(item_schema, extract(norm, item_schema))
        return item_gid, True, desc, "conservative"

    off_schema = resolver.schema(cls.category_id)
    off_ext = extract(norm, off_schema)
    descriptive = _descriptive_values(off_schema, off_ext)
    off_idv = off_ext.identity_values

    if cls.category_id == item_cat:
        # Same family. A subset of the item's identity is just a vaguer wording of
        # the same product -> inherit the item node; otherwise it's more specific
        # (or diverges) and earns its own (finer) node under the item generic.
        if all(item_idv.get(k) == v for k, v in off_idv.items()):
            return item_gid, True, descriptive, "same"
        return _assign_generic(resolver, off_schema, off_ext.values), True, descriptive, "refined"

    # Different family (Case B). Trust only a high-precision Tier-1 verdict to move
    # an offer off the item's family; a statistical (tier2) guess stays conservative.
    if getattr(cls, "tier", "tier1") == "tier1":
        return _assign_generic(resolver, off_schema, off_ext.values), False, descriptive, "recategorized"
    desc = _descriptive_values(item_schema, extract(norm, item_schema))
    return item_gid, True, desc, "conservative"


def _bind_offers(resolver, item_ref: SourceRef, offers: list[dict],
                 report: ResolutionReport, stats=None) -> None:
    """Bind an item's offers into the branded catalog — offer-aware: each offer
    resolves to the GenericProduct its OWN text supports (equal/finer/own-family),
    falling back to the item's node when the offer is vague or ambiguous. For each
    offer: extract its brand, upsert the deduped :Product = Brand × that-generic,
    and add the (:Oferta)-[:OFFERS {price…, conforming}]->(:Product) edge. The
    `conforming` flag is False when the offer was bound to a *different* family
    than the item requested (Case B) — a non-conforming / component bid."""
    catalog = resolver.catalog
    item_gid = report.node_id
    item_cat = report.classification.category_id
    item_fallback = bool(item_cat) and item_cat.startswith(UNSPSC_PREFIX)
    item_idv = report.extraction.identity_values if report.extraction is not None else {}
    item_schema = resolver.schema(item_cat) if (item_cat and not item_fallback) else None

    for o in offers:
        oid = o.get("offer_id")
        if oid is None:
            continue
        text = o.get("text") or ""
        brand_id, brand_name, _ = extract_brand(text, resolver.brand_map)
        target_gid, conforming, descriptive, outcome = _offer_target(
            resolver, text, item_gid, item_cat, item_idv, item_fallback, item_schema)
        if stats is not None:
            stats.offer_routing[outcome] += 1
        pid = branded_product_id(target_gid, brand_id)
        catalog.merge_branded_product(target_gid, brand_id, brand_name)
        price = {k: v for k, v in {
            "unit_price": o.get("unit_price"), "total_clp": o.get("total_clp"),
            "quantity": o.get("quantity"), "awarded": bool(o.get("awarded")),
            "currency": o.get("currency"), "date": o.get("date"),
            "supplier_text": text,
        }.items() if v is not None}
        price["conforming"] = conforming
        # This offer's own descriptive attributes ride its OFFERS edge (accurate
        # per-offer), instead of being smeared/frozen onto the shared Product node.
        price.update(descriptive)
        catalog.link_offer(oid, pid, price)
