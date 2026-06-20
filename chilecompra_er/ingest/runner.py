"""Batch resolution of retrieved source items.

Streams SourceItems through a Resolver, accumulating the coverage numbers the
design treats as headline metrics (§8): resolved vs. unresolved counts and
the basis mix — unresolved is visible debt, never silently dropped.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from ..resolve.assignment import (
    PRODUCT_LABEL,
    STATUS_PRODUCT,
    SourceRef,
    product_id_for,
)
from ..resolve.resolver import EXTRACTOR_VERSION, ResolutionReport, Resolver
from .neo4j_source import SOURCE_OFFER, SourceItem


@dataclass
class ResolutionStats:
    total: int = 0
    by_status: Counter = field(default_factory=Counter)
    by_category: Counter = field(default_factory=Counter)
    by_basis: Counter = field(default_factory=Counter)
    by_unresolved_reason: Counter = field(default_factory=Counter)
    resolved_without_attributes: int = 0  # anchored on a category root
    resolved_via_fallback: int = 0        # linked to a UNSPSC commodity bucket
    offers_bound: int = 0                 # offers tied to their item's node as :Product
    nodes_created: int = 0
    illegal_values: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "by_status": dict(self.by_status),
            "by_category": dict(self.by_category),
            "by_basis": dict(self.by_basis),
            "by_unresolved_reason": dict(self.by_unresolved_reason),
            "resolved_without_attributes": self.resolved_without_attributes,
            "resolved_via_fallback": self.resolved_via_fallback,
            "offers_bound": self.offers_bound,
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
            resolved_without_attributes=d.get("resolved_without_attributes", 0),
            resolved_via_fallback=d.get("resolved_via_fallback", 0),
            offers_bound=d.get("offers_bound", 0),
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
            f"price basis mix   : {dict(self.by_basis)}",
            f"resolved w/o attrs: {self.resolved_without_attributes} "
            "(anchored on category roots — honest partials, no product info)",
            f"offers bound      : {self.offers_bound} (as :Product variants under their item's node)",
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
        # Bind the item's offers to its single resolved node (the intra-item
        # invariant). Counted always (coverage metric); written only when
        # persisting — as :Product variants carrying the offer's price point.
        if item_mode and report.status == "resolved_generic" and report.node_id:
            offers = item.extra.get("offers") or []
            stats.offers_bound += len(offers)
            if persist:
                _bind_offers(resolver.catalog, item.ref, offers, report.node_id)
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


def _bind_offers(catalog, item_ref: SourceRef, offers: list[dict],
                 generic_id: str) -> None:
    """Write each of an item's offers as a :Product VARIANT_OF the item's one
    GenericProduct, plus the offer's own :SourceRecord -[:RESOLVED_TO]-> Product
    (the price point). Keyed by the offer's stable record id; derived from the
    item ref (tender + item) + the offer id."""
    for o in offers:
        oid = o.get("offer_id")
        if oid is None:
            continue
        offer_ref = SourceRef(SOURCE_OFFER, item_ref.tender_id,
                              f"{item_ref.line_no}:{oid}", o.get("text") or "")
        pid = product_id_for(offer_ref.record_key)
        props = {k: v for k, v in {
            "tender_id": item_ref.tender_id, "item_id": item_ref.line_no,
            "offer_id": str(oid),
            "unit_price": o.get("unit_price"), "total_clp": o.get("total_clp"),
            "quantity": o.get("quantity"), "awarded": bool(o.get("awarded")),
            "currency": o.get("currency"), "date": o.get("date"),
            # The Product is the price-point node, so it carries the supplier's
            # actual description (readable) — not the hash (that's the
            # SourceRecord's thin-reference job).
            "supplier_text": o.get("text"),
        }.items() if v is not None}
        catalog.bind_product(pid, generic_id, props)
        catalog.persist_resolution(offer_ref, STATUS_PRODUCT, pid,
                                   target_label=PRODUCT_LABEL,
                                   extractor_version=EXTRACTOR_VERSION)
