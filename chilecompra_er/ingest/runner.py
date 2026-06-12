"""Batch resolution of retrieved source items.

Streams SourceItems through a Resolver, accumulating the coverage numbers the
design treats as headline metrics (§8): resolved vs. unresolved counts and
the basis mix — unresolved is visible debt, never silently dropped.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field

from ..resolve.resolver import ResolutionReport, Resolver
from .neo4j_source import SourceItem


@dataclass
class ResolutionStats:
    total: int = 0
    by_status: Counter = field(default_factory=Counter)
    by_category: Counter = field(default_factory=Counter)
    by_basis: Counter = field(default_factory=Counter)
    by_unresolved_reason: Counter = field(default_factory=Counter)
    resolved_without_attributes: int = 0  # anchored on a category root
    nodes_created: int = 0
    illegal_values: int = 0

    def summary(self) -> str:
        lines = [
            f"records processed : {self.total}",
            f"by status         : {dict(self.by_status)}",
            f"unresolved reasons: {dict(self.by_unresolved_reason)}",
            f"by category       : {dict(self.by_category)}",
            f"price basis mix   : {dict(self.by_basis)}",
            f"resolved w/o attrs: {self.resolved_without_attributes} "
            "(anchored on category roots — honest partials, no product info)",
            f"nodes created     : {self.nodes_created}",
            f"illegal values    : {self.illegal_values} (dropped, counted — schema dry-run metric)",
        ]
        return "\n".join(lines)


def resolve_items(resolver: Resolver, items: Iterable[SourceItem],
                  persist: bool = True,
                  collect_reports: bool = False) -> tuple[ResolutionStats, list[ResolutionReport]]:
    """Resolve a stream of source items. With persist=False nothing is
    written (no SourceRecord either) — profiling/dry-run mode."""
    stats = ResolutionStats()
    reports: list[ResolutionReport] = []

    for item in items:
        report = resolver.resolve(
            item.raw_text,
            source=item.ref if persist else None,
            price_fields={"total": item.total, "quantity": item.quantity,
                          "unit_price": item.unit_price},
        )
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
        if report.created:
            stats.nodes_created += 1
        if report.extraction is not None:
            stats.illegal_values += len(report.extraction.illegal)
        if collect_reports:
            reports.append(report)

    return stats, reports
