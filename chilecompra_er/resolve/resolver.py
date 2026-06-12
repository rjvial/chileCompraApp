"""Resolution of one description — the strictly ordered steps of design §7.

(1) normalize -> (2) classify -> (3) extract -> (4) price basis ->
(5) find home node -> (6) create if absent (computing parent) ->
(7) persist :SourceRecord + versioned :RESOLVED_TO.

Each step gates the next; an unclassified description stops at (2) and is
persisted as explicitly unresolved. Branded (:Product) resolution is an M3
workstream — it needs the written "branded enough" rule first.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .. import __version__
from ..categories.schema import CategorySchema, load_register, schema_for
from ..normalize import Normalizer
from ..price.basis import PriceBasis, cross_check, infer_basis
from .assignment import (
    STATUS_GENERIC,
    STATUS_UNRESOLVED,
    SourceRef,
    plan_assignment,
)
from .classifier import CLASSIFIED, Classification, Tier1Classifier
from .extractor import Extraction, extract

EXTRACTOR_VERSION = f"chilecompra_er/{__version__}"


@dataclass
class ResolutionReport:
    raw_text: str
    normalized: str
    classification: Classification
    status: str  # resolved_generic | unresolved
    unresolved_reason: str | None = None
    extraction: Extraction | None = None
    price_basis: PriceBasis | None = None
    node_id: str | None = None
    identity_key: str | None = None
    created: bool = False
    parent_id: str | None = None
    is_complete: bool | None = None
    schema_version: str | None = None
    evidence: dict = field(default_factory=dict)


class Resolver:
    def __init__(self, catalog, normalizer: Normalizer | None = None,
                 classifier: Tier1Classifier | None = None, register: dict | None = None):
        self.catalog = catalog
        self.normalizer = normalizer or Normalizer()
        self.register = register or load_register()
        self.classifier = classifier or Tier1Classifier(self.register)
        self._schemas: dict[str, CategorySchema] = {}

    def schema(self, category_id: str) -> CategorySchema:
        if category_id not in self._schemas:
            self._schemas[category_id] = schema_for(category_id, self.register)
        return self._schemas[category_id]

    def resolve(self, raw_text: str, source: SourceRef | None = None,
                price_fields: dict | None = None) -> ResolutionReport:
        # UNSPSC rubric paths ("Equipamiento y suministros medicos / ... /
        # Vendas...") are taxonomy strings, not product descriptions: the
        # family word in them is the taxonomy's, not the buyer's. Routing
        # them into a category root would inflate it with zero-information
        # records — explicit unresolved instead (visible debt, design §8).
        if raw_text.count(" / ") >= 2:
            normalized = self.normalizer(raw_text)
            report = ResolutionReport(
                raw_text=raw_text,
                normalized=normalized,
                classification=Classification(None, "unclassified"),
                status=STATUS_UNRESOLVED,
                unresolved_reason="boilerplate_rubric",
            )
            if source is not None:
                self.catalog.persist_resolution(source, STATUS_UNRESOLVED, None)
            return report

        normalized = self.normalizer(raw_text)
        classification = self.classifier.classify(normalized)

        if classification.status != CLASSIFIED:
            report = ResolutionReport(
                raw_text=raw_text,
                normalized=normalized,
                classification=classification,
                status=STATUS_UNRESOLVED,
                unresolved_reason=classification.status,
            )
            if source is not None:
                self.catalog.persist_resolution(source, STATUS_UNRESOLVED, None)
            return report

        schema = self.schema(classification.category_id)
        extraction = extract(normalized, schema)
        basis = infer_basis(normalized)
        if price_fields:
            # Arithmetic cross-check (design §6): total ~= qty x price may
            # promote the basis — auditably, recorded in the evidence trail.
            promoted = cross_check(
                price_fields.get("total") or 0,
                price_fields.get("quantity") or 0,
                price_fields.get("unit_price") or 0,
                pack_size=basis.pack_size,
            )
            if promoted and promoted != basis.basis:
                basis.evidence.append({"cross_check_promoted": promoted})
                basis.basis = promoted

        existing = self.catalog.load(schema.category_id, schema)
        plan = plan_assignment(schema, extraction.values, existing)
        self.catalog.apply(plan, schema)

        evidence = {
            "normalized": normalized,
            "classifier": {"tier": classification.tier, "matched": list(classification.matched)},
            "attributes": extraction.provenance,
            "illegal": extraction.illegal,
            "price_basis": {"basis": basis.basis, "pack_size": basis.pack_size,
                            "evidence": basis.evidence},
        }
        if source is not None:
            self.catalog.persist_resolution(
                source,
                STATUS_GENERIC,
                plan.home_id,
                evidence=evidence,
                extractor_version=EXTRACTOR_VERSION,
                schema_version=f"{schema.category_id}/{schema.schema_version}",
                normalizer_version=self.normalizer.version,
            )

        created = plan.created is not None
        return ResolutionReport(
            raw_text=raw_text,
            normalized=normalized,
            classification=classification,
            status=STATUS_GENERIC,
            extraction=extraction,
            price_basis=basis,
            node_id=plan.home_id,
            identity_key=plan.created.identity_key if created else None,
            created=created,
            parent_id=plan.parent_id,
            is_complete=plan.created.is_complete if created else None,
            schema_version=schema.schema_version,
            evidence=evidence,
        )
