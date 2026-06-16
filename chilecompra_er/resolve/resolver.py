"""Resolution of one description — the strictly ordered steps of design §7.

(1) normalize -> (2) classify -> (3) extract -> (4) price basis ->
(5) find home node -> (6) create if absent (computing parent) ->
(7) persist :SourceRecord + versioned :RESOLVED_TO.

Each step gates the next; an unclassified description stops at (2) and is
persisted as explicitly unresolved. Branded (:Product) resolution is an M3
workstream — it needs the written "branded enough" rule first.
"""

from __future__ import annotations

from collections import Counter
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
from .classifier import AMBIGUOUS, CLASSIFIED, UNCLASSIFIED, Classification, Tier1Classifier
from .extractor import Extraction, extract

EXTRACTOR_VERSION = f"chilecompra_er/{__version__}"

# Joint (tender + offer) resolution outcomes that stop before assignment.
REASON_CONFLICT = "offer_buyer_conflict"   # the two texts classify differently
REASON_BOILERPLATE = "boilerplate_rubric"


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
                price_fields: dict | None = None,
                context_text: str | None = None) -> ResolutionReport:
        # Tender lines carry two texts: the line item (raw_text, authoritative)
        # and the parent tender's title (context_text). When the tender title
        # is present, resolve them together — item wins, tender is the fallback
        # for terse/boilerplate lines and fills attributes the item omits
        # (same rule as resolve_joint, design §2/§7).
        if context_text:
            return self._resolve_pair(
                raw_text, context_text,
                primary_name="item", secondary_name="tender",
                source=source, price_fields=price_fields,
                conflict_unresolved=False,
            )

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

    # --- item-centric resolution ---------------------------------------------

    def resolve_item(self, buyer_text: str | None, tender_text: str | None,
                     offers: list[dict] | None, *,
                     unspsc: int | None = None,
                     source: SourceRef | None = None,
                     price_fields: dict | None = None) -> ResolutionReport:
        """Resolve ONE ItemLicitacion to ONE GenericProduct by pooling every
        signal it carries. Category priority: the buyer line, then OFFER
        CONSENSUS (majority category across the item's offers — this is what
        rescues terse/boilerplate buyer lines), then the tender title. The
        winning text drives attribute extraction; the buyer line fills the
        attributes it omits. Because the item resolves once, all of its offers
        share this single node (the intra-item invariant) — binding them as
        :Product variants is a later step. Step 1: no UNSPSC fallback yet, so a
        zero-signal item is still left unresolved (visible debt)."""
        b_norm, b_cls, b_boiler = self._prepare(buyer_text)
        t_norm, t_cls, _t_boiler = self._prepare(tender_text)

        offer_sig: list[tuple[dict, str, Classification]] = []
        for o in offers or []:
            o_norm, o_cls, _ = self._prepare(o.get("text"))
            offer_sig.append((o, o_norm, o_cls))
        votes = Counter(c.category_id for _o, _n, c in offer_sig
                        if c.status == CLASSIFIED)

        chosen_cat: str | None = None
        category_source: str | None = None
        win_norm = ""
        if b_cls.status == CLASSIFIED:
            chosen_cat, category_source, win_norm = b_cls.category_id, "buyer", b_norm
        elif votes:
            chosen_cat = votes.most_common(1)[0][0]
            category_source = "offer"
            # extract from a representative offer for that category, awarded first
            reps = [(o, n) for o, n, c in offer_sig
                    if c.status == CLASSIFIED and c.category_id == chosen_cat]
            reps.sort(key=lambda on: 0 if on[0].get("awarded") else 1)
            win_norm = reps[0][1] if reps else ""
        elif t_cls.status == CLASSIFIED:
            chosen_cat, category_source, win_norm = t_cls.category_id, "tender", t_norm

        base_evidence = {
            "buyer_normalized": b_norm,
            "buyer_category": b_cls.category_id,
            "tender_category": t_cls.category_id,
            "offer_votes": dict(votes),
            "n_offers": len(offer_sig),
        }

        if chosen_cat is None:
            statuses = {b_cls.status, t_cls.status,
                        *(c.status for _o, _n, c in offer_sig)}
            if AMBIGUOUS in statuses:
                reason = AMBIGUOUS
            elif b_boiler and not votes:
                reason = REASON_BOILERPLATE
            else:
                reason = UNCLASSIFIED
            report = ResolutionReport(
                raw_text=buyer_text or "",
                normalized=b_norm or t_norm,
                classification=Classification(
                    None, AMBIGUOUS if reason == AMBIGUOUS else UNCLASSIFIED),
                status=STATUS_UNRESOLVED,
                unresolved_reason=reason,
                evidence=base_evidence,
            )
            if source is not None:
                self.catalog.persist_resolution(source, STATUS_UNRESOLVED, None)
            return report

        schema = self.schema(chosen_cat)
        win_ext = extract(win_norm, schema) if win_norm else Extraction()
        b_ext = extract(b_norm, schema) if b_norm else Extraction()
        # winning text wins; the buyer line fills attributes it omits
        values = {**b_ext.values, **win_ext.values}
        provenance = {k: {**v, "text": "buyer"} for k, v in b_ext.provenance.items()}
        provenance.update({k: {**v, "text": category_source}
                           for k, v in win_ext.provenance.items()})
        extraction = Extraction(values=values, provenance=provenance,
                                illegal=win_ext.illegal + b_ext.illegal)

        basis = infer_basis(win_norm or b_norm)
        if price_fields:
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
            **base_evidence,
            "category_source": category_source,
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
            raw_text=buyer_text or "",
            normalized=win_norm or b_norm,
            classification=Classification(chosen_cat, CLASSIFIED),
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

    # --- joint tender + offer resolution -------------------------------------

    def _prepare(self, raw_text: str | None) -> tuple[str, Classification, bool]:
        """Normalize + classify one text, flagging UNSPSC-rubric boilerplate."""
        normalized = self.normalizer(raw_text) if raw_text else ""
        boilerplate = bool(raw_text) and raw_text.count(" / ") >= 2
        if boilerplate or not normalized:
            return normalized, Classification(None, UNCLASSIFIED), boilerplate
        return normalized, self.classifier.classify(normalized), False

    def resolve_joint(self, offer_text: str | None, buyer_text: str | None,
                      source: SourceRef | None = None,
                      price_fields: dict | None = None,
                      awarded: bool | None = None) -> ResolutionReport:
        """Resolve a supplier offer using BOTH its own text and its tender
        line's buyer text (design §2/§7 — offer text states attributes the
        tender omits). Category preference: the offer text wins; the buyer
        text is a fallback when the offer doesn't classify. If the two
        classify to *different* categories the record is left unresolved
        (`offer_buyer_conflict`) for review. Attributes are extracted from
        both texts and merged, offer winning on conflict (it describes the
        product actually delivered)."""
        return self._resolve_pair(
            offer_text, buyer_text,
            primary_name="offer", secondary_name="buyer",
            source=source, price_fields=price_fields,
            conflict_unresolved=True,
            extra_evidence={"awarded": awarded},
        )

    def _resolve_pair(self, primary_text: str | None, secondary_text: str | None,
                      *, primary_name: str, secondary_name: str,
                      source: SourceRef | None = None,
                      price_fields: dict | None = None,
                      conflict_unresolved: bool = True,
                      extra_evidence: dict | None = None) -> ResolutionReport:
        """Resolve one record from two texts — a primary (authoritative) and a
        secondary (context/fallback). Used by both joint offer+buyer resolution
        (primary=offer, secondary=buyer) and tender-line resolution
        (primary=item, secondary=tender). Category preference: the primary text
        wins; the secondary is a fallback when the primary doesn't classify.
        With conflict_unresolved=True the record is left unresolved for review
        when the two classify to different categories (offer vs. buyer describe
        the *same* line, so a disagreement is a real signal); with it False the
        primary simply wins (a tender title is broader than its line item — a
        different family there is context, not a conflict). Attributes are
        extracted from both and merged, the primary winning on conflict."""
        p_norm, p_cls, p_boiler = self._prepare(primary_text)
        s_norm, s_cls, s_boiler = self._prepare(secondary_text)
        p_ok = p_cls.status == CLASSIFIED
        s_ok = s_cls.status == CLASSIFIED

        chosen: Classification | None = None
        category_source: str | None = None
        reason: str | None = None
        if conflict_unresolved and p_ok and s_ok and p_cls.category_id != s_cls.category_id:
            reason = REASON_CONFLICT
        elif p_ok:
            chosen, category_source = p_cls, primary_name
        elif s_ok:
            chosen, category_source = s_cls, secondary_name
        elif p_boiler and s_boiler:
            reason = REASON_BOILERPLATE
        elif p_cls.status == AMBIGUOUS or s_cls.status == AMBIGUOUS:
            reason = AMBIGUOUS
        else:
            reason = UNCLASSIFIED

        base_evidence = {
            f"{primary_name}_normalized": p_norm, f"{secondary_name}_normalized": s_norm,
            f"{primary_name}_category": p_cls.category_id,
            f"{secondary_name}_category": s_cls.category_id,
            **(extra_evidence or {}),
        }

        if chosen is None:
            candidates = tuple(c for c in (p_cls.category_id, s_cls.category_id) if c)
            status_for_report = AMBIGUOUS if reason in (REASON_CONFLICT, AMBIGUOUS) else UNCLASSIFIED
            report = ResolutionReport(
                raw_text=primary_text or secondary_text or "",
                normalized=p_norm or s_norm,
                classification=Classification(None, status_for_report, candidates),
                status=STATUS_UNRESOLVED,
                unresolved_reason=reason,
                evidence=base_evidence,
            )
            if source is not None:
                self.catalog.persist_resolution(source, STATUS_UNRESOLVED, None)
            return report

        schema = self.schema(chosen.category_id)
        p_ext = extract(p_norm, schema) if p_norm else Extraction()
        s_ext = extract(s_norm, schema) if s_norm else Extraction()
        # Merge: secondary first, primary overwrites — primary wins on conflict.
        values = {**s_ext.values, **p_ext.values}
        provenance = {k: {**v, "text": secondary_name} for k, v in s_ext.provenance.items()}
        provenance.update({k: {**v, "text": primary_name} for k, v in p_ext.provenance.items()})
        extraction = Extraction(values=values, provenance=provenance,
                                illegal=p_ext.illegal + s_ext.illegal)

        basis = infer_basis(p_norm or s_norm)
        if price_fields:
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
            **base_evidence,
            "category_source": category_source,
            "classifier": {"tier": chosen.tier, "matched": list(chosen.matched)},
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
            raw_text=primary_text or "",
            normalized=p_norm or s_norm,
            classification=chosen,
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
