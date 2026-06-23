# Design: Offer-Aware Resolution (Oferta ↔ Product ↔ GenericProduct consistency)

> Status: **DESIGN / proposal.** Supersedes the intra-item invariant of
> `item_centric_plan.md` for the offer side. Prerequisite (extraction trust)
> shipped: negation guard + canonical numeric domains (`0a67440`). Targets the
> two inconsistency classes measured below.

## 1. The problem (measured)

Today (`ingest/runner.py:_bind_offers`) an offer is **passively attached** to the
buyer line's generic: it reuses the item's `generic_id`, extracts only the
offer's *descriptive* attributes, and discards the offer's *identity*. The
offer's text is never reconciled with the generic it lands on.

Read-only graph sizing (30 k offers bound to curated generics):

| Signal | Rate | Meaning |
|---|---:|---|
| offer identity **equal** to its generic | 13% | consistent |
| offer identity **strictly refines** the generic (Case A) | **23%** | offer more specific than the buyer line |
| offer identity **contradicts** an attribute (B-attr) | 0.02% | negligible |
| offer's **own Tier-1 family ≠** the generic's family (Case B) | **17%** | offer belongs to a *different* product family |
| offer too short to classify | rest | consistent by default |

So ~40% of offers are bound to a generic their own text does not support: 23%
should resolve to a **finer** node, 17% to a **different family** (kit/rubric
buyer lines pull in component offers — e.g. `reguladores_oxigeno`,
`mascarillas`, `vasos_desechables` offers sitting under an `oxigeno_medicinal`
generic).

## 2. Revised invariant

The item-centric invariant ("one item → one generic; **all** its offers bind to
that node") is *itself* the cause: it forces unlike offers onto one node.

**New invariant:** every `Oferta —OFFERS→ Product —VARIANT_OF→ GenericProduct`
chain is consistent — the offer's extracted identity is **equal to, or a
refinement of, or correctly placed in** the generic it resolves to; never
contradictory, never cross-family by accident. The **buyer line** keeps its own
generic ("what was requested"); each **offer** resolves to "what was actually
offered." They are linked through the existing `(:Oferta)-[:PARA_ITEM]->(:ItemLicitacion)`.

This preserves cross-tender dedup (generics stay shared catalog nodes) and the
intra-item link, while letting offers stop lying about their identity.

## 3. Design — offers become first-class resolved records

Replace the passive attach in `_bind_offers` with a real per-offer resolve that
reuses the **same** classify → extract → assign path as items
(`resolve/resolver.py`, `resolve/assignment.py:plan_assignment`). For each offer,
compare its resolution to the item's buyer-line resolution and branch:

| Offer outcome vs buyer generic `g` | Action | Case |
|---|---|---|
| same family, identity ⊆ `g` (equal/coarser) | bind Product to **`g`** (today's behavior) | consistent |
| same family, identity ⊋ `g` (strict refinement) | find-or-create finer **`g'`**, `PARENT_OF` under `g`; bind Product to `g'` | **A** |
| confidently **different family** | resolve offer to its **own** generic `g_off` in that family; bind Product to `g_off`; flag offer↔item as cross-family bid | **B** |
| unclassified / ambiguous / multi-product bundle | bind to **`g`** (conservative — no worse than today) | — |

Find-or-create of `g'`/`g_off` is exactly `plan_assignment` (subsumption +
`PARENT_OF`), already used for items — offers just feed it their own extraction.

## 4. Data-model deltas

- `Product.generic_id` becomes the **offer's** resolved generic, not always the
  item's. `OFFERS` (price on edge), `OF_BRAND`, `VARIANT_OF` unchanged.
- The buyer line's `SourceRecord —RESOLVED_TO→ g` is **unchanged** (what was
  requested). `PARA_ITEM` carries the item↔offer link (what was offered).
- **Case B flag:** add `OFFERS.conforming = false` (or a `NON_CONFORMING` marker)
  when the offer resolves to a different family than the item — so analysis can
  separate "bid for the requested thing" from "bid for a component/substitute."
- No new node labels; `migration 003` schema already covers `Product`/`Brand`.

## 5. Price-series & bundles

- Offer prices now attach to the offer's **true** product. For kit/rubric buyer
  lines this is *more* correct: each component prices under its own generic
  (`price-series reguladores_oxigeno` finally sees the regulator offers that were
  hidden under `oxigeno_medicinal`).
- Trade-off: a kit no longer has a single aggregate price. Acceptable — the kit
  was never one product. If kit-level pricing is wanted later, derive it from the
  item's component set, not from mis-bound offers.
- `price/series.py` reads per-generic, so it benefits automatically; the
  `conforming` flag lets it optionally exclude cross-family bids.

## 6. Risks & guardrails

- **Over-splitting:** offers with slightly different identity could mint many
  fine generics. Mitigated by (a) the canonical numeric domains just shipped —
  values snap to a standard grid (`20fr` is one value), and (b) `PARENT_OF`
  rollups so fine nodes still aggregate.
- **Wrong re-categorization (Case B):** only move family when Tier-1 is
  **confident** (single category, not `AMBIGUOUS`) and the line is **not** a
  multi-product bundle (`ambiguity.looks_like_bundle`). Otherwise keep `g`.
- **Cost:** ~2–3× resolve time (2.16 M offers fully resolved vs cheap attach),
  one-time + incremental thereafter. Acceptable.
- **Classifier precision on offer text** must be validated before trusting Case
  B moves — hence the instrument-first phase.

## 7. Phased plan (each phase measured before the next)

1. **Instrument-only.** In `_bind_offers`, compute the offer's resolution and
   record the disagreement (a property/event) **without** changing the binding.
   Re-resolve; measure real corpus-wide A/B rates (vs the 30 k sample) and the
   classifier's offer-side precision. Go/no-go gate.
2. **Refinement (Case A).** Bind to the finer `g'` with `PARENT_OF` when the
   offer strictly refines, same family. Low risk. Measure: generic-count growth,
   consistency rate, price-series sanity.
3. **Re-categorization (Case B).** Bind to the offer's own family when
   confidently different and not a bundle; set `conforming=false`. Measure same.
4. **Cleanup.** Backfill the `conforming` flag; revisit kit-level pricing if
   needed.

## 8. Code-change map

- `ingest/runner.py:_bind_offers` — the core branch; reuse the item resolve path.
- `resolve/resolver.py` — factor a shared per-text resolve so offers and items
  use one code path (offers currently only get `extract` for descriptives).
- `resolve/assignment.py` — `plan_assignment` already does find-or-create +
  `PARENT_OF`; feed it offer identity. `link_offer` gains the `conforming` flag.
- Validation: reuse `data/_audit_offer_consistency.py` (the sizing harness) to
  track A/B rates per phase.

## 9. Dependencies

- **Done:** extraction trust (negation guard, canonical domains) — `0a67440`.
- **In flight:** full re-resolve `v4` banking those fixes (so the buyer-line
  generics are themselves correctly specified before offers resolve against them).
- **Then:** Phase 1 instrument-only on top of `v4`.
