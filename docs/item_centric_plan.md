# Implementation Plan: Item-Centric Resolution

> Status: **steps 1–3 DONE.** 1–2: `--kind item` + UNSPSC fallback (dry-run,
> commits `7af99d8`, `bf032dd`). 3: offers bound as `:Product` VARIANT_OF the
> item's GenericProduct (dry-run counted; Neo4j write path verified live).
> Steps 4–5 (full persist + migration, price-series over Product prices)
> pending. Supersedes the per-record model for the coverage goal;
> `tender|offer|oc|joint` stay for comparison.
>
> Measured (5,000 segment-42 items, dry run): per-record `tender` 34.2% →
> `item` curated 36.2% → `item` + UNSPSC fallback **100% linked**
> (1,809 curated + 3,191 fallback). Largest uncurated commodity buckets =
> next register targets: `42151602` (246), `42171903` (175), `42242003` (118).

## 1. Goal & the invariant

**Deliverable:** a catalog of shared `GenericProduct` nodes to which **(most)
`ItemLicitacion` link**. Coverage of items — not catalog purity — is the
success metric.

**Invariant:** one `ItemLicitacion` → exactly one `GenericProduct`; all of that
item's offers bind to the **same** node.

**Confirmed interpretation (the crux):**
`GenericProduct` nodes remain **shared catalog nodes**, deduped *across* items by
attribute identity (this is what enables price comparison across tenders). The
invariant is **intra-item consistency** — the offers on one item cannot scatter
to different generic products. Two **different** items describing the same
product still collapse onto the **same shared** node. "Same generic product" is
therefore a constraint on how an item's offers resolve, **not** a per-item node.

```
ItemLicitacion ──→ 1 GenericProduct  (shared catalog node; the thing being bought)
      │
      └─ Oferta, Oferta, … ──→ :Product (brand variants) ──→ VARIANT_OF that ONE GenericProduct
```

## 2. Why this changes the design

Today resolution is **per-record** (`resolve_items` loops one text at a time)
and `joint` resolves **each offer independently** → offers on one item can land
on different nodes (violates the invariant) and terse buyer lines go
`unresolved` even when an offer names the product. We flip the unit of work to
the **item**.

## 3. The new resolution unit

New ingest fetch — **`fetch_items(...)`** in `ingest/neo4j_source.py`: one row
per `ItemLicitacion`, aggregating its offers.

```cypher
MATCH (i:ItemLicitacion) WHERE <segment filter on i.codigo_unspsc_producto>
OPTIONAL MATCH (l:Licitacion)-[:TIENE_ITEM]->(i)
OPTIONAL MATCH (o:Oferta)-[:PARA_ITEM]->(i)
RETURN i.descripcion_comprador  AS buyer_text,
       l.titulo                 AS tender_text,
       i.codigo_unspsc_producto AS unspsc,
       collect({offer_id:o.id_oferta, text:o.descripcion_proveedor,
                price:o.precio_unitario_clean, qty:o.cantidad_clean,
                awarded:o.es_adjudicada}) AS offers
```

Yields a new `ItemRecord` (item `SourceRef` + buyer/tender/UNSPSC + list of
offers). Paging uses the source's stable scan order (no global ORDER BY), same
as `fetch_tender_items`.

## 4. New resolver path — `Resolver.resolve_item(...)`

Reuses the existing `_prepare` / classify / `plan_assignment` / catalog
machinery. Steps:

1. **Classify the item** by pooling signals in priority order; first confident
   `CLASSIFIED` wins:
   `buyer line → offer consensus (majority vote across offers) → tender title → UNSPSC fallback`.
   Offer consensus is the key lever: rubric-only buyer lines get the category
   from the offers that actually name the product.
2. **Pick the item's GenericProduct** once: extract identity attributes from the
   winning signal, merging attributes the buyer line omits but offers supply;
   `plan_assignment` → one `home_id`. This is the node the item links to.
3. **Bind every offer to that same node** (the invariant) — see §5.
4. **Persist** the item's `SourceRecord -[:RESOLVED_TO]-> GenericProduct`,
   exactly as today.

Ambiguity/conflict: instead of dropping to `unresolved`, item-level
**consensus + UNSPSC tiebreak** resolves it; only a genuinely zero-signal item
falls through to the fallback.

## 5. Offers as branded `:Product` nodes (the M3 layer — currently unimplemented)

`resolver.py` notes Product resolution is a pending workstream. Minimal version:

- Each offer → a `:Product` (brand-level SKU) `-[:VARIANT_OF]-> GenericProduct`
  (the item's node), `-[:OFFERED_IN]-> ItemLicitacion`, carrying price/awarded.
- This makes "all offers of an item = same generic product" literally true in
  the graph, and gives `price-series` its real price points (the offer is where
  price lives).
- New `assignment` method `bind_product(offer, generic_id)` + `InMemoryCatalog`
  / `Neo4jCatalog` support (parallel to `persist_resolution`).

## 6. Coverage of the residue

Items that no curated family matches even after pooling offers still need to
link. **Recommended default: UNSPSC fallback** — a coarse `GenericProduct` keyed
by the item's `codigo_unspsc_producto` (every item has one), category id like
`unspsc_42182200`. Two-tier catalog: rich curated nodes where a family matches,
coarse UNSPSC buckets elsewhere → ~100% of items link, nothing force-fit.
Flagged (`--fallback unspsc|none`) so curated-only runs stay possible.

## 7. CLI

- New `--kind item` (the item-centric path) alongside existing
  `tender|offer|oc|joint` (kept for comparison / back-compat).
- `--fallback unspsc|none` (default `unspsc`).
- Reuse the existing streaming / checkpoint / resume path and the throttled
  products-CSV write already in place.

## 8. Metrics

Extend `ResolutionStats` with the headline number: **% of items linked**
(curated vs. UNSPSC-fallback vs. unresolved), plus offers-bound count. The
summary leads with item coverage, not record counts.

## 9. Persistence & migration

New migration `003_item_resolution.cypher`: index on `Product.id`, the
`Oferta` / `ItemLicitacion` lookup keys used by the aggregation query, and the
new relationship types `VARIANT_OF`, `OFFERED_IN`.

## 10. Tests

- `test_resolve_item.py`:
  - offer rescues a rubric-only buyer line;
  - all offers of an item bind to one generic product;
  - offer-consensus category vote;
  - UNSPSC fallback on a zero-signal item;
  - ambiguity broken by consensus.
- Pure-function coverage for the signal-priority picker (no graph / no LLM),
  matching the existing offline-testable style.

## 11. Sequencing (each step independently verifiable)

1. `fetch_items` + `ItemRecord` + a `--kind item` dry run that classifies items
   (no Products yet) → measure curated item coverage vs. the 24% baseline.
2. Add UNSPSC fallback → measure total item coverage.
3. Add `:Product` binding for offers → price points land.
4. Persist path + migration + `price-series` reads the new Product prices.
5. Docs (`ejemplo_cli.md`) + memory update.

## Baseline (full segment-42 `tender` dry run, per-record model)

| Metric | Value |
|---|---|
| Items processed | 436,800 |
| Resolved (curated) | 105,809 (24.2%) |
| Unclassified | 294,205 |
| Boilerplate rubric | 29,740 |
| Ambiguous | 7,046 |

The boilerplate-rubric and a large slice of the unclassified buckets are the
direct target of the offer-pooling step.
