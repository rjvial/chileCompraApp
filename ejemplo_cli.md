# `chilecompra-er` — CLI guide

A complete guide to the `chilecompra-er` command-line tool: what it does, the
ideas behind it, and how to use every command. No prior knowledge of the
codebase is assumed.

The guide is organized so each topic has **one** home: §2 is the mental model,
§3 is the workflow in order, §4 is a pure command lookup, and §5–§7 cover files,
conventions, and internals.

---

## 1. What this is

ChileCompra (Mercado Público) is Chile's public-procurement marketplace. Every
purchase is described in **free text** by whoever wrote the tender — so the same
physical product ("Foley catheter, 16 Fr, 2-way, latex") appears under hundreds
of different descriptions, units, and spellings. That makes it impossible to ask
simple questions like *"what does this product usually cost?"* or *"who sells it
cheapest?"*.

`chilecompra-er` is an **entity-resolution pipeline** that turns those messy
descriptions into a clean, structured **catalog**: a set of canonical *generic
products*, each with typed attributes, that real purchase lines and supplier
offers are linked to. Once the catalog exists, price comparison and analysis
become straightforward.

The data already lives in a **Neo4j graph** (the transactional layer: tenders,
line items, offers, purchase orders). The pipeline reads from that graph and
*adds* a catalog layer on top — it never modifies the source data. The **CLI is
the single operational surface**: it profiles the corpus, builds the catalog,
resolves records into it, and reports prices.

Invoke it as `chilecompra-er <command>` (installed entry point) or, from inside
the repo, `.\.venv\Scripts\chilecompra-er.exe <command>`. Examples use PowerShell
paths (`data\...`).

---

## 2. The model

Read this once and the rest of the guide makes sense.

### Two graphs

The resolver reads a **source graph** (loaded by a separate ingestion pipeline)
and writes a **catalog** on top of it.

**Source graph (read-only input):**

| Node | What it is | Key text |
|---|---|---|
| `Licitacion` | A tender (procurement process) | `titulo` — the tender headline |
| `ItemLicitacion` | One **line item** a buyer wants | `descripcion_comprador` — the buyer's product text |
| `Oferta` | A supplier **offer/bid** for a line item | `descripcion_proveedor` — the supplier's text, plus prices |
| `OrdenCompra` / `ItemOC` | Purchase orders | — |

Relationships: `(Licitacion)-[:TIENE_ITEM]->(ItemLicitacion)` and
`(Oferta)-[:PARA_ITEM]->(ItemLicitacion)`. Each `ItemLicitacion` carries a
**UNSPSC code** (`codigo_unspsc_producto`) — the UN's standard product taxonomy
number (segment 42 = medical/lab supplies).

**Catalog (written by `resolve`):**

| Node | What it is |
|---|---|
| `Category` | A product **family** (e.g. `sondas_foley`) with a typed attribute **schema** |
| `GenericProduct` | A canonical product *within* a category, identified by its **identity attribute values** (Foley + 16Fr + latex). Shared across all tenders that describe it. |
| `Product` | A **brand × the offer's own spec** — `(Foley 16Fr latex 2-way) × B.Braun`. Deduped: **one node per distinct (generic, brand, offer identity)**, shared across every offer of that exact spec+brand. **Self-describing** (carries the offer's full identity values) and price-free — `VARIANT_OF` the item's GenericProduct and `OF_BRAND` a Brand; the price lives on the offer edge below. |
| `Brand` | A first-class trade-name node (`b.braun`, `3m`), shared across products and categories. |

```
  SOURCE GRAPH (read-only input)            CATALOG (written by `resolve`)
  ──────────────────────────────           ───────────────────────────────

   Licitacion
      │ :TIENE_ITEM
      ▼
   ItemLicitacion ───────:RESOLVED_TO───────────────────┐
      ▲  buyer text + UNSPSC                            │
      │ :PARA_ITEM                                      ▼
   Oferta ─:OFFERS {price}─▶ Product ──:VARIANT_OF──▶  GenericProduct ──:IN_CATEGORY──▶ Category
      supplier text          │  Brand × offer-spec      │   ▲   shared node, deduped
                             │  node — price NOT here   └───┘   by identity attributes
                       :OF_BRAND                     :PARENT_OF  (coarse → fine hierarchy)
                             ▼
                          Brand

  ──▶  stored edge        ┄┄▶  created by the resolver (a node, not a stored edge)
```

Each `ItemLicitacion` links **directly** to its `GenericProduct` via one
`(:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct)` edge (carrying the evidence;
overwritten on re-resolve). Each `Oferta` on that item attaches via an
explicit `(:Oferta)-[:OFFERS {price…}]->(:Product)` edge, where the `Product` is
the deduped `Brand × the offer's identity` node (`VARIANT_OF` the item's generic,
`OF_BRAND` the brand). **The price lives on the OFFERS edge, not on the node** —
so many offers of the same brand AND the same spec collapse onto one shared
`Product`. Identical generics across different tenders collapse onto one shared
`GenericProduct` (that is what makes cross-tender price comparison possible), and
coarser nodes parent finer ones via `:PARENT_OF`.

### How one line resolves

`resolve` runs each description through an ordered, **fully deterministic**
pipeline. These six steps are the **per-text core every `--kind` shares**; the
item-centric mode below wraps step 2 with its buyer → offer-consensus → title
priority and then runs steps 3–6 once for the chosen text:

1. **Normalize** — lowercase, strip accents, split digit/letter runs, expand a
   versioned abbreviation table. ("SONDA FOLEY 2VÍAS" → "sonda foley 2 vias").
2. **Classify (Tier-1)** — deterministic include/exclude **regexes** per category
   (from the register). Exactly one matches → *classified*; none → *unclassified*;
   more than one → *ambiguous*. Nothing is ever guessed.
3. **Extract attributes** — per the category's **schema**, pull identity +
   descriptive values (`calibre=16fr`, `material=latex`). The schema is loaded
   from its frozen file in git (see §3 Step 1); out-of-domain values are dropped
   and counted (the "illegal values" metric).
4. **Infer price basis** — a coarse hint from the text: `per_pack` when it carries
   pack evidence, otherwise `unknown` (reported, never assumed). The real per-offer
   unit-vs-pack normalization happens later, at `price-series` time, from price
   **magnitude** — the arithmetic cross-check is signal-less on Mercado Público,
   where `total = quantity × unit_price` is an accounting identity (§4.6 / §7).
5. **Find or create the generic product** — the node whose *present identity
   values* exactly match (same values, same absences). A line with no extracted
   identity attributes anchors honestly on the **category root** (specificity 0);
   more-specific nodes hang under coarser ones via `PARENT_OF` (strict-subset
   hierarchy), so "Foley" parents "Foley 16Fr".
6. **Persist** — write the direct `(:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct)`
   edge (carrying the evidence). One edge per item; re-resolving overwrites it
   (DELETE old, CREATE new). No SourceRecord / lineage / versioning.

### The item-centric model (`--kind item`) — the recommended way

Instead of resolving each record independently, resolve one **`ItemLicitacion`
at a time**, pooling every signal it has:

- **Category priority:** buyer line → **offer consensus** (majority vote across
  the item's offers) → tender title. Offers are the key lever — a buyer line that
  is just a rubric path ("Equipamiento médico / … / Sondas") gets its real
  category from the supplier offers that actually name the product.
- **One generic per item, offer-aware Products under it.** The buyer line gets
  one `GenericProduct` (its demand), enriched **upward only** by the offers'
  *minimum common* spec. Each offer then binds to a `Product` carrying that
  **offer's own** identity, `VARIANT_OF` that generic: equal (`same`), more
  specific (`refined`), or — when the offer is vaguer than or diverges from the
  demand — kept but flagged `conforming=false` (`nonconforming`). An offer that is
  plainly a **different family** resolves to its own generic (`recategorized`).
  The spec chain stays consistent: `Item = Generic ≤ Product ≤ Oferta`.
- **UNSPSC fallback** (see below) → coverage approaches **100%**.

### Curated vs fallback (the metric that matters)

Two outcomes for every line — and Phase 2 of the process exists to shift lines
from the second to the first:

- **Curated** = the line matched a real product family you defined (`agujas`,
  `suturas`, …). It gets typed attributes and a precise generic product. The
  valuable output.
- **Fallback** = no family matched, so the line links to a coarse bucket keyed
  purely by its raw UNSPSC code (`unspsc_42171903`), with a synthetic
  attribute-less schema (which is why fallback buckets carry no typed
  attributes). Nothing is force-fit into the wrong family — fallback is honest,
  visible debt.

The headline metric for a `--kind item` run is therefore
**items linked = curated + UNSPSC-fallback**.

---

## 3. The process, step by step

The whole pipeline is **two phases over one idea**: first build a catalog and
link every purchase line to it (Phase 1); then keep shrinking the fallback
residue (Phase 2).

**The one thing to internalize — where the LLM is (and isn't).** In the current
pipeline only **three** commands ever call the LLM: `register`,
`generate-schemas`, and `build-brand-lexicon`. Everything else is deterministic.
(A redesign in progress adds a fourth, experimental `canonicalize` — §4.9.) The LLM is used to
**invent structure** *once* — propose which families exist, draft their schemas,
list their brand names — looking only at small *aggregated samples*. It is
**never** asked to classify individual purchase lines; those are resolved
mechanically (§2) against the structure the LLM bootstrapped. That is why
resolving 1M+ lines is fast and cheap, and why "curated" describes a *family*,
not a hand-touched node.

### Setup (one-time)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .   # editable install
```

- **Neo4j** — `chilecompra-er instance start` boots the EC2 box; the public IP
  changes every start, so it rewrites `.mcp.json` automatically. (A bolt
  *timeout* right after a clean start usually means the `neo4j-sg` security group
  doesn't allow your current client IP — not that Neo4j is down.)
- **LLM auth** (only for the three LLM commands) — run `ant auth login` once;
  calls go through the Claude Max subscription.
- `chilecompra-er status` is a register + instance + graph sanity check anytime.
  Run the tests after register/schema changes: `python -m pytest tests -q`.

### The whole timeline, in the order you actually run it

This is one continuous sequence. **`register` and `resolve` each run more than
once** — every occurrence is listed at the point it runs, labeled with its run
number. The **"Reads"** column is the key to the ordering: each step depends on
what the step before it produced.

| Step | Command | Reads (its input) | LLM? | Writes | Result |
|---|---|---|---|---|---|
| 0 | `instance start` + `migrate` | — | no | graph constraints/indexes | Neo4j up; IP refreshed |
| 1 | `register` **(1st run)** | the **raw source corpus** | **yes** | `register.json` + schema files (git); `profiling.csv` cache | families defined from scratch |
| 2 | `resolve … ` *(dry run)* | source items **+ the register** | no | nothing | preview the curated/fallback split |
| 3 | `resolve … --persist` | source items **+ the register** | no | **the catalog** (graph) | items linked; **fallback residue created** |
| 4 | `fallback-report` | the **fallback residue** (graph) | no | `fallback_ranking.csv` | residue ranked (UNCOVERED families) |
| 4b | `ambiguity-report` *(optional)* | the **fallback residue** (graph) | no | stdout | register OVERLAPS ranked (the fixable-with-an-exclude backlog) |
| 5 | `register --from-fallback` **(2nd run)** | the **fallback residue** (graph) | **yes** | `register.json` + schema files (git); `fallback_ranking.csv` | **more** families defined |
| 5a | `train-tier2` *(optional)* | curated resolutions (graph) | no *(scikit-learn)* | `tier2_model.joblib` | wording classifier |
| 5b | `build-brand-lexicon` *(optional)* | corpus samples per family | **yes** | `brand_lexicon.json` | brand tokens |
| 6 | `resolve … --tier2 --brands --persist` **(2nd run)** | source items **+ the now-richer register** (+ model + lexicon) | no | **the catalog** (re-linked) | **lower fallback %** |
| ↺ | **repeat steps 4 → 6** | the new, **smaller** residue | — | — | until the fallback share stops shrinking |
| 7 | `price-series` / `status` | the catalog (graph) | no | CSV / stdout | prices & counts |

> **Why `register` appears twice (Steps 1 and 5).** It is the *same command*, but
> on **two different inputs**. Run 1 reads the **raw corpus** and invents families
> from scratch — it must come *before* any `resolve`, because you can't resolve
> into families that don't exist yet. Run 2 (`--from-fallback`) reads the
> **leftovers of a `resolve`** (the fallback residue) and curates *those* — so it
> must come *after* a `resolve`. Same idea for `resolve`: it runs at Steps 2, 3,
> and 6, each time against whatever the latest `register` defined. That back-and-
> forth **is** the loop.

Below, the same steps with detail, grouped into the two phases.

#### Phase 1 — build the catalog (Steps 0–3)

**Step 0 — bring up infra.** `instance start` boots Neo4j (rewrites `.mcp.json`);
`migrate` applies the uniqueness constraints + indexes the resolver relies on.
Idempotent.

**Step 1 — `register` (1st run): invent the families from the raw corpus.** The
only "thinking" step. Profiles the corpus by **head-noun × spend** to rank
candidate families, sends the top of that ranking to the LLM in batches asking
*"is this a real, coherent product family?"*, **mechanically validates** every
proposal (regexes compile; the family's own example classifies to it; no existing
family's example becomes ambiguous), then writes survivors to `register.json`
**and drafts a versioned attribute schema for each** under
`chilecompra_er\categories\schemas\<id>.json`. **Output:** the catalog
*definition* (family list + schema files), version-controlled in git. Nothing is
written to the graph yet; no purchase line has been classified.

**Step 2 — `resolve` (dry run): look before you write.** Run a sample
(`--limit 5000`, no `--persist`). The resolver pools each item's signals and runs
the §2 pipeline against the register from Step 1, reporting the
**curated-vs-fallback split** without touching the graph. Your quality gate: if
too much lands on fallback, fix schemas / add families and re-run.

**Step 3 — `resolve --persist`: build the catalog.** Same pipeline, now
**writing** (resumable; checkpointed). MERGEs the canonical `GenericProduct` per
item, links it via a direct `(:ItemLicitacion)-[:RESOLVED_TO]->` edge, and binds
**every offer via an `OFFERS {price}` edge to its `Brand × offer-spec`
`Product`** (the price on the edge). Unmatched
items link to their `unspsc_<code>` bucket → **coverage ≈ 100%**. The items that
landed on fallback are the **residue** Phase 2 attacks.

#### Phase 2 — shrink the fallback residue (Steps 4–6, then loop)

**Step 4 — `fallback-report`.** Reads the `unspsc_*` fallback nodes, splits
rubric-only boilerplate from real products, and surfaces the recurring head-noun
**families** in the tail, ranked by **item count**. (Residue *spend* is shown but
not used to rank: `fetch_fallback_items` can only attribute a bucket's spend
uniformly across its items, so it's a smeared average that floats junk to the top —
item count is the honest priority signal.) **Output:** `data\fallback_ranking.csv`
— a prioritized "curate these next" list.

**Step 4b — `ambiguity-report` (optional): the overlap counterpart.** Where
`fallback-report` ranks the **uncovered** families, `ambiguity-report` ranks the
**overlapping** ones. Some residue items fell to fallback not because *no*
category matched but because **two Tier-1 regexes both did** (ambiguous → dropped
to the bucket silently). It re-classifies the residue, keeps the ambiguous items,
and ranks the colliding category **sets** — splitting **spurious** overlaps (one
product two regexes both claim, e.g. "aguja de sutura" → `agujas ∩ suturas`,
fixable by adding an `--exclude`) from genuine **multi-product bundles**
("mascarillas, canulas, gasas", where ambiguity is correct and is left alone).
The spurious count is a trackable register-hygiene backlog; fix the top overlaps
with `add-category`/register excludes, then a full re-resolve clears them.

**Step 5 — `register --from-fallback` (2nd run): curate the tail.** The *same*
`register` machinery as Step 1, but candidates come from the **graph residue**
instead of the whole-corpus profile (and are ranked by **item count**, not spend —
see Step 4) — so the LLM vets exactly the highest-volume families that failed to
resolve. It re-queries the fallback nodes from the graph itself (and rewrites
`fallback_ranking.csv`), so Step 4 is a useful preview but not a strict
prerequisite. Adds **more** families + schemas to git.

**Steps 5a / 5b — close the phrasing & brand gaps (optional).** Some residue
isn't a *missing family* — it's a known family phrased so regex misses it, or a
bare **brand name**:
- **`train-tier2`** fits a TF-IDF + logistic-regression model on the curated
  resolutions already in the graph; it generalizes to unseen wording and
  *abstains* when unsure, so it only ever *adds* coverage. (scikit-learn, no LLM.)
- **`build-brand-lexicon`** asks the LLM for per-family trade-name tokens, then
  validates and stores them. (Ships **empty** — until populated, `--brands` is a
  no-op.)

**Step 6 — `resolve --tier2 --brands --persist` (2nd run): re-link.** Same
`resolve` as Step 3, but now against the **enriched** register, with the
classifier as a cascade — **Tier-1 regex → brand lexicon → Tier-2 model** (each
lower tier only promotes an *otherwise-unresolved* item; a Tier-1 hit always
wins). With the new families from Step 5 plus the tiers, **fewer items fall to
fallback**. Run it as a dry run to measure the reduction, then add `--persist`.

**↺ Loop.** Go back to Step 4 on the new, smaller residue and repeat 4 → 6 until
the fallback share stops shrinking. **Step 7** (`price-series` / `status`, §4)
analyzes the result whenever you want.

### The commands, in order

```powershell
# Phase 1 — build the catalog
chilecompra-er instance start                  # 0. infra up (rewrites .mcp.json)
chilecompra-er migrate                          #    constraints + indexes
chilecompra-er register --segment 42            # 1. register (1st run): families from the raw corpus
chilecompra-er resolve --kind item --segment 42 --limit 5000 --show 10 --out data\check   # 2. resolve (dry run)
chilecompra-er resolve --kind item --segment 42 --persist                                 # 3. resolve (persist): WRITE catalog

# Phase 2 — shrink the fallback residue (repeat this block)
chilecompra-er fallback-report --top 20         # 4. rank the residue (uncovered families)
chilecompra-er ambiguity-report --top 20        # 4b. (opt) rank register OVERLAPS (spurious vs bundle)
chilecompra-er register --from-fallback         # 5. register (2nd run): curate the residue
chilecompra-er train-tier2 --eval               # 5a. (opt) statistical tier
chilecompra-er build-brand-lexicon              # 5b. (opt) LLM: brand tokens
chilecompra-er resolve --kind item --segment 42 --tier2 --brands --persist   # 6. resolve (2nd run): re-link
#                                               ↺  repeat 4 → 6 until fallback stops shrinking

# Analyze, then shut down (graph data persists)
chilecompra-er price-series sondas_foley        # 7.
chilecompra-er status
chilecompra-er clean                            #    drop local scratch files
chilecompra-er instance stop
```

```
                 ┌──────────────────── the loop ────────────────────┐
                 ▼                                                   │
 register ─→ resolve --persist ─→ fallback-report ─→ register --from-fallback
 (1st run:      (3: links items,    (4: ranks the      (5, 2nd run: curates
  families       builds catalog,     fallback           the residue) │
  from raw       creates residue)    residue)                        │
  corpus)                                                            ▼
     ▲                                          resolve --tier2 --brands --persist
     │                                          (6, 2nd run: re-links against the
  (Step 1, once)                                 richer register → lower fallback)
                                                                     │
                                          then ─→ price-series  ◀─────┘
```

### Run the whole sequence as one resumable command

Everything above is automated by a single orchestrator. It runs Steps 0, 1, and
3–6 in order (one full Phase-2 pass) — going straight to the persisting build and
**skipping the Step 2 dry-run preview**, so use the manual sequence above first
when you want that quality gate:

```powershell
chilecompra-er pipeline --segment 42      # run the whole build end-to-end
chilecompra-er pipeline --resume          # continue after an interruption
```

It runs them as ordered **stages**, recording each completed stage in
`data\pipeline.checkpoint.json`. If a stage is interrupted — a kill, a crash, a
dropped Neo4j connection — re-run with `--resume`: completed stages are skipped
and the run picks up at the interrupted one. The four long LLM/resolve stages —
`register`, `register-fallback`, `build`, and `final-resolve` — *also* resume
**within** themselves (their own per-run checkpoints): a kill mid-`build`
continues from the exact record, and a kill mid-`register` continues from the
next un-vetted batch — no LLM work re-paid, no rows duplicated. After it
completes, keep iterating manually
with the ↺ loop (Steps 4 → 6) until the fallback share stops shrinking. See §4.2
for the nine stages and every flag.

### Keeping the catalog current

The steps above are a **full** build. To fold in new tenders/items/offers, re-run
`resolve --kind item --persist` — each `(:ItemLicitacion)-[:RESOLVED_TO]->`
edge is overwritten in place, so a re-resolve simply refreshes every item to its
current generic. (There is no incremental/watermark mode; the model is kept
deliberately simple — one direct edge per item, no lineage layer.)

---

## 4. Command reference

Pure lookup — see §3 for *when* to run each. Flags shown with defaults; `[ ]`
marks optional.

### 4.1 Infrastructure

**`status`** — register version, per-category schema status, Neo4j state, graph
node counts. Read-only; never crashes on a flaky graph.

```
register version : 1.78.0
category                  status      schema          identity attrs
sondas_foley              launched    1.2.0           calibre, material, vias
agujas                    candidate   1.0.0           calibre, tipo
neo4j instance   : i-06c721c54d821f3a8 running @ 52.91.37.106
graph            : 119 categories, 6059 generic products, 99386 resolved items
```

**`instance start|stop|status`** — Neo4j EC2 lifecycle. `start` boots the box and
prints the new bolt IP (changes each start, rewrites `.mcp.json`); `stop` shuts
it down (graph data persists); `status` prints state.

**`migrate [--dry-run]`** — apply graph migrations (uniqueness constraints +
indexes) under `chilecompra_er/migrations/*.cypher`. Idempotent. `--dry-run`
prints pending Cypher without running it.

### 4.2 `pipeline` — run the whole build end-to-end (resumable)

Orchestrates the entire §3 sequence as one ordered, checkpointed command. The
nine stages run in this order — the **bold token** is the exact name you pass to
`--from-step` / `--only`:

1. **`instance`** — boot Neo4j (`instance start`)
2. **`migrate`** — apply constraints + indexes
3. **`register`** — invent families from the raw corpus (1st `register` run)
4. **`build`** — `resolve --kind item --persist` (the catalog build)
5. **`fallback-report`** — rank the fallback residue
6. **`register-fallback`** — `register --from-fallback` (curate the residue)
7. **`train-tier2`** — train the Tier-2 classifier (**skipped if
   `tier2_model.joblib` already exists** — retrain out-of-band with `train-tier2`)
8. **`build-brand-lexicon`** — LLM brand tokens
9. **`final-resolve`** — `resolve --kind item --persist --tier2 --brands` (re-link)

Each stage reuses the corresponding command; completed stages are recorded in
`data\pipeline.checkpoint.json`.

```powershell
chilecompra-er pipeline --segment 42                 # fresh full run (one Phase-2 pass)
chilecompra-er pipeline --resume                     # continue at the interrupted stage
chilecompra-er pipeline --restart                    # discard the checkpoint and start over
chilecompra-er pipeline --from-step register-fallback  # resume at a CHOSEN stage + run the rest
chilecompra-er pipeline --only train-tier2           # run a single stage in isolation
chilecompra-er pipeline --status                     # snapshot: plan + per-step progress % + rate + ETA
chilecompra-er pipeline --watch                      # live monitor (refreshes until done / Ctrl-C)
```

**Resuming from any stage — the two ways.**
- **Automatic** (`--resume`): re-run after any interruption (a kill, a crash, a
  dropped Neo4j connection) and the pipeline skips every stage already marked
  done in the checkpoint and picks up at the one that was interrupted. This is
  the normal recovery path — you don't need to know which stage stopped.
- **Manual** (`--from-step <stage>`): force the run to begin at *any* stage you
  name (from the list above) and continue through the end, **ignoring** the done
  list. Use it to redo a stage you've changed your mind about, or to step over a
  stage that keeps failing for a reason you've decided to accept. `--only
  <stage>` runs exactly one stage and stops.

Two layers of resume compose: **stage level** (the above) and **within a stage**
— the `register`, `register-fallback` (§4.3), `build`, and `final-resolve`
(§4.4) stages each continue from their *own* per-run checkpoint. So a kill in the
middle of the multi-hour `build` resumes from the exact record, and a kill partway
through the long `register` vet scan resumes at the next un-vetted batch — not the
top of the stage. A stage's non-zero exit halts the run with the prior stages
still marked done — fix the cause and `--resume`, or `--from-step <next-stage>`
to skip past it.

| Flag | Default | Meaning |
|---|---|---|
| `--resume` | off | Continue `data\pipeline.checkpoint.json`, skipping completed stages. |
| `--restart` | off | Discard the pipeline checkpoint + the resolve sub-checkpoints + every step's progress timeline; start from the first stage. |
| `--from-step <stage>` | none | Force-run this stage and everything after it (ignores the done list). |
| `--only <stage>` | none | Run just this one stage. |
| `--status` | off | Print the plan (stages done/pending) + **per-step progress** — `N/total`, live %, **rate and ETA** for every step that loops (the resolve stages, the `register`/`register-fallback` vet scans, the brand-lexicon scan); `[done]`/`[running…]` for the rest. Read-only: exits without running anything (and without writing the checkpoint). |
| `--watch` | off | Like `--status` but **refresh on an interval** until the run completes or you Ctrl-C — a live monitor you can open in a second terminal alongside the run. |
| `--interval <n>` | `15` | `--watch` refresh seconds. |
| `--segment <n>` | `42` | UNSPSC segment scope for `register` + `resolve`. |
| `--all-segments` | off | Run over the WHOLE marketplace (overrides `--segment`). |
| `--limit <n>` | all | Cap records per resolve stage; `all`/`0` = no cap. |
| `--progress-every <n>` | `200` | Resolve progress/checkpoint cadence. |
| `--data-dir <path>` | `data\` | Directory for the checkpoint + resolve outputs. |

> A fresh `pipeline` run **refuses to start if a checkpoint already exists** (so
> an in-progress build is never silently clobbered) — use `--resume` or
> `--restart`. The build and final resolve write to the `pipeline_build` /
> `pipeline_final` `--out` prefixes (distinct so their checkpoints don't collide).
> Resuming with a different `--segment`/`--limit` than the checkpoint is refused.

> **Precomputed loop size.** A resolve stage iterates a *deterministic* number of
> records — the count of buyer lines in scope (resolution only adds catalog
> nodes, it never adds source items, so the count can't drift mid-run). The
> pipeline counts it **once, right after `migrate`** (the earliest point the data
> exists) and saves it to `pipeline.checkpoint.json` under `loop_sizes`. From then
> on `build`/`final-resolve` report `processed N / TOTAL (pct%)` instead of a bare
> count, the resumed-run banner shows how far in you are, and `pipeline --status`
> reads it back any time. The same number is stored in each resolve stage's own
> `*.checkpoint.json` (`total`), so resume restores the denominator without
> re-counting. (A checkpoint from an older build with no `loop_sizes` is
> backfilled automatically on the next `--resume`; `--status` is read-only and
> simply shows no % for a resolve stage until then.)

> **Monitoring the evolution.** Every step that loops over records or groups
> appends a point to a persistent timeline — `<step>.progress.jsonl` — each
> progress tick. The resolve stages write `pipeline_build` /
> `pipeline_final.progress.jsonl` with the full `{ts, processed, total, resolved,
> unresolved, created}`; the `register` / `register-fallback` vet scans and the
> `build-brand-lexicon` scan write `{ts, processed, total}`. A step clears its own
> timeline when it starts fresh, so two runs never blur together, yet it's
> append-only *within* a run, so the curve **spans kills and resumes**.
> `pipeline --status` reads every step's timeline the same way — `N/total (pct%)`,
> the recent **rate (per min)**, an **ETA** (extrapolated from the remaining loop)
> and the **elapsed** processing time — while the steps with no record loop
> (`instance`, `migrate`, `fallback-report`, `train-tier2`) show plain
> `[done]` / `[running…]` / pending. `pipeline --watch [--interval <s>]` reprints
> the whole view on a timer as a live monitor you can leave running in another
> terminal. Rate and elapsed are summed over *active* intervals only — a resume's
> rewind and a kill's idle gap are excluded — so neither is distorted by a pause,
> wherever it falls. Both read straight off disk: they never touch or slow the
> running job (a status check is read-only — it doesn't even write the checkpoint),
> and work from any terminal at any time, even after the run was killed.

### 4.3 `register` — build the category register

Profiles by head-noun × spend, walks the ranking in LLM **vet batches**,
validates every proposed regex mechanically, writes survivors to a proposals
file, and **registers them + drafts a schema for each**. No count cap by default
(proposes every viable family down to `--min-spend`). Progress streams to stderr.

```powershell
chilecompra-er register --segment 42                 # profile → vet → register + schemas
chilecompra-er register --segment 42 --count 10      # ...or cap at the top 10
chilecompra-er register --segment 42 --preview       # stop at data\proposals.json for review
chilecompra-er register --apply                      # ...then commit the edited file
chilecompra-er register --from-fallback --preview    # candidates from the graph residue (§3 Phase 2)
chilecompra-er register --segment 42 --resume        # continue an interrupted vet scan
```

`--preview` and `--apply` are mutually exclusive opt-outs that split the run at a
human-review gap: `--preview` = profile + vet only (write proposals, register
nothing); `--apply` = register an existing/edited proposals file without
re-profiling.

| Flag | Default | Meaning |
|---|---|---|
| `--preview` | off | Profile + vet only: write `--proposals` and stop. |
| `--apply` | off | Register an existing/edited `--proposals` file without re-profiling. |
| `--proposals <path>` | `data\proposals.json` | Proposals file — written by every run, read back by `--apply`. |
| `--ranking <path>` | `data\profiling.csv` | Cached spend ranking. Reused if present (skips the slow scan); rebuilt otherwise. |
| `--reprofile` | off | Force a fresh corpus profile, overwriting `--ranking`. |
| `--from-fallback` | off | Rank candidates from the UNSPSC fallback residue in the graph instead of the whole-corpus profile; needs a prior `--persist` item run. |
| `--segment <n>` | `42` | UNSPSC segment scope when profiling (42 = medical supplies). |
| `--all-segments` | off | Profile the WHOLE marketplace (overrides `--segment`). |
| `--limit <n>` | all | Profile only the first N tender items — fast dev runs. |
| `--count <n>` | none | Stop after N viable categories. Default: no limit (`0`/negative also = no limit). |
| `--min-samples <n>` | `15` | Minimum distinct corpus descriptions a candidate must have. |
| `--min-spend <f>` | `0.0005` | Share floor; the scan stops below it (0.05%). With no `--count`, this is the real stop — lower it to reach deeper into the tail. (Spend-share for the corpus profile; **record-share** for `--from-fallback`, where the ranking is by item count.) |
| `--revisit` | off | Re-evaluate tokens previously cached as junk (`categories\vet_rejections.json`). |
| `--resume` | off | Continue an interrupted vet scan from `data\register.checkpoint.json` — restores the families already chosen and skips the groups already vetted (`--from-fallback` uses `register_fallback.checkpoint.json`). |

> The first run does the slow full-corpus scan and caches the ranking. Re-running
> to tune `--count`/`--min-spend` reuses the cache; add `--reprofile` only when
> the underlying data changed.

> **Resumable vet scan.** A full-segment scan vets thousands of head-noun groups
> through the LLM over many minutes. The chosen families + scan cursor are
> checkpointed after **every batch**, so a kill loses at most the in-flight
> batch: re-run with `--resume` to continue where it stopped (the `pipeline`
> `register` / `register-fallback` stages do this automatically). The checkpoint
> is cleared once the proposals are written, and a `--resume` whose scope
> (`--segment`/`--count`/floors) differs from the checkpoint starts fresh rather
> than splicing two scans.

> **Builds over the existing register — never regenerates it.** Every `register`
> run (and so every `pipeline` run, fresh or resumed) reads the current register
> and schemas first and only adds what's new: the vet scan **skips families
> already covered** by a registered category's Tier-1 regex, `apply` **skips a
> proposed id that's already registered** (no duplicate, no version bump), and
> schema drafting **skips any schema already on disk**. So the 100+ categories and
> schemas you've already built are never re-vetted, re-added, or re-drafted — a
> re-run just extends coverage. Use `--revisit` to re-evaluate cached junk tokens,
> or `generate-schemas --overwrite` to deliberately redraft a schema.

**`add-category <id> --include <regex> [...]`** — manually append one known family
(skips the LLM vet). `--include` is required & repeatable (Tier-1 inclusion regex
over normalized text); `--exclude` (repeatable) resolves sibling overlaps;
`--name`, `--corpus` (raw-text sampling regex for schema generation), `--example`
(golden test fixture) are optional. Draft its schema afterward with
`generate-schemas --only <id>`.

```powershell
chilecompra-er add-category mascarillas --include "\bmascarilla\w*" --example "MASCARILLA QUIRURGICA 3 PLIEGUES"
```

**`generate-schemas [--only <id>] [--samples 50] [--overwrite]`** — LLM strawman
attribute schemas from corpus samples. `register` already drafts one per new
category. **A schema that already exists on disk is skipped** (no LLM call, no
clobbered hand-edits): re-running only fills in the *missing* ones. `--only`
limits to one category; `--samples` is how many descriptions to feed the LLM;
`--overwrite` forces a fresh redraft of schemas that already exist (or just
delete the schema file). This is what lets a `pipeline`/`register` re-run **build
over** an established register instead of regenerating it.

### 4.4 `resolve` — fill the catalog

Runs source records through the §2 pipeline. **Dry run unless `--persist`.** The
*unit of work* is set by `--kind`.

```powershell
# item-centric dry run (recommended)
chilecompra-er resolve --kind item --segment 42 --limit 5000 --show 10 --out data\check
# persist (resumable after a kill)
chilecompra-er resolve --kind item --segment 42 --persist
chilecompra-er resolve --kind item --segment 42 --persist --resume
```

| Flag | Default | Meaning |
|---|---|---|
| `--kind tender\|offer\|oc\|joint\|item` | `tender` | Unit / source of records — see below. |
| `--fallback unspsc\|none` | `unspsc` | **`--kind item` only.** Unmatched items link to a coarse `unspsc_NNNNNNNN` node → ~100% link. `none` leaves them unresolved (curated-only). |
| `--persist` | off | **WRITE** to the graph: GenericProducts, the direct `(:ItemLicitacion)-[:RESOLVED_TO]->` edges, and (item kind) the offers as `OFFERS {price}` edges to their `Brand × offer-spec` `Product`s. Off = dry run. |
| `--segment <n>` | none | UNSPSC segment filter (tender/offer/joint/item; ignored for oc). |
| `--contains <str>` | none | Filter on buyer text (e.g. `foley`). |
| `--limit <n>` | `200` | Max records. `all` (or `0`) = the whole filtered set. |
| `--skip <n>` | `0` | Skip N records (stable order; chunked builds). |
| `--show <n>` | `5` | Print the first N resolved examples (display only). |
| `--out <prefix>` | `data\resolve` | Output prefix (see §5). |
| `--progress-every <n>` | `200` | Progress line every N records (durable checkpoint ~every 5k when persisting, ~every 20k on a dry run). |
| `--resume` | off | Continue `<out>.checkpoint.json` (invocation must match kind/segment/contains/persist/limit). |
| `--brands` | off | Add the brand-lexicon tier after Tier-1 (`categories\brand_lexicon.json`). |
| `--tier2` | off | Add the trained Tier-2 classifier after Tier-1 (needs `train-tier2`). |
| `--tier2-model <path>` | `data\tier2_model.joblib` | Tier-2 model file to load when `--tier2` is set. |
| `--tier2-threshold <f>` | model default (`0.6`) | Override the Tier-2 confidence threshold for this run. |

**`--kind` values:** `item` *(recommended)* — whole `ItemLicitacion` at once
(buyer line + offer consensus + title → one generic product; offers bound via
`OFFERS {price}` edges to their `Brand × offer-spec` Products; UNSPSC fallback).
`tender` — one buyer line + its title as
context (curated only). `joint` — one offer paired with its buyer line (offer
wins; disagreement → review). `offer` — offers standalone. `oc` — purchase-order
items.

Sample `--kind item` summary:

```
mode: dry run (no writes)
records processed : 5000
items linked      : 5000 (100.0%)  = curated 1809 + UNSPSC-fallback 3191
by status         : {'resolved_generic': 5000}
unresolved reasons: {}
by category       : {'agujas': 138, 'canulas': 85, ... 'unspsc_42151602': 246, ...}
curated by tier   : {'tier1': 1503, 'brand': 198, 'tier2': 108}  (which classifier won each curated item — Tier-2's marginal lift)
price basis mix   : {'unknown': 1730, 'per_pack': 79}
resolved w/o attrs: 412 (anchored on category roots — honest partials, no product info)
offers bound      : 75 (as OFFERS edges to branded Products)
offer routing     : {} (per-offer outcome; populated only on --persist:
                        same / refined / nonconforming / recategorized / conservative)
nodes created     : 847
illegal values    : 452 (dropped, counted — schema dry-run metric)
written: data\check_resoluciones.csv
written: data\check_productos_genericos.csv
checkpoint: data\check.checkpoint.json
```

(With `--fallback unspsc`, every item links, so `unresolved reasons` is empty and
`by status` is all `resolved_generic`; `--fallback none` is where those fill in.)

> **Resumable long runs:** a killed `--persist` run leaves a checkpoint; re-run
> the *identical* command with `--resume`. The resolutions CSV is trimmed to the
> last durable checkpoint, so kill timing can never duplicate rows.

> **Live progress %.** For `--kind item`/`tender`, the per-tick progress line reads
> `...processed N/TOTAL (pct%)` against the deterministic loop size: an unbounded
> run counts the in-scope records once up front (index-backed); a bounded `--limit`
> run just uses the limit as the denominator (no count needed). The size is stored
> in the checkpoint (`total`) so `--resume` restores the % without re-counting, and
> the `pipeline` stages reuse the size precomputed at establishment (§4.2) rather
> than counting again.

> **Keeping current.** There is no incremental mode — to fold in new
> tenders/items/offers, re-run the full `resolve --kind item --persist`. Each
> `(:ItemLicitacion)-[:RESOLVED_TO]->` edge is overwritten in place, so the
> re-resolve just refreshes every item to its current generic (no duplicate edges,
> no watermark to maintain).

### 4.5 Coverage tools (Phase 2)

**`fallback-report [--top 20] [--min-count 5] [--out <path>]`** — ranks the UNSPSC
fallback nodes (codes carrying the most fallback items, with awarded spend and
rubric-only share) and the recurring head-noun families in the residue, **by item
count** (spend is shown but smeared/unreliable for the residue — see §3 Step 4).
Writes `data\fallback_ranking.csv`. Needs a prior `resolve --kind item --persist`.

**`ambiguity-report [--top 20] [--min-count 3]`** — the **overlap** counterpart to
`fallback-report`. Re-classifies the UNSPSC fallback residue and ranks the
colliding category **sets** — items that fell to fallback because **two Tier-1
regexes both matched** (ambiguous). Splits **spurious** overlaps (one product two
families both claim — fixable by adding an `--exclude`) from genuine
**multi-product bundles** (a line enumerating several products, ≥2 list
separators — ambiguity is correct, left alone), ranked by spurious/fixable
volume. Read-only (prints to stdout, writes nothing). Needs a prior
`resolve --kind item --persist`.

```
register overlaps: 1,204 ambiguous residue items across 23 colliding category sets
  spurious (one product, fixable with an exclude): 412
  multi-product bundles (ambiguity is correct):    792

top 20 colliding category sets (by spurious/fixable volume):
   spurious  bundle  categories
        198      14  agujas ∩ suturas
      e.g. AGUJA DE SUTURA CT-1 1/2 CIRCULO
```

**`build-brand-lexicon [--only <id>] [--samples 50] [--max-per-category 15]
[--overwrite] [--dry-run]`** — for each curated category, samples the corpus and
asks the LLM for brand/trade-name tokens (`relyx`, `panamax`). Each is validated
(single normalized token, present in samples, not generic filler, not already
regex-covered) and cross-category collisions are dropped; survivors merge into
`categories\brand_lexicon.json` (existing entries win on conflict; `--overwrite`
replaces). `--dry-run` prints without writing.

**`train-tier2 [--threshold 0.6] [--min-rows 500] [--eval] [--skip-if-exists]
[--out <path>]`** — trains the Tier-2 classifier (TF-IDF word+char n-grams +
logistic regression) on the curated resolutions in the graph, saving to
`data\tier2_model.joblib`. It abstains below `--threshold` (only ever adds
coverage). `--eval` reports held-out accuracy on a 10% split. The standalone
command **always retrains** (overwrites) — the out-of-band refresh after the
corpus grows; `--skip-if-exists` makes it a no-op when the model is already
present (this is what the `pipeline` uses, so a re-run trains only when there's no
`.joblib`).

Then enable per run: `resolve --kind item --tier2 --brands` (§4.4).

**`tier2-eval [--gold <csv>] [--cap 80000] [--test-size 0.1] [--min-rows 500]`** —
the **measure-before-tuning** harness. Trains a throwaway model on a held-out
split of the curated resolutions and prints a **coverage/precision curve** across
thresholds — so you can see where the `0.6` cutoff sits and pick an operating
point — computed both **text-only and +UNSPSC feature**, so a feature's lift is a
direct read. With `--gold`, scores the saved model against a human-labeled CSV
(true precision, with a residue-only breakdown). Touches no production model.

```
held-out eval (text only): train 34,000 / test 6,000 across 167 categories
  thresh  coverage  precision  n_classified
   0.60     74.0%      93.4%     4,442
   0.75     65.3%      96.1%     3,917
```

> The held-out precision is *agreement with Tier-1's labels* (an upper bound, since
> Tier-2 exists for the items Tier-1 missed). For **true** precision on the residue,
> build a gold set with `tier2-label-sample` and pass it to `--gold`.

**`tier2-label-sample [--n 300] [--segment 42] [--residue-only] [--out <path>]`** —
exports a sample of items + the current classifier's predictions to a CSV template
(blank `true_category` column) so a human can build a gold set for
`tier2-eval --gold`. `--residue-only` keeps only items Tier-1 misses — the rows
Tier-2 is actually judged on.

### 4.6 Analyze

**`price-series <category_id> [--csv <path>]`** — per-product price history for a
**persisted** category, read off the `(:Oferta)-[:OFFERS]->(:Product)` edges
under each generic product (the price lives on the edge; each row also carries the
offer's `brand` via `(:Product)-[:OF_BRAND]->(:Brand)`, so prices can be sliced by
brand). Each row carries the raw `unit_price` **and a
`normalized_unit_price` (per base unit) with its `basis`** — a per-pack quote is
divided by its stated pack size only when that lands it in the product's price
cluster, else `basis=unknown` and the point is flagged out (§7). Default
`data\price_series_<category>.csv`. Empty until that category was
`resolve --kind item --persist`ed (offers carry the prices).

```
5790 price observations across 229 generic products -> data\price_series_cintas_adhesivas.csv
products with the deepest price history:
  gp_f787b08e384c  n= 90 (+6 flagged)  median=  498 CLP/base-unit  range=[239 .. 1,423]
    {"largo": "9.1m", "tipo": "quirurgica", "ancho": "2.5cm", "material": "papel"}
```

(The `(+N flagged)` count is offers whose basis couldn't be determined — excluded
from the stats, never assumed. A category-**root** node mixes products, so it shows
a high flag rate: that's the signal it isn't a single comparable product.)

### 4.7 Housekeeping & diagnostics

**`clean [--all] [--dry-run] [--dir <path>]`** — delete **regenerable** `data\`
artifacts (resolve output triplets incl. `pipeline.checkpoint.json`,
`price_series_*`, loose `*.log`/`*.out`). Keeps the cached inputs
`profiling.csv`, `proposals.json`, and `fallback_ranking.csv` unless `--all`.
Does **not** remove `tier2_model.joblib` (regenerate it with `train-tier2`).
Never touches the graph (that's `wipe-catalog`).

| Command | What it does |
|---|---|
| `demo` | Offline pipeline demo — no graph, no LLM. Good first run to see the mechanics. |
| `smoke [--keep]` | Live graph round-trip test; cleans up after itself (`--keep` leaves the data). |
| `probe-offers [--limit 1500]` | Metric: how often offer text recovers a category for rubric-only buyer lines. Read-only. |

### 4.8 Destructive (gated by `--yes`)

| Command | What it does |
|---|---|
| `wipe-category <id> --yes` | Delete one category's catalog nodes (and their RESOLVED_TO edges). |
| `wipe-catalog --yes` | Delete ALL catalog data (Category / GenericProduct / Product / Brand). Source data + migrations untouched. |

### 4.9 Experimental — resolution redesign (`canonicalize`)

A new resolution pipeline is being built **alongside** the current one — it does
not touch the live `:GenericProduct` catalog yet. It replaces the regex
extractor + classifier with a single LLM **canonicalization** step feeding a
deterministic matcher:

```
L0 dedup → L1 canonicalize (Claude Haiku 4.5) → L2 match → L3 adjudicate → coherence-check
```

**`canonicalize [--from-file <path>] [--out data\profiles.jsonl] [--model claude-haiku-4-5] [--limit <n>] [--dry-run]`**
— **L1**: turns each distinct `descripcion_proveedor` into a structured
**profile** (category + **evidence-anchored** identity attributes + brand + model
token + packaging), persisted by text-hash so each distinct string is
canonicalized **once** (a cached pure function). The cardinal rule: every
identity attribute must quote the substring that anchors it — a bare number can
never become identity (the redesign's answer to the `2.5pct` false-merge class).

| Flag | Default | Meaning |
|---|---|---|
| `--from-file <path>` | none | Read newline-separated descriptions from a file instead of the graph (runnable now). |
| `--out <path>` | `data\profiles.jsonl` | Profile store — JSONL keyed by text-hash; the L1 cache (skip already-done). |
| `--model <id>` | `claude-haiku-4-5` | L1 model. |
| `--segment <n>` | all | UNSPSC segment scope for the graph read, e.g. `42` (bounds a run). |
| `--limit <n>` | all | Cap inputs (dev runs). |
| `--dry-run` | off | **L0 dedup only** — report distinct/cached counts, **no LLM calls, no API credits spent**. |

> **Status.** The graph read (streamed `descripcion_proveedor` + UNSPSC, deduped
> by text-hash) and the `--from-file`/`--dry-run`/`--segment` paths are
> implemented and tested. The only piece not yet exercised is the **bulk LLM
> batch run** — it uses the **Batch API + prompt caching** and therefore **bills
> API credits** (the `anthropic_sdk` backend), *not* the Claude Max subscription,
> so load credits before running it.

**`match [--store data\profiles.jsonl] [--attach-partials] [--persist] [--segment <n>] [--show 15]`**
— **L2**: clusters the L1 profile store into product clusters. The pairwise rule:
same `model_token` ⇒ same product (even cross-brand); a conflicting attribute is a
hard cut; identical signatures collapse; a coarser partial spec is linked by
`REFINES` rather than merged (unless `--attach-partials` and it has a unique finer
child). Default is an offline report (no graph, no LLM). With **`--persist`** it
writes `:ProductCluster` + `:REFINES` nodes and binds offers via
`(:Oferta)-[:PRICED_IN {normalized_price, rut, date}]->(:ProductCluster)` (price
per base unit on the edge); run `migrate` first (migration `005`).

**`coherence-check [--store data\profiles.jsonl] [--graph] [--tier all] [--out <csv>]`**
— **L4 auditor**: runs the named invariants in three tiers. **Structural** (e.g.
S1 every identity attribute has evidence, S2 no anchorless-number evidence, S5
unique cluster signatures, S7 strict-subset REFINES) are a contract — any breach
**fails the run (exit 1)**, suitable as a CI gate. **Semantic** (M1 weak-identity
clusters, M4 model-token conflicts, ambiguous partials; with `--graph`: unplaced
offers, price-incoherent clusters) are ranked review backlogs. **Health** is a
trend snapshot (confidence mix, placement). Offline by default; `--graph` adds
checks over the persisted catalog. L3 adjudication and the Phase-6 cutover from
the legacy `:GenericProduct` catalog remain.

---

## 5. Files & outputs

Outputs are split by **lifecycle**:

- **The catalog definition is version-controlled**, under
  `chilecompra_er\categories\`: `register.json` (the family list) and
  `schemas\*.json` (one attribute schema per category). This is the real
  deliverable — code-reviewed, diffed in PRs, asserted by tests — so `register`
  writes it here, **not** to `data\`.
- **`data\` is gitignored scratch** — all reproducible. Two sub-groups:
  - *Cached inputs `clean` keeps* (removed only with `--all`): `profiling.csv`
    (spend ranking), `proposals.json` (preview→apply handoff),
    `fallback_ranking.csv` (residue ranking).
  - *Run outputs `clean` always removes*: `<prefix>_resoluciones.csv` (every
    record + its resolution), `<prefix>_productos_genericos.csv` (the
    generic-product nodes, dry runs only), `<prefix>.checkpoint.json` (per-run
    resolve resume marker), `<step>.progress.jsonl` (the append-only per-step
    progress timelines — the resolve stages plus the `register`/`register-fallback`
    and `build-brand-lexicon` scans — that feed `pipeline --status`/`--watch`, §4.2),
    `pipeline.checkpoint.json` (stage-level `pipeline`
    resume marker), `register.checkpoint.json` / `register_fallback.checkpoint.json`
    (the `register` vet-scan resume markers, §4.3), `price_series_<cat>.csv`.
  - `tier2_model.joblib` (the trained classifier) also lives here but `clean`
    leaves it — regenerate it with `train-tier2`.
- **The populated catalog lives in Neo4j**, written only by `resolve --persist`:
  `Category` / `GenericProduct` / `Product` / `Brand` nodes and the
  `IN_CATEGORY` / `RESOLVED_TO` (direct, from `ItemLicitacion`) / `VARIANT_OF` /
  `OF_BRAND` / `OFFERS {price}` / `PARENT_OF` edges. `register` never touches the
  graph.

---

## 6. Conventions & troubleshooting

**Conventions**

- **`resolve` is a dry run by default** — nothing is written until `--persist`.
  Every other read-only command is always safe.
- **Fixed output filenames** — commands overwrite the same `data\` files each
  run; pass a path flag (`--out`, `--csv`) only to keep a snapshot elsewhere.
- **Destructive graph commands are gated** behind `--yes`; local-file cleanup is
  `clean`.
- **stderr vs stdout** — progress/diagnostics go to **stderr**; the actual
  report goes to **stdout**, so you can redirect the result cleanly (`... 2> run.log`).
- **Nothing is force-fit** — a line matching no family is never shoved into the
  wrong one: `--kind item` links it to a UNSPSC bucket; other kinds (or
  `--fallback none`) record it `unresolved`. The biggest residual buckets are the
  signal for the next `register` pass.

**Troubleshooting**

| Symptom | Cause / fix |
|---|---|
| `graph: unreachable` in `status` | Neo4j stopped (`instance start` — also rewrites `.mcp.json`). If *running* but bolt **times out**, `neo4j-sg` doesn't allow your client IP — add it. |
| Lots of `unspsc_*` categories after `resolve` | Expected — fallback buckets. Run Phase 2 (§3) to convert coarse coverage into rich coverage. |
| `register` / `generate-schemas` / `build-brand-lexicon` auth errors | Run `ant auth login` once (Claude Max). |
| A `--segment` run scans forever | The UNSPSC index is missing — run `migrate`. |
| `price-series` prints "no price observations" | That category isn't persisted yet, or was resolved with a kind other than `item` (only `--kind item --persist` binds `:Product` prices). |
| `resolve --resume` refuses | The invocation must match the checkpoint (kind/segment/contains/persist/limit). |
| `data\` filling up | `chilecompra-er clean` (keeps the cached rankings + proposals; `--all` removes those too). |

---

## 7. Internals & notes

- The ingestion source is the graph itself — the transactional layer. Resolution
  only *adds* the catalog layer and never mutates those nodes.
- The tender-title property is one constant (`TENDER_NAME = l.titulo` in
  `ingest/neo4j_source.py`); a missing/renamed property degrades gracefully to
  item-only resolution.
- `GenericProduct` identity is **exact match on present identity attribute
  values** (same values, same absences), embedded in `identity_key` — the
  uniqueness key that dedups a product across tenders. `PARENT_OF` builds the
  coarse→specific hierarchy by strict-subset subsumption.
- **Branded products (`Product = Brand × the offer's identity`).** A `:Product` is
  the deduped `(generic, brand, offer-identity)` node —
  `id = pr_<sha1(generic_id|brand=brand_id|attr=val|…)>`
  (`resolve/assignment.py:branded_product_id`) — so every offer of the same brand
  **and the same spec** collapses onto one **self-describing** node (the offer's
  identity values live on the node), `VARIANT_OF` the item's generic and `OF_BRAND`
  a shared `:Brand`. Brand and spec are independent axes, so one brand can have
  several Products under one generic (one per distinct offer spec). Each bid is an
  explicit `(:Oferta)-[:OFFERS {price…, conforming}]->(:Product)` edge: **the price
  (and per-offer descriptives) are on the edge, not the node**. The brand for an
  offer comes from the brand lexicon / offer text (`resolve/brand.py:extract_brand`)
  during `_bind_offers` (`ingest/runner.py`); offers with no recognizable brand fall
  to a shared `SIN_MARCA` sentinel. Schema (the `Brand.id` constraint +
  `Product.identity_key` index) is in migration `003_branded_products.cypher`,
  applied by `migrate`.
- **Offer-aware binding & the spec chain.** An item's offers don't all collapse
  onto its one generic. The generic is the buyer's demand, enriched **upward only**
  by the offers' *minimum common* spec (`resolve/resolver.py:offer_identity_floor`
  — dominant value adopted only when an awarded/winning offer carries it, else
  strict). Each offer then routes (`ingest/runner.py:_offer_target`,
  `stats.offer_routing`): `same` (== generic) / `refined` (a finer Product, same
  generic) / `nonconforming` (vaguer-or-divergent, `conforming=false`, kept) /
  `recategorized` (a confidently different family → its own generic,
  `conforming=false`) / `conservative` (too terse to classify → item node). The
  invariant is the chain `ItemLicitacion = GenericProduct ≤ Product ≤ Oferta`:
  refinement rides the **Product**, never a finer generic (the generic hierarchy is
  driven by demand only).
- **`RESOLVED_TO` is direct and current-state only.** One
  `(:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct)` edge per item, carrying the
  resolution `evidence` (normalized text, classifier match + winning tier, attribute
  values + provenance, price basis). Re-resolving **overwrites** it (DELETE old,
  CREATE new) — no versioning, no SourceRecord, no lineage/event layer. The model is
  deliberately flat: to update, re-run a full `resolve --persist`. (Migration
  `004_drop_lineage.cypher` removed the old `SourceRecord` / `ResolveRun` /
  `ResolutionEvent` / `ResolveState` schema.)
- The Tier-2 **UNSPSC feature** was evaluated (`tier2-eval`) and showed no frontier
  lift on this corpus — it trades precision for coverage like a threshold nudge — so
  it is intentionally **not** wired into production; only the eval harness folds it in.
- **Price-basis normalization** (unit vs box) is handled at read time in
  `price-series` (`price/basis.py:normalize_unit_prices`): in Mercado Público
  `total = qty × unit_price` is an accounting identity, so the basis is inferred
  from price **magnitude** instead — per-unit prices for one GenericProduct
  cluster, and a per-pack quote is divided by its stated pack size only when that
  lands it in the cluster (positive evidence); offers that fit neither are flagged
  `unknown` and excluded.
- **Anchorless-rule guard (extraction safety).** An identity extraction rule that
  could fire on a bare number (no unit or concept word) must now carry a
  `requires` context guard naming the attribute's concept
  (`categories/schema.py:Rule.requires`); the rule fires only when its guard also
  matches the text — a guard can only narrow matches, never widen them. A lint
  (`anchorless_identity_rules`) + test (`tests/test_schema_anchors.py`) enforce it
  across all schemas, closing the false-merge class where "Ca 2,5 mEq" and
  "cable 3x2,5 mm" were read as dextrose 2.5% (the `pr_fd4522…` bucket).
- Known follow-ups: **brand canonicalization** (the `Brand` nodes still hold
  spelling variants — `BIOLIGH`/`BIOLIGHT` — that should collapse), an `OFFERS`
  edge `date` for offers whose source `fecha` is null, registering the largest
  UNSPSC-fallback buckets as curated families, and a gold set for true Tier-2
  precision (`tier2-label-sample` → `tier2-eval --gold`).
