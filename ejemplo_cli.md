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
| `Product` | A brand-level variant — **one per offer** — carrying the offer's price. `VARIANT_OF` a GenericProduct. |
| `SourceRecord` | A thin reference back to a source line/offer, with a versioned `RESOLVED_TO` edge to its catalog node. |

```
  SOURCE GRAPH (read-only input)            CATALOG (written by `resolve`)
  ──────────────────────────────           ───────────────────────────────

   Licitacion
      │ :TIENE_ITEM
      ▼
   ItemLicitacion  ┄┄1 per item┄┄▶  SourceRecord ──:RESOLVED_TO──┐  (versioned)
      ▲  buyer text + UNSPSC                                      │
      │ :PARA_ITEM                                                ▼
   Oferta  ┄┄┄┄┄┄1 per offer┄┄┄▶  Product ──:VARIANT_OF──▶  GenericProduct ──:IN_CATEGORY──▶ Category
      supplier text + price                                  │   ▲   shared node, deduped
                                                             └───┘   by identity attributes
                                                          :PARENT_OF  (coarse → fine hierarchy)

  ──▶  stored edge        ┄┄▶  created by the resolver (a node, not a stored edge)
```

Each `ItemLicitacion` becomes one `SourceRecord` that `RESOLVED_TO` a
`GenericProduct`; each `Oferta` on that item becomes one `Product` that is
`VARIANT_OF` the **same** `GenericProduct`. Identical products across different
tenders collapse onto one shared `GenericProduct` (that is what makes
cross-tender price comparison possible), and coarser nodes parent finer ones via
`:PARENT_OF`.

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
4. **Infer price basis** — `per_pack` when the text carries pack evidence,
   otherwise `unknown`; the arithmetic cross-check (total ≈ quantity × unit
   price) can additionally confirm a `per_base_unit` price. Unknown is reported,
   never assumed.
5. **Find or create the generic product** — the node whose *present identity
   values* exactly match (same values, same absences). A line with no extracted
   identity attributes anchors honestly on the **category root** (specificity 0);
   more-specific nodes hang under coarser ones via `PARENT_OF` (strict-subset
   hierarchy), so "Foley" parents "Foley 16Fr".
6. **Persist** — upsert the `SourceRecord` and a **versioned** `RESOLVED_TO` edge
   (re-resolving adds a new version and retires the prior one — history is kept,
   see §7).

### The item-centric model (`--kind item`) — the recommended way

Instead of resolving each record independently, resolve one **`ItemLicitacion`
at a time**, pooling every signal it has:

- **Category priority:** buyer line → **offer consensus** (majority vote across
  the item's offers) → tender title. Offers are the key lever — a buyer line that
  is just a rubric path ("Equipamiento médico / … / Sondas") gets its real
  category from the supplier offers that actually name the product.
- **One generic product per item.** All of the item's offers bind beneath that
  single node as `:Product` variants — they can't scatter to different products.
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

**The one thing to internalize — where the LLM is (and isn't).** Only **three**
commands ever call the LLM: `register`, `generate-schemas`, and
`build-brand-lexicon`. Everything else is deterministic. The LLM is used to
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
| 4 | `fallback-report` | the **fallback residue** (graph) | no | `fallback_ranking.csv` | residue ranked |
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
item, links its `SourceRecord`, and binds **every offer as a priced `:Product`
variant**. Unmatched items link to their `unspsc_<code>` bucket → **coverage ≈
100%**. The items that landed on fallback are the **residue** Phase 2 attacks.

#### Phase 2 — shrink the fallback residue (Steps 4–6, then loop)

**Step 4 — `fallback-report`.** Reads the `unspsc_*` fallback nodes and ranks them
by item volume + awarded spend, splitting rubric-only boilerplate from real
products and surfacing the recurring head-noun **families** in the tail.
**Output:** `data\fallback_ranking.csv` — a prioritized "curate these next" list.

**Step 5 — `register --from-fallback` (2nd run): curate the tail.** The *same*
`register` machinery as Step 1, but candidates come from the **graph residue**
instead of the whole-corpus profile — so the LLM vets exactly the families that
failed to resolve. It re-queries the fallback nodes from the graph itself (and
rewrites `fallback_ranking.csv`), so Step 4 is a useful preview but not a strict
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
chilecompra-er fallback-report --top 20         # 4. rank the residue
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
graph            : 119 categories, 6059 generic products, 99386 source records
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
7. **`train-tier2`** — train the Tier-2 classifier
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
chilecompra-er pipeline --status                     # snapshot: plan + progress % + rate + ETA
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
| `--restart` | off | Discard the pipeline checkpoint + the resolve sub-checkpoints + their progress timelines; start from the first stage. |
| `--from-step <stage>` | none | Force-run this stage and everything after it (ignores the done list). |
| `--only <stage>` | none | Run just this one stage. |
| `--status` | off | Print the plan (stages done/pending) + each resolve stage's loop size, live progress %, **rate (records/min) and ETA**, then exit without running anything. |
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
> backfilled automatically on the next `--resume` or `--status`.)

> **Monitoring the evolution.** Each resolve stage appends a point to a persistent
> timeline — `<prefix>.progress.jsonl` (e.g. `pipeline_build.progress.jsonl`) —
> every progress tick: `{ts, processed, total, resolved, unresolved, created}`.
> It's append-only, so the curve **spans kills and resumes**. `pipeline --status`
> reads it to show the recent **rate (records/min)**, an **ETA** (extrapolated from
> the remaining loop) and the **elapsed** processing time; `pipeline --watch
> [--interval <s>]` reprints that on a timer as a live monitor you can leave running
> in another terminal. Rate and elapsed are summed over *active* intervals only —
> a resume's rewind and a kill's idle gap are excluded — so neither is distorted by
> a pause, wherever it falls. Both read straight off disk: they never touch or slow
> the running job, and work from any terminal at any time, even after the run was
> killed.

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
| `--min-spend <f>` | `0.0005` | Spend-share floor; the scan stops below it (0.05%). With no `--count`, this is the real stop — lower it to reach deeper into the tail. |
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
| `--persist` | off | **WRITE** to the graph: SourceRecords, GenericProducts + RESOLVED_TO, and (item kind) the offers' `:Product` variants. Off = dry run. |
| `--segment <n>` | none | UNSPSC segment filter (tender/offer/joint/item; ignored for oc). |
| `--contains <str>` | none | Filter on buyer text (e.g. `foley`). |
| `--limit <n>` | `200` | Max records. `all` (or `0`) = the whole filtered set. |
| `--skip <n>` | `0` | Skip N records (stable order; chunked builds). |
| `--show <n>` | `5` | Print the first N resolved examples (display only). |
| `--out <prefix>` | `data\resolve` | Output prefix (see §5). |
| `--progress-every <n>` | `200` | Progress line every N records (durable checkpoint ~every 20k). |
| `--resume` | off | Continue `<out>.checkpoint.json` (invocation must match kind/segment/contains/persist/limit). |
| `--brands` | off | Add the brand-lexicon tier after Tier-1 (`categories\brand_lexicon.json`). |
| `--tier2` | off | Add the trained Tier-2 classifier after Tier-1 (needs `train-tier2`). |
| `--tier2-model <path>` | `data\tier2_model.joblib` | Tier-2 model file to load when `--tier2` is set. |
| `--tier2-threshold <f>` | model default (`0.6`) | Override the Tier-2 confidence threshold for this run. |

**`--kind` values:** `item` *(recommended)* — whole `ItemLicitacion` at once
(buyer line + offer consensus + title → one generic product; offers bound as
priced variants; UNSPSC fallback). `tender` — one buyer line + its title as
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
price basis mix   : {'unknown': 1730, 'per_pack': 79}
resolved w/o attrs: 412 (anchored on category roots — honest partials, no product info)
offers bound      : 75 (as :Product variants under their item's node)
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

### 4.5 Coverage tools (Phase 2)

**`fallback-report [--top 20] [--min-count 5] [--out <path>]`** — ranks the UNSPSC
fallback nodes (codes carrying the most fallback items, with awarded spend and
rubric-only share) and the recurring head-noun families in the residue. Writes
`data\fallback_ranking.csv`. Needs a prior `resolve --kind item --persist`.

**`build-brand-lexicon [--only <id>] [--samples 50] [--max-per-category 15]
[--overwrite] [--dry-run]`** — for each curated category, samples the corpus and
asks the LLM for brand/trade-name tokens (`relyx`, `panamax`). Each is validated
(single normalized token, present in samples, not generic filler, not already
regex-covered) and cross-category collisions are dropped; survivors merge into
`categories\brand_lexicon.json` (existing entries win on conflict; `--overwrite`
replaces). `--dry-run` prints without writing.

**`train-tier2 [--threshold 0.6] [--min-rows 500] [--eval] [--out <path>]`** —
trains the Tier-2 classifier (TF-IDF word+char n-grams + logistic regression) on
the curated resolutions in the graph, saving to `data\tier2_model.joblib`. It
abstains below `--threshold` (only ever adds coverage). `--eval` reports held-out
accuracy on a 10% split.

Then enable per run: `resolve --kind item --tier2 --brands` (§4.4).

### 4.6 Analyze

**`price-series <category_id> [--csv <path>]`** — per-product price history for a
**persisted** category, read from the awarded `:Product` offers under each
generic product. Default `data\price_series_<category>.csv`. Empty until that
category was `resolve --kind item --persist`ed (offers carry the prices).

```
11 price observations across 7 generic products -> data\price_series_sondas_foley.csv
products with the deepest price history:
  gp_ffaf1899ba78  n=  4  median=       7,480 CLP  range=[330 .. 21,510]
    {"calibre": "16fr", "material": "latex"}
```

(Wide ranges usually reflect price-**basis** mixing — unit vs box — which
normalization will address; unknown basis is reported, never assumed.)

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
| `wipe-category <id> --yes` | Delete one category's catalog nodes + their SourceRecords. |
| `wipe-catalog --yes` | Delete ALL catalog data (Category / GenericProduct / Product / SourceRecord). Source data + migrations untouched. |

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
    resolve resume marker), `<prefix>.progress.jsonl` (the append-only progress
    timeline that feeds `pipeline --status`/`--watch`, §4.2),
    `pipeline.checkpoint.json` (stage-level `pipeline`
    resume marker), `register.checkpoint.json` / `register_fallback.checkpoint.json`
    (the `register` vet-scan resume markers, §4.3), `price_series_<cat>.csv`.
  - `tier2_model.joblib` (the trained classifier) also lives here but `clean`
    leaves it — regenerate it with `train-tier2`.
- **The populated catalog lives in Neo4j**, written only by `resolve --persist`:
  `Category` / `GenericProduct` / `Product` / `SourceRecord` nodes and
  `IN_CATEGORY` / `RESOLVED_TO` / `VARIANT_OF` / `PARENT_OF` edges. `register`
  never touches the graph.

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
- `RESOLVED_TO` edges are **versioned** — re-resolving adds a new version and
  retires the prior one (sets `current=false`) rather than deleting it, so the
  resolution history stays auditable.
- Known follow-ups: `:Product` cross-offer brand dedup (currently one Product per
  offer), price-**basis** normalization (unit vs box), and registering the
  largest UNSPSC-fallback buckets as curated families.
