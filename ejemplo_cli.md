# `chilecompra-er` — CLI guide

A complete guide to the `chilecompra-er` command-line tool: what it does, the
ideas behind it, and how to use every command. No prior knowledge of the
codebase is assumed.

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

The data already lives in a **Neo4j graph** (the transactional layer:
tenders, line items, offers, purchase orders). The pipeline reads from that
graph and *adds* a catalog layer on top of it — it never modifies the source
data.

The **CLI is the single operational surface.** It profiles the corpus, builds
the category catalog, resolves records into it, and reports prices.

Invoke it as `chilecompra-er <command>` (installed entry point) or, from inside
the repo, `.\.venv\Scripts\chilecompra-er.exe <command>`. Examples use
PowerShell paths (`data\...`).

---

## 2. Core concepts

Read this once and the rest of the guide makes sense.

### The source graph (read-only input)

Loaded by a separate ingestion pipeline; the resolver only reads it:

| Node | What it is | Key text |
|---|---|---|
| `Licitacion` | A tender (procurement process) | `titulo` — the tender headline |
| `ItemLicitacion` | One **line item** a buyer wants | `descripcion_comprador` — the buyer's product text |
| `Oferta` | A supplier **offer/bid** for a line item | `descripcion_proveedor` — the supplier's product text, plus prices |
| `OrdenCompra` / `ItemOC` | Purchase orders | — |

Relationships: `(Licitacion)-[:TIENE_ITEM]->(ItemLicitacion)` and
`(Oferta)-[:PARA_ITEM]->(ItemLicitacion)`. Each `ItemLicitacion` also carries a
**UNSPSC code** (`codigo_unspsc_producto`) — the UN's standard product taxonomy
number (segment 42 = medical/lab supplies).

### The catalog (what the pipeline builds)

| Node | What it is |
|---|---|
| `Category` | A product family (e.g. `sondas_foley`) with a typed attribute **schema** |
| `GenericProduct` | A canonical product *within* a category, identified by its **identity attribute values** (e.g. Foley + 16Fr + latex). Shared across all tenders that describe it. |
| `Product` | A brand-level variant — **one per offer** — carrying the offer's price. Linked `VARIANT_OF` a GenericProduct. |
| `SourceRecord` | A thin reference back to a source line/offer, with a versioned `RESOLVED_TO` edge to its catalog node. |

The shape — two graphs (the read-only **source** and the **catalog** the
resolver builds), bridged by the nodes `resolve` creates: one `SourceRecord`
per item, one `Product` per offer. `GenericProduct` is the hub everything
converges on.

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

Read it as: each `ItemLicitacion` becomes one `SourceRecord` that `RESOLVED_TO`
a `GenericProduct`; each `Oferta` on that item becomes one `Product` that is
`VARIANT_OF` the **same** `GenericProduct` — so every offer on an item shares
one canonical node (the intra-item invariant). Identical products across
different tenders collapse onto the same `GenericProduct`, and coarser nodes
parent finer ones via `:PARENT_OF`.

### How a description becomes a catalog node

`resolve` runs each description through an ordered pipeline:

1. **Normalize** — lowercase, strip accents, split digit/letter runs, expand a
   versioned abbreviation table. ("SONDA FOLEY 2VÍAS" → "sonda foley 2 vias").
2. **Classify (Tier-1)** — deterministic include/exclude **regexes** per category
   (from the register). Exactly one category matches → *classified*; none →
   *unclassified*; more than one → *ambiguous*. Nothing is ever guessed.
3. **Extract attributes** — per the category's schema, pull identity + descriptive
   values (e.g. `calibre=16fr`, `material=latex`). Out-of-domain values are
   dropped and counted (the "illegal values" metric).
4. **Infer price basis** — per-unit vs per-pack, cross-checked against
   total ≈ quantity × unit price.
5. **Find or create the generic product** — the node whose *present identity
   values* exactly match (same values, same absences). A description with no
   extracted identity attributes anchors honestly on the **category root**
   (specificity 0). More-specific nodes hang under coarser ones via `PARENT_OF`
   (a strict-subset hierarchy), so "Foley" is the parent of "Foley 16Fr".
6. **Persist** — upsert the `SourceRecord` and a **versioned** `RESOLVED_TO`
   edge (re-resolving adds a version, never overwrites).

Identity is exact-match on the present attribute values, so the **same product
described in two different tenders collapses onto one shared GenericProduct** —
which is what makes cross-tender price comparison possible.

### The item-centric model (`--kind item`)

The recommended way to resolve. Instead of treating each record independently,
it resolves one **`ItemLicitacion` at a time**, pooling every signal it has:

- **Category priority:** the buyer line → **offer consensus** (majority vote
  across the item's offers) → the tender title. Offers are the key lever: a
  buyer line that is just a rubric path ("Equipamiento médico / … / Sondas")
  gets its real category from the supplier offers that actually name the product.
- **One generic product per item.** All of the item's offers bind beneath that
  single node as `:Product` variants (the intra-item invariant) — they can't
  scatter to different products.
- **UNSPSC fallback.** An item no curated family matches still links to a coarse
  `GenericProduct` keyed by its UNSPSC commodity code (`unspsc_42182200`). Every
  item has a code, so coverage approaches **100%** — nothing is force-fit into
  the wrong family; the residue is just coarse-grained.

The headline metric for an `--kind item` run is **items linked = curated +
UNSPSC-fallback**.

### Building the catalog: the register

The `Category` definitions are produced by `register`, which: profiles the
corpus by **head-noun × spend** to rank candidate families; sends the top of the
ranking to an LLM that judges which token groups are coherent product families
and proposes a definition (id, include/exclude regexes, a canonical example);
then **mechanically validates** every proposal (regexes compile, the canonical
example classifies to its own new category, and no *existing* category's golden
example becomes ambiguous). Survivors are written to the register, and a draft
attribute **schema** is generated for each from real corpus samples. The LLM
proposes; deterministic code disposes.

---

## 3. Install & prerequisites

```powershell
# one-time: create the venv and install the package (editable)
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
```

Before running graph/LLM commands:

- **Neo4j up** — `chilecompra-er instance start` boots the EC2 box and prints
  the new bolt IP. The public IP **changes on every start**, but `instance
  start` rewrites `.mcp.json` to it automatically. (A bolt *timeout* right after
  a clean start usually means the `neo4j-sg` security group doesn't allow your
  current client IP — not that Neo4j is down.)
- **LLM auth** (only for `register` / `generate-schemas`, which call Claude) —
  run `ant auth login` once. Calls go through the Claude Max subscription.
- Run **`chilecompra-er status`** any time for a register + instance + graph
  sanity check.
- Run the **test suite** after register/schema changes:
  `.\.venv\Scripts\python.exe -m pytest tests -q`.

---

## 4. Quick start (end-to-end)

```powershell
# 0. Bring up infra and apply graph constraints/indexes
chilecompra-er instance start
chilecompra-er migrate

# 1. Build the register: profile the corpus, vet families, register them, and
#    draft their schemas — all in one command. (no --count = every family above
#    --min-spend; add --count 10 to cap.) Use --preview to stop at the proposals
#    file for review, then `register --apply` to commit the edited file.
chilecompra-er register --segment 42

# 2. Resolve a sample as a DRY RUN and inspect quality (no graph writes).
#    --kind item pools each ItemLicitacion's offers + UNSPSC fallback -> ~100%
#    item coverage; the summary leads with "items linked = curated + fallback".
chilecompra-er resolve --kind item --segment 42 --limit 5000 --show 10 --out data\check

# 3. Happy with the split? Persist for real (resumable). Offers land as
#    :Product price points under each item's generic product.
chilecompra-er resolve --kind item --segment 42 --persist

# 4. Analyze + verify (price-series reads the persisted :Product prices)
chilecompra-er price-series sondas_foley
chilecompra-er status

# 5. Tidy up local scratch files, then shut down (graph data persists)
chilecompra-er clean
chilecompra-er instance stop
```

The pipeline is a dependency chain — each stage produces what the next consumes:

```
register  →  resolve  →  price-series
(defines       (links real      (reads prices off
 categories     items to the     the persisted
 + schemas)     catalog)         :Product offers)
```

A fresh `register` run is the prerequisite for resolving any new family. To cut
the items that land on UNSPSC fallback after resolving, see §5.4 (`fallback-report`
→ `register --from-fallback` and/or `train-tier2` / `build-brand-lexicon` →
`resolve --kind item --tier2 --brands`).

---

## 5. Command reference

Every command. Flags shown with their defaults; `[ ]` marks optional.

### 5.1 Infrastructure

#### `status`
Register version, per-category schema status, Neo4j instance state, and graph
node counts. Read-only; never crashes on a flaky graph.

```
register version : 1.78.0
category                  status      schema          identity attrs
sondas_foley              launched    1.2.0           calibre, material, vias
agujas                    candidate   1.0.0           calibre, tipo
...
neo4j instance   : i-06c721c54d821f3a8 running @ 52.91.37.106
graph            : 119 categories, 6059 generic products, 99386 source records
```

#### `instance start|stop|status`
Neo4j EC2 lifecycle. `start` boots the box and prints the new bolt IP (which
changes each start); `stop` shuts it down (graph data persists); `status` prints
the current state.

```powershell
chilecompra-er instance start    # -> "running, bolt ready @ 52.91.37.106"
chilecompra-er instance stop
chilecompra-er instance status
```

#### `migrate [--dry-run]`
Apply the graph schema migrations (uniqueness constraints + indexes) under
`chilecompra_er/migrations/*.cypher`. Idempotent. `--dry-run` prints the pending
Cypher without running it.

---

### 5.2 Build the category register

#### `register` — the catalog-building loop, end to end
Profiles the corpus by head-noun × spend, walks the ranking in LLM **vet
batches** (each token group judged into a coherent family or rejected),
validates every proposed regex mechanically, writes the survivors to a proposals
file, and **registers them + drafts a schema for each one**.

By default there is **no count cap** — it proposes every viable family down to
the `--min-spend` floor. Progress (scan position, batches vetted, candidates
accepted) streams to stderr.

```powershell
chilecompra-er register --segment 42                 # profile → vet → register + schemas
chilecompra-er register --segment 42 --count 10      # ...or cap at the top 10
chilecompra-er register --segment 42 --preview       # stop at data\proposals.json for review
chilecompra-er register --apply                      # ...then commit the edited file
```

Two opt-out flags split the run when you want a human review gap
(mutually exclusive):
- **`--preview`** — profile + vet only; write the proposals file, register
  nothing.
- **`--apply`** — register an existing / hand-edited proposals file *without*
  re-profiling (the commit half on its own).

| Flag | Default | Meaning |
|---|---|---|
| `--preview` | off | Profile + vet only: write `--proposals` and stop. |
| `--apply` | off | Register an existing/edited `--proposals` file without re-profiling. |
| `--proposals <path>` | `data\proposals.json` | Proposals file — written by every run, read back by `--apply`. |
| `--ranking <path>` | `data\profiling.csv` | Cached spend ranking. Reused if present (skips the slow corpus scan); rebuilt otherwise. |
| `--reprofile` | off | Force a fresh corpus profile, overwriting `--ranking`. |
| `--from-fallback` | off | Rank candidates from the UNSPSC fallback residue in the graph (target what failed to resolve) instead of the whole-corpus profile; needs a prior `--persist` item run. See §5.4. |
| `--segment <n>` | `42` | UNSPSC segment scope when profiling (42 = medical supplies). |
| `--all-segments` | off | Profile the WHOLE marketplace (overrides `--segment`). |
| `--limit <n>` | all | Profile only the first N tender items — for fast dev runs. |
| `--count <n>` | none | Stop after N viable categories. **Default: no limit** (`0`/negative also = no limit). |
| `--min-samples <n>` | `15` | Minimum distinct corpus descriptions a candidate must have. |
| `--min-spend <f>` | `0.0005` | Spend-share floor; the scan stops below it (0.0005 = 0.05%). |
| `--revisit` | off | Re-evaluate tokens previously cached as junk (`categories\vet_rejections.json`). |

Sample run (stderr progress, then the chosen families):

```
profiling corpus (streamed)...
  ...fetched 120,000 items
  grouping 436,800 items by head-noun...
scanning 10,396 ranked groups for candidates...
  [37/10396] sampling 'termometros' (0.31% spend; chosen 2/all)...
  vetting batch of 12: ['termometro', 'jeringa', ...]
    + termometros (0.3% spend)
  ...
  scan ended: 79/all viable categories found

candidates (79):
  yoduros_de_potasio          5.9% spend  include=['\\byoduro\\w*']
    example   : 'YODURO DE POTASIO P.A.'
```

> The first run does the slow full-corpus scan and caches the ranking. Re-running
> `register` to tune `--count` / `--min-spend` reuses that cache; add
> `--reprofile` only when the underlying data changed. With no `--count`,
> `--min-spend` is the real stop — lower it to reach deeper into the long tail.

#### `add-category` — manually append one known family (skips the LLM vet)

```powershell
chilecompra-er add-category mascarillas `
  --include "\bmascarilla\w*" `
  --example "MASCARILLA QUIRURGICA 3 PLIEGUES"
```

| Flag | Meaning |
|---|---|
| `category_id` (positional) | Lowercase snake_case id, e.g. `mascarillas`. |
| `--include <regex>` | **Required, repeatable.** Tier-1 inclusion regex over NORMALIZED text. |
| `--exclude <regex>` | Repeatable. Tier-1 exclusion regex (resolves sibling overlaps). |
| `--name <str>` | Display name (default: derived from the id). |
| `--corpus <regex>` | Raw-text sampling regex for `generate-schemas` (default: derived from `--include`). |
| `--example <str>` | Canonical example description (used as a golden test fixture). |

After adding, draft its schema with `generate-schemas --only <id>`.

#### `generate-schemas` — LLM strawman attribute schemas from corpus samples
`register` already drafts a schema per new category; re-run this to refine or
regenerate a specific one.

```powershell
chilecompra-er generate-schemas --only mascarillas --samples 50
```

`--only <category_id>` limits to one category (default: all that need one);
`--samples <n>` is how many corpus descriptions to feed the LLM (default 50).
The schema defines the identity + descriptive attributes that `resolve` extracts.

---

### 5.3 Resolve source records into the catalog

#### `resolve` — fill the catalog
Runs source records through the pipeline (normalize → classify → extract →
price basis → find/create node → persist). **Dry run unless `--persist`.** The
*unit of work* depends on `--kind`.

```powershell
# item-centric dry run (recommended): one ItemLicitacion at a time, pooling its
# offers; UNSPSC fallback links the rest -> ~100% item coverage
chilecompra-er resolve --kind item --segment 42 --limit 5000 --show 10 --out data\check

# persist to the graph; resumable after a kill
chilecompra-er resolve --kind item --segment 42 --persist
chilecompra-er resolve --kind item --segment 42 --persist --resume

# per-record dry run (curated families only) — useful for comparison
chilecompra-er resolve --kind tender --segment 42 --limit 5000 --show 10 --out data\check
```

| Flag | Default | Meaning |
|---|---|---|
| `--kind tender\|offer\|oc\|joint\|item` | `tender` | Unit / source of records — see below. |
| `--fallback unspsc\|none` | `unspsc` | **`--kind item` only.** Items no curated family matches link to a coarse `GenericProduct` keyed by their UNSPSC code (`unspsc_NNNNNNNN`) → ~100% link. `none` leaves them unresolved (curated-only). |
| `--persist` | off | **WRITE** to the graph: SourceRecords, GenericProduct nodes + RESOLVED_TO edges — and, for `--kind item`, the offers' `:Product` variants (VARIANT_OF). Off = dry run. |
| `--segment <n>` | none | UNSPSC segment filter, e.g. 42 (tender/offer/joint/item; ignored for oc). |
| `--contains <str>` | none | Filter on buyer text (e.g. `foley`). |
| `--limit <n>` | `200` | Max records to process. `all` (or `0`) = no limit — the whole filtered set. |
| `--skip <n>` | `0` | Skip N records (stable order; for chunked builds). |
| `--show <n>` | `5` | Print the first N resolved examples as a spot-check (display only). |
| `--out <prefix>` | `data\resolve` | Output prefix (see Outputs). |
| `--progress-every <n>` | `200` | Emit a progress line every N records (durable checkpoint every ~20k). |
| `--resume` | off | Continue the run in `<out>.checkpoint.json` (invocation must match kind/segment/contains/persist/limit). |
| `--brands` | off | Add the brand-lexicon tier (`categories\brand_lexicon.json`) after Tier-1 — catches brand-only lines. See §5.4. |
| `--tier2` | off | Add the trained Tier-2 statistical classifier after Tier-1 (needs `train-tier2`). See §5.4. |
| `--tier2-model <path>` | `data\tier2_model.joblib` | Tier-2 model file to load when `--tier2` is set. |
| `--tier2-threshold <f>` | model default (`0.6`) | Override the Tier-2 confidence threshold for this run. |

**The `--kind` values:**

- **`item`** *(recommended)* — resolve a whole `ItemLicitacion` at once: buyer
  line + **all its offers** (majority consensus) + tender title → one generic
  product; offers bound beneath it as priced `:Product` variants. With
  `--fallback unspsc` (default), unmatched items link to a UNSPSC bucket →
  ~100% coverage.
- **`tender`** — one buyer line at a time, with its tender title as context
  (line wins; title rescues terse lines). Curated families only.
- **`joint`** — one offer at a time, paired with its tender line's buyer text
  (offer wins; if the two classify differently the record is sent to review).
- **`offer`** — supplier offers as standalone records.
- **`oc`** — purchase-order items.

Sample `--kind item` summary:

```
mode: dry run (no writes)
records processed : 5000
items linked      : 5000 (100.0%)  = curated 1809 + UNSPSC-fallback 3191
by status         : {'resolved_generic': 5000}
unresolved reasons: {}
by category       : {'agujas': 138, 'canulas': 85, ... 'unspsc_42151602': 246, ...}
price basis mix   : {'unknown': 1730, 'per_pack': 79}
offers bound      : 75 (as :Product variants under their item's node)
nodes created     : 847
illegal values    : 452 (dropped, counted — schema dry-run metric)
written: data\check_resoluciones.csv
written: data\check_productos_genericos.csv
checkpoint: data\check.checkpoint.json
```

> **Resumable long runs:** a killed `--persist` run leaves a checkpoint; re-run
> the *identical* command with `--resume` to continue. The resolutions CSV is
> trimmed to the last durable checkpoint so kill timing can never duplicate rows.

---

### 5.4 Improve coverage (cut UNSPSC fallback)

A `--kind item` run links ~100% of items, but the ones no curated family matched
land on coarse `unspsc_<code>` fallback buckets. These commands turn that residue
into a backlog and add coverage. The loop:

```
resolve --kind item --persist  →  fallback-report  →  register --from-fallback
                                                   ↘  build-brand-lexicon / train-tier2
                                   →  resolve --kind item --tier2 --brands
```

#### `fallback-report [--top 20] [--min-count 5] [--out <path>]`
Reads the UNSPSC fallback nodes from the graph and ranks them: the commodity
codes carrying the most fallback *items* (with awarded spend and the rubric-only
share), and the head-noun **families** recurring across the residue — the
categories worth registering next. Writes the family ranking to
`data\fallback_ranking.csv`. Needs a prior `resolve --kind item --persist`.

```powershell
chilecompra-er fallback-report --top 20
chilecompra-er register --from-fallback --preview    # vet the residue families
chilecompra-er register --from-fallback --apply      # ...and commit them
```

#### `build-brand-lexicon [--only <id>] [--samples 50] [--max-per-category 15] [--overwrite] [--dry-run]`
For each curated category, samples the corpus and asks the LLM for brand /
trade-name tokens (`relyx`, `panamax`) — the ones no head-noun regex can match.
Each proposal is validated (single normalized token, present in real samples,
not generic filler, not already regex-covered) and cross-category collisions are
dropped, then survivors merge into `categories\brand_lexicon.json` (existing
curated entries win on conflict; `--overwrite` replaces). `--dry-run` prints
without writing.

```powershell
chilecompra-er build-brand-lexicon --only suturas --dry-run
chilecompra-er build-brand-lexicon                   # all categories
```

#### `train-tier2 [--threshold 0.6] [--min-rows 500] [--eval] [--out <path>]`
Trains the Tier-2 statistical classifier (TF-IDF word+char n-grams + logistic
regression) on the curated resolutions already in the graph, saving the model to
`data\tier2_model.joblib`. It generalizes to wording the Tier-1 regex misses and
abstains below `--threshold`, so it only ever adds coverage. `--eval` reports
held-out accuracy on a 10% split.

```powershell
chilecompra-er train-tier2 --eval
```

**Using the extra tiers.** Pass `--brands` / `--tier2` on a resolve run (§5.3):
the classifier becomes **Tier-1 regex → brand lexicon → Tier-2**, each only
turning an otherwise-unresolved item into a curated match (Tier-1 `CLASSIFIED`
or `AMBIGUOUS` always wins). Re-run `resolve --kind item --tier2 --brands` and
compare the curated-vs-fallback split in the summary.

---

### 5.5 Analyze

#### `price-series <category_id> [--csv <path>]`
Per-product price history for a **persisted** category, read from the awarded
`:Product` offers under each generic product. Default output
`data\price_series_<category>.csv`. Empty until you've `resolve --kind item
--persist`ed that category (the offers carry the prices).

```powershell
chilecompra-er price-series sondas_foley
```

```
11 price observations across 7 generic products -> data\price_series_sondas_foley.csv

products with the deepest price history:
  gp_ffaf1899ba78  n=  4  median=  7,480 CLP  range=[330 .. 21,510]
    {"calibre": "16fr", "material": "latex"}
```

(Wide ranges usually reflect price-**basis** mixing — unit vs box — which
normalization will address; unknown basis is reported, never assumed.)

---

### 5.6 Housekeeping

#### `clean [--all] [--dry-run] [--dir <path>]`
Delete **regenerable** run artifacts from `data\` — the resolve output triplets
(`*_resoluciones.csv`, `*_productos_genericos.csv`, `*.checkpoint.json`),
`price_series_*`, and loose `*.log` / `*.out`. **Keeps** the cached spend ranking
(`profiling.csv`) and the proposals handoff (`proposals.json`) unless `--all`.
`--dry-run` previews. Never touches the graph (that's `wipe-catalog`).

```powershell
chilecompra-er clean --dry-run   # preview what would be removed
chilecompra-er clean             # remove artifacts, keep cached inputs
chilecompra-er clean --all       # also remove profiling.csv + proposals.json
```

---

### 5.7 Diagnostics

| Command | What it does |
|---|---|
| `demo` | Offline pipeline demo — no graph, no LLM. Good first run to see the mechanics. |
| `smoke [--keep]` | Live graph round-trip test; cleans up after itself (`--keep` leaves the data). |
| `probe-offers [--limit 1500]` | Feasibility metric: how often supplier offer text recovers a category for rubric-only buyer lines. Read-only. |

---

### 5.8 Destructive (gated by `--yes`)

| Command | What it does |
|---|---|
| `wipe-category <category_id> --yes` | Delete one category's catalog nodes + their SourceRecords from the graph. |
| `wipe-catalog --yes` | Delete ALL catalog data (Category / GenericProduct / Product / SourceRecord). Transactional source data and migrations are untouched. |

Both refuse to run without `--yes`.

---

## 6. Output artifacts — where everything lives

Outputs are split by **lifecycle**, deliberately not all in one place:

- **The catalog definition is version-controlled**, under
  `chilecompra_er\categories\`: `register.json` (the category list) and
  `schemas\*.json` (one attribute schema per category). This is the real
  deliverable — code-reviewed, diffed in PRs, and asserted by the test suite —
  so `register` writes it here, **not** to `data\`.
- **`data\` is gitignored scratch** — reproducible byproducts, safe to delete
  with `clean`:
  - `profiling.csv` — the cached spend ranking (rebuild with `--reprofile`).
  - `proposals.json` — the `register` preview→apply handoff.
  - `<prefix>_resoluciones.csv` — every source record + its resolution.
  - `<prefix>_productos_genericos.csv` — the generic-product nodes (created, or
    that would be created in a dry run).
  - `<prefix>.checkpoint.json` — progress marker for `--resume`.
  - `price_series_<cat>.csv` — analysis output from `price-series`.
- **The populated catalog lives in Neo4j**, written only by `resolve --persist`:
  `Category` / `GenericProduct` / `Product` / `SourceRecord` nodes and
  `RESOLVED_TO` / `VARIANT_OF` / `PARENT_OF` edges. `register` never touches the
  graph.

---

## 7. Conventions

- **`resolve` is a dry run by default** — nothing is written to the graph until
  you pass `--persist`. Every other read-only command is always safe.
- **Fixed output filenames.** Commands overwrite the same `data\` files each run;
  pass a path flag (`--out`, `--csv`) only to keep a snapshot elsewhere.
- **Destructive graph commands are gated** behind `--yes` (`wipe-category`,
  `wipe-catalog`); local-file cleanup is `clean`.
- **stderr vs stdout** — progress and diagnostics go to **stderr**; the actual
  report/result goes to **stdout**, so you can redirect or pipe the result
  cleanly (e.g. `... 2> run.log`).
- **Nothing is force-fit.** A description that matches no curated family is never
  shoved into the wrong one: `--kind item` links it to a coarse UNSPSC bucket;
  the other kinds (or `--fallback none`) record it as `unresolved` — visible
  debt, by design. The biggest residual buckets are the signal for the next
  `register` pass.

---

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `graph: unreachable` in `status` | Neo4j is stopped (`instance start` — it also rewrites `.mcp.json`). If it's *running* but bolt **times out**, the `neo4j-sg` security group doesn't allow your current client IP — add it. |
| Lots of `unspsc_*` categories after `resolve` | Expected — UNSPSC fallback buckets for items with no curated family. Run `fallback-report` to rank them, then `register --from-fallback` / `train-tier2` / `build-brand-lexicon` to convert coarse coverage into rich coverage (§5.4). |
| `register` / `generate-schemas` errors about auth | Run `ant auth login` once (Claude Max). |
| A `--segment` run scans forever | The UNSPSC index is missing — run `migrate`. |
| `price-series` prints "no price observations" | That category isn't persisted yet, or it was resolved with a kind other than `item` (only `--kind item --persist` binds the `:Product` price points). |
| `resolve --resume` refuses | The invocation must match the checkpoint (kind/segment/contains/persist/limit). Re-run with the same flags. |
| `data\` filling up with run files | `chilecompra-er clean` (keeps the cached ranking + proposals). |

---

## 9. Internals & notes

- The ingestion source is the graph itself — the transactional layer
  (Licitacion / ItemLicitacion / Oferta / OrdenCompra / ItemOC). Resolution only
  *adds* the catalog layer on top and never mutates those nodes.
- The tender-title property is one constant (`TENDER_NAME = l.titulo` in
  `ingest/neo4j_source.py`); a missing/renamed property degrades gracefully to
  item-only resolution.
- `GenericProduct` identity is **exact match on present identity attribute
  values** (same values, same absences), embedded in `identity_key`; that is the
  uniqueness key that dedups the same product across tenders. `PARENT_OF` builds
  a coarse→specific hierarchy by strict-subset subsumption.
- `RESOLVED_TO` edges are **versioned** — re-resolving a record adds a new
  version rather than overwriting, so the resolution history is auditable.
- Known follow-ups: `:Product` cross-offer brand dedup (currently one Product per
  offer), price-**basis** normalization (unit vs box), and registering the
  largest UNSPSC-fallback buckets as curated families.
