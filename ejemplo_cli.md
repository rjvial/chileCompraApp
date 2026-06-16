# `chilecompra-er` — CLI guide

Entity-resolution pipeline for ChileCompra. The CLI is the single operational
surface: it profiles the corpus, builds the category register, resolves source
records into a generic-product catalog, and reports prices.

Invoke it as `chilecompra-er <command>` (installed entry point) or, inside the
repo, `.\.venv\Scripts\chilecompra-er.exe <command>`. Examples use PowerShell
paths (`data\...`).

---

## Prerequisites

- **Neo4j up**: `chilecompra-er instance start` (the EC2 box). Its public IP
  changes on every start — update `.mcp.json` if you use the MCP server.
- **LLM auth** (only for `register` / `generate-schemas`, which call Claude):
  `ant auth login` once. Calls go through the Claude Max subscription.
- Run `chilecompra-er status` any time for a register + instance + graph
  sanity check.

## Conventions

- **`resolve` is a dry run by default** — nothing is written to the graph
  until you pass `--persist`. Every other read-only command is always safe.
- **Fixed output filenames.** Commands overwrite the same files under `data\`
  on each run; pass the relevant path flag only to keep a snapshot elsewhere.
- **Where outputs live — by lifecycle, not all in one place:**
  - **The catalog definition is version-controlled**, under
    `chilecompra_er\categories\`: `register.json` (the category list) and
    `schemas\*.json` (one attribute schema per category). These are the actual
    deliverable — code-reviewed, diffed in PRs, and asserted by the test suite —
    so `register` writes them here, **not** to `data\`.
  - **`data\` is gitignored scratch** — reproducible byproducts, safe to delete:
    `proposals.json` (the preview→apply handoff), `profiling.csv` (the cached
    spend ranking; rebuild with `--reprofile`), and `resolve`'s
    `*_resoluciones.csv` / `*_productos_genericos.csv` / `.checkpoint.json`.
  - **The populated catalog lives in Neo4j**, written only by `resolve --persist`
    (`GenericProduct` / `Product` / `SourceRecord` / `RESOLVED_TO` / `VARIANT_OF`).
    `register` never touches the graph.
- **Destructive commands are gated** behind `--yes` (`wipe-category`,
  `wipe-catalog`).
- Progress + diagnostics go to **stderr**; the actual report/result goes to
  **stdout**, so you can redirect or pipe the result cleanly.

---

## End-to-end workflow

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

# 5. Shut down (data persists; IP changes next start)
chilecompra-er instance stop
```

---

## Pipeline stages

The commands form a dependency chain — each stage produces what the next
consumes:

```
register → resolve → price-series
```

1. **`register`** — profile the corpus, vet candidate families, and in the same
   run add the survivors to the register **and** draft an attribute schema (the
   strawman) for each one. This is the step that makes the categories real.
   - Want a review gap first? `register --preview` stops after writing
     `data\proposals.json` (registers nothing); review/trim it, then
     `register --apply` commits the edited file without re-profiling.

2. **`generate-schemas`** *(optional / as needed)* — `register` already drafts a
   schema per new category; re-run this to refine or regenerate a specific one
   (`--only <category_id>`). The schema defines the identity attributes that
   `resolve` extracts.

3. **`resolve`** — fit the real source records into those categories. **Dry run
   first** (`--segment 42 --limit 5000 --show 10`) to inspect quality with no
   graph writes; then `--persist` once you're happy. This is what actually fills
   the catalog with generic-product nodes.

4. **`price-series <category_id>`** — read the persisted catalog to build
   per-product price histories. Empty until you've `--persist`ed that category.

Everything downstream (`resolve`, `price-series`) depends on the categories and
schemas `register` creates, so a fresh `register` run is the prerequisite for
resolving any new family.

---

## Command reference

### Infrastructure

| Command | What it does |
|---|---|
| `status` | Register version + per-category schema status + Neo4j instance + graph node counts. Read-only; never crashes on a flaky graph. |
| `instance start\|stop\|status` | Neo4j EC2 lifecycle. `start` prints the new bolt IP. |
| `migrate [--dry-run]` | Apply graph schema migrations (constraints + indexes). `--dry-run` prints the pending Cypher without running it. |

### Build the category register

**`register`** — the M4 expansion loop, end to end. Profiles the corpus by
head-noun × spend, walks the ranking in vet batches (the LLM judges each token
group into a coherent product family or rejects it), validates every proposed
regex mechanically, writes the survivors to a proposals file, and **registers
them + drafts a schema for each one**. By default there is **no count cap** — it
proposes every viable family down to the `--min-spend` floor; pass `--count N` to
stop after the top N. Progress (scan position, batches vetted, candidates
accepted) streams to stderr as it goes.

Two opt-out flags split the run when you want a review gap:
- **`--preview`** stops after writing the proposals file (registers nothing).
- **`--apply`** registers an existing / hand-edited proposals file *without*
  re-profiling — the commit half on its own.

```powershell
chilecompra-er register --segment 42                 # profile → vet → register + schemas
chilecompra-er register --segment 42 --count 10      # ...or cap at the top 10
chilecompra-er register --segment 42 --preview       # stop at data\proposals.json for review
chilecompra-er register --apply                      # ...then commit the edited file
```

| Flag | Default | Meaning |
|---|---|---|
| `--preview` | off | Profile + vet only: write `--proposals` and stop, registering nothing. Mutually exclusive with `--apply`. |
| `--apply` | off | Register an existing/edited `--proposals` file without re-profiling (the commit half on its own). Mutually exclusive with `--preview`. |
| `--proposals <path>` | `data\proposals.json` | Proposals file — written by every run, read back by `--apply`. |
| `--ranking <path>` | `data\profiling.csv` | Cached spend ranking. Reused if present (skips the slow corpus scan); rebuilt otherwise. |
| `--reprofile` | off | Force a fresh corpus profile, overwriting `--ranking`. |
| `--segment <n>` | `42` | UNSPSC segment scope when profiling (42 = medical supplies). |
| `--all-segments` | off | Profile the WHOLE marketplace (overrides `--segment`). |
| `--limit <n>` | all | Profile only the first N tender items — for fast dev runs. |
| `--count <n>` | none | Stop after N viable categories. **Default: no limit** — propose every viable family down to `--min-spend` (`0`/negative also = no limit). |
| `--min-samples <n>` | `15` | Minimum distinct corpus descriptions a candidate must have. |
| `--min-spend <f>` | `0.0005` | Spend-share floor; the scan stops below it (0.0005 = 0.05%). |
| `--revisit` | off | Re-evaluate tokens previously cached as junk (`categories\vet_rejections.json`). |

> The first run does the slow full-corpus scan and caches the ranking.
> Re-running `register` to tune `--count` / `--min-spend` reuses that cache;
> add `--reprofile` only when the underlying data changed. With no `--count`,
> `--min-spend` is the real stop — lower it to reach deeper into the long tail,
> raise it to keep the run short.

**`add-category`** — manually append one known family (skips the LLM vet).

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
| `--example <str>` | Canonical example description (golden test fixture). |

**`generate-schemas`** — LLM strawman attribute schemas from corpus samples.

```powershell
chilecompra-er generate-schemas --only mascarillas --samples 50
```

`--only <category_id>` limits to one category (default: all that need one);
`--samples <n>` is how many corpus descriptions to feed the LLM (default 50).

### Resolve source records into the catalog

**`resolve`** — runs each source record through the 7-step pipeline
(normalize → classify → extract attributes → infer price basis →
find/create catalog node → persist). **Dry run unless `--persist`.**

```powershell
# item-centric dry run (recommended): resolve each ItemLicitacion ONCE by
# pooling its offers; UNSPSC fallback links the rest -> ~100% item coverage
chilecompra-er resolve --kind item --segment 42 --limit 5000 --show 12 --out data\check

# per-record dry run (curated families only)
chilecompra-er resolve --kind tender --segment 42 --limit 5000 --show 12 --out data\check

# persist to the graph; resumable after a kill
chilecompra-er resolve --kind item --segment 42 --persist
chilecompra-er resolve --kind item --segment 42 --persist --resume
```

| Flag | Default | Meaning |
|---|---|---|
| `--kind tender\|offer\|oc\|joint\|item` | `tender` | Source of records. `item` (item-centric) resolves each `ItemLicitacion` ONCE, pooling buyer line + ALL its offers (majority consensus) + tender title, so every offer shares the item's one generic product — best coverage, and the metric the catalog targets. `tender` resolves each buyer line with its tender title as context (item wins; title is fallback for terse lines). `joint` resolves each offer with its tender line's buyer text (offer wins; disagreement → review). |
| `--fallback unspsc\|none` | `unspsc` | **`--kind item` only.** Items no curated family matches link to a coarse `GenericProduct` keyed by their UNSPSC commodity code (`unspsc_NNNNNNNN`) → ~100% link. `none` leaves them unresolved (curated-only). |
| `--persist` | off | **WRITE** SourceRecord + RESOLVED_TO edges + catalog nodes to the graph. Off = dry run. |
| `--segment <n>` | none | UNSPSC segment filter, e.g. 42 (tender/offer/joint/item kinds; ignored for oc). |
| `--contains <str>` | none | Filter on buyer text (e.g. `foley`). |
| `--limit <n>` | `200` | Max records to process. `all` (or `0`) = no limit — process the whole filtered set. |
| `--skip <n>` | `0` | Skip N records (stable order; for chunked corpus builds). |
| `--show <n>` | `5` | Print the first N resolved examples as a spot-check (display only). |
| `--out <prefix>` | `data\resolve` | Output prefix (see files below). |
| `--progress-every <n>` | `200` | Emit a progress line + checkpoint every N records. |
| `--resume` | off | Continue the run in `<out>.checkpoint.json` (invocation must match kind/segment/contains/persist/limit). |

Outputs (prefix from `--out`, e.g. `data\check`):
- `data\check_resoluciones.csv` — every source record + its resolution.
- `data\check_productos_genericos.csv` — the catalog nodes (created in the dry
  run, or that would be created).
- `data\check.checkpoint.json` — progress marker for `--resume`.

### Analyze

| Command | What it does |
|---|---|
| `price-series <category_id> [--csv <path>]` | Per-product price history for a **persisted** category, read from the awarded `:Product` offers under each generic product. Default output `data\price_series_<category>.csv`. Empty until you've `resolve --kind item --persist`ed that category (it's the offers that carry the prices). |

### Diagnostics

| Command | What it does |
|---|---|
| `demo` | Offline pipeline demo — no graph, no LLM. Good first run. |
| `smoke [--keep]` | Live graph round-trip test; cleans up after itself (`--keep` leaves the data). |
| `probe-offers [--limit 1500]` | M3 feasibility metric: offer-text recovery rate for rubric-only tender lines. Read-only. |
| `clean [--all] [--dry-run]` | Delete regenerable run artifacts from `data\` (resolve CSVs/checkpoints, `price_series_*`, logs). Keeps the cached ranking + proposals unless `--all`; `--dry-run` previews. Never touches the graph. |

### Destructive (gated by `--yes`)

| Command | What it does |
|---|---|
| `wipe-category <category_id> --yes` | Delete one category's catalog nodes + their SourceRecords from the graph. |
| `wipe-catalog --yes` | Delete ALL catalog data (Category / GenericProduct / Product / SourceRecord). Transactional source data and migrations are untouched. |

---

## Notes

- The ingestion source is the graph itself — the transactional layer
  (Licitacion / ItemLicitacion / Oferta / OrdenCompra / ItemOC). Resolution
  only adds `:SourceRecord` references and `:RESOLVED_TO` edges.
- Both `register` and `resolve` read the **item line plus its parent tender's
  title** (`Licitacion.titulo`): the line item is authoritative, the title is
  context that rescues terse/boilerplate lines and fills attributes the line
  omits. The tender-title property name lives in one constant (`TENDER_NAME` in
  `ingest/neo4j_source.py`); a missing/renamed property degrades to item-only.
- Unclassified spend is visible debt by design: a description that matches no
  category is recorded as `unresolved`, never force-fit. Closing that gap is
  what `register` is for.
- Run the test suite after register/schema changes:
  `.\.venv\Scripts\python.exe -m pytest tests -q`.
