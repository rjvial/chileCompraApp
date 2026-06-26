# `chilecompra-er` — CLI guide

A complete guide to the `chilecompra-er` command-line tool: what it does, the
ideas behind it, and how to run every command. No prior knowledge of the codebase
is assumed.

The guide is organized so each topic has **one** home: §2 is the mental model, §3
is the workflow in order, §4 is a pure command lookup, and §5–§7 cover files,
conventions, and internals.

---

## 1. What this is

ChileCompra (Mercado Público) is Chile's public-procurement marketplace. Every
purchase is described in **free text** by whoever wrote the tender — so the same
physical product ("Foley catheter, 16 Fr, 2-way, latex") appears under hundreds of
different descriptions, units, and spellings. That makes it impossible to ask
simple questions like *"what does this product usually cost?"* or *"who sells it
cheapest?"*.

`chilecompra-er` turns those messy descriptions into **canonical product
clusters** — groups of supplier bids that are the *same substitutable product*,
independent of brand and packaging — so price comparison **over time** and
**across competitors** becomes straightforward.

The data already lives in a **Neo4j graph** (the transactional layer: tenders,
line items, offers). The pipeline reads from that graph and *adds* a cluster
catalog on top — it never modifies the source data. The **CLI is the single
operational surface**, and **`chilecompra-er pipeline` runs the whole build in one
resumable command** (§4.1) — the per-stage commands below are what it drives.

The heart of the pipeline is one Claude **canonicalization** step: each distinct
supplier description becomes a structured *profile*, and a deterministic matcher
clusters those profiles into products. All LLM work runs on the **Claude Max
subscription** by default (`CHILECOMPRA_LLM_BACKEND`, §4.8).

Invoke it as `chilecompra-er <command>` (installed entry point) or, from inside
the repo, `.\.venv\Scripts\chilecompra-er.exe <command>`. Examples use PowerShell
paths (`data\...`).

---

## 2. The model

Read this once and the rest of the guide makes sense.

### Two graphs

The pipeline reads a **source graph** (loaded by a separate ingestion pipeline)
and writes a **cluster catalog** on top of it.

**Source graph (read-only input):**

| Node | What it is | Key text |
|---|---|---|
| `Licitacion` | A tender (procurement process) | `titulo` — the tender headline |
| `ItemLicitacion` | One **line item** a buyer wants | `descripcion_comprador` + a **UNSPSC** code (`codigo_unspsc_producto`) |
| `Oferta` | A supplier **offer/bid** for a line item | `descripcion_proveedor` — the supplier's text, plus prices |

Relationships: `(Licitacion)-[:TIENE_ITEM]->(ItemLicitacion)` and
`(Oferta)-[:PARA_ITEM]->(ItemLicitacion)`. The UNSPSC code is the UN's standard
product-taxonomy number (segment 42 = medical/lab supplies); it scopes runs.

**Cluster catalog (written by `match --persist`):** three tiers — raw bid →
brand-specific product → brand-independent cluster.

| Node / edge | What it is |
|---|---|
| `ProductCluster` | A canonical **substitutable product**, brand-**independent** — the set of bids sharing one identity **signature** (category + sorted identity attributes). The unit of price comparison. |
| `Product` | The bid's resolved, brand-**specific** offering — `{brand, category, signature, pack_size, pack_unit}`. Deduped by `(cluster, brand, packaging)`, so every bid of the same brand+spec+pack shares one node. |
| `(Oferta)-[:OFFERS {normalized_price, unit_price, rut, date, currency}]->(Product)` | Each bid priced into its Product — **price per base unit on the edge** (price is per-bid, so it lives here, not on the node). `rut` = supplier (competition), `date` = time series. |
| `(Product)-[:VARIANT_OF]->(ProductCluster)` | A brand-specific Product rolls up to its brand-independent cluster. |
| `(finer)-[:REFINES]->(coarser)` | A more-specific cluster **refines** a coarser one (the partial-spec hierarchy: "Foley 16Fr" refines "Foley"). |

A price reaches its cluster two hops out:
`(:Oferta)-[:OFFERS]->(:Product)-[:VARIANT_OF]->(:ProductCluster)` — so you can
slice by **brand** (on the Product) or aggregate brand-independently (at the
cluster).

```
  SOURCE GRAPH (read-only input)              CLUSTER CATALOG (written by `match`)
  ──────────────────────────────             ─────────────────────────────────────

   Licitacion
      │ :TIENE_ITEM
      ▼
   ItemLicitacion   (buyer text + UNSPSC)
      ▲
      │ :PARA_ITEM
      │                                                          ┌───────────┐
      │                                                          │ :REFINES  │  (self-loop: a finer
      │                                                          ▼           │   cluster refines a coarser)
   Oferta ─:OFFERS {price,rut,date}─▶ Product ─:VARIANT_OF─▶ ProductCluster ─┘
    supplier text                      {brand, specs,         (signature =
                                        packaging}             category + attrs)
```

A `ProductCluster` is keyed by its **signature**, so identical signatures across
different tenders collapse onto one shared cluster — that is what makes
cross-tender, cross-competitor price comparison possible.

### The pipeline

There is **one** pipeline — an ordered, resumable sequence that
`chilecompra-er pipeline` runs end to end (§4.1), or you drive a stage at a time
(§3 Part 2):

```
instance → migrate → register → canonicalize → match → adjudicate → coherence-check     price-clusters
 starter    schema    vocabulary   profiles      cluster   settle       audit / gate    ·  (price query)
```

- **instance / migrate** — the **starter** brings Neo4j up + reachable, then the
  graph constraints + indexes are applied.
- **register** — build or extend the **vocabulary** (families + attribute schemas)
  in four stages, R1→R4 (§3 Part 1). Skipped when it's already complete.
- **canonicalize** — dedup the corpus to **distinct normalized descriptions** (one
  `text_hash`, canonicalized once), then turn each into a structured **profile** via
  Claude/Haiku: `category`, evidence-anchored **identity attributes**, `brand`,
  `model_token`, `packaging`. The text-hash store is the cache + resume state.
- **match** — a deterministic matcher clusters the profiles into `:ProductCluster`
  nodes, materializes each bid's brand-specific `:Product` (`VARIANT_OF` its
  cluster), and prices offers onto their Product.
- **adjudicate** — Claude/Sonnet settles the small residue the matcher couldn't
  decide deterministically.
- **coherence-check** — an auditor of named invariants over the profiles + clusters
  (+ the persisted graph); structural breaches **fail the build**.

After the build, **`price-clusters`** is a *query* over the finished catalog —
per-base-unit price over time and across competition. It isn't part of the build;
run it when you need prices.

### What a profile is (and the rules that make clustering work)

A profile is the structured form of one description. Four ideas make it reliable:

- **Evidence-anchoring (the cardinal rule).** Every identity attribute must quote,
  in its `evidence`, the exact source substring that *names* it. A bare number can
  **never** become identity. This closes the false-merge class where a cable
  ("3x2,5 mm") and calcium ("Ca 2,5 mEq") could both be read as "2.5%".
- **Constrained vocabulary.** For a known family, the attribute **names** are
  pinned to that family's registered schema (so the model uses `calibre`, never
  `gauge`/`length_mm`), and values are **Spanish / snake_case**. Consistent names
  are what let the matcher actually merge equivalent bids.
- **`model_token`.** A manufacturer model / reference token (e.g. `mic-g`). Two
  bids with the **same `model_token` are the same product, even cross-brand** — a
  sufficient merge signal.
- **Brand and packaging are not cluster identity.** They're excluded from the
  cluster **signature** (a cluster is the substitutable product *across* brands).
  Instead they define the **`:Product`** tier — the brand-specific offering keyed by
  `(cluster, brand, packaging)` — so brand-level slicing lives there, while the
  cluster stays brand-independent. Packaging also drives price normalization
  (a per-pack quote ÷ pack size).

### How matching decides (the pairwise rule)

Profiles are **blocked by category**, then compared pairwise (`same_product`):

- same `model_token` ⇒ **same** (even across brands);
- a **conflicting** identity attribute ⇒ a **hard cut** (different);
- identical signatures ⇒ **merge**;
- a coarser **partial** spec (a subset of a finer one) ⇒ linked by **`REFINES`**,
  not merged — unless `--attach-partials` and it has a single finer child.

The result is one cluster per distinct signature, plus the REFINES hierarchy.

---

## 3. The process, step by step

Two parts: **build the vocabulary** (once, then occasionally extend it), then
**run the pipeline** (per `--segment`, repeatable and resumable).

### Setup (one-time)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .   # editable install
```

- **Neo4j** — `chilecompra-er instance start` ensures the box is up **and reachable
  from here**: it boots it if stopped, **allowlists your current public IP** on the
  bolt port in the `neo4j-sg` security group (a managed `chilecompra_app` ingress
  rule), rewrites `.mcp.json` (the public IP changes every start), and waits for
  bolt. The pipeline's first step does the same. Adding the SG rule needs
  `ec2:AuthorizeSecurityGroupIngress`; without that permission a bolt *timeout*
  means your IP still isn't allowed — add it by hand.
- **Max auth** — the efficient backend (`claude_oauth`, §4.8) needs **(a)** the
  `anthropic` package (`pip install -r requirements.txt` — it's only the OAuth HTTP
  transport, still **Max** billing, not API credits) and **(b)** a token from
  `claude setup-token` placed in `secrets.env` as `CLAUDE_CODE_OAUTH_TOKEN`
  (gitignored). It bills the **Claude Max subscription** — never API credits.
- `chilecompra-er status` is a register + instance + graph sanity check anytime.
  Run the tests after vocabulary changes: `python -m pytest tests -q`.

### Part 1 — build the vocabulary

The pipeline canonicalizes descriptions into **product families** (categories),
each with an **attribute schema** that defines its identity attribute *names*.
That family list + the per-family attribute names **are** the vocabulary `canonicalize` fills
in. They live version-controlled under `chilecompra_er\categories\`
(`register.json` + `schemas\<id>.json`).

`register` builds them in four stages:

```
R1 profile → R2 vet (Claude) → R3 proposals → R4 apply (register + draft schemas)
```

- **R1 — profile** (`profiling.py`). Rank the corpus by **head-noun × spend** —
  which candidate families carry the most procurement. Cached to
  `data\profiling.csv` (reused unless `--reprofile`); `--from-fallback` instead
  ranks the UNSPSC fallback residue in the graph.
- **R2 — vet** (`register.propose`, Claude). Walk the ranking and ask, per family,
  *"is this a real, coherent product family?"*, then mechanically validate each
  survivor. Resumable via `data\register.checkpoint.json`.
- **R3 — proposals** (`register.write_proposals`). Survivors are written to
  `data\proposals.json` — the review / handoff point. `--preview` **stops here**.
- **R4 — apply** (`register.apply`). Register the survivors (bumps
  `register_version`) and **draft each family's schema** from corpus samples
  (`strawman.generate`). `--apply` **resumes from** an edited proposals file.

`add-category` is the manual one-family alternative to R1–R2 (it skips the LLM vet);
`generate-schemas` runs R4's schema draft on its own.

```powershell
chilecompra-er register --segment 42        # R1→R4: profile, vet, draft schemas, register
chilecompra-er add-category mascarillas --include "\bmascarilla\w*" --example "MASCARILLA QUIRURGICA 3 PLIEGUES"
chilecompra-er generate-schemas --only mascarillas    # R4 only: draft / refine one family's schema
```

You mostly do this once; thereafter you only **extend** it (a new family, a richer
schema). `chilecompra-er pipeline` also bootstraps it — its `register` step builds
the vocabulary from nothing, fills only missing schemas, or skips when complete (§3
Part 2) — so this Part is **deliberate curation**, not a hard prerequisite. **Editing
a schema's attribute names changes the canonicalization vocabulary**, so re-canonicalize that family
afterward (§3 Part 2, and `wipe-clusters` first).

### Part 2 — run the pipeline

**One command runs the whole build end-to-end** — infra, vocabulary, and the build —
resumable at any step (`pipeline`, §4.1):

```powershell
chilecompra-er pipeline --segment 42             # instance→migrate→register→canonicalize→match→adjudicate→coherence-check
chilecompra-er pipeline --resume                 # continue after a Ctrl-C / failure (skips finished steps)
chilecompra-er pipeline --status                 # show which steps are done / pending
chilecompra-er price-clusters --category sondas  # prices over time + competition (a query — run when you need it)
```

- **Vocabulary is incremental.** The `register` step builds the families + schemas
  **from nothing** when none exist, **fills only missing schemas** when some do, and
  **skips** entirely when the vocabulary is already complete — it never regenerates
  what's there. `--rebuild-vocab` forces a fresh profile+vet that *extends* coverage.
  (Curate it deliberately with the Part 1 commands.)
- **Build only.** The pipeline ends at `coherence-check` (the structural gate).
  `adjudicate` runs but is **non-fatal** (a backlog, not a gate); `price-clusters`
  is a separate read you run per category/signature.
- **Per `--segment`, resumable.** Work one segment at a time; validate a small
  `--limit` slice first. Every step is safe to **Ctrl-C and `--resume`** — each long
  stage also resumes *within* itself (the profile store, the match `:OFFERS` offset,
  the adjudicate verdict store, the `register` vet checkpoint).
- Keep `--workers` low (default 2) so a long canonicalize run doesn't burst the Max usage limit.

Or **run a stage at a time** (the same handlers the pipeline drives) — useful for
development and validating a slice before scaling up:

```powershell
chilecompra-er instance start
chilecompra-er migrate                                       # graph constraints + indexes (incl. migrations 005+006)
chilecompra-er canonicalize --segment 42 --limit 1000        # descriptions → profiles (resumable, --workers 2)
chilecompra-er match --persist --segment 42 --limit 1000     # write clusters + products + OFFERS (resumable)
chilecompra-er adjudicate                                    # (optional) settle the matcher residue
chilecompra-er coherence-check --graph                       # structural gate + review backlogs
chilecompra-er price-clusters --category sondas              # prices over time + competition
chilecompra-er instance stop                                 #     graph data persists
```

### Re-running & keeping current

- **Resume an interrupted build**: `chilecompra-er pipeline --resume` continues at
  the unfinished step (and that step continues *within* itself). `--status` shows
  what's done; `--restart` begins anew.
- **More data** (new tenders/offers): re-run the pipeline (or just `canonicalize`
  then `match --persist`) for the segment. The text-hash store skips everything
  already canonicalized; all graph writes are `MERGE` (idempotent).
- **After a vocabulary / prompt change** (you edited a schema or the canonicalize prompt):
  the cluster signatures change, so first **`wipe-clusters --yes`** (clears the
  `:ProductCluster` namespace + the `match` checkpoints), then re-canonicalize the
  affected families and re-match.
- **If you hit the Max usage limit** mid-run, `canonicalize` stops cleanly ("re-run to
  resume") instead of churning — just re-run the same command after your usage
  window resets (§4.8).

---

## 4. Command reference

Pure lookup — see §3 for *when* to run each. Flags shown with defaults; `[ ]`
marks optional.

### 4.1 Infrastructure

**`pipeline [--segment 42] [--resume] [--restart] [--status] [--watch] [--only <step>] [--from-step <step>] [--rebuild-vocab]`**
— **the whole build in one command.** Runs the ordered steps `instance → migrate →
register → canonicalize → match → adjudicate → coherence-check`, recording completed
steps in `data\pipeline.checkpoint.json` so `--resume` continues at the interrupted
one (each long step also resumes *within* itself). **It runs from start to finish,
skipping stages already done and naming every skip + why:** a step recorded in the
checkpoint is skipped (`SKIP <step>: already completed…`), and a cheap per-stage probe
skips work that's externally complete — `instance` when Neo4j is already reachable
on bolt (else the starter boots it + allowlists your IP), `register` when the
vocabulary is present — while `canonicalize`/`match`/`adjudicate`
are idempotent and self-skip finished work via their own caches. A run-end summary
lists exactly what **ran** vs **skipped**. The `register` step is **incremental** (§3
Part 2): it builds the vocabulary from nothing, fills only missing schemas, or skips
when complete — `--rebuild-vocab` forces a profile+vet that extends it. Ends at `coherence-check` (the structural gate); `adjudicate` is non-fatal, and
`price-clusters` stays a separate query. `--status`/`--watch` print the plan
(steps done/pending) without running anything; `--only <step>` runs one step,
`--from-step <step>` re-runs from a chosen point; `--restart` discards the checkpoint
(and the match/register sub-checkpoints) to begin anew. canonicalize tuning passes through:
`--workers`, `--group-size`, `--model` (canonicalize), `--adjudicate-model` (adjudicate).

**`status`** — register version, per-category schema status, Neo4j state, and
graph node counts. Read-only; never crashes on a flaky graph.

**`instance start|stop|status`** — Neo4j EC2 lifecycle. `start` is the **starter**:
it boots the box if stopped, **allowlists your current public IP** on the bolt port
in the `neo4j-sg` security group (a managed `chilecompra_app` rule — prior ones for a
different IP are pruned), rewrites `.mcp.json` with the new IP (it changes each
start), and waits until bolt answers. `stop` shuts it down (graph data persists);
`status` prints state. The SG name is overridable via `NEO4J_SG_NAME`.

**`migrate [--dry-run]`** — apply graph migrations (uniqueness constraints +
indexes) under `chilecompra_er/migrations/*.cypher`, including `005` (the
`ProductCluster` id + signature constraints) and `006` (the `Product` id
constraint + category/brand indexes). Idempotent. `--dry-run` prints pending
Cypher without running it.

### 4.2 Vocabulary (`register` / `add-category` / `generate-schemas`)

These define the **families + attribute schemas** that `canonicalize` turns descriptions into.

**`register [--segment 42] [--count <n>] [--preview|--apply] [--proposals <path>] [--resume]`**
— runs the four vocabulary stages **R1→R4** (§3 Part 1): profile the corpus by
head-noun (R1), LLM-**vet** each family (R2), write survivors to `data\proposals.json`
(R3), then register them + draft a schema each (R4). Resumable (`--resume`);
`--preview` stops at **R3** (the proposals file) for review and `--apply` resumes at
**R4** from an edited proposals file. It **builds over** the existing register —
already-covered families are skipped, so a re-run only extends coverage.

**`add-category <id> --include <regex> [--exclude <regex>] [--name <txt>] [--example <txt>]`**
— manually append one family (skips the LLM vet). `--include` is the required,
repeatable head-noun regex over normalized text. Draft its schema afterward with
`generate-schemas --only <id>`.

**`generate-schemas [--only <id>] [--samples 50] [--overwrite]`** — LLM strawman
attribute schema from corpus samples. A schema already on disk is **skipped** (no
LLM call, no clobbered hand-edits); `--overwrite` forces a redraft. The schema's
identity attribute **names** become that family's canonicalization vocabulary.

> Editing a schema's attribute names is a vocabulary change → `wipe-clusters` and
> re-canonicalize that family to apply it.

### 4.3 `canonicalize` (descriptions → profiles)

```powershell
chilecompra-er canonicalize --segment 42 --limit 1000        # from the graph (bounded slice)
chilecompra-er canonicalize --from-file data\descs.txt       # from a newline-separated file
```

Turns each distinct `descripcion_proveedor` into a structured **profile**
(category + **evidence-anchored** identity attributes + brand + model token +
packaging), persisted by `text_hash` so each distinct string is canonicalized
**once** (a cached pure function). The cardinal rule: every identity attribute must
quote the substring that anchors it — a bare number can never become identity. For
a known family, the attribute **names** are constrained to that family's schema
vocabulary, and values are Spanish/snake_case — which is what lets `match` merge
equivalent bids.

| Flag | Default | Meaning |
|---|---|---|
| `--from-file <path>` | none | Read newline-separated descriptions from a file instead of the graph. |
| `--out <path>` | `data\profiles.jsonl` | Profile store — JSONL keyed by text-hash; the profile cache (skip already-done). |
| `--model <id>` | `claude-haiku-4-5` | canonicalize model. Haiku is the right tier — cheapest, gentlest on Max usage (~5× less than Opus). |
| `--workers <n>` | `2` | Concurrent LLM calls on the Max backend. Kept low so a long run doesn't burst the usage limit; raise it for short jobs. |
| `--group-size <n>` | `25` | Descriptions **batched** per LLM call — the per-call overhead is paid once per *group*, not per item (≈ N× fewer calls). |
| `--segment <n>` | all | UNSPSC segment scope for the graph read, e.g. `42`. |
| `--limit <n>` | all | Cap inputs (validation / dev runs). |
| `--dry-run` | off | **dedup only** — report distinct/cached counts, **no LLM calls**. |

### 4.4 `match` (cluster the profiles)

```powershell
chilecompra-er match --segment 42 --show 15                  # offline report (no graph)
chilecompra-er match --persist --segment 42 --limit 1000     # write clusters + products + OFFERS (resumable)
```

Clusters the profile store into product clusters by the pairwise rule (§2): same
`model_token` ⇒ same; conflicting attribute ⇒ hard cut; identical signature ⇒
merge; coarser partial spec ⇒ `REFINES` (unless `--attach-partials` and it has a
unique finer child). Default is an **offline report** (no graph, no LLM). With
**`--persist`** it writes `:ProductCluster` + `:REFINES` and prices offers via
`(:Oferta)-[:OFFERS]->(:Product)-[:VARIANT_OF]->(:ProductCluster)` (price on the
OFFERS edge); run `migrate` first.

| Flag | Default | Meaning |
|---|---|---|
| `--store <path>` | `data\profiles.jsonl` | The profile store to cluster. |
| `--persist` | off | **WRITE** clusters, brand-specific Products (VARIANT_OF), REFINES, and OFFERS edges to the graph. |
| `--segment <n>` | none | UNSPSC segment for the offer-price read when persisting. |
| `--limit <n>` | all | Cap the offer-price read (a bounded validation slice). |
| `--attach-partials` | off | Attach a coarse partial to its unique finer child instead of leaving it a REFINES parent. |
| `--resume` | off | Continue the `:OFFERS` write from its stream-offset checkpoint (same `--segment`). |
| `--show <n>` | `15` | Print the top clusters by bid count. |

### 4.5 `adjudicate` (settle the residue)

```powershell
chilecompra-er adjudicate                                    # Claude settles the matcher residue
chilecompra-er adjudicate --dry-run                          # just count the cases
```

Claude adjudicates the small residue the matcher couldn't settle deterministically
— a shared `model_token` whose specs conflict, or a coarse partial compatible with
several divergent children. Each case becomes a structured verdict (`same` /
`different` / `anchor`, with rationale), persisted by case key so re-runs don't
re-pay. The residue is small, so this is cheap.

| Flag | Default | Meaning |
|---|---|---|
| `--store <path>` | `data\profiles.jsonl` | The profile store. |
| `--verdicts <path>` | `data\adjudications.jsonl` | The verdict store (resume state). |
| `--model <id>` | `claude-sonnet-4-6` | adjudicate model (a stronger tier; the residue is tiny). |
| `--dry-run` | off | Report the case count with no LLM calls. |

### 4.6 `coherence-check` (the auditor)

```powershell
chilecompra-er coherence-check                               # offline tiers (profiles + recomputed clusters)
chilecompra-er coherence-check --graph                       # + checks over the persisted catalog
```

Runs named invariants in three tiers:

- **Structural** — a contract; any breach **fails the run (exit 1)**, suitable as
  a CI gate. E.g. **S1** every identity attribute has evidence; **S2** no
  *ungrounded bare-number* identity (a value with no unit whose evidence doesn't
  ground it — so legit `70pct`/`12fr`/`6/0`/code values are accepted); **S4** each
  offer in exactly one cluster; **S5** unique cluster signatures; **S7**
  strict-subset `REFINES`; **S8** no brand/packaging in identity; **S9** no orphan
  clusters.
- **Semantic** — ranked review backlogs (expected nonzero): **M1** weak-identity
  clusters, **M4** model-token conflicts, ambiguous partials; with `--graph`:
  unplaced offers (**S10**), price-incoherent clusters (**M2**).
- **Health** — a trend snapshot (confidence mix, product share, cluster
  placement).

`--tier all|structural|semantic|health` selects tiers; `--out <csv>` writes the
findings. Offline by default; `--graph` adds the catalog checks.

### 4.7 `price-clusters` (prices)

```powershell
chilecompra-er price-clusters --category sondas --top 10
chilecompra-er price-clusters --signature "sondas|calibre=16fr|material=latex"
```

Price series over the clusters, read two hops out via
`(:Oferta)-[:OFFERS]->(:Product)-[:VARIANT_OF]->(:ProductCluster)` (normalization
already on the edge), sliceable by **brand** (on the Product) or supplier `rut`. A cluster is the substitutable-product comparison unit, so
the summary answers both goals at once — per-base-unit price **over time** and
**across competition** (distinct supplier RUTs + the price spread among them).
Needs a persisted catalog (`match --persist`). `--csv <path>` writes the series;
default `data\price_clusters_<cat>.csv`.

### 4.8 Backends & efficiency

All LLM stages run on the **Claude Max subscription** by default — **no per-token
cost**, bounded by Max usage limits. Select with `CHILECOMPRA_LLM_BACKEND`:

- **`claude_oauth`** — **the efficient Max path.** A `claude setup-token`
  credential used directly with `messages.create` (Authorization: Bearer + the
  `oauth-2025-04-20` beta header): bare, cacheable calls billed to Max, **no agent
  scaffolding** — measured **~11× fewer tokens/call** than `claude_cli`, and fast
  (no subprocess). Set `CLAUDE_CODE_OAUTH_TOKEN` in `secrets.env`.
- **`claude_cli`** (default) — Max via `claude -p` subprocesses; correct but each
  carries ~28K tokens of Claude Code scaffolding per call.
- **`anthropic_sdk`** — first-party Batch API; bills **API credits**, not Max.

Three things keep a long run within the Max limit, and they compose:

1. **Model tier** — `canonicalize` is on **Haiku 4.5**, ~5× gentler on the usage limit than
   Opus, for no quality loss (the constrained vocabulary carries the consistency).
2. **Batching** — `--group-size` amortizes the per-call overhead across many
   descriptions.
3. **Graceful abort** — on a sustained usage/rate limit, `canonicalize` **stops cleanly**
   (cancels pending calls, keeps everything done, prints "re-run to resume")
   instead of churning. Re-run the same command after the window resets; the
   text-hash store skips all completed work.

> **Every stage is killable and resumable.** `canonicalize` persists each profile
> as it lands (durable ~every 100) and **skips anything already in the store** on
> re-run. `adjudicate` works the same way against its verdict store. `match
> --persist` is idempotent (`MERGE`), and the long `:OFFERS` write keeps a
> stream-offset checkpoint (`--resume`). So the practical workflow — run `canonicalize` per
> segment, killing and resuming freely — never repeats finished work.

### 4.9 Housekeeping

**`clean [--all] [--dry-run] [--dir <path>]`** — delete **regenerable** `data\`
scratch (checkpoints, CSV outputs, loose `*.log`/`*.out`). Never touches the graph
or the version-controlled vocabulary.

**`demo`** — offline pipeline demo (no graph, no LLM). **`smoke [--keep]`** — live
graph round-trip test, cleans up after itself.

### 4.10 Destructive (gated by `--yes`)

| Command | What it does |
|---|---|
| `wipe-clusters --yes` | Delete the cluster catalog (`:ProductCluster` + `:Product` nodes and their `OFFERS`/`VARIANT_OF`/`REFINES` edges) and clear the `match` checkpoints, so a re-match starts fresh. APOC-batched (edges first — OOM-safe at millions of edges, edge-type-agnostic). The source graph is untouched. Use before re-matching after a vocabulary/prompt change. |

---

## 5. Files & outputs

Outputs split by **lifecycle**:

- **The vocabulary is version-controlled**, under `chilecompra_er\categories\`:
  `register.json` (the family list) and `schemas\*.json` (one attribute schema per
  family). This is the real deliverable — code-reviewed, diffed in PRs, asserted by
  tests. `register` / `generate-schemas` write it here, **never** to `data\`.
- **`data\` is gitignored scratch** — all reproducible:
  - `pipeline.checkpoint.json` — the `pipeline` step-level resume state (which steps
    are done; the scope it was started with).
  - `profiles.jsonl` — the **profile store** (the cache and resume state).
  - `adjudications.jsonl` — the adjudicate verdict store.
  - `match_seg<seg>.checkpoint.json` — the `:OFFERS` write resume offset.
  - `register.checkpoint.json` — the `register` vet-scan resume state.
  - `price_clusters_<cat>.csv` — `price-clusters` output.
  - `proposals.json` / `profiling.csv` — `register` preview/handoff + spend ranking.
- **The cluster catalog lives in Neo4j**, written only by `match --persist`:
  `:ProductCluster` + `:Product` nodes and `:OFFERS` / `:VARIANT_OF` / `:REFINES` edges (migrations `005`+`006`).
  Reset it with `wipe-clusters --yes` (§4.10). The pipeline never modifies the
  source graph.

---

## 6. Conventions & troubleshooting

**Conventions**

- **`match` is an offline report by default** — nothing is written until
  `--persist`. `canonicalize` writes only its local store; every read-only command
  is always safe.
- **Fixed output filenames** — commands overwrite the same `data\` files each run;
  pass a path flag (`--out`, `--csv`) only to keep a snapshot elsewhere.
- **Destructive graph commands are gated** behind `--yes`; local-file cleanup is
  `clean`.
- **stderr vs stdout** — progress/diagnostics go to **stderr**; the report goes to
  **stdout**, so you can redirect the result cleanly (`... 2> run.log`).
- **Run per `--segment`** — the profile store is a resumable cache, so canonicalize
  a segment at a time rather than the whole corpus at once.

**Troubleshooting**

| Symptom | Cause / fix |
|---|---|
| `pipeline` refuses to start ("a checkpoint already exists") | A prior run is in progress — `pipeline --resume` to continue it, or `pipeline --restart` to discard it and begin anew. |
| `pipeline` "refusing to resume: scope differs" | You changed `--segment`/`--limit` between runs. Resume with the **same** scope, or `--restart`. |
| `pipeline` re-ran the `register` step you didn't expect | The vocabulary was absent or had a missing schema, so the incremental step built/filled it. To skip it, `--from-step canonicalize`; to deliberately extend it, `--rebuild-vocab`. |
| `graph: unreachable` in `status` | Run `instance start` (the starter) — it boots the box, allowlists your IP (`chilecompra_app` on `neo4j-sg`), and refreshes `.mcp.json`. A *timeout* that persists after that means the starter couldn't add the SG rule (no `ec2:AuthorizeSecurityGroupIngress`) — add your IP to `neo4j-sg` on 7687 by hand. |
| `pipeline` halts: "starter could not reach Neo4j bolt" | Same cause — the box is up but bolt isn't reachable and the SG rule couldn't be added automatically. Add your IP to `neo4j-sg` (7687), then `pipeline --resume`. |
| LLM stages fail `ModuleNotFoundError: anthropic` | The `claude_oauth` Max backend needs the `anthropic` package — `pip install -r requirements.txt`. It's only the transport; billing stays on **Max**. |
| `claude_oauth` backend: "needs a Max OAuth token" | Run `claude setup-token` and put `CLAUDE_CODE_OAUTH_TOKEN=…` in `secrets.env` (gitignored). |
| `canonicalize` stops with "usage limit reached" | You hit the Max usage window. Re-run the same command after it resets — completed work is saved. |
| A `--segment` run scans forever | The UNSPSC index is missing — run `migrate`. |
| `coherence-check --graph` reports orphan clusters (S9) | You matched a different segment than you canonicalized; match the **same** `--segment` (or all). |
| `price-clusters` prints nothing | That category isn't persisted yet — run `match --persist` for its segment first. |
| Stale clusters after a schema change | `wipe-clusters --yes`, then re-canonicalize and re-match. |

---

## 7. Internals & notes

- **The source is the graph itself** — the transactional layer. The pipeline only
  *adds* the cluster catalog and never mutates source nodes.
- **Evidence-anchoring** (`resolve/profile.py`). Every identity attribute carries
  the source substring that names it; the canonicalize prompt forbids turning a bare number
  into identity and asks for the full anchored span (`"70%"`, not `"70"`). This is
  the structural answer to the false-merge class where "Ca 2,5 mEq" and "cable
  3x2,5 mm" could read as "dextrose 2.5%".
- **Constrained vocabulary** (`profile.category_vocabulary`). Each family's schema
  identity attribute **names** are injected into the canonicalize prompt, and the model must
  use exactly those names for that category (no `gauge` when the schema says
  `calibre`), values Spanish/snake_case. This is what collapses false splits —
  validated at 1.27→1.67 profiles/cluster on a 1,000-offer slice.
- **`model_token` as a merge signal** (`resolve/matcher.py`). A shared
  manufacturer model/reference token merges two bids even across brands; a
  conflicting identity attribute is a hard cut. Blocking is by category, matching
  by signature/model/attributes.
- **Anchorless-rule guard** (`categories/schema.py`). A schema rule that could fire
  on a bare number must carry a `requires` concept guard; a lint + test enforce it,
  and the guard recognizes unit symbols (`°`, `%`, `µ`) as anchors.
- **The Product tier** (`ingest/clusters.py:build_records`). Each clustered offer
  maps to a `:Product` keyed by `(cluster, brand, packaging)` — `pr_<sha1(cluster_id
  | brand | pack_key)>`, brand `sin_marca` when none. Many bids of the same
  brand+spec+pack dedup onto one Product; the Product carries `{brand, category,
  signature, pack_size, pack_unit}` and is `VARIANT_OF` exactly one cluster. Price
  is per-bid, so it stays on the `(:Oferta)-[:OFFERS]->(:Product)` edge, not the
  node. Constraints in migration `006`.
- **`normalized_price`** (`ingest/clusters.py`). Packaging is normalization, never
  identity: a per-pack quote is divided by its stated pack size to a per-base-unit
  price, stored on the `:OFFERS` edge. Offers are matched to their Product on the
  composite offer key `(id_licitacion, id_item, id_oferta)` — the unique identity,
  so one offer is bound to exactly one Product (coherence S4).
- **The coherence contract** (`coherence.py`). Structural invariants (S1/S2/S4/S5/
  S7/S8/S9) are a CI gate; semantic checks (M1/M2/M4/S10) are ranked review
  backlogs; health metrics are trend snapshots. The graph tier runs against the
  persisted catalog.
- **Max efficiency** (`llm.py`). The `claude_oauth` backend makes bare
  `messages.create` calls billed to Max (~11× leaner than `claude -p`); `canonicalize` batches
  `--group-size` items per call; a sustained rate/usage limit aborts the run
  cleanly for a clean resume. `canonicalize` is on Haiku (the cheapest tier, ~5× gentler on the
  Max limit than Opus) — the constrained vocabulary makes that the right
  quality/cost point.
- **Pipeline orchestration** (`pipeline.py` + `cli.cmd_pipeline`). `pipeline.py` is
  pure state — the ordered step list + a `done`-tracking checkpoint — so ordering and
  resume are unit-testable with no graph; `cmd_pipeline` drives the existing per-stage
  `cmd_*` handlers. The vocabulary step is incremental: empty register → full
  profile+vet+draft; present-with-gaps → draft only the missing schemas; complete →
  skip (`--rebuild-vocab` forces an extend). The build ends at `coherence-check`; adjudicate
  `adjudicate` is non-fatal.
- **Resumability (two layers).** STEP level: `pipeline --resume` skips steps already
  in the checkpoint's `done` list. WITHIN a step: the text-hash profile store and the
  verdict store *are* the resume state, the `:OFFERS` write keeps a stream-offset
  checkpoint, and the `register` vet keeps its own checkpoint — so re-entering an
  unfinished step continues from the exact item. Re-running any stage skips finished
  work.
