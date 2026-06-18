# chilecompra_er — entity resolution for ChileCompra medical devices

Implements the architecture in `entity_resolution_design.pdf`: structured
attributes decide identity, text similarity only generates candidates;
partial knowledge is represented honestly; every source record stays linked
to its resolved catalog node by a versioned edge.

## Layout

| Module | Design section | What it does |
|---|---|---|
| `normalize/` | §3 box | Shared normalization: lowercase, accent strip, versioned abbreviation table (`abbreviations_v1.csv`) |
| `categories/` | §3 | Category register (Tier-1 include/exclude rules) + per-category schema JSONs (`attribute_defs`: name, domain, role, canonicalization rules) |
| `resolve/classifier.py` | §8 | Tier-1 deterministic classification; unmatched/ambiguous stays unclassified (visible debt) |
| `resolve/extractor.py` | §7 box | Layer-1 regex/keyword extraction; missing stays absent; out-of-domain dropped and counted |
| `resolve/assignment.py` | §4, §7 | The single write path: write-time schema validation, home-node lookup via `identity_key`, parent computation (`PARENT_OF`), derived `specificity`/`is_complete`, `SourceRecord` + versioned `RESOLVED_TO` |
| `resolve/resolver.py` | §7 | The strictly ordered 7-step resolution of one description; UNSPSC fallback for items no curated family matches |
| `resolve/brand_lexicon.py`, `resolve/tier2.py`, `resolve/layered.py` | §8 | Extra classifier tiers behind Tier-1: brand/trade-name lexicon, TF-IDF + logistic-regression statistical classifier, and the layered composition (Tier-1 regex → brand → Tier-2) |
| `price/basis.py` | §6 | Explicit price basis (per_base_unit / per_pack / unknown), pack regexes, arithmetic cross-check; flag-don't-guess |
| `price/fx.py` | §6 | UF/USD/EUR rates + CPI deflation via mindicador.cl, cached under `data/reference/` |
| `ingest/` | §2, §10 | Retrieval from the existing Mercado Público graph (ItemLicitacion / Oferta / ItemOC) with stable record keys + price fields; batch runner with coverage stats |
| `profiling.py` | §3 steps 1–3 | M0 head-noun × awarded-spend ranking, UNSPSC-segment scoping, curated synonym merge |
| `fallback.py`, `brands.py` | §3/§8 | Coverage-improvement loop: rank the UNSPSC fallback residue (`fallback-report`) and LLM-build the brand lexicon (`build-brand-lexicon`) |
| `llm.py` | §3/§7/§8 LLM slots | Claude on the user's Max subscription via headless Claude Code (`claude -p`, default backend) with the `anthropic` SDK as opt-in alternative (`CHILECOMPRA_LLM_BACKEND=anthropic_sdk`; bills API credits, not the subscription). NOT Bedrock — provider decision 2026-06-12. |
| `migrations/` | §10 | Cypher constraints/indexes; `python -m chilecompra_er.migrations.migrate` |
| `graphdb.py` | — | Connection factory over the existing `funcionesNeo4j` / `funcionesNeo4jEC2` infra (EC2 lookup or `NEO4J_URI`) |

## CLI

Installed via `pip install -e .` (console script `chilecompra-er`, also
`python -m chilecompra_er`):

```powershell
chilecompra-er status                               # register + instance + graph counts
chilecompra-er instance start|stop|status           # Neo4j EC2 lifecycle
chilecompra-er migrate [--dry-run]                  # graph schema migrations
chilecompra-er resolve --kind item --out data\foley # DRY RUN by default (item-centric)
chilecompra-er resolve --kind item --persist        # explicit writes to the graph
chilecompra-er resolve --kind item --tier2 --brands # add the layered Tier-2 + brand-lexicon tiers
chilecompra-er register --count 10                  # profile + preview next N categories (no writes)
chilecompra-er register --count 10 --apply          # ...and add to the register + draft their schemas
chilecompra-er register --from-fallback --apply     # register families mined from the UNSPSC fallback residue
chilecompra-er add-category mascarillas --include "\bmascarilla\w*" --example "MASCARILLA QUIRURGICA 3 PLIEGUES"   # manual single add
chilecompra-er generate-schemas --only mascarillas  # LLM strawman (Max subscription)
chilecompra-er fallback-report                      # rank UNSPSC fallback + candidate categories to register next
chilecompra-er build-brand-lexicon --only mascarillas  # LLM brand tokens -> categories\brand_lexicon.json
chilecompra-er train-tier2 --eval                   # train the Tier-2 statistical classifier from the graph
chilecompra-er price-series bandas_molares          # per-product price history (persisted cats)
chilecompra-er wipe-category sondas_foley --yes     # destructive, gated
chilecompra-er wipe-catalog --yes                   # reset ALL catalog data (source data untouched)
```

Output convention: commands write to FIXED filenames under `data\` and
overwrite them on every run — `data\resolve_resoluciones.csv` +
`data\resolve_productos_genericos.csv` (resolve), `data\profiling.csv` +
`data\proposals.json` (register), `data\price_series_<category>.csv`. Pass
`--out`/`--proposals`/`--ranking` only when you want to keep a snapshot under
another name.

## Diagnostics (also CLI)

```powershell
chilecompra-er demo                 # offline pipeline demo (no graph, no LLM)
chilecompra-er smoke [--keep]       # live graph round-trip, cleans up after itself
chilecompra-er probe-offers         # M3 feasibility metric (read-only)
.\.venv\Scripts\python.exe -m pytest tests -q   # unit tests
```

There is no examples\ folder — the CLI is the single operational surface.

The ingestion source is the graph itself: the transactional layer
(Licitacion/ItemLicitacion/Oferta/OrdenCompra/ItemOC with run_id/record_hash
provenance) is the source store of design §2; resolution adds only
:SourceRecord references + :RESOLVED_TO edges keyed by the stable ids in
`ingest/neo4j_source.py`.

The EC2 instance gets a new public IP on each start; `instance start` rewrites
`.mcp.json` to the new bolt IP automatically (or assign an Elastic IP). A bolt
*timeout* after a clean start usually means the `neo4j-sg` security group
doesn't allow your current client IP.

## Status vs. the design's milestones

- Done (this scaffold): graph model + constraints, normalization v1,
  Tier-1 classification, Layer-1 extraction, exact-attribute resolution with
  parent linking and repointing, price-basis fields, traceability
  (forward/reverse queries verified live).
- **M0 (next, gates everything):** lock the ingestion source (API vs. datos
  abiertos vs. exports), field mapping, stable record ids; profile spend;
  author the real category register from the corpus.
- **M1:** run the §3 schema procedure on the real #1 category corpus — the
  shipped `sondas_foley.json` is a *draft template*, not a curated schema.
- **M2:** labeled pairs, precision/recall per category, thresholds, review queue.
- **M3:** "branded enough" rule, `:Product` resolution, fuzzy matching
  (rapidfuzz + embeddings + splink) for supplier offers.
- **M4:** lineage edges (`SPLIT_INTO`/`MERGED_INTO`) before any re-cluster;
  register new categories in spend order; daily incremental linkage.
