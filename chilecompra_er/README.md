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
| `resolve/resolver.py` | §7 | The strictly ordered 7-step resolution of one description |
| `price/basis.py` | §6 | Explicit price basis (per_base_unit / per_pack / unknown), pack regexes, arithmetic cross-check; flag-don't-guess |
| `price/fx.py` | §6 | UF/USD/EUR rates + CPI deflation via mindicador.cl, cached under `data/reference/` |
| `ingest/` | §2, §10 | Retrieval from the existing Mercado Público graph (ItemLicitacion / Oferta / ItemOC) with stable record keys + price fields; batch runner with coverage stats |
| `profiling.py` | §3 steps 1–3 | M0 head-noun × awarded-spend ranking, UNSPSC-segment scoping, curated synonym merge |
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
chilecompra-er profile --segment 42 --csv data\m0.csv
chilecompra-er resolve --contains foley --limit 500 --out data\foley   # DRY RUN by default
chilecompra-er resolve --contains foley --persist   # explicit writes to the graph
chilecompra-er widen --count 10                     # propose next N categories (preview)
chilecompra-er widen --count 10 --apply             # ...and register + draft their schemas
chilecompra-er add-category mascarillas --include "\bmascarilla\w*" --example "MASCARILLA QUIRURGICA 3 PLIEGUES"   # manual single add
chilecompra-er generate-schemas --only mascarillas  # LLM strawman (Max subscription)
chilecompra-er wipe-category sondas_foley --yes     # destructive, gated
```

## Run (dev extras)

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q          # offline unit tests
.\.venv\Scripts\python.exe examples\resolve_demo.py    # offline end-to-end demo
.\.venv\Scripts\python.exe examples\smoke_neo4j.py     # live round-trip (cleans up after itself)
```

The ingestion source is the graph itself: the transactional layer
(Licitacion/ItemLicitacion/Oferta/OrdenCompra/ItemOC with run_id/record_hash
provenance) is the source store of design §2; resolution adds only
:SourceRecord references + :RESOLVED_TO edges keyed by the stable ids in
`ingest/neo4j_source.py`.

The EC2 instance gets a new public IP on each start; `.mcp.json` must be
updated accordingly (or assign an Elastic IP).

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
  widen in spend order; daily incremental linkage.
