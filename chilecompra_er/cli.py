"""Command-line interface for the entity-resolution pipeline.

    chilecompra-er status                       # register + instance overview
    chilecompra-er instance start|stop|status   # Neo4j EC2 lifecycle
    chilecompra-er migrate [--dry-run]          # apply graph schema migrations
    chilecompra-er register [--segment 42] [--reprofile]  # profile, vet, register + schemas
    chilecompra-er register --preview                      # stop after data\\proposals.json
    chilecompra-er register --apply                        # register an edited proposals file
    chilecompra-er resolve [--kind tender|offer|oc] [--contains foley]
                           [--limit 200] [--persist] [--out data\\run1] [--show 5]
    chilecompra-er generate-schemas [--only jeringas] [--samples 50]
    chilecompra-er wipe-category <category_id> --yes

`register` runs the whole expansion loop in one shot: it profiles the corpus
(caching the spend ranking to data\\profiling.csv, reused on later runs unless
--reprofile), vets the candidates into data\\proposals.json, then adds the
survivors to the register + drafts the schemas that `resolve` consumes. Use
--preview to stop after writing the proposals file (registering nothing), or
--apply to register a pre-existing / hand-edited proposals file without
re-profiling.

`resolve` is a DRY RUN by default (nothing written to the graph) — pass
--persist explicitly to write SourceRecords/RESOLVED_TO edges and catalog
nodes. Destructive commands require --yes.
"""

from __future__ import annotations

import argparse
import time
import sys
from pathlib import Path

from .categories.schema import CATEGORIES_DIR, load_register, load_schema


def _opt_limit(value: str) -> int | None:
    """resolve --limit parser: 'all'/'none'/'0'/negative -> None (no limit),
    any positive integer -> that cap."""
    if value.strip().lower() in ("all", "none"):
        return None
    n = int(value)
    return None if n <= 0 else n


def _utf8_stdout() -> None:
    # Windows consoles default to cp1252; infra modules print arrows etc.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


# --- commands -----------------------------------------------------------------

def cmd_status(args) -> int:
    register = load_register()
    print(f"register version : {register['register_version']}")
    print(f"{'category':<26}{'status':<12}{'schema':<16}identity attrs")
    for cat in register["categories"]:
        try:
            schema = load_schema(CATEGORIES_DIR / cat["schema_file"])
            version, attrs = schema.schema_version, ", ".join(schema.identity_names)
        except FileNotFoundError:
            version, attrs = "MISSING", "-"
        print(f"{cat['category_id']:<26}{cat['status']:<12}{version:<16}{attrs}")

    from .graphdb import instance_status

    instance_id, state, ip = instance_status()
    print(f"\nneo4j instance   : {instance_id} {state}" + (f" @ {ip}" if ip else ""))
    if state == "running":
        try:
            from .graphdb import get_connection

            conn = get_connection()
            try:
                rec = conn.query(
                    "OPTIONAL MATCH (g:GenericProduct) WITH count(g) AS gp "
                    "OPTIONAL MATCH (s:SourceRecord) WITH gp, count(s) AS sr "
                    "OPTIONAL MATCH (c:Category) RETURN gp, sr, count(c) AS cat"
                )[0]
                print(f"graph            : {rec['cat']} categories, "
                      f"{rec['gp']} generic products, {rec['sr']} source records")
            finally:
                conn.close()
        except Exception as exc:  # status must never crash on a flaky graph
            print(f"graph            : unreachable ({exc})")
    return 0


def cmd_instance(args) -> int:
    from . import graphdb

    if args.action == "status":
        instance_id, state, ip = graphdb.instance_status()
        print(f"{instance_id} {state}" + (f" @ {ip}" if ip else ""))
    elif args.action == "start":
        ip = graphdb.start_neo4j_instance()
        print(f"running, bolt ready @ {ip}")
        print("note: the public IP changes on each start — update .mcp.json if you use the MCP server")
    elif args.action == "stop":
        print(graphdb.stop_neo4j_instance())
    return 0


def cmd_migrate(args) -> int:
    from .migrations.migrate import migrate

    if args.dry_run:
        ran = migrate(None, dry_run=True)
    else:
        from .graphdb import get_connection

        conn = get_connection()
        try:
            ran = migrate(conn)
        finally:
            conn.close()
    print(f"migrations {'printed' if args.dry_run else 'applied'}: {ran or 'none (up to date)'}")
    return 0


def cmd_resolve(args) -> int:
    from .graphdb import get_connection
    from .ingest import (
        fetch_items,
        fetch_oc_items,
        fetch_offers,
        fetch_tender_items,
        resolve_items,
    )
    from .ingest.export import write_products_csv
    from .ingest.resume import (
        Checkpoint,
        StreamingResolutionWriter,
        checkpoint_path,
        load_checkpoint,
        products_path,
        resolutions_path,
        save_checkpoint,
        seed_inmemory_catalog,
        truncate_resolutions,
    )
    from .ingest.runner import ResolutionStats
    from .resolve import InMemoryCatalog, Neo4jCatalog, Resolver

    fetchers = {"tender": fetch_tender_items, "offer": fetch_offers,
                "oc": fetch_oc_items, "joint": fetch_offers, "item": fetch_items}
    joint = args.kind == "joint"
    item_mode = args.kind == "item"

    prefix = args.out
    cp_path = checkpoint_path(prefix)
    res_csv = resolutions_path(prefix)
    prod_csv = products_path(prefix)

    # --- resume bookkeeping ---------------------------------------------------
    base_stats = ResolutionStats()
    run_start_skip = args.skip
    effective_skip = args.skip
    remaining_limit = args.limit
    append = False

    if args.resume:
        cp = load_checkpoint(cp_path)
        if cp is None:
            print(f"no checkpoint at {cp_path} — nothing to resume "
                  "(start a fresh run without --resume)")
            return 1
        bad = cp.mismatches(kind=args.kind, contains=args.contains,
                            segment=args.segment, persist=args.persist,
                            limit=args.limit)
        if bad:
            print("refusing to resume: invocation differs from the checkpoint:")
            for m in bad:
                print(f"  {m}")
            return 1
        if cp.done:
            print(f"checkpoint already complete ({cp.processed} records) — nothing to do")
            return 0
        base_stats = cp.stats()
        run_start_skip = cp.start_skip
        effective_skip = cp.start_skip + cp.processed
        remaining_limit = (args.limit - cp.processed) if args.limit else None
        # Align the CSV to the checkpoint exactly so kill timing can't dup rows.
        kept = truncate_resolutions(res_csv, cp.processed)
        append = True
        print(f"resuming: {cp.processed} records already done "
              f"(CSV trimmed to {kept} rows); continuing from skip {effective_skip}")

    conn = get_connection()
    try:
        kwargs = {"contains": args.contains, "limit": remaining_limit}
        if args.kind in ("tender", "offer", "joint", "item"):
            kwargs.update(skip=effective_skip, unspsc_segment=args.segment)
        elif args.kind == "oc":
            kwargs.update(skip=effective_skip)
        items = fetchers[args.kind](conn, **kwargs)

        catalog = Neo4jCatalog(conn) if args.persist else InMemoryCatalog()
        if args.resume and not args.persist:
            seeded = seed_inmemory_catalog(catalog, prod_csv)
            print(f"reseeded {seeded} existing products from {prod_csv.name}")
        resolver = Resolver(catalog)

        mode = "PERSIST (writing to graph)" if args.persist else "dry run (no writes)"
        print(f"mode: {mode}")

        writer = StreamingResolutionWriter(res_csv, append=append)
        shown: list = []

        def on_report(r) -> None:
            writer.write(r)
            if len(shown) < args.show and r.status != "unresolved":
                shown.append(r)

        def checkpoint(st, done: bool) -> None:
            """Durable, resumable checkpoint: for dry runs, rewrite the products
            CSV AND save the checkpoint JSON together so the two always describe
            the same `processed` point (resume reseeds the catalog from the CSV,
            then trims resoluciones to `processed`). The products CSV is a FULL
            rewrite — the expensive, OneDrive-lock-prone part — so this is called
            sparsely (see show_progress), not on every progress tick. A transient
            lock mid-run skips the whole checkpoint (both files stay at the last
            in-sync point) rather than killing a long run; the final checkpoint
            retries before giving up."""
            writer.flush()
            if not args.persist:
                for attempt in range(5 if done else 1):
                    try:
                        write_products_csv(catalog, prod_csv)
                        break
                    except PermissionError:
                        if done and attempt < 4:
                            time.sleep(0.5)
                            continue
                        if done:
                            print(f"  warning: could not write {prod_csv} "
                                  "(file locked — OneDrive/AV?); products CSV may "
                                  "be stale. Resoluciones CSV is complete.",
                                  file=sys.stderr, flush=True)
                            break  # still record the (complete) checkpoint below
                        return     # mid-run: leave both files at last good point
            save_checkpoint(cp_path, Checkpoint(
                kind=args.kind, contains=args.contains, segment=args.segment,
                persist=args.persist, limit=args.limit,
                start_skip=run_start_skip, processed=st.total, done=done,
                stats_dict=st.to_dict()))

        # The products-CSV rewrite is expensive at scale and lock-prone under a
        # syncing folder, so checkpoint durably only every ~20k records; the
        # cheap progress line + resoluciones flush still happen every tick.
        durable_every = max(1, 20_000 // max(args.progress_every, 1))
        ticks = 0

        def show_progress(st) -> None:
            nonlocal ticks
            ticks += 1
            writer.flush()
            if ticks % durable_every == 0:
                checkpoint(st, done=False)
            denom = f"/{args.limit}" if args.limit else ""
            res = st.by_status.get("resolved_generic", 0)
            unr = st.by_status.get("unresolved", 0)
            print(f"  ...processed {st.total}{denom}  resolved={res}  "
                  f"unresolved={unr}  created={st.nodes_created}",
                  file=sys.stderr, flush=True)

        # Initial checkpoint so even a kill before the first progress tick
        # leaves a resumable marker.
        checkpoint(base_stats, done=False)
        print("fetching + resolving (streamed in pages of 1000)...",
              file=sys.stderr, flush=True)

        try:
            stats, _ = resolve_items(resolver, items, persist=args.persist,
                                     collect_reports=False, on_report=on_report,
                                     progress=show_progress,
                                     progress_every=args.progress_every,
                                     stats=base_stats, joint=joint,
                                     item_mode=item_mode)
        finally:
            writer.flush()
            writer.close()

        checkpoint(stats, done=True)  # final: writes products CSV (with retries)

        print(stats.summary())
        print(f"written: {res_csv}")
        if not args.persist:
            print(f"written: {prod_csv}")
        print(f"checkpoint: {cp_path}")

        for r in shown:
            print(f"  {r.raw_text[:70]!r}")
            print(f"    -> {r.node_id}  attrs={r.extraction.values}  "
                  f"basis={r.price_basis.basis}")
    finally:
        conn.close()
    return 0


def cmd_generate_schemas(args) -> int:
    from .graphdb import get_connection
    from .strawman import generate

    conn = get_connection()
    try:
        written = generate(conn, only=args.only, samples=args.samples)
    finally:
        conn.close()
    print(f"\nschemas written: {[p.name for p in written] or 'none'}")
    return 0


def cmd_register(args) -> int:
    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    # Default: the whole pipeline in one shot — profile + vet, write the
    # proposals file, then register the survivors + draft their schemas.
    # --preview stops after writing the proposals file (registers nothing).
    # --apply registers a pre-existing (possibly hand-edited) proposals file
    # without re-profiling.
    if args.apply:
        return _register_apply(args, log)
    return _register_build(args, log, register=not args.preview)


def _register_build(args, log, register: bool) -> int:
    from .graphdb import get_connection
    from .profiling import fetch_item_spend, load_ranking, profile, write_ranking
    from .register import apply, propose, write_proposals

    conn = get_connection()
    try:
        # Profiling the corpus is the slow phase, so the ranking is cached to a
        # file: reuse it unless it's missing or --reprofile forces a fresh scan.
        if args.ranking.exists() and not args.reprofile:
            stats = load_ranking(args.ranking)
            log(f"reusing ranking from {args.ranking} ({len(stats)} groups; "
                "--reprofile to rebuild)")
        else:
            segment = None if args.all_segments else args.segment
            log("profiling corpus (streamed)...")
            rows = fetch_item_spend(conn, unspsc_segment=segment, limit=args.limit,
                                    progress=lambda n: log(f"  ...fetched {n:,} items"))
            log(f"  grouping {len(rows):,} items by head-noun...")
            stats = profile(rows)
            write_ranking(stats, args.ranking)
            log(f"  wrote ranking ({len(stats)} groups) to {args.ranking}")

        count = None if (args.count is None or args.count <= 0) else args.count
        chosen, rejected = propose(conn, stats, count=count,
                                   min_samples=args.min_samples,
                                   min_spend_share=args.min_spend,
                                   revisit=args.revisit, log=log)

        if rejected:
            print("\nrejected by the vet:")
            for c in rejected:
                print(f"  {c.token:<22}{c.reason}")
        if not chosen:
            print("\nno viable candidates found")
            return 1

        print(f"\ncandidates ({len(chosen)}):")
        for c in chosen:
            print(f"  {c.category_id:<26}{c.spend_share:>6.1%} spend  include={c.include}"
                  + (f"  exclude={c.exclude}" if c.exclude else ""))
            print(f"    {'example':<10}: {c.canonical_example[:90]!r}")
            print(f"    {'reason':<10}: {c.reason}")

        write_proposals(chosen, args.proposals)
        print(f"\nwrote {len(chosen)} candidates to {args.proposals}")

        if not register:
            print("preview only (--preview) — nothing registered. Commit them with: "
                  f"chilecompra-er register --apply --proposals {args.proposals}")
            return 0

        log(f"registering {len(chosen)} categories + drafting schemas...")
        apply(conn, chosen, log=log)
    finally:
        conn.close()

    print("\ndone. next: run the test suite and a corpus dry run:")
    print("  python -m pytest tests -q")
    print("  chilecompra-er resolve --limit 5000 --show 0 --out data\\corpus_check")
    return 0


def _register_apply(args, log) -> int:
    from .graphdb import get_connection
    from .register import apply, load_proposals

    if not args.proposals.exists():
        print(f"proposals file not found: {args.proposals}\n"
              f"run `chilecompra-er register` first (writes the proposals)")
        return 1
    chosen = load_proposals(args.proposals)
    log(f"loaded {len(chosen)} proposals from {args.proposals}")
    if not chosen:
        print("no proposals to apply")
        return 1

    conn = get_connection()
    try:
        apply(conn, chosen, log=log)
    finally:
        conn.close()
    print("\ndone. next: run the test suite and a corpus dry run:")
    print("  python -m pytest tests -q")
    print("  chilecompra-er resolve --limit 5000 --show 0 --out data\\corpus_check")
    return 0


def cmd_add_category(args) -> int:
    from .categories.schema import add_category

    entry = add_category(
        category_id=args.category_id,
        name=args.name or args.category_id.replace("_", " ").capitalize(),
        include=args.include,
        exclude=args.exclude or [],
        corpus_regex=args.corpus,
        canonical_example=args.example,
    )
    from .categories.schema import load_register

    print(f"added '{entry['category_id']}' (register now v"
          f"{load_register()['register_version']}, {entry['status']})")
    print(f"  include      : {entry['include']}")
    print(f"  exclude      : {entry['exclude']}")
    print(f"  corpus_regex : {entry['corpus_regex']}")
    print("\nnext steps:")
    print(f"  chilecompra-er generate-schemas --only {entry['category_id']}")
    print(f"  chilecompra-er resolve --contains <token> --limit 200 --out data\\check")
    return 0


def cmd_demo(args) -> int:
    from .devtools import run_demo

    run_demo()
    return 0


def cmd_smoke(args) -> int:
    from .devtools import run_smoke
    from .graphdb import get_connection

    conn = get_connection()
    try:
        ok = run_smoke(conn, keep=args.keep)
    finally:
        conn.close()
    return 0 if ok else 1


def cmd_probe_offers(args) -> int:
    from .devtools import probe_offers
    from .graphdb import get_connection

    conn = get_connection()
    try:
        probe_offers(conn, limit=args.limit)
    finally:
        conn.close()
    return 0


def cmd_price_series(args) -> int:
    from .graphdb import get_connection
    from .price.series import build_series, summarize, write_series_csv

    conn = get_connection()
    try:
        rows = build_series(conn, args.category_id)
    finally:
        conn.close()
    if not rows:
        print(f"no price observations for '{args.category_id}' — is the "
              "category persisted? (resolve --contains ... --persist)")
        return 1
    out = args.csv or Path(f"data/price_series_{args.category_id}.csv")
    write_series_csv(rows, out)
    products = len({r["product"] for r in rows})
    print(f"{len(rows)} price observations across {products} generic products -> {out}")
    print("\nproducts with the deepest price history:")
    for line in summarize(rows):
        print(line)
    return 0


def cmd_wipe_catalog(args) -> int:
    """Delete ALL catalog data (Category/GenericProduct/Product/SourceRecord).
    The transactional layer (Licitacion/Oferta/...) and the schema migrations
    are untouched — this resets the catalog, not the source data."""
    if not args.yes:
        print("refusing to wipe the entire catalog without --yes")
        return 1
    from .graphdb import get_connection

    conn = get_connection()
    try:
        rec = conn.query(
            """
            MATCH (n)
            WHERE n:GenericProduct OR n:Product OR n:SourceRecord OR n:Category
            WITH n LIMIT 100000
            DETACH DELETE n
            RETURN count(*) AS deleted
            """
        )
        print(f"catalog wiped: {rec[0]['deleted']} nodes deleted "
              "(transactional data and migrations untouched)")
    finally:
        conn.close()
    return 0


def cmd_wipe_category(args) -> int:
    if not args.yes:
        print("refusing to wipe without --yes (deletes the category's catalog "
              "nodes and their SourceRecords from the graph)")
        return 1
    from .graphdb import get_connection

    conn = get_connection()
    try:
        conn.query(
            """
            MATCH (g:GenericProduct {category_id: $cid})
            OPTIONAL MATCH (g)<-[:RESOLVED_TO]-(s:SourceRecord)
            DETACH DELETE g, s
            """,
            parameters={"cid": args.category_id},
        )
        conn.query("MATCH (c:Category {category_id: $cid}) DETACH DELETE c",
                   parameters={"cid": args.category_id})
    finally:
        conn.close()
    print(f"wiped category {args.category_id}")
    return 0


# --- parser --------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chilecompra-er",
        description="Entity resolution pipeline for ChileCompra medical devices.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("status", help="register + Neo4j instance overview")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("instance", help="Neo4j EC2 lifecycle")
    p.add_argument("action", choices=["start", "stop", "status"])
    p.set_defaults(func=cmd_instance)

    p = sub.add_parser("migrate", help="apply graph schema migrations")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_migrate)

    p = sub.add_parser("resolve", help="resolve source records (dry run by default)")
    p.add_argument("--kind", choices=["tender", "offer", "oc", "joint", "item"],
                   default="tender",
                   help="tender = resolve each buyer line with its tender title as "
                        "context (item wins; title is fallback for terse lines). "
                        "joint = resolve each offer with its tender line's buyer "
                        "text together (offer wins; disagreement -> review). "
                        "item = item-centric: resolve each ItemLicitacion ONCE by "
                        "pooling buyer line + ALL its offers (consensus) + title, "
                        "so every offer shares the item's one generic product")
    p.add_argument("--contains", default=None, help="filter on buyer text")
    p.add_argument("--limit", type=_opt_limit, default=200,
                   help="max records to process; 'all' or 0 = no limit (default 200)")
    p.add_argument("--skip", type=int, default=0,
                   help="skip N records (stable order; chunked corpus builds)")
    p.add_argument("--segment", type=int, default=None,
                   help="UNSPSC segment filter, e.g. 42 (tender/offer/joint kinds; ignored for oc)")
    p.add_argument("--persist", action="store_true",
                   help="WRITE results to the graph (default: dry run)")
    p.add_argument("--out", type=Path, default=Path("data/resolve"),
                   help="CSV output prefix; default data\\resolve — the same "
                        "two files are overwritten on every run")
    p.add_argument("--show", type=int, default=5)
    p.add_argument("--progress-every", type=int, default=200,
                   help="emit a progress line + checkpoint every N records (default 200)")
    p.add_argument("--resume", action="store_true",
                   help="continue the run recorded in <out>.checkpoint.json "
                        "(must match kind/segment/contains/persist/limit)")
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("generate-schemas", help="LLM strawman drafts from corpus samples")
    p.add_argument("--only", default=None, help="single category_id")
    p.add_argument("--samples", type=int, default=50)
    p.set_defaults(func=cmd_generate_schemas)

    p = sub.add_parser("register", help="profile the spend ranking, vet families, and register them + draft schemas (all viable families by default)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--preview", action="store_true",
                      help="profile + vet only: write --proposals without registering "
                           "anything (default: also register the categories + draft schemas)")
    mode.add_argument("--apply", action="store_true",
                      help="register an existing/edited --proposals file without "
                           "re-profiling (skips the profile + vet phase)")
    p.add_argument("--proposals", type=Path, default=Path("data/proposals.json"),
                   help="proposals file — written by every run, read back by "
                        "--apply (default data\\proposals.json)")
    # preview: corpus profiling phase, cached to --ranking
    p.add_argument("--ranking", type=Path, default=Path("data/profiling.csv"),
                   help="cached spend ranking; reused if present, else built by "
                        "profiling the corpus (default data\\profiling.csv)")
    p.add_argument("--reprofile", action="store_true",
                   help="force a fresh corpus profile, overwriting --ranking")
    p.add_argument("--segment", type=int, default=42,
                   help="UNSPSC segment scope when profiling (default 42 = medical supplies)")
    p.add_argument("--all-segments", action="store_true",
                   help="profile the WHOLE marketplace (overrides --segment)")
    p.add_argument("--limit", type=int, default=None,
                   help="profile only the first N tender items (dev/testing)")
    p.add_argument("--count", type=int, default=None,
                   help="stop after N viable categories (default: no limit — propose "
                        "every viable family above --min-spend; 0 or negative also = no limit)")
    p.add_argument("--min-samples", type=int, default=15,
                   help="minimum distinct corpus descriptions per candidate")
    p.add_argument("--min-spend", type=float, default=0.0005,
                   help="spend-share floor; scan stops below it (0.0005 = 0.05%%)")
    p.add_argument("--revisit", action="store_true",
                   help="re-evaluate tokens previously vetted as junk")
    p.set_defaults(func=cmd_register)

    p = sub.add_parser("add-category", help="append a category to the register")
    p.add_argument("category_id", help="lowercase snake_case id, e.g. mascarillas")
    p.add_argument("--name", default=None, help="display name (default: from id)")
    p.add_argument("--include", action="append", required=True,
                   help="Tier-1 inclusion regex over NORMALIZED text (repeatable), "
                        "e.g. \\bmascarilla\\w*")
    p.add_argument("--exclude", action="append", default=None,
                   help="Tier-1 exclusion regex (repeatable)")
    p.add_argument("--corpus", default=None,
                   help="raw-text sampling regex for generate-schemas "
                        "(default: derived from --include)")
    p.add_argument("--example", default=None,
                   help="canonical example description (golden test fixture)")
    p.set_defaults(func=cmd_add_category)

    p = sub.add_parser("wipe-category", help="delete a category's catalog nodes (destructive)")
    p.add_argument("category_id")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_wipe_category)

    p = sub.add_parser("demo", help="offline pipeline demo (no graph, no LLM)")
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("smoke", help="live graph round-trip test (cleans up after itself)")
    p.add_argument("--keep", action="store_true", help="keep the smoke data in the graph")
    p.set_defaults(func=cmd_smoke)

    p = sub.add_parser("probe-offers",
                       help="M3 feasibility: offer-text recovery rate for rubric-only lines")
    p.add_argument("--limit", type=int, default=1500)
    p.set_defaults(func=cmd_probe_offers)

    p = sub.add_parser("price-series",
                       help="per-product price history for a persisted category")
    p.add_argument("category_id")
    p.add_argument("--csv", type=Path, default=None,
                   help="output path; default data\\price_series_<category>.csv")
    p.set_defaults(func=cmd_price_series)

    p = sub.add_parser("wipe-catalog",
                       help="delete ALL catalog data from the graph (destructive; "
                            "transactional source data untouched)")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_wipe_catalog)

    return parser


def main(argv: list[str] | None = None) -> int:
    _utf8_stdout()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
