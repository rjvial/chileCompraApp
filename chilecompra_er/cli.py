"""Command-line interface for the entity-resolution pipeline.

    chilecompra-er status                       # register + instance overview
    chilecompra-er instance start|stop|status   # Neo4j EC2 lifecycle
    chilecompra-er migrate [--dry-run]          # apply graph schema migrations
    chilecompra-er profile [--segment 42] [--top 30] [--csv data\\m0.csv]
    chilecompra-er resolve [--kind tender|offer|oc] [--contains foley]
                           [--limit 200] [--persist] [--out data\\run1] [--show 5]
    chilecompra-er generate-schemas [--only jeringas] [--samples 50]
    chilecompra-er wipe-category <category_id> --yes

`resolve` is a DRY RUN by default (nothing written to the graph) — pass
--persist explicitly to write SourceRecords/RESOLVED_TO edges and catalog
nodes. Destructive commands require --yes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .categories.schema import CATEGORIES_DIR, load_register, load_schema


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


def cmd_profile(args) -> int:
    from .graphdb import get_connection
    from .profiling import fetch_item_spend, profile

    conn = get_connection()
    try:
        rows = fetch_item_spend(conn, unspsc_segment=args.segment)
    finally:
        conn.close()
    print(f"tender items profiled: {len(rows)}")
    stats = profile(rows)

    header = f"{'group':<22}{'records':>9}{'distinct':>10}{'spend CLP':>18}{'share':>8}{'cum':>8}"
    print(header)
    print("-" * len(header))
    for s in stats[: args.top]:
        print(f"{s.group:<22}{s.records:>9}{s.distinct_texts:>10}"
              f"{s.spend_clp:>18,.0f}{s.spend_share:>8.1%}{s.cum_share:>8.1%}")

    if args.csv:
        import csv

        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with open(args.csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["group", "records", "distinct_texts", "spend_clp",
                        "spend_share", "cum_share"])
            for s in stats:
                w.writerow([s.group, s.records, s.distinct_texts, f"{s.spend_clp:.0f}",
                            f"{s.spend_share:.6f}", f"{s.cum_share:.6f}"])
        print(f"\nfull ranking written to {args.csv}")
    return 0


def cmd_resolve(args) -> int:
    from .graphdb import get_connection
    from .ingest import fetch_oc_items, fetch_offers, fetch_tender_items, resolve_items
    from .ingest.export import export_csv
    from .resolve import InMemoryCatalog, Neo4jCatalog, Resolver

    fetchers = {"tender": fetch_tender_items, "offer": fetch_offers, "oc": fetch_oc_items}
    conn = get_connection()
    try:
        items = fetchers[args.kind](conn, contains=args.contains, limit=args.limit)
        catalog = Neo4jCatalog(conn) if args.persist else InMemoryCatalog()
        resolver = Resolver(catalog)
        mode = "PERSIST (writing to graph)" if args.persist else "dry run (no writes)"
        print(f"mode: {mode}")

        stats, reports = resolve_items(resolver, items, persist=args.persist,
                                       collect_reports=True)
        print(stats.summary())

        if args.out:
            for path in export_csv(args.out, reports, catalog):
                print(f"written: {path}")

        shown = 0
        for r in reports:
            if r.status == "unresolved" or shown >= args.show:
                continue
            print(f"  {r.raw_text[:70]!r}")
            print(f"    -> {r.node_id}  attrs={r.extraction.values}  "
                  f"basis={r.price_basis.basis}")
            shown += 1
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


def cmd_widen(args) -> int:
    from .graphdb import get_connection
    from .widen import apply, propose

    conn = get_connection()
    try:
        chosen, rejected = propose(conn, count=args.count, segment=args.segment,
                                   min_samples=args.min_samples,
                                   min_spend_share=args.min_spend,
                                   revisit=args.revisit)
        if rejected:
            print("\nrejected by the vet:")
            for c in rejected:
                print(f"  {c.token:<22}{c.reason}")
        if not chosen:
            print("\nno viable candidates found")
            return 1

        print(f"\nproposed ({len(chosen)}):")
        for c in chosen:
            print(f"  {c.category_id:<26}{c.spend_share:>6.1%} spend  include={c.include}"
                  + (f"  exclude={c.exclude}" if c.exclude else ""))
            print(f"    {'example':<10}: {c.canonical_example[:90]!r}")
            print(f"    {'reason':<10}: {c.reason}")

        if not args.apply:
            print("\npreview only — rerun with --apply to register these and "
                  "generate their schemas")
            return 0

        apply(conn, chosen)
        print("\ndone. next: run the test suite and a corpus dry run:")
        print("  python -m pytest tests -q")
        print("  chilecompra-er resolve --limit 5000 --show 0 --out data\\corpus_check")
    finally:
        conn.close()
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

    p = sub.add_parser("profile", help="M0 head-noun x spend profiling")
    p.add_argument("--segment", type=int, default=None,
                   help="UNSPSC segment scope, e.g. 42 = medical supplies")
    p.add_argument("--top", type=int, default=30)
    p.add_argument("--csv", type=Path, default=None)
    p.set_defaults(func=cmd_profile)

    p = sub.add_parser("resolve", help="resolve source records (dry run by default)")
    p.add_argument("--kind", choices=["tender", "offer", "oc"], default="tender")
    p.add_argument("--contains", default=None, help="filter on buyer text")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--persist", action="store_true",
                   help="WRITE results to the graph (default: dry run)")
    p.add_argument("--out", type=Path, default=None, help="CSV output prefix")
    p.add_argument("--show", type=int, default=5)
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("generate-schemas", help="LLM strawman drafts from corpus samples")
    p.add_argument("--only", default=None, help="single category_id")
    p.add_argument("--samples", type=int, default=50)
    p.set_defaults(func=cmd_generate_schemas)

    p = sub.add_parser("widen", help="propose and add the next N categories from the spend ranking")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--segment", type=int, default=42,
                   help="UNSPSC segment scope (default 42 = medical supplies)")
    p.add_argument("--min-samples", type=int, default=15,
                   help="minimum distinct corpus descriptions per candidate")
    p.add_argument("--min-spend", type=float, default=0.0005,
                   help="spend-share floor; scan stops below it (0.0005 = 0.05%%)")
    p.add_argument("--revisit", action="store_true",
                   help="re-evaluate tokens previously vetted as junk")
    p.add_argument("--apply", action="store_true",
                   help="register the proposals and generate their schemas "
                        "(default: preview only)")
    p.set_defaults(func=cmd_widen)

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

    return parser


def main(argv: list[str] | None = None) -> int:
    _utf8_stdout()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
