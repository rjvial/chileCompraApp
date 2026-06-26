"""Command-line interface for the entity-resolution pipeline.

    chilecompra-er status                       # register + instance overview
    chilecompra-er pipeline [--resume]          # run the WHOLE redesign build end-
                                                # to-end (instance -> migrate ->
                                                # register -> canonicalize -> match
                                                # -> adjudicate -> coherence-check),
                                                # resumable at any interrupted step
    chilecompra-er instance start|stop|status   # Neo4j EC2 lifecycle
    chilecompra-er migrate [--dry-run]          # apply graph schema migrations
    chilecompra-er register [--segment 42] [--reprofile] [--from-fallback]  # profile, vet, register + schemas
    chilecompra-er register --preview                      # stop after data\\proposals.json
    chilecompra-er register --apply                        # register an edited proposals file
    chilecompra-er resolve [--kind item|tender|offer|oc|joint] [--contains foley]
                           [--limit 200] [--persist] [--out data\\run1] [--show 5]
                           [--brands] [--tier2]            # extra classifier tiers
    chilecompra-er fallback-report                # rank UNSPSC fallback + candidate categories
    chilecompra-er generate-schemas [--only jeringas] [--samples 50]
    chilecompra-er build-brand-lexicon [--only jeringas] [--dry-run]  # LLM brand tokens per category
    chilecompra-er train-tier2 [--eval]          # train the Tier-2 statistical classifier
    chilecompra-er wipe-category <category_id> --yes

`register` runs the whole expansion loop in one shot: it profiles the corpus
(caching the spend ranking to data\\profiling.csv, reused on later runs unless
--reprofile), vets the candidates into data\\proposals.json, then adds the
survivors to the register + drafts the schemas that `resolve` consumes. Use
--preview to stop after writing the proposals file (registering nothing),
--apply to register a pre-existing / hand-edited proposals file without
re-profiling, or --from-fallback to rank candidates from the UNSPSC fallback
residue in the graph instead of the whole-corpus profile.

`resolve` is a DRY RUN by default (nothing written to the graph) — pass
--persist explicitly to write the catalog nodes + the direct
(:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct) edges. Destructive commands
require --yes.

Coverage-improvement loop (cut items that land on UNSPSC fallback):
`resolve --kind item --persist` -> `fallback-report` (what's missing) ->
`register --from-fallback` (register the missing families) and/or
`train-tier2` + `build-brand-lexicon` -> `resolve --kind item --tier2 --brands`
(the layered Tier-1 regex -> brand lexicon -> Tier-2 classifier).
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
                    "OPTIONAL MATCH (:ItemLicitacion)-[rt:RESOLVED_TO]->(:GenericProduct) "
                    "WITH gp, count(rt) AS res "
                    "OPTIONAL MATCH (c:Category) RETURN gp, res, count(c) AS cat"
                )[0]
                print(f"graph            : {rec['cat']} categories, "
                      f"{rec['gp']} generic products, {rec['res']} resolved items")
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
        print("note: the public IP changes on each start — .mcp.json was updated to it automatically")
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


def _build_classifier(args):
    """Default None (Resolver uses plain Tier-1). With --brands/--tier2, wrap
    Tier-1 in a LayeredClassifier adding the brand lexicon and/or the trained
    Tier-2 model so more items classify instead of falling back."""
    if not getattr(args, "brands", False) and not getattr(args, "tier2", False):
        return None
    from .resolve.classifier import Tier1Classifier
    from .resolve.layered import LayeredClassifier

    brand = tier2 = None
    if args.brands:
        from .resolve.brand_lexicon import BrandLexicon
        brand = BrandLexicon.load()
        print(f"brand lexicon: {len(brand.brands)} brands", file=sys.stderr)
    if args.tier2:
        from .resolve.tier2 import TIER2_MODEL_PATH, Tier2Classifier
        path = args.tier2_model or TIER2_MODEL_PATH
        if path.exists():
            tier2 = Tier2Classifier.load(path, threshold=args.tier2_threshold)
            print(f"tier-2 model: {path} (threshold {tier2.threshold})", file=sys.stderr)
        else:
            print(f"warning: --tier2 set but no model at {path} "
                  "(run `train-tier2`) — skipping Tier-2", file=sys.stderr)
    return LayeredClassifier(Tier1Classifier(), brand=brand, tier2=tier2)


def cmd_build_brand_lexicon(args) -> int:
    """LLM-propose brand/trade-name tokens per curated category, validate them,
    drop cross-category collisions, and merge into categories/brand_lexicon.json."""
    from .brands import build, merge_brand_maps, save_brand_map
    from .graphdb import get_connection
    from .resolve.brand_lexicon import BRAND_LEXICON_PATH, load_brand_map

    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    conn = get_connection()
    try:
        generated, dropped = build(conn, only=args.only, samples=args.samples,
                                   max_per_category=args.max_per_category, log=log,
                                   progress=_progress_writer(getattr(args, "progress", None)))
    finally:
        conn.close()

    for brand, cats in sorted(dropped.items()):
        print(f"  dropped ambiguous brand {brand!r}: claimed by {cats}")
    print(f"\n{len(generated)} brand(s) proposed (1 category each), "
          f"{len(dropped)} dropped as ambiguous")

    if args.dry_run:
        for brand, cat in sorted(generated.items()):
            print(f"  {brand:<22} -> {cat}")
        print("\ndry run — categories/brand_lexicon.json not written")
        return 0

    existing = load_brand_map()
    merged, added, conflicts = merge_brand_maps(existing, generated,
                                                overwrite=args.overwrite)
    save_brand_map(merged)
    print(f"wrote {len(merged)} brands to {BRAND_LEXICON_PATH} "
          f"(+{added} new, {conflicts} kept existing on conflict)")
    print("use it: chilecompra-er resolve --kind item --brands [--tier2] ...")
    return 0


def cmd_train_tier2(args) -> int:
    """Train the Tier-2 statistical classifier on the curated resolutions in the
    graph (items linked to non-fallback families) and save the model.

    With skip_if_exists (the pipeline's default), an existing model file short-
    circuits training entirely — no graph connection, no fit — so the main build
    reuses a trained model. The standalone `train-tier2` command always retrains
    (overwrites), which is the out-of-band way to refresh the model after the
    corpus grows."""
    from .normalize import Normalizer
    from .resolve.tier2 import TIER2_MODEL_PATH, fetch_training_rows, train

    out = args.out or TIER2_MODEL_PATH
    if getattr(args, "skip_if_exists", False) and Path(out).exists():
        print(f"Tier-2 model already exists at {out} — skipping training "
              "(retrain out-of-band with `chilecompra-er train-tier2`)")
        return 0

    from .graphdb import get_connection

    conn = get_connection()
    try:
        rows = fetch_training_rows(conn)
    finally:
        conn.close()
    if len(rows) < args.min_rows:
        print(f"only {len(rows)} curated-resolution rows (need >= {args.min_rows}) "
              "— run a `resolve --kind item --persist` run with curated categories first")
        return 1

    norm = Normalizer()
    texts = [norm(t) for t, _lab, _u in rows]
    labels = [lab for _t, lab, _u in rows]
    # Cap the training set: a saga fit over the full ~300k+ curated rows takes
    # ~20 min, and 80k rows is already ample signal for a fallback classifier
    # that only fires above a confidence threshold. Seeded for reproducibility.
    _CAP = 80_000
    if len(texts) > _CAP:
        import random
        idx = random.Random(0).sample(range(len(texts)), _CAP)
        texts = [texts[i] for i in idx]
        labels = [labels[i] for i in idx]
        print(f"subsampled to {_CAP:,} training rows (from {len(rows):,})")
    n_classes = len(set(labels))
    print(f"training Tier-2 on {len(texts):,} examples across {n_classes} categories...")

    if args.eval:
        from sklearn.model_selection import train_test_split
        from .resolve.tier2 import train_pipeline
        xtr, xte, ytr, yte = train_test_split(texts, labels, test_size=0.1,
                                              random_state=0)
        acc = train_pipeline(xtr, ytr).score(xte, yte)
        print(f"  held-out accuracy (10% split): {acc:.1%}")

    clf = train(texts, labels, threshold=args.threshold)
    clf.save(out)
    print(f"saved Tier-2 model to {out} (threshold {args.threshold})")
    print("use it: chilecompra-er resolve --kind item --tier2 [--brands] ...")
    return 0


def cmd_tier2_eval(args) -> int:
    """Held-out coverage/precision curve for Tier-2 — TEXT-ONLY vs +UNSPSC feature,
    so the feature's lift is visible before wiring it into production. With --gold,
    also score a human-labeled CSV (true precision, incl. residue-only)."""
    import random

    from .graphdb import get_connection
    from .normalize import Normalizer
    from .resolve import tier2_eval
    from .resolve.tier2 import fetch_training_rows

    conn = get_connection()
    try:
        rows = fetch_training_rows(conn)
    finally:
        conn.close()
    if len(rows) < args.min_rows:
        print(f"only {len(rows)} curated rows (need >= {args.min_rows}) — "
              "resolve some curated items first")
        return 1
    if len(rows) > args.cap:
        rows = random.Random(0).sample(rows, args.cap)
        print(f"sampled {args.cap:,} rows for the eval")

    norm = Normalizer()
    texts = [norm(t) for t, _l, _u in rows]
    labels = [lab for _t, lab, _u in rows]
    unspsc = [u for _t, _l, u in rows]

    base = tier2_eval.evaluate_holdout(texts, labels, None, test_size=args.test_size)
    withu = tier2_eval.evaluate_holdout(texts, labels, unspsc, test_size=args.test_size)
    print(tier2_eval.format_curve(base))
    print()
    print(tier2_eval.format_curve(withu))

    def at(res, t):
        return next((r for r in res["curve"] if abs(r["threshold"] - t) < 1e-9), None)

    b, w = at(base, 0.60), at(withu, 0.60)
    if b and w and b["precision"] is not None and w["precision"] is not None:
        print(f"\nUNSPSC lift @0.60:  coverage {b['coverage']:.1%} -> {w['coverage']:.1%}   "
              f"precision {b['precision']:.1%} -> {w['precision']:.1%}")

    if args.gold:
        import csv

        from .resolve.tier2 import TIER2_MODEL_PATH, Tier2Classifier
        path = args.tier2_model or TIER2_MODEL_PATH
        if not path.exists():
            print(f"no model at {path} for the gold eval — train one first")
            return 1
        clf = Tier2Classifier.load(path, threshold=args.tier2_threshold)
        with open(args.gold, encoding="utf-8-sig", newline="") as f:
            grows = [{"text": norm(r["text"]), "true_category": r["true_category"].strip(),
                      "residue": str(r.get("residue", "")).strip().lower() in ("1", "true", "yes")}
                     for r in csv.DictReader(f) if r.get("true_category", "").strip()]
        print()
        print(f"gold rows: {len(grows)}")
        print(tier2_eval.format_gold(tier2_eval.evaluate_gold(clf, grows)))
    return 0


def cmd_tier2_label_sample(args) -> int:
    """Export a sample of items + the current classifier's predictions to a CSV for
    human labeling — fill `true_category` to build a gold set. --residue-only keeps
    only items Tier-1 misses (what Tier-2 is actually judged on)."""
    import csv

    from .graphdb import get_connection
    from .ingest.neo4j_source import fetch_tender_items
    from .normalize import Normalizer
    from .resolve.classifier import CLASSIFIED, Tier1Classifier
    from .resolve.tier2 import TIER2_MODEL_PATH, Tier2Classifier

    norm = Normalizer()
    tier1 = Tier1Classifier()
    path = args.tier2_model or TIER2_MODEL_PATH
    tier2 = Tier2Classifier.load(path, threshold=args.tier2_threshold) if path.exists() else None

    conn = get_connection()
    try:
        fetch_n = args.n * (8 if args.residue_only else 2)
        items = list(fetch_tender_items(conn, limit=fetch_n, unspsc_segment=args.segment))
    finally:
        conn.close()

    out_rows = []
    for it in items:
        normalized = norm(it.raw_text)
        t1 = tier1.classify(normalized)
        t1_hit = t1.category_id if t1.status == CLASSIFIED else ""
        if args.residue_only and t1_hit:
            continue
        t2_pred = t2_p = ""
        if tier2 is not None:
            v = tier2.classify(normalized)
            t2_pred = v.category_id or ""
            t2_p = v.matched[0].split("=")[1] if v.matched else ""
        out_rows.append({
            "text": normalized, "raw_text": it.raw_text, "unspsc": it.unspsc,
            "tier1": t1_hit, "tier2_pred": t2_pred, "tier2_proba": t2_p,
            "residue": "0" if t1_hit else "1", "true_category": ""})
        if len(out_rows) >= args.n:
            break

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["text", "raw_text", "unspsc", "tier1",
                                          "tier2_pred", "tier2_proba", "residue",
                                          "true_category"])
        w.writeheader()
        w.writerows(out_rows)
    print(f"wrote {len(out_rows)} rows to {args.out}")
    print(f"fill 'true_category', then: chilecompra-er tier2-eval --gold {args.out}")
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
        append_progress,
        checkpoint_path,
        load_checkpoint,
        products_path,
        progress_path,
        resolutions_path,
        save_checkpoint,
        seed_inmemory_catalog,
        truncate_resolutions,
    )
    from .ingest.runner import ResolutionStats
    from .resolve import BatchedNeo4jCatalog, InMemoryCatalog, Resolver

    fetchers = {"tender": fetch_tender_items, "offer": fetch_offers,
                "oc": fetch_oc_items, "joint": fetch_offers, "item": fetch_items}
    joint = args.kind == "joint"
    item_mode = args.kind == "item"
    # Records this run will iterate. The pipeline passes the value it precomputed
    # at establishment (args.total); a standalone resolve over the buyer-text
    # corpus counts it once up front (cheap, index-backed). It's the deterministic
    # denominator for progress %, ETA, and the resumed-run banner. None -> the
    # loop size is unknown (an exotic kind), so progress falls back to a bare
    # count with no percentage.
    loop_total = getattr(args, "total", None)

    prefix = args.out
    cp_path = checkpoint_path(prefix)
    res_csv = resolutions_path(prefix)
    prod_csv = products_path(prefix)
    prog_path = progress_path(prefix)

    # --- resume bookkeeping ---------------------------------------------------
    base_stats = ResolutionStats()
    run_start_skip = args.skip
    effective_skip = args.skip
    remaining_limit = args.limit
    append = False

    # A fresh run starts a clean progress timeline; a --resume continues the
    # existing one (append-only, so the curve spans the kill).
    if not args.resume and prog_path.exists():
        prog_path.unlink()

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
        # Reuse the loop size recorded at the run's start (deterministic, so it
        # still holds) when the caller didn't pass one.
        if loop_total is None:
            loop_total = cp.total
        # Align the CSV to the checkpoint exactly so kill timing can't dup rows.
        kept = truncate_resolutions(res_csv, cp.processed)
        append = True
        of_total = f"/{loop_total:,} ({cp.processed / loop_total:.1%})" if loop_total \
            else ""
        print(f"resuming: {cp.processed:,}{of_total} records already done "
              f"(CSV trimmed to {kept} rows); continuing from skip {effective_skip}")

    conn = get_connection()
    try:
        # Resolve the deterministic loop size if the caller didn't supply one.
        # A bounded run stops at --limit, so the limit IS the loop size. An unbounded
        # run counts once (count_resolve_items mirrors the fetchers' filter exactly).
        # Other kinds have no cheap precount -> loop_total stays None and progress
        # falls back to a bare count.
        if loop_total is None and args.kind in ("item", "tender"):
            if args.limit:
                loop_total = args.limit
            else:
                from .ingest.neo4j_source import count_resolve_items
                loop_total = count_resolve_items(conn, contains=args.contains,
                                                 unspsc_segment=args.segment)
        elif loop_total is not None and args.limit:
            # caller-supplied total (pipeline), still capped by an explicit limit
            loop_total = min(loop_total, args.limit)

        kwargs = {"contains": args.contains, "limit": remaining_limit}
        if args.kind in ("tender", "offer", "joint", "item"):
            kwargs.update(skip=effective_skip, unspsc_segment=args.segment)
        elif args.kind == "oc":
            kwargs.update(skip=effective_skip)
        items = fetchers[args.kind](conn, **kwargs)

        catalog = BatchedNeo4jCatalog(conn) if args.persist else InMemoryCatalog()
        if args.persist:
            # One bulk snapshot read instead of one per category — collapses the
            # dominant round-trip cost on a high-latency link (see preload()).
            n = catalog.preload()
            print(f"preloaded catalog snapshot: {n:,} generic products (1 query)")
        if args.resume and not args.persist:
            seeded = seed_inmemory_catalog(catalog, prod_csv)
            print(f"reseeded {seeded} existing products from {prod_csv.name}")
        resolver = Resolver(catalog, classifier=_build_classifier(args))

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
            if args.persist:
                # Flush buffered graph writes so the checkpoint's processed count
                # matches what's durably in the graph (BatchedNeo4jCatalog).
                catalog.flush()
            else:
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
                stats_dict=st.to_dict(), total=loop_total))

        # How often to checkpoint durably. A persist checkpoint is cheap (just
        # catalog.flush() + a small JSON), so bank progress OFTEN — on a flaky box a
        # crash then loses at most ~this many records of resume position, not 20k. A
        # dry run rewrites the products CSV here (expensive, lock-prone under a
        # syncing folder), so it stays sparse.
        bank_every = 5_000 if args.persist else 20_000
        durable_every = max(1, bank_every // max(args.progress_every, 1))
        ticks = 0

        def show_progress(st) -> None:
            nonlocal ticks
            ticks += 1
            writer.flush()
            if ticks % durable_every == 0:
                checkpoint(st, done=False)
            # Denominator + % against the deterministic loop size; falls back to
            # a bare count when the loop size is unknown.
            if loop_total:
                pct = f" ({st.total / loop_total:.1%})"
                denom = f"/{loop_total:,}{pct}"
            else:
                denom = f"/{args.limit}" if args.limit else ""
            res = st.by_status.get("resolved_generic", 0)
            unr = st.by_status.get("unresolved", 0)
            print(f"  ...processed {st.total:,}{denom}  resolved={res:,}  "
                  f"unresolved={unr:,}  created={st.nodes_created:,}",
                  file=sys.stderr, flush=True)
            # Record the point on the persistent timeline so the evolution
            # (rate, ETA) can be consulted any time via `pipeline --status`.
            append_progress(prog_path, {
                "ts": time.time(), "processed": st.total, "total": loop_total,
                "resolved": res, "unresolved": unr, "created": st.nodes_created})

        # Initial checkpoint so even a kill before the first progress tick
        # leaves a resumable marker.
        checkpoint(base_stats, done=False)
        from .ingest.neo4j_source import _BATCH as _fetch
        print(f"fetching + resolving (single streamed scan, pull batch {_fetch:,})...",
              file=sys.stderr, flush=True)

        try:
            stats, _ = resolve_items(resolver, items, persist=args.persist,
                                     collect_reports=False, on_report=on_report,
                                     progress=show_progress,
                                     progress_every=args.progress_every,
                                     stats=base_stats, joint=joint,
                                     item_mode=item_mode, fallback=args.fallback)
        finally:
            writer.flush()
            writer.close()

        checkpoint(stats, done=True)  # final: writes products CSV (with retries)
        # Final point on the timeline: anchors the last rate sample at the true
        # end and lands the row at 100%. (Whether a step shows [done] is decided
        # by the pipeline checkpoint's done-set, not this flag — the `done` field
        # stays for human/debug inspection of the timeline.)
        append_progress(prog_path, {
            "ts": time.time(), "processed": stats.total, "total": loop_total,
            "resolved": stats.by_status.get("resolved_generic", 0),
            "unresolved": stats.by_status.get("unresolved", 0),
            "created": stats.nodes_created, "done": True})

        print(stats.summary())
        print(f"written: {res_csv}")
        if not args.persist:
            print(f"written: {prod_csv}")
        print(f"checkpoint: {cp_path}")

        for r in shown:
            print(f"  {r.raw_text[:70]!r}")
            attrs = r.extraction.values if r.extraction is not None else {}
            basis = r.price_basis.basis if r.price_basis is not None else "n/a"
            print(f"    -> {r.node_id}  attrs={attrs}  basis={basis}")
    finally:
        conn.close()
    return 0


def _ns(**kw):
    import argparse
    return argparse.Namespace(**kw)


def _progress_writer(path, *, every: float = 1.0):
    """A (processed, total) -> None callback that appends a timeline sample to
    `path` so `pipeline --status` can show the step's N/total + rate + ETA — the
    same shape the resolve steps already write. Throttled to one sample per
    `every` seconds (plus always the final point). None path -> no-op."""
    if path is None:
        return None
    from .ingest.resume import append_progress
    # One writer == one step run: clear any prior run's timeline so `--status`
    # never mixes two runs' samples (resolve steps clear theirs in cmd_resolve;
    # the loop steps clear here). A within-run resume just re-fills from the
    # resume point.
    try:
        Path(path).unlink()
    except OSError:
        pass
    last = [0.0]

    def write(processed, total) -> None:
        now = time.time()
        if now - last[0] >= every or (total and processed >= total):
            append_progress(path, {"ts": now, "processed": processed, "total": total})
            last[0] = now

    return write


def cmd_pipeline(args) -> int:
    """Run the whole redesign build end-to-end (instance -> migrate -> register
    -> canonicalize -> match -> adjudicate -> coherence-check) as an ordered,
    step-level-resumable sequence (see chilecompra_er/pipeline.py). Each step
    reuses the existing cmd_* handler; the checkpoint records completed steps so
    --resume continues at the interrupted one. The long steps additionally resume
    WITHIN themselves via their own stores/checkpoints (the profile store, the
    match :COTIZA stream offset, the adjudicate verdict store, the register vet checkpoint).
    """
    from .pipeline import (
        PIPELINE_STEPS,
        STEP_ADJUDICATE,
        STEP_CANONICALIZE,
        STEP_COHERENCE,
        STEP_INSTANCE,
        STEP_MATCH,
        STEP_MIGRATE,
        STEP_REGISTER,
        PipelineCheckpoint,
        load_pipeline_checkpoint,
        pipeline_checkpoint_path,
        remaining_steps,
        save_pipeline_checkpoint,
    )

    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    data = args.data_dir
    segment = None if args.all_segments else args.segment
    limit = args.limit
    cp_path = pipeline_checkpoint_path(data)
    store = data / "profiles.jsonl"
    verdicts = data / "adjudications.jsonl"

    # --- consultation: print the plan (optionally watch) ----------------------
    def render_status() -> str:
        """Print the plan: steps done/pending. Returns 'absent'|'complete'|'running'."""
        cp = load_pipeline_checkpoint(cp_path)
        if cp is None:
            print(f"no pipeline checkpoint at {cp_path} — nothing established yet")
            return "absent"
        done = set(cp.done)
        todo = [s for s in PIPELINE_STEPS if s not in done]
        current = todo[0] if todo else None
        stamp = time.strftime("%H:%M:%S")
        scope = f"segment={cp.segment}" if cp.segment is not None else "all segments"
        print(f"pipeline status  ({scope}  limit={cp.limit})  {stamp}")
        print(f"  checkpoint: {cp_path}")
        print(f"  {len(done)}/{len(PIPELINE_STEPS)} steps done\n")
        for step in PIPELINE_STEPS:
            mark = "[x]" if step in done else ("[>]" if step == current else "[ ]")
            tail = "  [done]" if step in done else ("  [next]" if step == current else "")
            print(f"  {mark} {step}{tail}")
        return "complete" if cp.is_complete() else "running"

    if getattr(args, "status", False) or getattr(args, "watch", False):
        if not getattr(args, "watch", False):
            render_status()
            return 0
        interval = max(2, getattr(args, "interval", None) or 15)
        print(f"watching pipeline status every {interval}s (Ctrl-C to stop)...\n")
        try:
            while True:
                state = render_status()
                if state == "complete":
                    print("\npipeline complete — stopping watch.")
                    return 0
                if state == "absent":
                    return 0  # nothing to watch yet
                print("\n" + "-" * 60)
                sys.stdout.flush()  # show live even when piped (block-buffered)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nstopped watching.")
            return 0

    # --- resume / restart bookkeeping -----------------------------------------
    if args.restart:
        victims = [cp_path, data / "register.checkpoint.json"]
        victims += list(data.glob("match_seg*.checkpoint.json"))  # match :COTIZA offset
        for p in victims:
            if p.exists():
                p.unlink()
        log(f"--restart: cleared {cp_path.name}, the match sub-checkpoint(s) and the "
            "register vet checkpoint (the profile + adjudicate verdict stores are left "
            "intact — delete data\\profiles.jsonl by hand to recanonicalize from scratch)")

    cp = load_pipeline_checkpoint(cp_path)
    if cp is not None and not (args.resume or args.restart or args.from_step or args.only):
        print(f"a pipeline checkpoint already exists at {cp_path} "
              f"(done: {cp.done or 'nothing yet'}).\n"
              "  resume it:  chilecompra-er pipeline --resume\n"
              "  start over: chilecompra-er pipeline --restart")
        return 1
    if cp is None:
        cp = PipelineCheckpoint(segment=segment, limit=limit)
    else:
        bad = cp.mismatches(segment=segment, limit=limit)
        if bad and not args.restart:
            print("refusing to resume: scope differs from the checkpoint "
                  "(would mix two builds):")
            for m in bad:
                print(f"  {m}")
            print("  re-run with the same --segment/--limit, or --restart to begin anew")
            return 1

    todo = remaining_steps(cp.done, from_step=args.from_step, only=args.only)
    if not todo:
        print(f"pipeline already complete ({len(cp.done)} steps done) — nothing to do")
        return 0

    # --- step implementations -------------------------------------------------
    def run_instance() -> int:
        """Starter: boot the box if stopped, allowlist this machine's IP on the bolt
        port, refresh .mcp.json, and wait until bolt answers. A clean failure (e.g.
        the security group couldn't be updated for lack of perms) halts with an
        actionable message instead of a deep stack trace at the next graph step."""
        from . import graphdb
        try:
            ip = graphdb.start_neo4j_instance(wait_for_bolt=90)
            log(f"    Neo4j up + reachable @ {ip}")
            return 0
        except Exception as exc:  # noqa: BLE001 — turn infra errors into a clean halt
            print(f"\nstarter could not reach Neo4j bolt: {exc}\n"
                  "  the instance is up but bolt isn't answering from here — the "
                  "security group must allow your IP on 7687. The starter tries to "
                  "add it automatically; that needs ec2:AuthorizeSecurityGroupIngress "
                  "permission in this shell.", file=sys.stderr)
            return 1

    def run_register() -> int:
        """VOCABULARY (incremental). Build from nothing when the register is empty;
        when families already exist, do NOT re-profile/re-vet — only DRAFT any
        missing schemas (a cheap gap-fill). --rebuild-vocab forces a full
        profile+vet pass that builds over what's there (extends coverage)."""
        from .categories.schema import CATEGORIES_DIR, load_register

        reg = load_register()
        cats = reg.get("categories", [])
        missing = [c for c in cats
                   if not (CATEGORIES_DIR / c.get("schema_file", "")).exists()]
        force = getattr(args, "rebuild_vocab", False)

        if not force and cats and not missing:
            log(f"    vocabulary present: {len(cats)} families, all schemas drafted "
                "— skipping (use --rebuild-vocab to extend/redraft)")
            return 0
        if not force and cats and missing:
            log(f"    vocabulary present ({len(cats)} families) but {len(missing)} "
                "schema(s) missing — drafting only the gaps (no re-vet)")
            return cmd_generate_schemas(_ns(only=None, samples=50, overwrite=False))

        # Empty register (from nothing) OR --rebuild-vocab: full profile + vet +
        # register + draft. `register` is itself incremental — it builds over the
        # existing register (already-covered families skipped) and generate()
        # leaves existing schema files untouched — so a forced run only EXTENDS.
        ckpt = data / "register.checkpoint.json"
        return cmd_register(_ns(
            apply=False, preview=False, from_fallback=False,
            proposals=data / "proposals.json", ranking=data / "profiling.csv",
            reprofile=False, all_segments=args.all_segments, segment=args.segment,
            limit=None, count=None, min_samples=15, min_spend=0.0005, revisit=False,
            checkpoint=ckpt, resume=ckpt.exists(), progress=None))

    def run_canonicalize() -> int:
        return cmd_canonicalize(_ns(
            from_file=None, out=store, model=args.model, workers=args.workers,
            group_size=args.group_size, segment=segment, limit=limit, dry_run=False))

    def run_match() -> int:
        seg = segment if segment is not None else "all"
        ckpt = data / f"match_seg{seg}.checkpoint.json"
        return cmd_match(_ns(
            store=store, attach_partials=False, persist=True, resume=ckpt.exists(),
            segment=segment, limit=limit, show=15))

    def run_adjudicate() -> int:
        """adjudicate is non-fatal: the residue is a review backlog, not a build gate. A
        failure (or usage-limit abort) is logged but never halts the pipeline."""
        rc = cmd_adjudicate(_ns(
            store=store, verdicts=verdicts, model=args.adjudicate_model,
            dry_run=False))
        if rc != 0:
            log(f"    adjudicate returned rc={rc} — continuing (adjudicate is non-fatal)")
        return 0

    def run_coherence() -> int:
        # Structural breaches DO fail the run (rc=1) — the build's gate.
        return cmd_coherence_check(_ns(
            store=store, graph=True, tier="all", out=None))

    def precheck(step) -> str | None:
        """Cheap 'already done?' probe per stage — returns a skip reason, or None
        to run it. (The work stages canonicalize/match/adjudicate are themselves
        idempotent and self-skip finished work via their own caches; and any stage
        already in the checkpoint's `done` list is skipped before this.)"""
        if step == STEP_INSTANCE:
            try:
                from . import graphdb
                if graphdb.graph_reachable():
                    return "Neo4j already running and reachable on bolt"
            except Exception:  # noqa: BLE001 — a probe failure must never block the run
                return None
        if step == STEP_REGISTER and not getattr(args, "rebuild_vocab", False):
            from .categories.schema import CATEGORIES_DIR, load_register
            cats = load_register().get("categories", [])
            if cats and all((CATEGORIES_DIR / c.get("schema_file", "")).exists()
                            for c in cats):
                return (f"vocabulary present ({len(cats)} families, all schemas "
                        "drafted) — --rebuild-vocab to extend")
        return None

    steps = {
        STEP_INSTANCE: run_instance,
        STEP_MIGRATE: lambda: cmd_migrate(_ns(dry_run=False)),
        STEP_REGISTER: run_register,
        STEP_CANONICALIZE: run_canonicalize,
        STEP_MATCH: run_match,
        STEP_ADJUDICATE: run_adjudicate,
        STEP_COHERENCE: run_coherence,
    }

    # Stages a PRIOR pipeline run already finished (recorded in the checkpoint) are
    # not in `todo` — name each so the skip is never silent.
    if not (args.from_step or args.only):
        for s in (s for s in PIPELINE_STEPS if s in cp.done):
            log(f"SKIP {s}: already completed in a prior pipeline run (checkpoint)")

    log(f"pipeline: {len(cp.done)}/{len(PIPELINE_STEPS)} steps done; "
        f"{len(todo)} to consider -> {todo}")
    ran, skipped = [], []
    for i, step in enumerate(todo, 1):
        head = (f"step {i}/{len(todo)}: {step} "
                f"({PIPELINE_STEPS.index(step) + 1}/{len(PIPELINE_STEPS)} overall)")
        # Per-stage completion probe: skip (and say why) if its work is already done.
        reason = precheck(step)
        if reason:
            log(f"\n=== {head} — SKIP: {reason} ===")
            skipped.append((step, reason))
            cp.mark_done(step)
            save_pipeline_checkpoint(cp_path, cp)
            continue
        log(f"\n=== {head} — running ===")
        rc = steps[step]()
        if rc != 0:
            print(f"\nstep {step!r} failed (rc={rc}) — pipeline halted. "
                  f"Fix the cause then `chilecompra-er pipeline --resume`, or skip "
                  f"it with `--from-step <next-step>`.", file=sys.stderr)
            return rc
        # Mark done + persist immediately so an interrupt after this point
        # never re-runs a completed step.
        ran.append(step)
        cp.mark_done(step)
        save_pipeline_checkpoint(cp_path, cp)
        log(f"=== step {step!r} done (checkpoint saved) ===")

    # Run summary: exactly which stages ran vs were skipped (and why).
    log("\n--- pipeline summary ---")
    log(f"  ran     : {', '.join(ran) or '(none)'}")
    for s, r in skipped:
        log(f"  skipped : {s} — {r}")

    if args.only or args.from_step:
        print(f"\nran {todo} (manual selection). Checkpoint: {cp_path}")
    elif cp.is_complete():
        print(f"\npipeline COMPLETE — all {len(PIPELINE_STEPS)} steps done.")
        print(f"  profile store : {store}")
        print(f"  checkpoint    : {cp_path}")
        print("  next: chilecompra-er price-clusters --category <id>   (price-clusters price query)")
    return 0

def cmd_canonicalize(args) -> int:
    """canonicalization (redesign): turn descriptions into persisted canonical
    profiles via Claude Haiku 4.5 (batch + caching). SCAFFOLD — the graph source
    fetch lands in the Phase-1 build; --from-file + --dry-run are runnable now."""
    from .resolve.canonicalize import (
        ProfileStore,
        canonicalize,
        fetch_distinct_descriptions,
    )

    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    store = ProfileStore(args.out)
    if args.from_file:
        records = [ln for ln in Path(args.from_file).read_text(
            encoding="utf-8").splitlines() if ln.strip()]
        if args.limit:
            records = records[:args.limit]
        stats = canonicalize(records, store, model=args.model,
                             workers=args.workers, group_size=args.group_size,
                             dry_run=args.dry_run, log=log)
    else:
        # Streamed graph read — the connection must stay open while
        # canonicalize() consumes the lazy generator.
        from .graphdb import get_connection
        conn = get_connection()
        try:
            records = fetch_distinct_descriptions(
                conn, unspsc_segment=args.segment, limit=args.limit)
            stats = canonicalize(records, store, model=args.model,
                                 workers=args.workers, group_size=args.group_size,
                                 dry_run=args.dry_run, log=log)
        finally:
            conn.close()
    print(f"inputs       : {stats.total_inputs:,}")
    print(f"distinct     : {stats.distinct:,}")
    print(f"cached (skip): {stats.cached:,}")
    if not args.dry_run:
        print(f"canonicalized: {stats.canonicalized:,}")
        print(f"failed       : {stats.failed:,}")
        print(f"store        : {args.out} ({len(store):,} profiles)")
    return 0


def cmd_match(args) -> int:
    """match (redesign): cluster the profile store into product clusters. Offline
    report by default; with --persist, write :ProductoCanonico + brand-specific
    :Producto nodes (VARIANTE_DE their cluster) and bind offers via :COTIZA edges to
    their Producto (price per base unit on the edge)."""
    from .resolve.canonicalize import ProfileStore
    from .resolve.matcher import cluster

    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    store = ProfileStore(args.store)
    items = store.items()
    if not items:
        print(f"no profiles in {args.store} — run `canonicalize` first")
        return 1
    profiles = [p for _h, p in items]
    res = cluster(profiles, attach_partials=args.attach_partials, log=log)
    print(f"profiles        : {len(profiles):,}")
    print(f"product clusters : {len(res.clusters):,}")
    print(f"ESPECIFICA edges    : {len(res.refines):,}")
    print(f"residue          : {len(res.residue):,} "
          f"(model-token conflicts + ambiguous partials)")
    top = sorted(res.clusters, key=lambda c: len(c.members), reverse=True)[:args.show]
    print(f"\ntop {args.show} clusters by bid count:")
    for c in top:
        print(f"  {len(c.members):>6}  {c.signature}")

    if args.persist:
        from .graphdb import get_connection
        from .ingest.clusters import (
            build_records,
            read_offers_checkpoint,
            write_clusters,
            write_offers,
            write_products,
        )
        from .ingest.neo4j_source import fetch_offer_prices
        from .normalize import Normalizer

        # Clustering is deterministic, so on resume the records rebuild identically
        # and the cluster writes (MERGE) are idempotent; only the long edge write
        # resumes from a checkpointed stream offset.
        cluster_rows, refines_rows, product_rows, hash_to_product, pack_by_hash = \
            build_records(res, items)
        ckpt = Path(f"data/match_seg{args.segment if args.segment is not None else 'all'}.checkpoint.json")
        start = read_offers_checkpoint(ckpt) if args.resume else 0
        conn = get_connection()
        try:
            print("PERSIST: writing clusters + products + binding offers", file=sys.stderr)
            write_clusters(conn, cluster_rows, refines_rows, log=log)
            write_products(conn, product_rows, log=log)
            if start:
                log(f"resuming COTIZA from offer {start:,}")
            offers = fetch_offer_prices(conn, unspsc_segment=args.segment,
                                        skip=start, limit=args.limit)
            written, skipped = write_offers(
                conn, offers, hash_to_product, pack_by_hash, Normalizer(),
                start=start, checkpoint_path=ckpt, log=log)
        finally:
            conn.close()
        print(f"\npersisted: {len(cluster_rows):,} clusters, {len(product_rows):,} products, "
              f"{written:,} COTIZA edges ({skipped:,} offers unplaced)")
    return 0


def cmd_adjudicate(args) -> int:
    """adjudicate (redesign): Claude adjudicates the matcher's residue (model-token
    conflicts + ambiguous partials). Verdicts persisted by case key. Uses
    Sonnet/Opus → API credits; --dry-run reports the case count with no spend."""
    from .resolve.adjudicate import (
        VerdictStore,
        adjudicate,
        build_questions,
        signature_profiles,
    )
    from .resolve.canonicalize import ProfileStore
    from .resolve.matcher import cluster

    def log(msg) -> None:
        print(msg, file=sys.stderr, flush=True)

    items = ProfileStore(args.store).items()
    if not items:
        print(f"no profiles in {args.store} — run `canonicalize` first")
        return 1
    result = cluster([p for _h, p in items])
    questions = build_questions(result, signature_profiles(items))
    store = VerdictStore(args.verdicts)
    stats = adjudicate(questions, store, model=args.model,
                       dry_run=args.dry_run, log=log)
    print(f"cases       : {stats.questions:,}")
    print(f"cached      : {stats.cached:,}")
    if not args.dry_run:
        print(f"adjudicated : {stats.adjudicated:,}")
        print(f"failed      : {stats.failed:,}")
        print(f"verdicts    : {args.verdicts} ({len(store):,} total)")
    return 0


def cmd_coherence_check(args) -> int:
    """Coherence auditor (redesign): run the named invariants over the canonicalize
    profiles + clusters (and, with --graph, the persisted catalog). STRUCTURAL
    breaches fail the run (exit 1); SEMANTIC are ranked review backlogs; HEALTH is
    a trend snapshot. Read-only."""
    from .coherence import audit_offline, check_graph
    from .resolve.canonicalize import ProfileStore

    store = ProfileStore(args.store)
    items = store.items()
    if not items:
        print(f"no profiles in {args.store} — run `canonicalize` first")
        return 1
    findings = audit_offline(items)
    if args.graph:
        from .graphdb import get_connection
        conn = get_connection()
        try:
            findings += check_graph(conn)
        finally:
            conn.close()

    tiers = ("structural", "semantic", "health") if args.tier == "all" else (args.tier,)
    failed = 0
    for tier in tiers:
        rows = [f for f in findings if f.tier == tier]
        if not rows:
            continue
        print(f"\n{tier.upper()}:")
        for f in rows:
            mark = "  ✗ FAIL" if (f.fail and f.count) else ""
            tail = f": {f.title}" if tier == "health" else f"  {f.title}"
            print(f"  {f.id:<14}{f.count:>8,}{tail}{mark}")
            for ex in f.examples[:3]:
                print(f"        e.g. {ex}")
            if f.fail and f.count:
                failed += f.count

    if args.out:
        import csv
        sem = [f for f in findings if f.tier == "semantic"]
        with open(args.out, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["id", "title", "count", "examples"])
            for f in sem:
                w.writerow([f.id, f.title, f.count, "; ".join(map(str, f.examples[:5]))])
        print(f"\nwrote semantic backlog to {args.out}", file=sys.stderr)

    if failed:
        print(f"\nFAIL: {failed:,} structural breach(es)")
    else:
        print("\nOK: no structural breaches")
    return 1 if failed else 0


def cmd_fallback_report(args) -> int:
    """Rank the UNSPSC fallback residue from the graph: which commodity codes
    carry the most un-categorized items, and which head-noun families recur
    across the residue (the categories worth registering next). Reads the
    persisted resolution — run a `--kind item --persist` run first."""
    from .fallback import bucket_ranking, fetch_fallback_items, residue_ranking
    from .graphdb import get_connection
    from .profiling import RESIDUE, write_ranking

    conn = get_connection()
    try:
        rows = fetch_fallback_items(conn)
    finally:
        conn.close()
    if not rows:
        print("no UNSPSC fallback nodes in the graph — run a "
              "`resolve --kind item --persist` run first")
        return 1

    total_items = len(rows)
    total_spend = sum(float(r.get("spend_clp") or 0) for r in rows)
    buckets = bucket_ranking(rows, min_count=args.min_count)
    families = residue_ranking(rows, min_count=args.min_count)
    rubric_total = sum(b.rubric_items for b in buckets)

    print(f"fallback residue: {total_items:,} items across {len(buckets):,} "
          f"UNSPSC buckets, {total_spend/1e6:,.0f}M CLP awarded")
    print(f"  rubric-only buyer lines: {rubric_total:,} "
          f"({rubric_total/total_items:.0%}) — genuinely uninformative, fallback is correct\n")

    print(f"top {args.top} commodity codes by fallback items:")
    print(f"  {'code':<18}{'items':>9}{'spend(M)':>11}{'rubric%':>9}  top families")
    for b in buckets[:args.top]:
        fams = ", ".join(f"{f}({n})" for f, n in b.top_families) or "-"
        print(f"  {b.code:<18}{b.items:>9,}{b.spend_clp/1e6:>11,.0f}"
              f"{b.rubric_items/b.items:>9.0%}  {fams}")

    print(f"\ntop {args.top} residue head-noun families (candidate categories):")
    print(f"  {'family':<22}{'items':>9}{'distinct':>10}{'spend(M)':>11}")
    shown = [s for s in families if s.group != RESIDUE][:args.top]
    for s in shown:
        print(f"  {s.group:<22}{s.records:>9,}{s.distinct_texts:>10,}"
              f"{s.spend_clp/1e6:>11,.0f}")

    write_ranking(families, args.out)
    print(f"\nwrote residue family ranking to {args.out} "
          f"(feed it: chilecompra-er register --from-fallback)")
    return 0


def cmd_ambiguity_report(args) -> int:
    """Rank the register-overlap backlog: which category SETS collide on the
    fallback residue (ambiguous items), separating spurious overlaps (one
    product, fixable with an exclude) from genuine multi-product bundles
    (ambiguity is correct). The counterpart to `fallback-report`: that ranks the
    UNCOVERED families, this the OVERLAPPING ones. Reads the persisted residue —
    run a `--kind item --persist` run first."""
    from .ambiguity import ambiguity_ranking
    from .fallback import fetch_fallback_items
    from .graphdb import get_connection

    conn = get_connection()
    try:
        rows = fetch_fallback_items(conn)
    finally:
        conn.close()
    if not rows:
        print("no UNSPSC fallback nodes in the graph — run a "
              "`resolve --kind item --persist` run first")
        return 1

    stats = ambiguity_ranking(rows, min_count=args.min_count)
    total_amb = sum(s.items for s in stats)
    total_spurious = sum(s.spurious_items for s in stats)
    print(f"register overlaps: {total_amb:,} ambiguous residue items across "
          f"{len(stats):,} colliding category sets")
    print(f"  spurious (one product, fixable with an exclude): {total_spurious:,}")
    print(f"  multi-product bundles (ambiguity is correct):    "
          f"{total_amb - total_spurious:,}\n")
    print(f"top {args.top} colliding category sets (by spurious/fixable volume):")
    print(f"  {'spurious':>9}{'bundle':>8}  categories")
    for s in stats[:args.top]:
        print(f"  {s.spurious_items:>9,}{s.bundle_items:>8,}  {' ∩ '.join(s.pair)}")
        for ex in s.samples[:2]:
            print(f"      e.g. {ex}")
    return 0


def cmd_generate_schemas(args) -> int:
    from .graphdb import get_connection
    from .strawman import generate

    conn = get_connection()
    try:
        written = generate(conn, only=args.only, samples=args.samples,
                           overwrite=args.overwrite)
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

    # Within-step resume for the (long, LLM-heavy) vet scan. The fallback scan
    # gets its own checkpoint file so the two never collide.
    ckpt = getattr(args, "checkpoint", None) or (
        Path("data/register_fallback.checkpoint.json") if args.from_fallback
        else Path("data/register.checkpoint.json"))
    resume = getattr(args, "resume", False)

    def clear_checkpoint() -> None:
        if ckpt.exists():
            ckpt.unlink()

    conn = get_connection()
    try:
        # Source of the candidate ranking: either the UNSPSC fallback residue in
        # the graph (--from-fallback: target exactly what failed to resolve) or
        # the whole-corpus head-noun x spend profile (the default M0 scan).
        if args.from_fallback:
            from .fallback import fetch_fallback_items, residue_ranking

            log("profiling the UNSPSC fallback residue from the graph...")
            rows = fetch_fallback_items(conn)
            if not rows:
                print("no UNSPSC fallback nodes in the graph — run a "
                      "`resolve --kind item --persist` run first")
                return 1
            stats = residue_ranking(rows)
            residue_csv = Path("data/fallback_ranking.csv")  # never clobber the corpus profile cache
            write_ranking(stats, residue_csv)
            log(f"  {len(rows):,} residue items -> {len(stats)} head-noun "
                f"families (ranking written to {residue_csv})")
        # Profiling the corpus is the slow phase, so the ranking is cached to a
        # file: reuse it unless it's missing or --reprofile forces a fresh scan.
        elif args.ranking.exists() and not args.reprofile:
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
                                   revisit=args.revisit,
                                   checkpoint_path=ckpt, resume=resume,
                                   progress=_progress_writer(getattr(args, "progress", None)),
                                   log=log)

        if rejected:
            print("\nrejected by the vet:")
            for c in rejected:
                print(f"  {c.token:<22}{c.reason}")
        if not chosen:
            print("\nno viable candidates found")
            clear_checkpoint()  # scan completed (just barren) — no resume value
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
            # Proposals are durably saved; the scan need not be resumed again.
            clear_checkpoint()
            print("preview only (--preview) — nothing registered. Commit them with: "
                  f"chilecompra-er register --apply --proposals {args.proposals}")
            return 0

        log(f"registering {len(chosen)} categories + drafting schemas...")
        apply(conn, chosen, log=log)
        clear_checkpoint()  # registered — drop the now-complete vet checkpoint
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
              "category persisted? (resolve --kind item --persist binds offer prices)")
        return 1
    out = args.csv or Path(f"data/price_series_{args.category_id}.csv")
    write_series_csv(rows, out)
    products = len({r["product"] for r in rows})
    print(f"{len(rows)} price observations across {products} generic products -> {out}")
    print("\nproducts with the deepest price history:")
    for line in summarize(rows):
        print(line)
    return 0


def cmd_price_clusters(args) -> int:
    """price-clusters (redesign): price series over product clusters — per-base-unit price
    over time and across competition (distinct supplier RUTs / brands). Reads
    (:Oferta)-[:COTIZA]->(:Producto)-[:VARIANTE_DE]->(:ProductoCanonico); scope with
    --category or a single --signature."""
    from .graphdb import get_connection
    from .price.cluster_series import (
        build_cluster_series,
        summarize,
        write_cluster_series_csv,
    )

    if not args.category and not args.signature:
        print("pass --category <id> or --signature <sig> to scope the series")
        return 1
    conn = get_connection()
    try:
        rows = build_cluster_series(conn, category=args.category,
                                    signature=args.signature)
    finally:
        conn.close()
    if not rows:
        print("no price observations — is the catalog persisted "
              "(`match --persist`) for that scope?")
        return 1
    out = args.csv or Path(f"data/price_clusters_{args.category or 'sig'}.csv")
    write_cluster_series_csv(rows, out)
    clusters = len({r["cluster"] for r in rows})
    print(f"{len(rows):,} price observations across {clusters:,} clusters -> {out}")
    print("\nclusters with the deepest price history (competition + time):")
    for line in summarize(rows, top=args.top):
        print(line)
    return 0


def _apoc_wipe_labels(conn, labels: list[str]) -> int:
    """Delete every node carrying any of `labels` plus ALL relationships incident to
    them. APOC-batched so it never DETACH-DELETEs an edge-heavy node (which OOMs the
    2G heap at millions of edges), and edge-type-agnostic — it drops whatever edges
    touch the nodes, so a new/forgotten edge type (e.g. IN_CATEGORY) can't leave
    nodes stranded. Relationships first, then the edge-free nodes. Returns nodes deleted."""
    pred = " OR ".join(f"n:{lbl}" for lbl in labels)
    conn.query("CALL apoc.periodic.iterate("
               f"'MATCH (n)-[r]-() WHERE {pred} WITH DISTINCT r RETURN r', "
               "'DELETE r', {batchSize: 10000})")
    total = 0
    for lbl in labels:
        rec = conn.query("CALL apoc.periodic.iterate("
                         f"'MATCH (n:{lbl}) RETURN n', 'DELETE n', {{batchSize: 10000}}) "
                         "YIELD total RETURN total")
        total += rec[0]["total"] if rec else 0
    return total


def cmd_wipe_clusters(args) -> int:
    """Delete the cluster catalog — `:ProductoCanonico` + `:Producto` nodes and their
    `:COTIZA` / `:VARIANTE_DE` / `:ESPECIFICA` edges — leaving the source graph
    untouched. Use before re-matching after a canonicalize change. The match checkpoints are
    cleared so a re-run starts fresh."""
    if not args.yes:
        print("refusing to wipe clusters without --yes")
        return 1
    from .graphdb import get_connection

    conn = get_connection()
    try:
        deleted = _apoc_wipe_labels(conn, ["ProductoCanonico", "Producto"])
        print(f"wiped {deleted:,} ProductoCanonico + Producto nodes "
              "(+ COTIZA/VARIANTE_DE/ESPECIFICA edges; source untouched)")
    finally:
        conn.close()
    cleared = 0
    for p in Path("data").glob("match_seg*.checkpoint.json"):
        p.unlink()
        cleared += 1
    if cleared:
        print(f"cleared {cleared} match checkpoint(s) — re-match starts fresh")
    return 0


def cmd_wipe_catalog(args) -> int:
    """Delete ALL legacy catalog data (Category/GenericProduct/Product/Brand) and every
    edge incident to it (RESOLVED_TO/OFFERS/OF_BRAND/VARIANT_OF/PARENT_OF/IN_CATEGORY).
    APOC-batched, relationships first — OOM-safe at millions of edges and edge-type-
    agnostic. The transactional layer and migrations are untouched."""
    if not args.yes:
        print("refusing to wipe the entire catalog without --yes")
        return 1
    from .graphdb import get_connection

    conn = get_connection()
    try:
        deleted = _apoc_wipe_labels(conn, ["GenericProduct", "Product", "Brand", "Category"])
        print(f"catalog wiped: {deleted:,} nodes + all incident edges "
              "(transactional data and migrations untouched)")
    finally:
        conn.close()
    return 0


def cmd_wipe_category(args) -> int:
    if not args.yes:
        print("refusing to wipe without --yes (deletes the category's catalog "
              "nodes from the graph)")
        return 1
    from .graphdb import get_connection

    conn = get_connection()
    try:
        conn.query(
            """
            MATCH (g:GenericProduct {category_id: $cid})
            DETACH DELETE g
            """,
            parameters={"cid": args.category_id},
        )
        conn.query("MATCH (c:Category {category_id: $cid}) DETACH DELETE c",
                   parameters={"cid": args.category_id})
    finally:
        conn.close()
    print(f"wiped category {args.category_id}")
    return 0


# Files under data\ that are inputs reused across runs, not throwaway output:
# the cached spend ranking and the register preview->apply handoff. `clean`
# keeps these unless --all.
_DATA_KEEP = {"profiling.csv", "proposals.json", "fallback_ranking.csv"}
# Regenerable run artifacts `clean` removes: the resolve output triplets plus
# loose run logs/redirects.
_DATA_TEMP_GLOBS = ("*_resoluciones.csv", "*_productos_genericos.csv",
                    "*.checkpoint.json", "*.progress.jsonl",
                    "price_series_*.csv", "*.log", "*.out")


def cmd_clean(args) -> int:
    """Remove regenerable run artifacts from data\\ (resolve CSVs/checkpoints
    and logs). Keeps the cached ranking + proposals unless --all. The graph is
    never touched — use wipe-catalog for that."""
    data = args.dir
    if not data.exists():
        print(f"no {data}\\ directory — nothing to clean")
        return 0
    keep = set() if args.all else _DATA_KEEP
    globs = _DATA_TEMP_GLOBS + (("*.csv", "*.json") if args.all else ())
    victims = sorted({p for g in globs for p in data.glob(g)
                      if p.is_file() and p.name not in keep})
    if not victims:
        print(f"{data}\\ already clean")
        return 0
    freed = sum(p.stat().st_size for p in victims)
    for p in victims:
        if args.dry_run:
            print(f"  would remove {p.name} ({p.stat().st_size/1e6:.2f} MB)")
        else:
            p.unlink()
    verb = "would free" if args.dry_run else "removed"
    print(f"{verb} {len(victims)} file(s), {freed/1e6:.1f} MB"
          + (" (dry run — nothing deleted)" if args.dry_run else ""))
    if keep:
        print(f"kept {', '.join(sorted(keep))} (cached inputs) — --all removes these too")
    return 0


# --- parser --------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    from .pipeline import PIPELINE_STEPS

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
    p.add_argument("--fallback", choices=["unspsc", "none"], default="unspsc",
                   help="--kind item only: link items no curated family matches "
                        "to a coarse GenericProduct keyed by their UNSPSC code "
                        "(default unspsc); 'none' leaves them unresolved")
    p.add_argument("--progress-every", type=int, default=200,
                   help="emit a progress line + checkpoint every N records (default 200)")
    p.add_argument("--resume", action="store_true",
                   help="continue the run recorded in <out>.checkpoint.json "
                        "(must match kind/segment/contains/persist/limit)")
    p.add_argument("--brands", action="store_true",
                   help="add the brand-lexicon tier (categories/brand_lexicon.json) "
                        "after Tier-1 to catch brand-only lines")
    p.add_argument("--tier2", action="store_true",
                   help="add the trained Tier-2 statistical classifier after Tier-1 "
                        "(needs a model from `train-tier2`)")
    p.add_argument("--tier2-model", type=Path, default=None,
                   help="Tier-2 model path (default data\\tier2_model.joblib)")
    p.add_argument("--tier2-threshold", type=float, default=None,
                   help="override the Tier-2 confidence threshold for this run")
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("pipeline",
                       help="run the whole redesign build end-to-end (instance -> "
                            "migrate -> register -> canonicalize -> match -> "
                            "adjudicate -> coherence-check) with step-level resume")
    p.add_argument("--resume", action="store_true",
                   help="continue the run in data\\pipeline.checkpoint.json, "
                        "skipping completed steps")
    p.add_argument("--restart", action="store_true",
                   help="discard the pipeline checkpoint + match/register sub-"
                        "checkpoints and start from the first step")
    p.add_argument("--from-step", default=None, dest="from_step",
                   choices=PIPELINE_STEPS,
                   help="force-run this step and everything after it (ignores the "
                        "done list — use to skip past a benign failure)")
    p.add_argument("--only", default=None, choices=PIPELINE_STEPS,
                   help="run just this one step")
    p.add_argument("--status", action="store_true",
                   help="print the plan (steps done/pending) and exit")
    p.add_argument("--watch", action="store_true",
                   help="like --status but refresh on an interval until complete or Ctrl-C")
    p.add_argument("--interval", type=int, default=15,
                   help="--watch refresh seconds (default 15)")
    p.add_argument("--segment", type=int, default=42,
                   help="UNSPSC segment scope for the build (default 42)")
    p.add_argument("--all-segments", action="store_true",
                   help="run over the whole marketplace (overrides --segment)")
    p.add_argument("--limit", type=_opt_limit, default=None,
                   help="cap inputs per stage; 'all'/0 = no cap (default all)")
    p.add_argument("--rebuild-vocab", action="store_true", dest="rebuild_vocab",
                   help="force the vocabulary step to re-profile + vet and extend "
                        "register/schemas (default: build only when absent, else just "
                        "fill missing schemas — never regenerate what already exists)")
    p.add_argument("--model", default="claude-haiku-4-5",
                   help="canonicalize model (default Haiku 4.5)")
    p.add_argument("--workers", type=int, default=2,
                   help="canonicalize concurrent LLM calls on the Max backend (default 2)")
    p.add_argument("--group-size", type=int, default=25, dest="group_size",
                   help="canonicalize descriptions per LLM call (default 25)")
    p.add_argument("--adjudicate-model", default="claude-sonnet-4-6",
                   dest="adjudicate_model",
                   help="adjudicate model (default Sonnet 4.6)")
    p.add_argument("--data-dir", type=Path, default=Path("data"), dest="data_dir",
                   help="directory for the checkpoint + profile/verdict stores (default data\\)")
    p.set_defaults(func=cmd_pipeline)

    p = sub.add_parser("canonicalize",
                       help="canonicalize (redesign): canonicalize descriptions into profiles "
                            "via Haiku 4.5 batch (scaffold; --from-file/--dry-run runnable)")
    p.add_argument("--from-file", default=None,
                   help="read newline-separated descriptions from a file instead of the graph")
    p.add_argument("--out", type=Path, default=Path("data/profiles.jsonl"),
                   help="profile store (JSONL, keyed by text-hash; the canonicalize cache)")
    p.add_argument("--model", default="claude-haiku-4-5", help="canonicalize model (default Haiku 4.5)")
    p.add_argument("--workers", type=int, default=2,
                   help="concurrent LLM calls on the Max backend (default 2 — keeps "
                        "burst rate low so a long run doesn't trip the usage limit)")
    p.add_argument("--group-size", type=int, default=25, dest="group_size",
                   help="descriptions canonicalized per LLM call (default 25; "
                        "amortizes the per-call overhead)")
    p.add_argument("--segment", type=int, default=None,
                   help="UNSPSC segment scope for the graph read, e.g. 42 (bounds a run)")
    p.add_argument("--limit", type=int, default=None, help="cap inputs (dev runs)")
    p.add_argument("--dry-run", action="store_true",
                   help="dedup only — report distinct/cached counts, no LLM calls")
    p.set_defaults(func=cmd_canonicalize)

    p = sub.add_parser("match",
                       help="match (redesign): cluster the profile store into "
                            "product clusters (offline report; no graph writes yet)")
    p.add_argument("--store", type=Path, default=Path("data/profiles.jsonl"),
                   help="profile store to cluster (default data\\profiles.jsonl)")
    p.add_argument("--attach-partials", action="store_true",
                   help="merge a coarse partial spec into its unique finer cluster "
                        "(default off = keep separate, linked by ESPECIFICA)")
    p.add_argument("--persist", action="store_true",
                   help="WRITE :ProductoCanonico/:Producto/:ESPECIFICA + bind offers via "
                        ":COTIZA (default: offline report only)")
    p.add_argument("--resume", action="store_true",
                   help="resume the COTIZA write from its checkpoint (same scope)")
    p.add_argument("--segment", type=int, default=None,
                   help="UNSPSC segment scope for the offer-price read on --persist")
    p.add_argument("--limit", type=int, default=None,
                   help="cap the offer-price read on --persist (dev/validation runs)")
    p.add_argument("--show", type=int, default=15, help="top clusters to print")
    p.set_defaults(func=cmd_match)

    p = sub.add_parser("adjudicate",
                       help="adjudicate (redesign): Claude adjudicates the match residue "
                            "(model-token conflicts + ambiguous partials)")
    p.add_argument("--store", type=Path, default=Path("data/profiles.jsonl"),
                   help="profile store (default data\\profiles.jsonl)")
    p.add_argument("--verdicts", type=Path, default=Path("data/adjudications.jsonl"),
                   help="verdict store, keyed by case (default data\\adjudications.jsonl)")
    p.add_argument("--model", default="claude-sonnet-4-6",
                   help="adjudicate model (default Sonnet 4.6; Opus for the hardest)")
    p.add_argument("--dry-run", action="store_true",
                   help="report the case count only — no LLM calls, no credits")
    p.set_defaults(func=cmd_adjudicate)

    p = sub.add_parser("coherence-check",
                       help="coherence-check (redesign): run coherence invariants over the canonicalize "
                            "profiles + clusters (structural gate / semantic backlog / health)")
    p.add_argument("--store", type=Path, default=Path("data/profiles.jsonl"),
                   help="profile store to audit (default data\\profiles.jsonl)")
    p.add_argument("--graph", action="store_true",
                   help="also run graph-tier checks against the persisted catalog")
    p.add_argument("--tier", choices=["structural", "semantic", "health", "all"],
                   default="all", help="which tier(s) to report (default all)")
    p.add_argument("--out", type=Path, default=None,
                   help="write the semantic backlog to a CSV")
    p.set_defaults(func=cmd_coherence_check)

    p = sub.add_parser("fallback-report",
                       help="rank the UNSPSC fallback residue (graph): commodity "
                            "codes + candidate categories to register next")
    p.add_argument("--top", type=int, default=20, help="rows to show per ranking (default 20)")
    p.add_argument("--min-count", type=int, default=5,
                   help="min distinct residue descriptions for a head-noun to count as a family")
    p.add_argument("--out", type=Path, default=Path("data/fallback_ranking.csv"),
                   help="residue family ranking CSV (feeds register --from-fallback)")
    p.set_defaults(func=cmd_fallback_report)

    p = sub.add_parser("ambiguity-report",
                       help="rank register overlaps (ambiguous residue items by "
                            "colliding category set; spurious vs bundle)")
    p.add_argument("--top", type=int, default=20, help="colliding sets to show (default 20)")
    p.add_argument("--min-count", type=int, default=3,
                   help="min ambiguous items for a colliding set to be shown (default 3)")
    p.set_defaults(func=cmd_ambiguity_report)

    p = sub.add_parser("generate-schemas", help="LLM strawman drafts from corpus samples")
    p.add_argument("--only", default=None, help="single category_id")
    p.add_argument("--samples", type=int, default=50)
    p.add_argument("--overwrite", action="store_true",
                   help="redraft schemas that already exist on disk (default: "
                        "skip them and only draft the missing ones)")
    p.set_defaults(func=cmd_generate_schemas)

    p = sub.add_parser("build-brand-lexicon",
                       help="LLM-propose brand/trade-name tokens per category and "
                            "merge them into categories/brand_lexicon.json")
    p.add_argument("--only", default=None, help="single category_id")
    p.add_argument("--samples", type=int, default=50,
                   help="corpus descriptions sampled per category (default 50)")
    p.add_argument("--max-per-category", type=int, default=15,
                   help="cap on brand proposals considered per category (default 15)")
    p.add_argument("--overwrite", action="store_true",
                   help="replace the lexicon with the generated brands (default: "
                        "merge, keeping existing curated entries on conflict)")
    p.add_argument("--dry-run", action="store_true",
                   help="print proposed brands without writing the file")
    p.set_defaults(func=cmd_build_brand_lexicon)

    p = sub.add_parser("train-tier2",
                       help="(re)train the Tier-2 statistical classifier from the "
                            "curated resolutions in the graph — the out-of-band "
                            "retrain path; overwrites the model by default")
    p.add_argument("--threshold", type=float, default=0.60,
                   help="confidence below which Tier-2 abstains (default 0.60)")
    p.add_argument("--min-rows", type=int, default=500,
                   help="minimum curated training rows required (default 500)")
    p.add_argument("--eval", action="store_true",
                   help="also report held-out accuracy on a 10%% split")
    p.add_argument("--out", type=Path, default=None,
                   help="model output path (default data\\tier2_model.joblib)")
    p.add_argument("--skip-if-exists", action="store_true",
                   help="no-op if the model file already exists (what the pipeline "
                        "uses so it trains only when there is no .joblib; off here, "
                        "so a direct run always retrains)")
    p.set_defaults(func=cmd_train_tier2)

    p = sub.add_parser("tier2-eval",
                       help="held-out coverage/precision curve for Tier-2 (text-only "
                            "vs +UNSPSC feature); optional gold-set scoring")
    p.add_argument("--gold", type=Path, default=None,
                   help="CSV of human labels (text,true_category[,residue]) to score "
                        "the saved model on — true precision, not agreement-with-Tier-1")
    p.add_argument("--min-rows", type=int, default=500,
                   help="minimum curated rows required (default 500)")
    p.add_argument("--cap", type=int, default=80_000,
                   help="subsample curated rows to this many for a fast eval (default 80k)")
    p.add_argument("--test-size", type=float, default=0.1,
                   help="held-out fraction (default 0.1)")
    p.add_argument("--tier2-model", type=Path, default=None,
                   help="model path for --gold (default data\\tier2_model.joblib)")
    p.add_argument("--tier2-threshold", type=float, default=None,
                   help="override the abstain threshold for the gold eval")
    p.set_defaults(func=cmd_tier2_eval)

    p = sub.add_parser("tier2-label-sample",
                       help="export items + classifier predictions to a CSV for human "
                            "labeling (build a gold set for tier2-eval --gold)")
    p.add_argument("--n", type=int, default=300, help="rows to export (default 300)")
    p.add_argument("--segment", type=int, default=None,
                   help="UNSPSC segment filter, e.g. 42")
    p.add_argument("--residue-only", action="store_true",
                   help="keep only items Tier-1 misses — the rows Tier-2 is judged on")
    p.add_argument("--out", type=Path, default=Path("data/tier2_gold_template.csv"),
                   help="output CSV (default data\\tier2_gold_template.csv)")
    p.add_argument("--tier2-model", type=Path, default=None)
    p.add_argument("--tier2-threshold", type=float, default=None)
    p.set_defaults(func=cmd_tier2_label_sample)

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
    p.add_argument("--from-fallback", action="store_true",
                   help="rank candidates from the UNSPSC fallback residue in the "
                        "graph (target what failed to resolve) instead of the "
                        "whole-corpus profile; needs a prior --persist item run")
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
    p.add_argument("--resume", action="store_true",
                   help="continue an interrupted vet scan from "
                        "data\\register.checkpoint.json (skips groups already "
                        "vetted; restores the categories already chosen)")
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

    p = sub.add_parser("clean",
                       help="remove regenerable run artifacts from data\\ "
                            "(resolve CSVs/checkpoints + logs); keeps cached inputs")
    p.add_argument("--all", action="store_true",
                   help="also remove the cached ranking + proposals (the kept inputs)")
    p.add_argument("--dry-run", action="store_true",
                   help="list what would be removed without deleting")
    p.add_argument("--dir", type=Path, default=Path("data"),
                   help="directory to clean (default data\\)")
    p.set_defaults(func=cmd_clean)

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

    p = sub.add_parser("price-clusters",
                       help="price-clusters (redesign): price series over product clusters "
                            "(per-base-unit, over time + across competition)")
    p.add_argument("--category", default=None, help="scope to a cluster category")
    p.add_argument("--signature", default=None, help="scope to one cluster signature")
    p.add_argument("--csv", type=Path, default=None,
                   help="output path; default data\\price_clusters_<category>.csv")
    p.add_argument("--top", type=int, default=10, help="clusters to summarize")
    p.set_defaults(func=cmd_price_clusters)

    p = sub.add_parser("wipe-catalog",
                       help="delete ALL catalog data from the graph (destructive; "
                            "transactional source data untouched)")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_wipe_catalog)

    p = sub.add_parser("wipe-clusters",
                       help="delete the cluster catalog — :ProductoCanonico + :Producto "
                            "nodes and their COTIZA/VARIANTE_DE/ESPECIFICA edges "
                            "(destructive; source untouched). Use before re-matching.")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(func=cmd_wipe_clusters)

    return parser


def main(argv: list[str] | None = None) -> int:
    _utf8_stdout()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
