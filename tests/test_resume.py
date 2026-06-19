"""Partial-save + resume: checkpoint, streaming CSV, catalog reseed."""

import csv

from chilecompra_er.ingest import SourceItem, resolve_items
from chilecompra_er.ingest.export import write_products_csv
from chilecompra_er.ingest.resume import (
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
from chilecompra_er.ingest.runner import ResolutionStats
from chilecompra_er.resolve import InMemoryCatalog, Resolver, SourceRef

FOLEY = [
    "SONDA FOLEY CH16 SILICONA 2 VIAS",
    "SONDA FOLEY CH18 LATEX 2 VIAS",
    "SONDA FOLEY CH16 SILICONA 2 VIAS",  # exact dup of #1 -> same node
    "SONDA FOLEY CH20 SILICONA 3 VIAS",
    "GUANTE NITRILO TALLA M ESTERIL",
    "SONDA FOLEY CH18 LATEX 2 VIAS",     # dup of #2, crosses the resume boundary
]


def _items(texts):
    return [SourceItem(ref=SourceRef("mp", "T", str(i), t), kind="tender_item",
                       raw_text=t)
            for i, t in enumerate(texts)]


def _data_rows(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))[1:]  # drop header


def test_checkpoint_roundtrip(tmp_path):
    cp = Checkpoint(kind="tender", contains=None, segment=42, persist=False,
                    limit=5000, start_skip=0, processed=1400, done=False,
                    stats_dict={"total": 1400}, total=1342833)
    p = tmp_path / "x.checkpoint.json"
    save_checkpoint(p, cp)
    back = load_checkpoint(p)
    assert back.processed == 1400 and back.segment == 42 and back.done is False
    assert back.stats().total == 1400
    assert back.total == 1342833  # deterministic loop size survives the round-trip


def test_checkpoint_total_defaults_none_on_old_json(tmp_path):
    # a checkpoint written before `total` existed loads with total=None
    p = tmp_path / "old.checkpoint.json"
    p.write_text('{"kind": "item", "processed": 10, "done": false, "stats": {}}',
                 encoding="utf-8")
    assert load_checkpoint(p).total is None


def test_checkpoint_mismatch_detection():
    cp = Checkpoint(kind="tender", contains=None, segment=42, persist=False,
                    limit=5000, start_skip=0, processed=10, done=False, stats_dict={})
    assert cp.mismatches(kind="tender", contains=None, segment=42,
                         persist=False, limit=5000) == []
    bad = cp.mismatches(kind="tender", contains=None, segment=12,
                        persist=True, limit=5000)
    assert any("segment" in m for m in bad) and any("persist" in m for m in bad)


def test_stats_dict_roundtrip():
    s = ResolutionStats()
    s.total = 3
    s.by_status["resolved_generic"] += 2
    s.by_status["unresolved"] += 1
    s.nodes_created = 2
    back = ResolutionStats.from_dict(s.to_dict())
    assert back.total == 3 and back.nodes_created == 2
    assert back.by_status["resolved_generic"] == 2


def test_truncate_keeps_n_rows(tmp_path):
    cat = InMemoryCatalog()
    resolver = Resolver(cat)
    path = tmp_path / "r_resoluciones.csv"
    w = StreamingResolutionWriter(path, append=False)
    _, reports = resolve_items(resolver, _items(FOLEY), persist=False,
                               collect_reports=True)
    for r in reports:
        w.write(r)
    w.close()
    assert len(_data_rows(path)) == len(FOLEY)
    truncate_resolutions(path, 2)
    assert len(_data_rows(path)) == 2


def test_streaming_append_has_single_bom(tmp_path):
    cat = InMemoryCatalog()
    resolver = Resolver(cat)
    path = tmp_path / "r_resoluciones.csv"
    rep = resolve_items(resolver, _items(FOLEY[:1]), persist=False,
                        collect_reports=True)[1]
    w = StreamingResolutionWriter(path, append=False)
    w.write(rep[0]); w.close()
    w2 = StreamingResolutionWriter(path, append=True)
    w2.write(rep[0]); w2.close()
    raw = path.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")            # BOM once at the front
    assert raw.count(b"\xef\xbb\xbf") == 1            # not re-inserted on append


def test_writer_flush_after_close_is_noop(tmp_path):
    # the final done=True checkpoint flushes after the finally already closed
    # the writer — must not raise (regression: I/O on closed file).
    w = StreamingResolutionWriter(tmp_path / "r_resoluciones.csv", append=False)
    w.close()
    w.flush()  # no-op, not an error
    w.close()  # idempotent


def test_resume_end_to_end_dedups_across_kill(tmp_path):
    """Process the corpus in two passes (simulating a kill at the midpoint).
    The resumed pass must reseed the catalog and not recreate the duplicate
    product that crosses the boundary."""
    prefix = tmp_path / "run"
    res_csv, prod_csv = resolutions_path(prefix), products_path(prefix)

    # one-shot reference run for the expected totals
    ref_cat = InMemoryCatalog()
    ref_stats, _ = resolve_items(Resolver(ref_cat), _items(FOLEY), persist=False)

    half = len(FOLEY) // 2

    # --- pass 1: first half, then "killed" (state persisted to CSV) ---
    cat1 = InMemoryCatalog()
    w1 = StreamingResolutionWriter(res_csv, append=False)
    stats1, _ = resolve_items(Resolver(cat1), _items(FOLEY[:half]), persist=False,
                              on_report=w1.write)
    w1.close()
    write_products_csv(cat1, prod_csv)
    save_checkpoint(checkpoint_path(prefix), Checkpoint(
        kind="tender", contains=None, segment=None, persist=False, limit=None,
        start_skip=0, processed=stats1.total, done=False, stats_dict=stats1.to_dict()))

    # --- pass 2: resume from the checkpoint ---
    cp = load_checkpoint(checkpoint_path(prefix))
    kept = truncate_resolutions(res_csv, cp.processed)
    assert kept == half
    cat2 = InMemoryCatalog()
    seeded = seed_inmemory_catalog(cat2, prod_csv)
    assert seeded == len(cat1.specs)
    w2 = StreamingResolutionWriter(res_csv, append=True)
    stats2, _ = resolve_items(Resolver(cat2), _items(FOLEY[half:]), persist=False,
                              on_report=w2.write, stats=cp.stats())
    w2.close()

    # cumulative stats match the one-shot run, and no rows were lost/duped
    assert stats2.total == ref_stats.total == len(FOLEY)
    assert len(_data_rows(res_csv)) == len(FOLEY)
    # the boundary-crossing duplicate did not create a second node
    assert stats2.nodes_created == ref_stats.nodes_created
    assert len(cat2.specs) == len(ref_cat.specs)
