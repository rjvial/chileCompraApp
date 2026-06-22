"""Incremental resolve + event-level lineage.

Covers the parts that run without a live graph: the InMemoryCatalog's content_hash
storage and append-only event log (the persist contract every backend mirrors), and
the lineage watermark/run helpers against a fake connection. The Cypher delta/fetch
is exercised by the live smoke test, not here.
"""

from chilecompra_er.resolve import InMemoryCatalog, SourceRef
from chilecompra_er.resolve import lineage


# --- InMemoryCatalog: content_hash + event log -------------------------------

def _src(line, chash):
    return SourceRef("mp", "T-1", str(line), f"text {line}", content_hash=chash)


def test_content_hash_stored_on_resolution_and_event():
    cat = InMemoryCatalog(run_uid="run-1")
    cat.persist_resolution(_src(1, "h1"), "resolved_generic", "gp_x")
    row = cat.resolutions[0]
    assert row["content_hash"] == "h1"
    ev = cat.events[0]
    assert ev["content_hash"] == "h1"
    assert ev["event_kind"] == "new"
    assert ev["run_uid"] == "run-1"


def test_event_per_touch_new_confirm_retarget():
    cat = InMemoryCatalog()
    src = _src(1, "h1")
    cat.persist_resolution(src, "resolved_generic", "gp_x")   # new -> v1
    cat.persist_resolution(src, "resolved_generic", "gp_x")   # confirm (refresh)
    cat.persist_resolution(src, "resolved_generic", "gp_y")   # retarget -> v2

    assert [e["event_kind"] for e in cat.events] == ["new", "confirm", "retarget"]
    assert [e["version"] for e in cat.events] == [1, 1, 2]
    # one event per touch, immutable log
    assert len(cat.events) == 3

    # edge state: v1 retired, v2 current on the new target
    versions = [r for r in cat.resolutions if r["record_key"] == src.record_key]
    assert [r["version"] for r in versions] == [1, 2]
    assert [r["current"] for r in versions] == [False, True]
    assert [r["target_id"] for r in versions] == ["gp_x", "gp_y"]


def test_first_resolved_preserved_last_confirmed_advances():
    cat = InMemoryCatalog()
    src = _src(1, "h1")
    cat.persist_resolution(src, "resolved_generic", "gp_x")
    cat.persist_resolution(src, "resolved_generic", "gp_x")  # refresh in place
    row = cat.resolutions[0]
    assert row["first_resolved_at"] < row["last_confirmed_at"]
    assert row["version"] == 1  # no new version on same-target re-confirm


def test_unresolved_emits_event_but_no_edge():
    cat = InMemoryCatalog(run_uid="run-1")
    cat.persist_resolution(_src(1, "h1"), "unresolved", None,
                           evidence={"unresolved_reason": "unclassified"})
    assert cat.resolutions == []            # no edge for an unresolved record
    assert len(cat.events) == 1
    assert cat.events[0]["event_kind"] == "unresolved"
    assert cat.events[0]["content_hash"] == "h1"
    # the WHY of an unresolved touch is reconstructable from the event
    assert cat.events[0]["evidence"]["unresolved_reason"] == "unclassified"


def test_event_carries_self_contained_evidence():
    # each event stores the full evidence (what + why), so a past resolution is
    # reconstructable even after the edge is later overwritten
    cat = InMemoryCatalog()
    src = _src(1, "h1")
    ev1 = {"normalized": "sonda foley 16", "attribute_values": {"calibre": "16Fr"},
           "classifier": {"tier": "tier1"}}
    cat.persist_resolution(src, "resolved_generic", "gp_x", evidence=ev1)
    # a later confirm with DIFFERENT evidence overwrites the edge but not the events
    ev2 = {"normalized": "cateter foley n16", "attribute_values": {"calibre": "16Fr"}}
    cat.persist_resolution(src, "resolved_generic", "gp_x", evidence=ev2)

    assert cat.events[0]["evidence"]["normalized"] == "sonda foley 16"
    assert cat.events[0]["evidence"]["attribute_values"] == {"calibre": "16Fr"}
    assert cat.events[1]["evidence"]["normalized"] == "cateter foley n16"
    # the edge kept only the latest evidence; the event log kept both
    assert cat.resolutions[0]["evidence"]["normalized"] == "cateter foley n16"


# --- lineage: watermark + run provenance -------------------------------------

class FakeConn:
    """Records queries; returns the first canned result whose key substring is in
    the cypher (else [])."""

    def __init__(self, responses=None):
        self.calls: list[tuple[str, dict]] = []
        self.responses = responses or []

    def query(self, cypher, parameters=None):
        self.calls.append((cypher, parameters or {}))
        for substr, rows in self.responses:
            if substr in cypher:
                return rows
        return []


def test_load_resolved_runs_empty_and_populated():
    assert lineage.load_resolved_runs(FakeConn()) == set()
    conn = FakeConn([("ResolveState", [{"runs": ["r1", "r2"]}])])
    assert lineage.load_resolved_runs(conn) == {"r1", "r2"}


def test_compute_new_runs_subtracts_resolved():
    conn = FakeConn([("RETURN DISTINCT",
                      [{"run_id": "r1"}, {"run_id": "r2"}, {"run_id": "r3"}])])
    assert lineage.compute_new_runs(conn, {"r1"}) == {"r2", "r3"}


def test_record_resolved_runs_merges_state():
    conn = FakeConn()
    lineage.record_resolved_runs(conn, {"r2", "r1"})
    cypher, params = conn.calls[-1]
    assert "MERGE (st:ResolveState" in cypher
    assert params["runs"] == ["r1", "r2"]  # sorted, deterministic


def test_start_and_finish_run():
    conn = FakeConn()
    uid = lineage.start_run(conn, "incremental", {"r1"})
    assert uid.startswith("resolve_incremental-")
    create_cypher, create_params = conn.calls[-1]
    assert "CREATE (run:ResolveRun" in create_cypher
    assert create_params["new_runs"] == ["r1"]

    lineage.finish_run(conn, uid, {"processed": 5})
    finish_cypher, finish_params = conn.calls[-1]
    assert "MATCH (run:ResolveRun" in finish_cypher
    assert finish_params["counts"] == {"processed": 5}
    assert finish_params["uid"] == uid
