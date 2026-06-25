"""BatchedNeo4jCatalog: defer graph writes, flush in bulk via UNWIND.

A fake connection records queries so we can assert that resolution buffers all
mutations (no writes until flush), that flush emits one UNWIND per kind in
dependency order, and that auto-flush fires at batch_size."""

from chilecompra_er.resolve import BatchedNeo4jCatalog, Resolver, SourceRef


class FakeConn:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def query(self, cypher, parameters=None):
        self.calls.append((cypher, parameters or {}))
        return []  # empty graph: load() returns no existing nodes


def _writes(conn):
    return [c for c, _ in conn.calls if "MERGE" in c or "CREATE" in c]


def test_resolution_buffers_no_writes_until_flush():
    conn = FakeConn()
    cat = BatchedNeo4jCatalog(conn, batch_size=10_000)
    r = Resolver(cat)
    src = SourceRef("mp_item_licitacion", "L1", "1", "SONDA FOLEY CH16 2 VIAS")
    rep = r.resolve("SONDA FOLEY CH16 2 VIAS", source=src)
    assert rep.status == "resolved_generic"
    assert _writes(conn) == []          # nothing written yet (only load reads)
    cat.flush()
    joined = "\n".join(c for c, _ in conn.calls)
    assert "MERGE (c:Category" in joined
    assert "MERGE (g:GenericProduct" in joined
    assert "RESOLVED_TO" in joined          # the direct ItemLicitacion->generic edge


def test_flush_orders_dependencies():
    conn = FakeConn()
    cat = BatchedNeo4jCatalog(conn, batch_size=10_000)
    r = Resolver(cat)
    r.resolve("SONDA FOLEY CH16 2 VIAS",
              source=SourceRef("mp_item_licitacion", "L1", "1", "SONDA FOLEY"))
    cat.flush()
    qs = [c for c, _ in conn.calls]
    cat_i = next(i for i, c in enumerate(qs) if "MERGE (c:Category" in c)
    gp_i = next(i for i, c in enumerate(qs) if "MERGE (g:GenericProduct" in c)
    rt_i = next(i for i, c in enumerate(qs) if "RESOLVED_TO" in c)
    # category before generic product before the resolution edge
    assert cat_i < gp_i < rt_i


def test_auto_flush_at_batch_size():
    conn = FakeConn()
    cat = BatchedNeo4jCatalog(conn, batch_size=1)
    r = Resolver(cat)
    r.resolve("SONDA FOLEY CH16 2 VIAS",
              source=SourceRef("mp_item_licitacion", "L1", "1", "SONDA FOLEY"))
    # one resolution buffered hits batch_size=1 -> auto-flush, writes happen
    assert _writes(conn)


def test_unresolved_writes_no_edge():
    conn = FakeConn()
    cat = BatchedNeo4jCatalog(conn, batch_size=10_000)
    r = Resolver(cat)
    rep = r.resolve("xyzzy nada que clasificar",
                    source=SourceRef("mp_item_licitacion", "L1", "1", "xyzzy"))
    assert rep.status == "unresolved"
    cat.flush()
    joined = "\n".join(c for c, _ in conn.calls)
    # an unresolved item leaves no RESOLVED_TO edge, and there is no SourceRecord layer
    assert "RESOLVED_TO" not in joined
    assert "SourceRecord" not in joined


def test_batched_uses_unwind_not_per_row():
    conn = FakeConn()
    cat = BatchedNeo4jCatalog(conn, batch_size=10_000)
    r = Resolver(cat)
    for n in range(5):
        r.resolve("SONDA FOLEY CH16 2 VIAS",
                  source=SourceRef("mp_item_licitacion", "L1", str(n), "SONDA FOLEY"))
    cat.flush()
    # the 5 resolution edges go out in ONE UNWIND, not five writes
    rt_calls = [(c, p) for c, p in conn.calls if "RESOLVED_TO" in c]
    assert len(rt_calls) == 1
    assert len(rt_calls[0][1]["rows"]) == 5
