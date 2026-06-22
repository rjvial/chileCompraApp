"""Assignment service — the single write path into the catalog (design §4, §7).

Responsibilities, in one code path only:
  - write-time validation against the schema (Neo4j has no CHECK constraints);
  - home-node lookup: the generic product whose present identity attributes
    exactly match the extraction, with the same absences (equally specific);
  - create-if-absent, computing the :PARENT_OF parent (most-specific existing
    same-category node whose present attributes are a strict matching subset);
  - re-pointing existing children whose best parent the new node becomes;
  - derived flags specificity / is_complete at write time;
  - :SourceRecord upsert + versioned :RESOLVED_TO edges (re-resolution adds a
    version, never overwrites).

The planning logic is pure (plan_assignment over a snapshot of the category's
nodes) so it is unit-testable offline; catalogs (in-memory / Neo4j) load the
snapshot and apply the plan.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from ..categories.schema import CategorySchema

GENERIC_LABEL = "GenericProduct"
PRODUCT_LABEL = "Product"

# Node properties that are bookkeeping, not attribute values.
_RESERVED_PROPS = {"id", "category_id", "identity_key", "specificity", "is_complete", "created_at"}


def identity_key(category_id: str, identity_values: dict[str, str]) -> str:
    """Canonical serialization of present identity values — equality is exact
    match on this key (same values, same absences). Includes the category so a
    single-property uniqueness constraint covers the pair."""
    parts = [f"{k}={identity_values[k]}" for k in sorted(identity_values)]
    return "|".join([category_id, *parts])


def node_id_for(key: str) -> str:
    return "gp_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def branded_identity_key(generic_id: str, brand_id: str) -> str:
    """A :Product is the pairing Brand × GenericProduct; its identity is the
    generic node id plus the brand id."""
    return f"{generic_id}|brand={brand_id}"


def branded_product_id(generic_id: str, brand_id: str) -> str:
    """Stable :Product id for the (generic, brand) pairing — every offer of the
    same brand for the same generic collapses onto this one node."""
    key = branded_identity_key(generic_id, brand_id)
    return "pr_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def subsumes(parent_values: dict[str, str], child_values: dict[str, str]) -> bool:
    """P may parent C iff every identity attribute of P equals C's and P has
    strictly fewer present attributes (design §4 subsumption)."""
    return len(parent_values) < len(child_values) and all(
        child_values.get(k) == v for k, v in parent_values.items()
    )


@dataclass
class NodeView:
    """Snapshot of an existing catalog node, as the planner sees it."""
    id: str
    identity_values: dict[str, str]
    parent_id: str | None = None


@dataclass
class NodeSpec:
    """A node to be created."""
    id: str
    category_id: str
    identity_key: str
    properties: dict[str, str]  # identity + descriptive values
    specificity: int
    is_complete: bool


@dataclass
class AssignmentPlan:
    home_id: str
    created: NodeSpec | None = None
    parent_id: str | None = None                      # parent of the created node
    repoint: list[tuple[str, str]] = field(default_factory=list)  # (child_id, new_parent_id)


def _most_specific(candidates: list[NodeView]) -> NodeView:
    # Ties (incomparable equally-specific parents) are possible, e.g. {16Fr}
    # and {silicona} both subsuming {16Fr, silicona}. The tree keeps one
    # parent; pick deterministically. Rollup reads remain correct because they
    # traverse PARENT_OF from an explicitly chosen anchor node.
    return max(candidates, key=lambda n: (len(n.identity_values), n.id))


def plan_assignment(
    schema: CategorySchema,
    values: dict[str, str],
    existing: dict[str, NodeView],
) -> AssignmentPlan:
    """Pure planning: extraction values + category snapshot -> plan.

    `values` may include descriptive attributes; equality uses identity only.
    A description with zero extracted identity attributes still anchors — on
    the category-root partial node (specificity 0), the honest representation
    of "a Foley catheter, nothing else known".
    """
    schema.validate_values(values)  # the write-path gate
    id_names = set(schema.identity_names)
    identity_values = {k: v for k, v in values.items() if k in id_names}

    key = identity_key(schema.category_id, identity_values)
    nid = node_id_for(key)

    if nid in existing:
        return AssignmentPlan(home_id=nid)

    spec = NodeSpec(
        id=nid,
        category_id=schema.category_id,
        identity_key=key,
        properties=dict(values),
        specificity=len(identity_values),
        is_complete=len(identity_values) == len(id_names),
    )

    parents = [n for n in existing.values() if subsumes(n.identity_values, identity_values)]
    parent_id = _most_specific(parents).id if parents else None

    new_view = NodeView(id=nid, identity_values=identity_values, parent_id=parent_id)
    pool = dict(existing)
    pool[nid] = new_view

    repoint: list[tuple[str, str]] = []
    for child in existing.values():
        if not subsumes(identity_values, child.identity_values):
            continue
        # Child's parent must be the most specific node that subsumes it,
        # now that the new node exists.
        best = _most_specific(
            [n for n in pool.values() if subsumes(n.identity_values, child.identity_values)]
        )
        if best.id == nid and child.parent_id != nid:
            repoint.append((child.id, nid))

    return AssignmentPlan(home_id=nid, created=spec, parent_id=parent_id, repoint=repoint)


@dataclass(frozen=True)
class SourceRef:
    """Reference to an original record: identifiers plus the raw text, which is
    stored verbatim on the :SourceRecord node."""
    source: str          # e.g. "mercado_publico"
    tender_id: str
    line_no: str
    raw_text: str        # stored verbatim on the SourceRecord node
    content_hash: str | None = None  # composite hash of the source content (item
                                     # + its offers); the incremental skip key
                                     # stored on :SourceRecord. None for offers.

    @property
    def record_key(self) -> str:
        return f"{self.source}|{self.tender_id}|{self.line_no}"


STATUS_GENERIC = "resolved_generic"
STATUS_PRODUCT = "resolved_product"
STATUS_UNRESOLVED = "unresolved"


class InMemoryCatalog:
    """Same contract as Neo4jCatalog, for tests and dry runs."""

    def __init__(self, run_uid: str | None = None):
        self.nodes: dict[str, NodeView] = {}
        self.specs: dict[str, NodeSpec] = {}
        self.resolutions: list[dict] = []
        self.products: dict[str, dict] = {}   # branded Product (generic × brand)
        self.brands: dict[str, dict] = {}     # brand_id -> {id, name}
        self.offers: list[dict] = []          # one per (:Oferta)-[:OFFERS]->(:Product)
        self.has_record: set[tuple[str, str]] = set()  # (record_key, item_key) edges
        self.events: list[dict] = []   # append-only lineage, one per persist touch
        self.run_uid = run_uid
        self._tick = 0                 # monotonic stamp (deterministic for tests)

    def merge_branded_product(self, generic_id: str, brand_id: str,
                              brand_name: str, descriptive: dict) -> str:
        """Upsert the Brand node and the (generic × brand) Product pairing
        (descriptive attrs ON CREATE — first offer wins). Returns the Product id."""
        self.brands.setdefault(brand_id, {"id": brand_id, "name": brand_name})
        pid = branded_product_id(generic_id, brand_id)
        if pid not in self.products:
            self.products[pid] = {
                "id": pid, "generic_id": generic_id, "brand_id": brand_id,
                "identity_key": branded_identity_key(generic_id, brand_id),
                **descriptive}
        return pid

    def link_offer(self, oferta_id, product_id: str, price_props: dict) -> None:
        """One (:Oferta)-[:OFFERS {price…}]->(:Product) edge per bid."""
        self.offers.append({"oferta_id": oferta_id, "product_id": product_id,
                            "run_uid": self.run_uid, **price_props})

    def flush(self) -> None:
        """No-op: in-memory writes are already applied. Present so callers can
        flush any catalog uniformly (see BatchedNeo4jCatalog)."""

    def load(self, category_id: str, schema: CategorySchema) -> dict[str, NodeView]:
        return {nid: n for nid, n in self.nodes.items() if self.specs[nid].category_id == category_id}

    def apply(self, plan: AssignmentPlan, schema: CategorySchema) -> None:
        if plan.created is None:
            return
        id_names = set(schema.identity_names)
        view = NodeView(
            id=plan.created.id,
            identity_values={k: v for k, v in plan.created.properties.items() if k in id_names},
            parent_id=plan.parent_id,
        )
        self.nodes[plan.created.id] = view
        self.specs[plan.created.id] = plan.created
        for child_id, new_parent in plan.repoint:
            self.nodes[child_id].parent_id = new_parent

    def persist_resolution(self, source: SourceRef, status: str, target_id: str | None,
                           target_label: str = GENERIC_LABEL, **edge_props) -> None:
        # Refresh-in-place: a re-resolution to the SAME (target, label) updates the
        # current edge's provenance instead of appending a redundant version; a new
        # version is added only when the resolution actually changes the target.
        # Every touch also appends an immutable lineage event (event-level audit).
        self._tick += 1
        ts = self._tick
        content_hash = source.content_hash
        # explicit (:ItemLicitacion)-[:HAS_RECORD]->(:SourceRecord) edge
        self.has_record.add((source.record_key, f"{source.tender_id}|{source.line_no}"))
        cur = next((r for r in self.resolutions
                    if r["record_key"] == source.record_key and r["current"]), None)
        same = (cur is not None and target_id is not None
                and cur.get("target_id") == target_id
                and cur.get("target_label") == target_label)
        if target_id is None:
            event_kind = "unresolved"
        elif cur is None:
            event_kind = "new"
        elif same:
            event_kind = "confirm"
        else:
            event_kind = "retarget"

        if target_id is None:
            # Unresolved: no edge row (mirrors the graph's "no RESOLVED_TO"), only
            # the lineage event below.
            version = 0
        elif same:
            cur.update({"status": status, "last_confirmed_at": ts,
                        "content_hash": content_hash, **edge_props})
            version = cur["version"]
        else:
            version = 1 + sum(1 for r in self.resolutions if r["record_key"] == source.record_key)
            for r in self.resolutions:
                if r["record_key"] == source.record_key:
                    r["current"] = False
            self.resolutions.append({
                "record_key": source.record_key,
                "status": status,
                "target_id": target_id,
                "target_label": target_label,
                "version": version,
                "current": True,
                "first_resolved_at": ts,
                "last_confirmed_at": ts,
                "content_hash": content_hash,
                **edge_props,
            })

        self.events.append({
            "ts": ts, "record_key": source.record_key, "target_id": target_id,
            "target_label": target_label, "status": status, "event_kind": event_kind,
            "version": version, "content_hash": content_hash, "run_uid": self.run_uid,
            "evidence": edge_props.get("evidence"),
            "extractor_version": edge_props.get("extractor_version"),
            "schema_version": edge_props.get("schema_version"),
            "normalizer_version": edge_props.get("normalizer_version"),
        })


class Neo4jCatalog:
    """Catalog backed by the Neo4j property graph (funcionesNeo4j connection).

    Category snapshots are cached in memory and maintained by apply() — one
    graph read per category per process instead of one per record. Safe while
    this process is the only writer (the batch runner); drop the instance to
    refresh."""

    def __init__(self, conn, run_uid: str | None = None):
        self.conn = conn
        self.run_uid = run_uid   # :ResolveRun this persist run's events link to
        self._categories_synced: set[str] = set()
        self._snapshots: dict[str, dict[str, NodeView]] = {}
        # Raw preloaded GenericProduct rows grouped by category (filled by
        # preload()); converted to NodeViews lazily in load() once the category's
        # schema is known. None until preload() runs.
        self._preloaded: dict[str, list] | None = None

    def preload(self) -> int:
        """Load the WHOLE curated catalog (all GenericProducts + parent edges) in
        ONE query, instead of one query per category in load(). Over a high-
        latency link this turns hundreds/thousands of per-category round-trips
        into a single one — the dominant cost of a remote `resolve` (see the
        latency analysis). UNSPSC fallback buckets are excluded: they are single
        attribute-less roots with nothing to dedup against, so load() serves them
        from memory without ever touching the graph. Returns the rows preloaded."""
        rows = self.conn.query(
            """
            MATCH (g:GenericProduct)
            WHERE NOT g.category_id STARTS WITH 'unspsc_'
            OPTIONAL MATCH (p:GenericProduct)-[:PARENT_OF]->(g)
            RETURN g{.*} AS props, p.id AS parent_id
            """
        )
        grouped: dict[str, list] = {}
        for rec in rows:
            grouped.setdefault(rec["props"]["category_id"], []).append(rec)
        self._preloaded = grouped
        return len(rows)

    def _snapshot_from_rows(self, rows: list, id_names: set) -> dict[str, NodeView]:
        out: dict[str, NodeView] = {}
        for rec in rows:
            props = rec["props"]
            out[props["id"]] = NodeView(
                id=props["id"],
                identity_values={k: v for k, v in props.items()
                                 if k in id_names and v is not None},
                parent_id=rec["parent_id"],
            )
        return out

    def ensure_category(self, schema: CategorySchema) -> None:
        if schema.category_id in self._categories_synced:
            return
        self.conn.query(
            """
            MERGE (c:Category {category_id: $cid})
            SET c.name = $name,
                c.base_unit = $base_unit,
                c.schema_version = $version,
                c.attribute_defs = $defs
            """,
            parameters={
                "cid": schema.category_id,
                "name": schema.name,
                "base_unit": schema.base_unit,
                "version": schema.schema_version,
                "defs": schema.raw_json,
            },
        )
        self._categories_synced.add(schema.category_id)

    def load(self, category_id: str, schema: CategorySchema) -> dict[str, NodeView]:
        if category_id in self._snapshots:
            return self._snapshots[category_id]
        # Fallback buckets are attribute-less single roots: nothing to dedup, so
        # never read the graph — serve (and grow) them purely in memory. The
        # idempotent MERGE in apply() keeps the root correct across resumes.
        if schema.status == "fallback":
            return self._snapshots.setdefault(category_id, {})
        id_names = set(schema.identity_names)
        # Prefer the bulk preload (one round-trip for the whole catalog) over a
        # per-category query; pop the raw rows once converted so we don't hold
        # both the raw preload and the NodeView snapshot in memory.
        if self._preloaded is not None:
            out = self._snapshot_from_rows(self._preloaded.pop(category_id, []), id_names)
        else:
            records = self.conn.query(
                """
                MATCH (g:GenericProduct {category_id: $cid})
                OPTIONAL MATCH (p:GenericProduct)-[:PARENT_OF]->(g)
                RETURN g{.*} AS props, p.id AS parent_id
                """,
                parameters={"cid": category_id},
            )
            out = self._snapshot_from_rows(records, id_names)
        self._snapshots[category_id] = out
        return out

    def apply(self, plan: AssignmentPlan, schema: CategorySchema) -> None:
        if plan.created is None:
            return
        self.ensure_category(schema)
        # keep the cached snapshot in step with what is written below
        snapshot = self._snapshots.setdefault(schema.category_id, {})
        id_names = set(schema.identity_names)
        snapshot[plan.created.id] = NodeView(
            id=plan.created.id,
            identity_values={k: v for k, v in plan.created.properties.items()
                             if k in id_names},
            parent_id=plan.parent_id,
        )
        for child_id, new_parent in plan.repoint:
            if child_id in snapshot:
                snapshot[child_id].parent_id = new_parent
        spec = plan.created
        clean_props = {k: v for k, v in spec.properties.items() if k not in _RESERVED_PROPS}
        self.conn.query(
            """
            MERGE (g:GenericProduct {identity_key: $key})
            ON CREATE SET g.id = $id, g.category_id = $cid, g.created_at = datetime()
            SET g += $props, g.specificity = $spec, g.is_complete = $complete
            WITH g
            MATCH (c:Category {category_id: $cid})
            MERGE (g)-[:IN_CATEGORY]->(c)
            """,
            parameters={
                "key": spec.identity_key,
                "id": spec.id,
                "cid": spec.category_id,
                "props": clean_props,
                "spec": spec.specificity,
                "complete": spec.is_complete,
            },
        )
        if plan.parent_id:
            self.conn.query(
                """
                MATCH (p:GenericProduct {id: $pid}), (ch:GenericProduct {id: $chid})
                MERGE (p)-[:PARENT_OF]->(ch)
                """,
                parameters={"pid": plan.parent_id, "chid": spec.id},
            )
        for child_id, new_parent in plan.repoint:
            self.conn.query(
                """
                MATCH (ch:GenericProduct {id: $chid})
                OPTIONAL MATCH (:GenericProduct)-[r:PARENT_OF]->(ch)
                DELETE r
                WITH DISTINCT ch
                MATCH (p:GenericProduct {id: $pid})
                MERGE (p)-[:PARENT_OF]->(ch)
                """,
                parameters={"chid": child_id, "pid": new_parent},
            )

    def merge_branded_product(self, generic_id: str, brand_id: str,
                              brand_name: str, descriptive: dict) -> str:
        """Upsert the :Brand node and the deduped :Product = Brand × GenericProduct
        pairing (descriptive attrs ON CREATE — first offer wins), linked
        VARIANT_OF the generic and OF_BRAND the brand. Returns the Product id."""
        pid = branded_product_id(generic_id, brand_id)
        self.conn.query(
            """
            MERGE (b:Brand {id: $brand_id}) ON CREATE SET b.name = $brand_name
            MERGE (p:Product {id: $pid})
              ON CREATE SET p.identity_key = $ik, p.generic_id = $gid, p += $descriptive
            WITH p, b
            MATCH (g:GenericProduct {id: $gid})
            MERGE (p)-[:VARIANT_OF]->(g)
            MERGE (p)-[:OF_BRAND]->(b)
            """,
            parameters={"pid": pid, "gid": generic_id, "brand_id": brand_id,
                        "brand_name": brand_name, "descriptive": descriptive,
                        "ik": branded_identity_key(generic_id, brand_id)},
        )
        return pid

    def link_offer(self, oferta_id, product_id: str, price_props: dict) -> None:
        """One explicit (:Oferta)-[:OFFERS {price…}]->(:Product) edge per bid —
        the price lives on the edge, not on the (price-free) Product."""
        self.conn.query(
            """
            MATCH (o:Oferta {id_oferta: $oid})
            MATCH (p:Product {id: $pid})
            MERGE (o)-[r:OFFERS]->(p)
            SET r += $price, r.resolved_at = datetime(), r.run_uid = $run
            """,
            parameters={"oid": oferta_id, "pid": product_id, "price": price_props,
                        "run": self.run_uid},
        )

    def persist_resolution(self, source: SourceRef, status: str, target_id: str | None,
                           target_label: str = GENERIC_LABEL, **edge_props) -> None:
        """Upsert :SourceRecord (carrying the incremental content_hash), refresh
        or version the :RESOLVED_TO edge, and append an immutable :ResolutionEvent
        for the touch. Unresolved records get the status + an 'unresolved' event,
        no edge — still total and explicit, nothing silently dropped."""
        params = {
            "rk": source.record_key, "src": source.source, "tid": source.tender_id,
            "line": source.line_no, "text": source.raw_text, "status": status,
            "chash": source.content_hash, "run_uid": self.run_uid,
            "evidence": json.dumps(edge_props.get("evidence", {}), ensure_ascii=False),
            "extractor_version": edge_props.get("extractor_version", ""),
            "schema_version": edge_props.get("schema_version", ""),
            "normalizer_version": edge_props.get("normalizer_version", ""),
        }
        self.conn.query(
            """
            MERGE (s:SourceRecord {record_key: $rk})
            ON CREATE SET s.source = $src, s.tender_id = $tid, s.line_no = $line
            SET s.raw_text = $text, s.resolution_status = $status, s.content_hash = $chash
            WITH s
            OPTIONAL MATCH (i:ItemLicitacion {id_licitacion: $tid, id_item: toInteger($line)})
            FOREACH (ii IN CASE WHEN i IS NOT NULL THEN [i] ELSE [] END |
                MERGE (ii)-[:HAS_RECORD]->(s))
            """,
            parameters=params,
        )
        if target_id is None:
            # Unresolved touch: no edge, but still an auditable lineage event.
            self.conn.query(
                """
                MATCH (s:SourceRecord {record_key: $rk})
                OPTIONAL MATCH (run:ResolveRun {run_uid: $run_uid})
                CREATE (s)-[:HAS_EVENT]->(ev:ResolutionEvent {
                    ts: datetime(), record_key: $rk, status: $status,
                    event_kind: 'unresolved', content_hash: $chash, evidence: $evidence,
                    extractor_version: $extractor_version,
                    schema_version: $schema_version, normalizer_version: $normalizer_version})
                FOREACH (r IN CASE WHEN run IS NOT NULL THEN [run] ELSE [] END |
                    CREATE (r)-[:EMITTED]->(ev))
                """,
                parameters=params,
            )
            return
        if target_label not in (GENERIC_LABEL, PRODUCT_LABEL):
            raise ValueError(f"invalid resolution target label {target_label!r}")
        # Single round trip: refresh-in-place when the current edge already points
        # at this target (no new version), else retire + version; then emit the event.
        params["target"] = target_id
        params["label"] = target_label
        self.conn.query(
            f"""
            MATCH (s:SourceRecord {{record_key: $rk}})
            MATCH (t:{target_label} {{id: $target}})
            OPTIONAL MATCH (run:ResolveRun {{run_uid: $run_uid}})
            OPTIONAL MATCH (s)-[cur:RESOLVED_TO {{current: true}}]->(curt)
            WITH s, t, run, cur, (curt IS NULL) AS isnew,
                 (curt IS NOT NULL AND curt.id = t.id) AS same,
                 [(s)-[e:RESOLVED_TO]->() | e.version] AS versions
            WITH s, t, run, cur, isnew, same,
                 reduce(m = 0, v IN versions | CASE WHEN v > m THEN v ELSE m END) AS maxv
            FOREACH (r IN CASE WHEN same THEN [cur] ELSE [] END |
                SET r.resolved_at = datetime(), r.last_confirmed_at = datetime(),
                    r.evidence = $evidence, r.extractor_version = $extractor_version,
                    r.schema_version = $schema_version, r.normalizer_version = $normalizer_version)
            FOREACH (r IN CASE WHEN (NOT same) AND cur IS NOT NULL THEN [cur] ELSE [] END |
                SET r.current = false)
            FOREACH (_x IN CASE WHEN same THEN [] ELSE [1] END |
                CREATE (s)-[:RESOLVED_TO {{
                    current: true, version: maxv + 1, resolved_at: datetime(),
                    first_resolved_at: datetime(), last_confirmed_at: datetime(),
                    evidence: $evidence, extractor_version: $extractor_version,
                    schema_version: $schema_version, normalizer_version: $normalizer_version
                }}]->(t))
            WITH s, t, run, same, isnew,
                 CASE WHEN same THEN maxv ELSE maxv + 1 END AS version
            CREATE (s)-[:HAS_EVENT]->(ev:ResolutionEvent {{
                ts: datetime(), record_key: $rk, target_id: $target, target_label: $label,
                status: $status, version: version, content_hash: $chash, evidence: $evidence,
                event_kind: CASE WHEN isnew THEN 'new' WHEN same THEN 'confirm' ELSE 'retarget' END,
                extractor_version: $extractor_version, schema_version: $schema_version,
                normalizer_version: $normalizer_version}})
            FOREACH (r IN CASE WHEN run IS NOT NULL THEN [run] ELSE [] END |
                CREATE (r)-[:EMITTED]->(ev))
            """,
            parameters=params,
        )

    def flush(self) -> None:
        """No-op: each write is its own round trip. See BatchedNeo4jCatalog for
        the buffered variant used by large persist runs."""


class BatchedNeo4jCatalog(Neo4jCatalog):
    """Neo4jCatalog that buffers graph mutations and flushes them in bulk via
    UNWIND — for large `resolve --persist` runs against a remote graph, where
    one round trip per node/edge (the base class) caps throughput at a few
    writes/sec on network latency alone.

    Planning stays correct because the in-memory category snapshots are updated
    synchronously (same as the base class); only the actual graph writes are
    deferred. Buffers flush in dependency order (categories → generic products →
    parent/repoint edges → products → resolutions) when the resolution buffer
    reaches `batch_size`, and on any explicit flush() (the runner calls it at
    each checkpoint and at the end). MERGE keys make every flush idempotent, so
    a kill + --resume re-merges safely."""

    def __init__(self, conn, batch_size: int = 10_000, run_uid: str | None = None):
        super().__init__(conn, run_uid=run_uid)
        self.batch_size = batch_size
        self._cat_buf: list[dict] = []
        self._gp_buf: list[dict] = []
        self._parent_buf: list[dict] = []
        self._repoint_buf: list[dict] = []
        self._branded_buf: list[dict] = []   # Brand + (generic × brand) Product
        self._offers_buf: list[dict] = []     # (:Oferta)-[:OFFERS]->(:Product)
        self._res_buf: list[dict] = []

    def ensure_category(self, schema: CategorySchema) -> None:
        if schema.category_id in self._categories_synced:
            return
        self._cat_buf.append({"cid": schema.category_id, "name": schema.name,
                              "base_unit": schema.base_unit,
                              "version": schema.schema_version, "defs": schema.raw_json})
        self._categories_synced.add(schema.category_id)

    def apply(self, plan: AssignmentPlan, schema: CategorySchema) -> None:
        if plan.created is None:
            return
        self.ensure_category(schema)
        # keep the in-memory snapshot current so planning sees buffered creates
        snapshot = self._snapshots.setdefault(schema.category_id, {})
        id_names = set(schema.identity_names)
        snapshot[plan.created.id] = NodeView(
            id=plan.created.id,
            identity_values={k: v for k, v in plan.created.properties.items()
                             if k in id_names},
            parent_id=plan.parent_id)
        for child_id, new_parent in plan.repoint:
            if child_id in snapshot:
                snapshot[child_id].parent_id = new_parent
        spec = plan.created
        self._gp_buf.append({
            "key": spec.identity_key, "id": spec.id, "cid": spec.category_id,
            "props": {k: v for k, v in spec.properties.items()
                      if k not in _RESERVED_PROPS},
            "spec": spec.specificity, "complete": spec.is_complete})
        if plan.parent_id:
            self._parent_buf.append({"pid": plan.parent_id, "chid": spec.id})
        for child_id, new_parent in plan.repoint:
            self._repoint_buf.append({"chid": child_id, "pid": new_parent})

    def merge_branded_product(self, generic_id: str, brand_id: str,
                              brand_name: str, descriptive: dict) -> str:
        pid = branded_product_id(generic_id, brand_id)
        self._branded_buf.append({
            "pid": pid, "gid": generic_id, "brand_id": brand_id,
            "brand_name": brand_name, "descriptive": descriptive,
            "ik": branded_identity_key(generic_id, brand_id)})
        return pid

    def link_offer(self, oferta_id, product_id: str, price_props: dict) -> None:
        self._offers_buf.append({"oid": oferta_id, "pid": product_id, "price": price_props})

    def persist_resolution(self, source: SourceRef, status: str, target_id: str | None,
                           target_label: str = GENERIC_LABEL, **edge_props) -> None:
        if target_id is not None and target_label not in (GENERIC_LABEL, PRODUCT_LABEL):
            raise ValueError(f"invalid resolution target label {target_label!r}")
        self._res_buf.append({
            "rk": source.record_key, "src": source.source, "tid": source.tender_id,
            "line": source.line_no, "text": source.raw_text, "status": status,
            "target": target_id, "label": target_label, "chash": source.content_hash,
            "evidence": json.dumps(edge_props.get("evidence", {}), ensure_ascii=False),
            "extractor": edge_props.get("extractor_version", ""),
            "schema": edge_props.get("schema_version", ""),
            "norm": edge_props.get("normalizer_version", "")})
        if len(self._res_buf) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if self._cat_buf:
            self.conn.query(
                """
                UNWIND $rows AS row
                MERGE (c:Category {category_id: row.cid})
                SET c.name = row.name, c.base_unit = row.base_unit,
                    c.schema_version = row.version, c.attribute_defs = row.defs
                """, parameters={"rows": self._cat_buf})
            self._cat_buf = []
        if self._gp_buf:
            self.conn.query(
                """
                UNWIND $rows AS row
                MERGE (g:GenericProduct {identity_key: row.key})
                  ON CREATE SET g.id = row.id, g.category_id = row.cid, g.created_at = datetime()
                SET g += row.props, g.specificity = row.spec, g.is_complete = row.complete
                WITH g, row
                MATCH (c:Category {category_id: row.cid})
                MERGE (g)-[:IN_CATEGORY]->(c)
                """, parameters={"rows": self._gp_buf})
            self._gp_buf = []
        if self._parent_buf:
            self.conn.query(
                """
                UNWIND $rows AS row
                MATCH (p:GenericProduct {id: row.pid}), (ch:GenericProduct {id: row.chid})
                MERGE (p)-[:PARENT_OF]->(ch)
                """, parameters={"rows": self._parent_buf})
            self._parent_buf = []
        if self._repoint_buf:
            self.conn.query(
                """
                UNWIND $rows AS row
                MATCH (ch:GenericProduct {id: row.chid})
                OPTIONAL MATCH (:GenericProduct)-[r:PARENT_OF]->(ch)
                DELETE r
                WITH DISTINCT ch, row
                MATCH (p:GenericProduct {id: row.pid})
                MERGE (p)-[:PARENT_OF]->(ch)
                """, parameters={"rows": self._repoint_buf})
            self._repoint_buf = []
        if self._branded_buf:
            # Dedup by Product id (many offers of one brand share the node); MERGE is
            # idempotent regardless, but fewer rows = fewer locks. ON CREATE descriptive
            # = first occurrence wins.
            rows = list({r["pid"]: r for r in self._branded_buf}.values())
            self.conn.query(
                """
                UNWIND $rows AS row
                MERGE (b:Brand {id: row.brand_id}) ON CREATE SET b.name = row.brand_name
                MERGE (p:Product {id: row.pid})
                  ON CREATE SET p.identity_key = row.ik, p.generic_id = row.gid, p += row.descriptive
                WITH p, b, row
                MATCH (g:GenericProduct {id: row.gid})
                MERGE (p)-[:VARIANT_OF]->(g)
                MERGE (p)-[:OF_BRAND]->(b)
                """, parameters={"rows": rows})
            self._branded_buf = []
        if self._offers_buf:
            self.conn.query(
                """
                UNWIND $rows AS row
                MATCH (o:Oferta {id_oferta: row.oid})
                MATCH (p:Product {id: row.pid})
                MERGE (o)-[r:OFFERS]->(p)
                SET r += row.price, r.resolved_at = datetime(), r.run_uid = $run
                """, parameters={"rows": self._offers_buf, "run": self.run_uid})
            self._offers_buf = []
        if self._res_buf:
            self._flush_resolutions(self._res_buf)
            self._res_buf = []

    def _flush_resolutions(self, rows: list[dict]) -> None:
        # Collapse duplicate source rows (same record_key) to their last occurrence
        # before writing. Within a run such duplicates carry the SAME target (a
        # record resolves deterministically once); UNWIND has no reliable
        # read-your-writes, so two identical rows would otherwise each append an
        # edge. Last wins — same "latest run" semantics as the single-write path.
        rows = list({r["rk"]: r for r in rows}.values())
        # SourceRecord upserts always (carrying the incremental content_hash); the
        # versioned RESOLVED_TO edge + its event only when a target exists, split by
        # target label so the MATCH uses the right index.
        self.conn.query(
            """
            UNWIND $rows AS row
            MERGE (s:SourceRecord {record_key: row.rk})
              ON CREATE SET s.source = row.src, s.tender_id = row.tid, s.line_no = row.line
            SET s.raw_text = row.text, s.resolution_status = row.status,
                s.content_hash = row.chash
            WITH s, row
            OPTIONAL MATCH (i:ItemLicitacion {id_licitacion: row.tid, id_item: toInteger(row.line)})
            FOREACH (ii IN CASE WHEN i IS NOT NULL THEN [i] ELSE [] END |
                MERGE (ii)-[:HAS_RECORD]->(s))
            """, parameters={"rows": rows})
        # Unresolved touches emit an 'unresolved' lineage event (no edge).
        unresolved = [r for r in rows if r["target"] is None]
        if unresolved:
            self.conn.query(
                """
                OPTIONAL MATCH (run:ResolveRun {run_uid: $run_uid})
                UNWIND $rows AS row
                MATCH (s:SourceRecord {record_key: row.rk})
                CREATE (s)-[:HAS_EVENT]->(ev:ResolutionEvent {
                    ts: datetime(), record_key: row.rk, status: row.status,
                    event_kind: 'unresolved', content_hash: row.chash, evidence: row.evidence,
                    extractor_version: row.extractor, schema_version: row.schema,
                    normalizer_version: row.norm})
                FOREACH (r IN CASE WHEN run IS NOT NULL THEN [run] ELSE [] END |
                    CREATE (r)-[:EMITTED]->(ev))
                """, parameters={"rows": unresolved, "run_uid": self.run_uid})
        for label in (GENERIC_LABEL, PRODUCT_LABEL):
            targeted = [r for r in rows if r["target"] is not None and r["label"] == label]
            if not targeted:
                continue
            # Refresh-in-place: re-resolution to the same target updates the current
            # edge's provenance; a new version is created only when the target changes.
            # Every touch appends an immutable :ResolutionEvent linked to the run.
            self.conn.query(
                f"""
                OPTIONAL MATCH (run:ResolveRun {{run_uid: $run_uid}})
                UNWIND $rows AS row
                MATCH (s:SourceRecord {{record_key: row.rk}})
                MATCH (t:{label} {{id: row.target}})
                OPTIONAL MATCH (s)-[cur:RESOLVED_TO {{current: true}}]->(curt)
                WITH run, row, s, t, cur, (curt IS NULL) AS isnew,
                     (curt IS NOT NULL AND curt.id = t.id) AS same,
                     [(s)-[e:RESOLVED_TO]->() | e.version] AS versions
                WITH run, row, s, t, cur, isnew, same,
                     reduce(m = 0, v IN versions | CASE WHEN v > m THEN v ELSE m END) AS maxv
                FOREACH (r IN CASE WHEN same THEN [cur] ELSE [] END |
                    SET r.resolved_at = datetime(), r.last_confirmed_at = datetime(),
                        r.evidence = row.evidence, r.extractor_version = row.extractor,
                        r.schema_version = row.schema, r.normalizer_version = row.norm)
                FOREACH (r IN CASE WHEN (NOT same) AND cur IS NOT NULL THEN [cur] ELSE [] END |
                    SET r.current = false)
                FOREACH (_x IN CASE WHEN same THEN [] ELSE [1] END |
                    CREATE (s)-[:RESOLVED_TO {{
                        current: true, version: maxv + 1, resolved_at: datetime(),
                        first_resolved_at: datetime(), last_confirmed_at: datetime(),
                        evidence: row.evidence, extractor_version: row.extractor,
                        schema_version: row.schema, normalizer_version: row.norm
                    }}]->(t))
                WITH run, row, s, isnew, same,
                     CASE WHEN same THEN maxv ELSE maxv + 1 END AS version
                CREATE (s)-[:HAS_EVENT]->(ev:ResolutionEvent {{
                    ts: datetime(), record_key: row.rk, target_id: row.target,
                    target_label: row.label, status: row.status, version: version,
                    content_hash: row.chash, evidence: row.evidence,
                    event_kind: CASE WHEN isnew THEN 'new' WHEN same THEN 'confirm' ELSE 'retarget' END,
                    extractor_version: row.extractor, schema_version: row.schema,
                    normalizer_version: row.norm}})
                FOREACH (r IN CASE WHEN run IS NOT NULL THEN [run] ELSE [] END |
                    CREATE (r)-[:EMITTED]->(ev))
                """, parameters={"rows": targeted, "run_uid": self.run_uid})
