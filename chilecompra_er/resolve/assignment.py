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
    """Thin reference to an original record — identifiers only, never payload."""
    source: str          # e.g. "mercado_publico"
    tender_id: str
    line_no: str
    raw_text: str        # hashed for the node; payload stays in the source store

    @property
    def record_key(self) -> str:
        return f"{self.source}|{self.tender_id}|{self.line_no}"

    @property
    def raw_text_hash(self) -> str:
        return hashlib.sha256(self.raw_text.encode("utf-8")).hexdigest()


STATUS_GENERIC = "resolved_generic"
STATUS_PRODUCT = "resolved_product"
STATUS_UNRESOLVED = "unresolved"


class InMemoryCatalog:
    """Same contract as Neo4jCatalog, for tests and dry runs."""

    def __init__(self):
        self.nodes: dict[str, NodeView] = {}
        self.specs: dict[str, NodeSpec] = {}
        self.resolutions: list[dict] = []

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
            **edge_props,
        })


class Neo4jCatalog:
    """Catalog backed by the Neo4j property graph (funcionesNeo4j connection)."""

    def __init__(self, conn):
        self.conn = conn
        self._categories_synced: set[str] = set()

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
        records = self.conn.query(
            """
            MATCH (g:GenericProduct {category_id: $cid})
            OPTIONAL MATCH (p:GenericProduct)-[:PARENT_OF]->(g)
            RETURN g{.*} AS props, p.id AS parent_id
            """,
            parameters={"cid": category_id},
        )
        id_names = set(schema.identity_names)
        out: dict[str, NodeView] = {}
        for rec in records:
            props = rec["props"]
            out[props["id"]] = NodeView(
                id=props["id"],
                identity_values={k: v for k, v in props.items()
                                 if k in id_names and v is not None},
                parent_id=rec["parent_id"],
            )
        return out

    def apply(self, plan: AssignmentPlan, schema: CategorySchema) -> None:
        if plan.created is None:
            return
        self.ensure_category(schema)
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

    def persist_resolution(self, source: SourceRef, status: str, target_id: str | None,
                           target_label: str = GENERIC_LABEL, **edge_props) -> None:
        """Upsert :SourceRecord, retire the current :RESOLVED_TO edge, add the
        next version. Unresolved records get the status on the node and no
        edge — still total and explicit, nothing silently dropped."""
        self.conn.query(
            """
            MERGE (s:SourceRecord {record_key: $rk})
            ON CREATE SET s.source = $src, s.tender_id = $tid, s.line_no = $line
            SET s.raw_text_hash = $hash, s.resolution_status = $status
            """,
            parameters={
                "rk": source.record_key,
                "src": source.source,
                "tid": source.tender_id,
                "line": source.line_no,
                "hash": source.raw_text_hash,
                "status": status,
            },
        )
        if target_id is None:
            return
        if target_label not in (GENERIC_LABEL, PRODUCT_LABEL):
            raise ValueError(f"invalid resolution target label {target_label!r}")
        rec = self.conn.query(
            """
            MATCH (s:SourceRecord {record_key: $rk})-[r:RESOLVED_TO]->()
            RETURN coalesce(max(r.version), 0) AS v
            """,
            parameters={"rk": source.record_key},
        )
        next_version = (rec[0]["v"] if rec else 0) + 1
        self.conn.query(
            f"""
            MATCH (s:SourceRecord {{record_key: $rk}})
            OPTIONAL MATCH (s)-[old:RESOLVED_TO {{current: true}}]->()
            SET old.current = false
            WITH DISTINCT s
            MATCH (t:{target_label} {{id: $target}})
            CREATE (s)-[r:RESOLVED_TO {{
                current: true,
                version: $version,
                resolved_at: datetime(),
                evidence: $evidence,
                extractor_version: $extractor_version,
                schema_version: $schema_version,
                normalizer_version: $normalizer_version
            }}]->(t)
            """,
            parameters={
                "rk": source.record_key,
                "target": target_id,
                "version": next_version,
                "evidence": json.dumps(edge_props.get("evidence", {}), ensure_ascii=False),
                "extractor_version": edge_props.get("extractor_version", ""),
                "schema_version": edge_props.get("schema_version", ""),
                "normalizer_version": edge_props.get("normalizer_version", ""),
            },
        )
