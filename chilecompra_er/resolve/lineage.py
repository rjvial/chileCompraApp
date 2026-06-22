"""Incremental-resolve watermark + run/event lineage (design: incremental delta
+ event-level timestamp audit).

Two pieces of durable state live in the graph, alongside the catalog:

  (:ResolveState {key:'er'})  — the set of ingestion ``run_id``s already
      incorporated. The incremental delta is everything whose run_id is NOT yet
      in this set (see compute_new_runs). Advanced only when a run completes.

  (:ResolveRun {run_uid, ...}) — one node per persist run (full | incremental |
      seed), opened by start_run and closed by finish_run. Every :ResolutionEvent
      a run emits links back to it (assignment.py), so "what did the run at time T
      touch / when was record X resolved by which run" is answerable.

All node timestamps use Cypher ``datetime()`` so they are graph-clock consistent;
only the opaque run_uid is stamped app-side.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

STATE_KEY = "er"


def load_resolved_runs(conn) -> set[str]:
    """The ingestion run_ids already incorporated (empty before the first run)."""
    rows = conn.query(
        "MATCH (st:ResolveState {key: $key}) RETURN st.resolved_runs AS runs",
        parameters={"key": STATE_KEY},
    )
    if not rows or rows[0]["runs"] is None:
        return set()
    return set(rows[0]["runs"])


def record_resolved_runs(conn, run_ids: set[str]) -> None:
    """Mark ``run_ids`` as incorporated. Unions with whatever is already stored
    (idempotent, and safe if two runs overlap), so callers can pass just the new
    runs or the full union interchangeably."""
    conn.query(
        """
        MERGE (st:ResolveState {key: $key})
        SET st.resolved_runs =
              [r IN coalesce(st.resolved_runs, []) WHERE NOT r IN $runs] + $runs,
            st.updated_at = datetime()
        """,
        parameters={"key": STATE_KEY, "runs": sorted(run_ids)},
    )


def compute_new_runs(conn, resolved: set[str]) -> set[str]:
    """Distinct ingestion run_ids present on the source nodes that are not yet in
    ``resolved`` — the coarse delta the incremental fetch then scopes to. The
    DISTINCT scan is index-backed (item_run_id / oferta_run_id) and returns a
    small set; the heavy item fetch is pruned to these runs."""
    rows = conn.query(
        """
        MATCH (i:ItemLicitacion) WHERE i.run_id IS NOT NULL
        RETURN DISTINCT i.run_id AS run_id
        UNION
        MATCH (o:Oferta) WHERE o.run_id IS NOT NULL
        RETURN DISTINCT o.run_id AS run_id
        """
    )
    return {r["run_id"] for r in rows} - resolved


def all_source_runs(conn) -> set[str]:
    """Every ingestion run_id currently present on the source nodes — used by
    --seed-watermark to mark a separately-built corpus as fully incorporated."""
    return compute_new_runs(conn, set())


def _new_run_uid(kind: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"resolve_{kind}-{ts}-{uuid4().hex[:8]}"


def start_run(conn, kind: str, new_runs: set[str] | None = None) -> str:
    """Open a :ResolveRun provenance node and return its run_uid. ``kind`` is
    full | incremental | seed. Events emitted during the run link to this uid."""
    uid = _new_run_uid(kind)
    conn.query(
        """
        CREATE (run:ResolveRun {
            run_uid: $uid, kind: $kind, new_runs: $new_runs,
            started_at: datetime(), status: 'running'
        })
        """,
        parameters={"uid": uid, "kind": kind,
                    "new_runs": sorted(new_runs or set())},
    )
    return uid


def finish_run(conn, run_uid: str, counts: dict | None = None) -> None:
    """Close the :ResolveRun: stamp finished_at and merge in the run's counts
    (processed / created / unresolved / skipped, whatever the caller supplies)."""
    conn.query(
        """
        MATCH (run:ResolveRun {run_uid: $uid})
        SET run.finished_at = datetime(), run.status = 'done', run += $counts
        """,
        parameters={"uid": run_uid, "counts": counts or {}},
    )


# --- point-in-time reconstruction (event-level lineage) ----------------------

def resolution_as_of(conn, ts: str, record_key: str | None = None) -> list[dict]:
    """Reconstruct the resolution state as it stood at `ts` (ISO-8601): for each
    record (or just `record_key`), the LATEST :ResolutionEvent with ev.ts <= ts.

    Because each event is self-contained (target + status + the full evidence:
    normalized text, classifier match, attribute_values + provenance, price
    basis), this rebuilds both the WHAT and the WHY of every past resolution —
    even after the live RESOLVED_TO edge has since been overwritten/retargeted.
    Returns one row per record; a record with no event at/before `ts` is absent
    (it did not exist yet)."""
    return conn.query(
        """
        MATCH (:SourceRecord)-[:HAS_EVENT]->(ev:ResolutionEvent)
        WHERE ev.ts <= datetime($ts)
          AND ($rk IS NULL OR ev.record_key = $rk)
        WITH ev.record_key AS record_key, ev ORDER BY ev.ts DESC
        WITH record_key, head(collect(ev)) AS latest
        RETURN record_key, latest.status AS status,
               latest.target_id AS target_id, latest.target_label AS target_label,
               latest.event_kind AS event_kind, latest.version AS version,
               latest.schema_version AS schema_version,
               latest.extractor_version AS extractor_version,
               latest.normalizer_version AS normalizer_version,
               latest.content_hash AS content_hash,
               latest.evidence AS evidence, latest.ts AS as_of
        ORDER BY record_key
        """,
        parameters={"ts": ts, "rk": record_key},
    )


def record_timeline(conn, record_key: str) -> list[dict]:
    """Full ordered event history for one record — every touch with its evidence,
    target, kind, version and the run that produced it. The audit trail behind
    resolution_as_of."""
    return conn.query(
        """
        MATCH (s:SourceRecord {record_key: $rk})-[:HAS_EVENT]->(ev:ResolutionEvent)
        OPTIONAL MATCH (run:ResolveRun)-[:EMITTED]->(ev)
        RETURN ev.ts AS ts, ev.event_kind AS event_kind, ev.status AS status,
               ev.target_id AS target_id, ev.version AS version,
               ev.schema_version AS schema_version, ev.content_hash AS content_hash,
               ev.evidence AS evidence, run.run_uid AS run_uid
        ORDER BY ev.ts
        """,
        parameters={"rk": record_key},
    )
