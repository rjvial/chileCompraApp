// Incremental resolve + event-level lineage (design: delta detection + timestamp audit).
//
// run_id indexes on the transactional nodes make the incremental delta seek
// (MATCH ... WHERE i.run_id IN $new_runs) index-backed instead of a full scan.

CREATE INDEX item_run_id IF NOT EXISTS
FOR (i:ItemLicitacion) ON (i.run_id);

CREATE INDEX oferta_run_id IF NOT EXISTS
FOR (o:Oferta) ON (o.run_id);

// Watermark singleton (resolved ingestion run_ids) and per-run provenance identity.

CREATE CONSTRAINT resolvestate_key IF NOT EXISTS
FOR (st:ResolveState) REQUIRE st.key IS UNIQUE;

CREATE CONSTRAINT resolverun_uid IF NOT EXISTS
FOR (run:ResolveRun) REQUIRE run.run_uid IS UNIQUE;

// Time-window audits over the append-only event log (record-scoped audits
// traverse :HAS_EVENT, so no record_key index is needed here).

CREATE INDEX resolutionevent_ts IF NOT EXISTS
FOR (ev:ResolutionEvent) ON (ev.ts);
