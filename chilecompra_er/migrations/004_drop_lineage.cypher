// Simplify the resolution model: an :ItemLicitacion now links DIRECTLY to its
// :GenericProduct via (:ItemLicitacion)-[:RESOLVED_TO]->(:GenericProduct).
//
// This removes the :SourceRecord reference layer and the whole incremental /
// lineage machinery (:SourceRecord, :ResolveRun, :ResolutionEvent, :ResolveState
// and their HAS_RECORD / HAS_EVENT / EMITTED edges). Their nodes are deleted out
// of band (APOC-batched); here we drop the now-orphaned schema. Idempotent.
//
// The persist path matches (:ItemLicitacion {id_licitacion, id_item}), already
// covered by the existing item_licitacion_key index — no new index needed.

DROP CONSTRAINT sourcerecord_key_unique IF EXISTS;
DROP INDEX sourcerecord_status IF EXISTS;

DROP CONSTRAINT resolvestate_key IF EXISTS;
DROP CONSTRAINT resolverun_uid IF EXISTS;
DROP INDEX resolutionevent_ts IF EXISTS;

DROP INDEX item_run_id IF EXISTS;
DROP INDEX oferta_run_id IF EXISTS;
