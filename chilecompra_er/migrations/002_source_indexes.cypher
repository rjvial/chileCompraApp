// Source-layer index for scoped retrieval (profiling + resolve fetch).
// codigo_unspsc_producto is an integer; the fetchers filter by a numeric
// range (segment_bounds) so this RANGE index turns a full ItemLicitacion
// scan into an index seek. Without it, any --segment run scans all ~3M
// tender items. The transactional layer is otherwise owned by the ingestion
// pipeline; this index is read-only schema and safe to add here.

CREATE INDEX item_unspsc IF NOT EXISTS
FOR (i:ItemLicitacion) ON (i.codigo_unspsc_producto);
