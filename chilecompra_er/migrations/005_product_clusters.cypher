// L2 product clusters (design: the L0->L3 resolution redesign).
//
// One :ProductCluster per substitutable product — the price-comparison unit.
// Offers attach via (:Oferta)-[:PRICED_IN {normalized_price, rut, date}]->(:ProductCluster)
// (price on the edge, as with the legacy OFFERS edge); a coarser cluster is
// parented by its finer refinements via (finer)-[:REFINES]->(coarser).
//
// This is additive: it coexists with the legacy :GenericProduct/:Product catalog
// during the shadow-run, and is retired-from / cut-over-to in Phase 6.

CREATE CONSTRAINT product_cluster_id IF NOT EXISTS
FOR (c:ProductCluster) REQUIRE c.id IS UNIQUE;

// Cluster-level dedup invariant (coherence auditor S5): no two clusters share a
// canonical signature. The write path MERGEs on id (derived from signature), so
// this also guards against a signature/id mismatch.
CREATE CONSTRAINT product_cluster_signature IF NOT EXISTS
FOR (c:ProductCluster) REQUIRE c.signature IS UNIQUE;

CREATE INDEX product_cluster_category IF NOT EXISTS
FOR (c:ProductCluster) ON (c.category);
