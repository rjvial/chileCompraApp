// L2 product clusters (design: the L0->L3 resolution redesign).
//
// One :ProductCluster per substitutable product — the brand-INDEPENDENT
// price-comparison unit. Brand-specific :Product nodes roll up to it via
// (:Product)-[:VARIANT_OF]->(:ProductCluster) (see migration 006), and a coarser
// cluster is parented by its finer refinements via (finer)-[:REFINES]->(coarser).

CREATE CONSTRAINT product_cluster_id IF NOT EXISTS
FOR (c:ProductCluster) REQUIRE c.id IS UNIQUE;

// Cluster-level dedup invariant (coherence auditor S5): no two clusters share a
// canonical signature. The write path MERGEs on id (derived from signature), so
// this also guards against a signature/id mismatch.
CREATE CONSTRAINT product_cluster_signature IF NOT EXISTS
FOR (c:ProductCluster) REQUIRE c.signature IS UNIQUE;

CREATE INDEX product_cluster_category IF NOT EXISTS
FOR (c:ProductCluster) ON (c.category);
