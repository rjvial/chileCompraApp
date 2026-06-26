// canonical products (design: the resolution redesign).
//
// One :ProductoCanonico per substitutable product — the brand-INDEPENDENT
// price-comparison unit. Brand-specific :Producto nodes roll up to it via
// (:Producto)-[:VARIANTE_DE]->(:ProductoCanonico) (see migration 006), and a coarser
// canonical is parented by its finer refinements via (finer)-[:ESPECIFICA]->(coarser).

CREATE CONSTRAINT producto_canonico_id IF NOT EXISTS
FOR (c:ProductoCanonico) REQUIRE c.id IS UNIQUE;

// Canonical-level dedup invariant (coherence auditor S5): no two canonicals share a
// canonical signature. The write path MERGEs on id (derived from signature), so
// this also guards against a signature/id mismatch.
CREATE CONSTRAINT producto_canonico_signature IF NOT EXISTS
FOR (c:ProductoCanonico) REQUIRE c.signature IS UNIQUE;

CREATE INDEX producto_canonico_category IF NOT EXISTS
FOR (c:ProductoCanonico) ON (c.category);
