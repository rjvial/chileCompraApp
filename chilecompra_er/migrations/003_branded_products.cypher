// Branded Product = Brand × GenericProduct (design: branded-product redesign).
//
// :Product is now the deduped (generic, brand) pairing; :Brand is a first-class
// node shared across products and categories. The (:Oferta)-[:OFFERS]->(:Product)
// and (:ItemLicitacion)-[:HAS_RECORD]->(:SourceRecord) edges need no constraints
// (MERGE dedups them), and their source-side match keys are already indexed:
// oferta_id (Oferta.id_oferta) and item_licitacion_key (ItemLicitacion
// id_licitacion+id_item). product_id_unique (Product.id) already exists.

CREATE CONSTRAINT brand_id IF NOT EXISTS
FOR (b:Brand) REQUIRE b.id IS UNIQUE;

// Brand × GenericProduct identity, for lookups/audits (the write path MERGEs on
// Product.id, already unique, so this is a convenience index, not a correctness one).
CREATE INDEX product_identity_key IF NOT EXISTS
FOR (p:Product) ON (p.identity_key);
