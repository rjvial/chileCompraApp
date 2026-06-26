// brand-specific Product layer (design: the resolution redesign).
//
// A :Product is the offer's resolved, brand-specific form — deduped by
// (cluster, brand, packaging). Bids attach to it (price per bid on the edge):
//   (:Oferta)-[:OFFERS {normalized_price, unit_price, rut, date}]->(:Product)
// and each Product rolls up to its brand-INDEPENDENT cluster:
//   (:Product)-[:VARIANT_OF]->(:ProductCluster)
// So a price reaches its cluster as (:Oferta)-[:OFFERS]->(:Product)-[:VARIANT_OF]->(:ProductCluster).

CREATE CONSTRAINT product_id IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

CREATE INDEX product_category IF NOT EXISTS
FOR (p:Product) ON (p.category);

CREATE INDEX product_brand IF NOT EXISTS
FOR (p:Product) ON (p.brand);
