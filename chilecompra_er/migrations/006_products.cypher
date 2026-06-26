// brand-specific Producto layer (design: the resolution redesign).
//
// A :Producto is the offer's resolved, brand-specific form — deduped by
// (cluster, brand, packaging). Bids attach to it (price per bid on the edge):
//   (:Oferta)-[:COTIZA {precio_normalizado, precio_unitario, rut, fecha}]->(:Producto)
// and each Producto rolls up to its brand-INDEPENDENT canonical:
//   (:Producto)-[:VARIANTE_DE]->(:ProductoCanonico)
// So a price reaches its canonical as (:Oferta)-[:COTIZA]->(:Producto)-[:VARIANTE_DE]->(:ProductoCanonico).

CREATE CONSTRAINT producto_id IF NOT EXISTS
FOR (p:Producto) REQUIRE p.id IS UNIQUE;

CREATE INDEX producto_category IF NOT EXISTS
FOR (p:Producto) ON (p.category);

CREATE INDEX producto_brand IF NOT EXISTS
FOR (p:Producto) ON (p.brand);
