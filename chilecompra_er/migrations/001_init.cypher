// Catalog graph model — constraints and indexes (design note §2, §10).
// identity_key embeds the category id, so a single-property uniqueness
// constraint enforces "one node per (category, identity values)" on
// community edition (no composite/node-key constraints there).

CREATE CONSTRAINT category_id_unique IF NOT EXISTS
FOR (c:Category) REQUIRE c.category_id IS UNIQUE;

CREATE CONSTRAINT genericproduct_id_unique IF NOT EXISTS
FOR (g:GenericProduct) REQUIRE g.id IS UNIQUE;

CREATE CONSTRAINT genericproduct_identity_key_unique IF NOT EXISTS
FOR (g:GenericProduct) REQUIRE g.identity_key IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT sourcerecord_key_unique IF NOT EXISTS
FOR (s:SourceRecord) REQUIRE s.record_key IS UNIQUE;

CREATE INDEX genericproduct_category IF NOT EXISTS
FOR (g:GenericProduct) ON (g.category_id);

CREATE INDEX genericproduct_complete IF NOT EXISTS
FOR (g:GenericProduct) ON (g.is_complete);

CREATE INDEX sourcerecord_status IF NOT EXISTS
FOR (s:SourceRecord) ON (s.resolution_status);

CREATE INDEX resolvedto_current IF NOT EXISTS
FOR ()-[r:RESOLVED_TO]-() ON (r.current);
