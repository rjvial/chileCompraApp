import kumoai.experimental.rfm as rfm, os, random
from dotenv import load_dotenv
import boto3
import numpy as np
import pandas as pd
import funcionesNeo4j as fn
import funcionesNeo4jEC2 as fne

SEED = 42
os.environ["PYTHONHASHSEED"] = str(SEED)
random.seed(SEED)
np.random.seed(SEED)

# Local directory where CSVs are stored (for type inference)
LOCAL_CSV_DIR = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"

load_dotenv("secrets.env")
KUMO_API_KEY = os.getenv("KUMO_API_KEY")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

INSTANCIA_EC2 = 'Neo4j-EC2'

os.environ["KUMO_API_KEY"] = KUMO_API_KEY

rfm.init()


# ─── CONNECT TO NEO4J OFF-INSTANCE ──────────────────────────────────────────────
ec2 = boto3.client('ec2', region_name='us-east-1')
instance_id, public_ip, state = fne.find_instance_by_name(ec2, INSTANCIA_EC2)
bolt_uri = f"bolt://{public_ip}:7687" # principal instance
# bolt_uri = f"bolt://{public_ip}:7688" # neo4j-dev-instance
conn_neo4j = fn.Neo4jConnection(
    uri=bolt_uri,
    user=NEO4J_USER,
    pwd=NEO4J_PASSWORD,
    encrypted=False)

query = f"""
MATCH (p:Proveedor)-[:OFERTA_ITEM|ADJUDICA_ITEM]->(it:ItemLicitacion)
WHERE p.rut = '78.566.250-4'
MATCH (comp:Proveedor)-[:OFERTA_ITEM|ADJUDICA_ITEM]->(it)
  WITH DISTINCT comp
  RETURN
    comp.rut                         AS rut_proveedor,
    comp.razon_social                AS razon_social,
    toInteger(comp.codigo_actividad)            AS codigo_actividad,
    toInteger(comp.num_trabajadores)            AS num_trabajadores,
    toInteger(comp.tramo_ventas)                AS tramo_ventas,
    toInteger(comp.tramo_capital_positivo)      AS tramo_capital_positivo,
    toString(comp.fecha_inicio_actividades)    AS fecha_inicio_actividades,
    comp.pais                        AS pais,
    comp.region                      AS region,
    comp.comuna_domicilio            AS comuna_domicilio
  ORDER BY razon_social;
"""
df_proveedores = fn.neo4jToDataframe(query, conn_neo4j)


query = f"""
MATCH (org:Organismo)
  RETURN
      org.buyer_id       AS buyer_id,
      org.rut            AS rut_organismo,
      org.nombre         AS nombre_organismo,
      org.region         AS region,
      org.street_address AS direccion
  ORDER BY rut_organismo;
"""
df_organismos = fn.neo4jToDataframe(query, conn_neo4j)


query = f"""
MATCH (target:Proveedor {{rut:'78.566.250-4'}})-[:OFERTA_ITEM|ADJUDICA_ITEM]->(it:ItemLicitacion)
WITH collect(DISTINCT it) AS items
UNWIND items AS it
MATCH (bidder:Proveedor)-[r:OFERTA_ITEM|ADJUDICA_ITEM]->(it)
  OPTIONAL MATCH (lic:Licitacion)-[:REQUIERE_ITEM]->(it)
  OPTIONAL MATCH (it)-[:ES_PRODUCTO_DE]->(pu:Producto_Ungm)
  WITH bidder, it, lic, pu, collect(r) AS rels
  WITH bidder, it, lic, pu,
       any(x IN rels WHERE type(x) = 'ADJUDICA_ITEM')                AS adjudicacion_flag,
       head([x IN rels WHERE type(x) = 'ADJUDICA_ITEM'] + rels)      AS r
  RETURN
    bidder.rut             AS rut_proveedor,
    it.id_item             AS id_item,
    lic.buyer_id           AS buyer_id,
    toString(lic.fecha_cierre) AS fecha_cierre,
    r.cantidad             AS cantidad,
    r.precio_unitario      AS precio_unitario,
    r.precio_total         AS precio_total,
    r.precio_equiv_clp     AS precio_clp,
    r.moneda               AS moneda,
    it.specs_comprador     AS specs_comprador,
    r.specs_proveedor      AS specs_proveedor,
    pu.cod_producto        AS cod_producto_ungm
  ORDER BY id_item, rut_proveedor;
"""
df_bids = fn.neo4jToDataframe(query, conn_neo4j)
df_bids["bid_id"] = df_bids.index.astype("int64")
df_bids["fecha_cierre"] = pd.to_datetime(df_bids["fecha_cierre"], errors="coerce", utc=True).dt.tz_localize(None)

# Drop rows with missing or implausible fecha_cierre (upstream parsing produced years like 2073)
_valid = df_bids["fecha_cierre"].between("2010-01-01", "2030-01-01")
print(f"df_bids: dropping {(~_valid).sum()} rows with bad fecha_cierre "
      f"(min={df_bids['fecha_cierre'].min()}, max={df_bids['fecha_cierre'].max()})")
df_bids = df_bids.loc[_valid].reset_index(drop=True)
df_bids["bid_id"] = df_bids.index.astype("int64")

df_bids["precio_unitario"] = pd.to_numeric(df_bids["precio_unitario"], errors="coerce")
df_bids["precio_clp"] = pd.to_numeric(df_bids["precio_clp"], errors="coerce")
df_bids = df_bids[df_bids["precio_clp"] > 1].reset_index(drop=True)
df_bids = df_bids[df_bids["buyer_id"].notna()].reset_index(drop=True)
df_bids["bid_id"] = df_bids.index.astype("int64")

# Within-item competitive features
df_bids["price_rank"] = df_bids.groupby("id_item")["precio_unitario"].rank(method="min").astype("Int64")

# Shift competitors' fecha_cierre +1 day so target proveedor's bids close strictly before
# competitors'. Lets anchor = target's fecha_cierre mask competitor siblings from context
# while keeping the target row's own features visible (it's the prediction query).
_competitor_mask = df_bids["rut_proveedor"] != "78.566.250-4"
df_bids.loc[_competitor_mask, "fecha_cierre"] = (
    df_bids.loc[_competitor_mask, "fecha_cierre"] + pd.Timedelta(days=1)
)


query = f"""
MATCH (pu:Producto_Ungm)
  OPTIONAL MATCH (pu)-[:PERTENECE_A_CLASE]->(cl:Clase_Ungm)
  OPTIONAL MATCH (cl)-[:PERTENECE_A_FAMILIA]->(fam:Familia_Ungm)
  OPTIONAL MATCH (fam)-[:PERTENECE_A_SEGMENTO]->(seg:Segmento_Ungm)
  RETURN
    seg.cod_segmento    AS cod_segmento,
    fam.cod_familia     AS cod_familia,
    cl.cod_clase        AS cod_clase,
    pu.cod_producto     AS cod_producto_ungm,
    pu.nombre_producto  AS nombre_producto;
"""
df_productos_ungm = fn.neo4jToDataframe(query, conn_neo4j)

# ─── Items table (one row per id_item, with UNGM hierarchy inlined) ────────────
df_items = (
    df_bids[["id_item", "buyer_id", "fecha_cierre", "specs_comprador", "cod_producto_ungm"]]
    .drop_duplicates(subset=["id_item"])
    .merge(df_productos_ungm, on="cod_producto_ungm", how="left")
    .reset_index(drop=True)
)


print("bids.fecha_cierre   :", df_bids["fecha_cierre"].min(), "→", df_bids["fecha_cierre"].max(), "dtype:", df_bids["fecha_cierre"].dtype)
print("rows beyond 2030 in bids:", (df_bids["fecha_cierre"] > "2030-01-01").sum())

graph = rfm.LocalGraph.from_data({
    "proveedores": df_proveedores,
    "organismos": df_organismos,
    "bids": df_bids,
    "items": df_items,
})


# Primary keys
graph["proveedores"].primary_key = "rut_proveedor"
graph["organismos"].primary_key  = "buyer_id"
graph["bids"].primary_key        = "bid_id"
graph["items"].primary_key       = "id_item"

# Time columns
graph["bids"].time_column  = "fecha_cierre"
graph["items"].time_column = "fecha_cierre"

# Foreign keys
graph.link(src_table="items", fkey="buyer_id",      dst_table="organismos")
graph.link(src_table="bids",  fkey="id_item",       dst_table="items")
graph.link(src_table="bids",  fkey="rut_proveedor", dst_table="proveedores")


# Inspect the graph - requires graphviz to be installed
graph.print_metadata()
graph.print_links()
graph.validate()
# graph.visualize()

model = rfm.KumoRFM(graph)

# ─── PREDICT adjudicacion_flag for all 2025 bids by proveedor 78.566.250-4 ──────
target_rut = "78.566.250-4"
mask_dec_2025 = (
    (df_bids["fecha_cierre"].dt.year == 2025)
    & (df_bids["fecha_cierre"].dt.month == 12)
)
bid_ids_dec_2025 = df_bids.loc[mask_dec_2025, "bid_id"].tolist()

pql = f"""
PREDICT bids.price_rank = 1
FOR bids.bid_id IN ({", ".join(str(b) for b in bid_ids_dec_2025)})
WHERE bids.rut_proveedor = '78.566.250-4'
"""

# Held-out evaluation via Kumo's built-in temporal splits
try:
    metrics = model.evaluate(pql, run_mode="fast")
except KeyError:
    try:
        metrics = model.evaluate(pql, run_mode="normal")
    except KeyError:
        metrics = model.evaluate(pql)
print("Kumo evaluate() metrics:")
print(metrics)
