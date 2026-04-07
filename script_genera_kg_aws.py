import boto3
import pandas as pd
import funcionesNeo4j as fn
import funcionesNeo4jEC2 as fne

import os
from dotenv import load_dotenv

INSTANCIA_EC2 = 'Neo4j-EC2'

load_dotenv("secrets.env")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Local directory where CSVs are stored (for type inference)
LOCAL_CSV_DIR = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"

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


######################################################################################
# 1) CREATE UNIQUE CONSTRAINTS ON ALL KEY NODE LABELS
######################################################################################
print("==> Generating unique constraints")

constraints = [
    ("propIdConstraint_cm"           , "Convenio_Marco",          "convenio_marco_id"),
    ("propIdConstraint_lcm"          , "Licitacion",            "licitacion_id"),
    ("propIdConstraint_poi_category" , "Proveedor",   "proveedor_id"),
    ("propIdConstraint_bus_stop"     , "Producto",       "producto_id")
]


for name, label, prop in constraints:
    q = (
        f"CREATE CONSTRAINT {name} IF NOT EXISTS "
        f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE;"
    )
    fn.neo4jQuery(q, conn_neo4j)
    print(f"  • Constraint on {label}.{prop}")

# Optional spatial indexes
indexes = [
    "CREATE POINT INDEX IF NOT EXISTS FOR (n:Subway_Entrance) ON (n.location);",
]
for q in indexes:
    fn.neo4jQuery(q, conn_neo4j)
    print(f"  • {q}")
a = 1


#######################################################################################
# Carga Nodos a Neo4j
#######################################################################################

print('Carga Nodos Convenio_Marco')
df_convenio_marco = pd.read_csv(f'{LOCAL_CSV_DIR}convenios_marco.csv', sep=',', nrows=100)
query_nodo = fn.generate_cypher_query(
    df=df_convenio_marco,
    node_label='Convenio_Marco',
    id_column='convenio_marco_id',
    file_placeholder=':file',
    batch_size=500,
    parallel=False,
    separator=','
)
query_nodo = query_nodo.replace(':file', f'file:///convenios_marco.csv')
fn.neo4jQuery(query_nodo, conn_neo4j)
a=1



print('Carga Nodos Producto')
df_producto = pd.read_csv(f'{LOCAL_CSV_DIR}productos_convenios_marco.csv', sep=',', nrows=100)
query_nodo = fn.generate_cypher_query(
    df=df_producto,
    node_label='Producto',
    id_column='producto_id',
    file_placeholder=':file',
    batch_size=500,
    parallel=False,
    separator=','
)
query_nodo = query_nodo.replace(':file', f'file:///productos_convenios_marco.csv')
fn.neo4jQuery(query_nodo, conn_neo4j)
a=1



print('Carga Nodos Proveedor')
df_proveedor = pd.read_csv(f'{LOCAL_CSV_DIR}proveedores_convenios_marco.csv', sep=',', nrows=100)
query_nodo = fn.generate_cypher_query(
    df=df_proveedor,
    node_label='Proveedor',
    id_column='proveedor_id',
    file_placeholder=':file',
    batch_size=500,
    parallel=False,
    separator=','
)
query_nodo = query_nodo.replace(':file', f'file:///proveedores_convenios_marco.csv')
fn.neo4jQuery(query_nodo, conn_neo4j)
a=1



print('Query Relacion Producto -> Convenio_Marco')
query_relacion = """
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM ':file' AS row FIELDTERMINATOR ',' RETURN row",
  "
  MATCH (source: Producto {producto_id: toInteger(row.producto_id)})
  MATCH (target: Convenio_Marco {convenio_marco_id: toInteger(row.convenio_marco_id)})
  MERGE (source)-[:ES_PRODUCTO_DE]->(target)
  ",
  {batchSize: 500, parallel: false}
);
"""
query_relacion = query_relacion.replace(':file', f'file:///rel_convenios_productos.csv')
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1


print('Query Relacion Proveedor -> Producto')
query_relacion = """
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM ':file' AS row FIELDTERMINATOR ',' RETURN row",
  "
  MATCH (source: Proveedor {proveedor_id: toInteger(row.proveedor_id)})
  MATCH (target: Producto {producto_id: toInteger(row.producto_id)})
  MERGE (source)-[r:ES_PROVEEDOR_DE]->(target)
  ON CREATE SET r.stock_flag = row.stock_flag, r.precio_tienda = toInteger(row.precio_tienda), r.precio_regular = toInteger(row.precio_regular)  
  ",
  {batchSize: 500, parallel: false}
);
"""
query_relacion = query_relacion.replace(':file', f'file:///rel_proveedores_productos.csv')
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1

