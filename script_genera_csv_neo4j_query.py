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

fileDir = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"

# ─── CONNECT TO NEO4J ──────────────────────────────────────────────
ec2 = boto3.client('ec2', region_name='us-east-1')
instance_id, public_ip, state = fne.find_instance_by_name(ec2, INSTANCIA_EC2)
bolt_uri = f"bolt://{public_ip}:7687"
conn_neo4j = fn.Neo4jConnection(
    uri=bolt_uri,
    user=NEO4J_USER,
    pwd=NEO4J_PASSWORD,
    encrypted=False)


# ─── QUERY: Top 1000 productos by distinct proveedores ─────────────
print("=== Querying Neo4j: productos por proveedores distintos ===\n")

query = """
MATCH (oc:Orden_Compra)<-[:ES_PARTE_DE]-(lt:Linea_Transaccion)-[:CONTIENE_PRODUCTO]->(pd:Producto_Detallado)
MATCH (oc)-[:GENERADA_PARA]->(pr:Proveedor)
WHERE pd.producto_detallado_id > 0
WITH
  pd,
  COUNT(DISTINCT pr.rut_proveedor) AS proveedores_distintos
RETURN
  pd.producto_detallado_id AS producto_detallado_id,
  pd.nombre_producto AS nombre_producto,
  proveedores_distintos
ORDER BY proveedores_distintos DESC
LIMIT 1000;
"""

df = fn.neo4jToDataframe(query, conn_neo4j)
print(f"Rows returned: {len(df)}")

OUTPUT_FILE = "productos_por_proveedores.csv"
df.to_csv(f"{fileDir}{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
print(f"Saved to {fileDir}{OUTPUT_FILE}")
