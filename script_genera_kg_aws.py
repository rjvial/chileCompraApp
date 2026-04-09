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


# ######################################################################################
# # 1) CREATE UNIQUE CONSTRAINTS ON ALL KEY NODE LABELS
# ######################################################################################
# print("==> Generating unique constraints")

# constraints = [
#     ("propIdConstraint_cm"                      , "Convenio_Marco"      , "convenio_marco_id"),
#     ("propIdConstraint_oc"                     , "Orden_Compra"          , "id_oc"),
#     ("propIdConstraint_linea_transaccion"       , "Linea_Transaccion"   , "id_linea_transaccion"),
#     ("propIdConstraint_proveedor"               , "Proveedor"           , "rut_proveedor"),
#     ("propIdConstraint_comprador"               , "Comprador"           , "rut_unidad_compra"),
#     ("propIdConstraint_producto_detallado"      , "Producto_Detallado"  , "producto_detallado_id"),
#     ("propIdConstraint_segmento"                , "Segmento_Ungm"       , "cod_segmento"),
#     ("propIdConstraint_familia"                 , "Familia_Ungm"        , "cod_familia"),
#     ("propIdConstraint_clase"                   , "Clase_Ungm"          , "cod_clase"),
#     ("propIdConstraint_producto"                , "Producto_Ungm"       , "cod_producto")
# ]


# for name, label, prop in constraints:
#     q = (
#         f"CREATE CONSTRAINT {name} IF NOT EXISTS "
#         f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE;"
#     )
#     fn.neo4jQuery(q, conn_neo4j)
#     print(f"  • Constraint on {label}.{prop}")



# #######################################################################################
# # Carga Nodos a Neo4j
# #######################################################################################

# print('Carga Nodos Convenio_Marco')
# df_convenio_marco = pd.read_csv(f'{LOCAL_CSV_DIR}convenios_marco.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_convenio_marco,
#     node_label='Convenio_Marco',
#     id_column='convenio_marco_id',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///convenios_marco.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Carga Nodos Orden_Compra')
# df_oc = pd.read_csv(f'{LOCAL_CSV_DIR}ordenes_compra.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_oc,
#     node_label='Orden_Compra',
#     id_column='id_oc',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///ordenes_compra.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Carga Nodos Linea_Transaccion')
# df_linea_transaccion = pd.read_csv(f'{LOCAL_CSV_DIR}lineas_transaccion.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_linea_transaccion,
#     node_label='Linea_Transaccion',
#     id_column='id_linea_transaccion',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///lineas_transaccion.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Carga Nodos Producto_Detallado')
# df_producto = pd.read_csv(f'{LOCAL_CSV_DIR}productos.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_producto,
#     node_label='Producto_Detallado',
#     id_column='producto_detallado_id',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///productos.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1



# print('Carga Nodos Proveedor')
# df_proveedor = pd.read_csv(f'{LOCAL_CSV_DIR}proveedores.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_proveedor,
#     node_label='Proveedor',
#     id_column='rut_proveedor',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///proveedores.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Carga Nodos Comprador')
# df_comprador = pd.read_csv(f'{LOCAL_CSV_DIR}compradores.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_comprador,
#     node_label='Comprador',
#     id_column='rut_unidad_compra',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///compradores.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1



# print('Query Relacion Producto_Detallado -> Convenio_Marco')
# query_relacion = """
# CALL apoc.periodic.iterate(
#   "
#   MATCH (source:Producto_Detallado)
#   MATCH (target:Convenio_Marco)
#   WHERE source.convenio_marco_id = target.convenio_marco_id
#   RETURN source, target
#   ",
#   "
#   MERGE (source)-[:ES_PRODUCTO_DE]->(target)
#   ",
#   {batchSize: 100, parallel: false}
# );
# """
# fn.neo4jQuery(query_relacion, conn_neo4j)
# a=1


# print('Query Relacion Orden_Compra -> Proveedor')
# query_relacion = """
# CALL apoc.periodic.iterate(
#   "
#   MATCH (source:Orden_Compra)
#   MATCH (target:Proveedor)
#   WHERE source.rut_proveedor = target.rut_proveedor
#   RETURN source, target
#   ",
#   "
#   MERGE (source)-[:GENERADA_PARA]->(target)
#   ",
#   {batchSize: 100, parallel: false}
# );
# """
# fn.neo4jQuery(query_relacion, conn_neo4j)
# a=1


# print('Query Relacion Orden_Compra -> Comprador')
# query_relacion = """
# CALL apoc.periodic.iterate(
#   "
#   MATCH (source:Orden_Compra)
#   MATCH (target:Comprador)
#   WHERE source.rut_unidad_compra = target.rut_unidad_compra
#   RETURN source, target
#   ",
#   "
#   MERGE (source)-[:GENERADA_POR]->(target)
#   ",
#   {batchSize: 100, parallel: false}
# );
# """
# fn.neo4jQuery(query_relacion, conn_neo4j)
# a=1


print('Query Relacion Linea_Transaccion -> Orden_Compra')
query_relacion = """
CALL apoc.periodic.iterate(
  "
  MATCH (source:Linea_Transaccion)
  MATCH (target:Orden_Compra)
  WHERE source.id_oc = target.id_oc
  RETURN source, target
  ",
  "
  MERGE (source)-[:ES_PARTE_DE]->(target)
  ",
  {batchSize: 100, parallel: false}
);
"""
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1


print('Query Relacion Linea_Transaccion -> Producto_Detallado')
query_relacion = """
CALL apoc.periodic.iterate(
  "
  MATCH (source:Linea_Transaccion)
  MATCH (target:Producto_Detallado)
  WHERE toInteger(source.producto_detallado_id) = toInteger(target.producto_detallado_id)
  RETURN source, target
  ",
  "
  MERGE (source)-[:CONTIENE_PRODUCTO]->(target)
  ",
  {batchSize: 100, parallel: false}
);
"""
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1


# print('Query Relacion Producto_Detallado -> Proveedor')
# query_relacion = """
# CALL apoc.periodic.iterate(
#   "LOAD CSV WITH HEADERS FROM ':file' AS row FIELDTERMINATOR '|' RETURN row",
#   "
#   MATCH (source: Producto_Detallado {producto_detallado_id: toInteger(row.producto_detallado_id)})
#   MATCH (target: Proveedor {rut_proveedor: row.rut_proveedor})
#   MERGE (source)-[r:ES_OFRECIDO_POR]->(target)
#   ",
#   {batchSize: 500, parallel: false}
# );
# """
# query_relacion = query_relacion.replace(':file', f'file:///rel_producto_proveedor.csv')
# fn.neo4jQuery(query_relacion, conn_neo4j)
# a=1



# print('Carga Nodos Segmento_Ungm')
# df_segmento = pd.read_csv(f'{LOCAL_CSV_DIR}segmentos_ungm.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_segmento,
#     node_label='Segmento_Ungm',
#     id_column='cod_segmento',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///segmentos_ungm.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Carga Nodos Familia_Ungm')
# df_familia = pd.read_csv(f'{LOCAL_CSV_DIR}familias_ungm.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_familia,
#     node_label='Familia_Ungm',
#     id_column='cod_familia',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///familias_ungm.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1

# print('Carga Nodos Clase_Ungm')
# df_clase = pd.read_csv(f'{LOCAL_CSV_DIR}clases_ungm.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_clase,
#     node_label='Clase_Ungm',
#     id_column='cod_clase',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///clases_ungm.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1

# print('Carga Nodos Producto_Ungm')
# df_producto = pd.read_csv(f'{LOCAL_CSV_DIR}productos_ungm.csv', sep='|', nrows=100)
# query_nodo = fn.generate_cypher_query(
#     df=df_producto,
#     node_label='Producto_Ungm',
#     id_column='cod_producto',
#     file_placeholder=':file',
#     batch_size=500,
#     parallel=False,
#     separator='|'
# )
# query_nodo = query_nodo.replace(':file', f'file:///productos_ungm.csv')
# fn.neo4jQuery(query_nodo, conn_neo4j)
# a=1


# print('Query Relacion Producto_Detallado -> Producto_Ungm')
# query_relacion = """
# CALL apoc.periodic.iterate(
#   "LOAD CSV WITH HEADERS FROM ':file' AS row FIELDTERMINATOR '|' RETURN row",
#   "
#   MATCH (source: Producto_Detallado {producto_detallado_id: toInteger(row.producto_detallado_id)})
#   MATCH (target: Producto_Ungm {cod_producto: toInteger(row.codigo_onu)})
#   MERGE (source)-[r:ES_UN_PRODUCTO_UNGM]->(target)
#   ",
#   {batchSize: 500, parallel: false}
# );
# """
# query_relacion = query_relacion.replace(':file', f'file:///rel_producto_codigo_onu.csv')
# fn.neo4jQuery(query_relacion, conn_neo4j)
# a=1


print('Query Relacion Producto_Ungm -> Clase_Ungm')
query_relacion = """
CALL apoc.periodic.iterate(
  "
  MATCH (source:Producto_Ungm)
  MATCH (target:Clase_Ungm)
  WHERE toInteger(floor(source.cod_producto / 100)) = target.cod_clase
  RETURN source, target
  ",
  "
  MERGE (source)-[:PERTENECE_A_CLASE]->(target)
  ",
  {batchSize: 100, parallel: false}
);
"""
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1


print('Query Relacion Clase_Ungm -> Familia_Ungm')
query_relacion = """
CALL apoc.periodic.iterate(
  "
  MATCH (source:Clase_Ungm)
  MATCH (target:Familia_Ungm)
  WHERE toInteger(floor(source.cod_clase / 100)) = target.cod_familia
  RETURN source, target
  ",
  "
  MERGE (source)-[:PERTENECE_A_FAMILIA]->(target)
  ",
  {batchSize: 100, parallel: false}
);
"""
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1


print('Query Relacion Familia_Ungm -> Segmento_Ungm')
query_relacion = """
CALL apoc.periodic.iterate(
  "
  MATCH (source:Familia_Ungm)
  MATCH (target:Segmento_Ungm)
  WHERE toInteger(floor(source.cod_familia / 100)) = target.cod_segmento
  RETURN source, target
  ",
  "
  MERGE (source)-[:PERTENECE_A_SEGMENTO]->(target)
  ",
  {batchSize: 100, parallel: false}
);
"""
fn.neo4jQuery(query_relacion, conn_neo4j)
a=1

########################################################################################
########################################################################################

# MATCH (pd:Producto_Detallado)-[:ES_UN_PRODUCTO_UNGM]->(pu:Producto_Ungm)
# WHERE pu.cod_producto = 47121701
# RETURN 
#   pu.cod_producto AS cod_producto,
#   pu.nombre_producto AS nombre_producto,
#   pd.producto_detallado_id AS producto_detallado_id, pd.nombre_producto AS nombre_producto_detallado
# ORDER BY producto_detallado_id

# MATCH (pd:Producto_Detallado)-[:ES_UN_PRODUCTO_UNGM]->(pu:Producto_Ungm)
# RETURN 
#   pu.cod_producto AS cod_producto,
#   pu.nombre_producto AS nombre_producto,
#   COUNT(DISTINCT pd) AS cantidad_productos_diferentes
# ORDER BY cantidad_productos_diferentes DESC

# MATCH (oc:Orden_Compra)<-[:ES_PARTE_DE]-(lt:Linea_Transaccion)-[:CONTIENE_PRODUCTO]->(pd:Producto_Detallado)
# RETURN 
#   pd.producto_detallado_id AS producto_id,
#   pd.nombre_producto AS nombre_producto,
#   COUNT(DISTINCT oc) AS cantidad_ordenes_compra
# ORDER BY cantidad_ordenes_compra DESC

# MATCH (oc:Orden_Compra)<-[:ES_PARTE_DE]-(lt:Linea_Transaccion)-[:CONTIENE_PRODUCTO]->(pd:Producto_Detallado)
# WHERE pd.producto_detallado_id = 2230143
# RETURN oc.fecha_envio_oc AS fecha, oc.id_oc AS id_oc, 
# oc.rut_proveedor AS rut_proveedor, oc.rut_unidad_compra AS rut_comprador, 
# oc.moneda AS moneda, lt.id_linea_transaccion, lt.cantidad AS cantidad, 
# lt.precio_unitario, pd.producto_detallado_id AS producto_detallado_id, 
# pd.nombre_producto AS nombre_producto
# ORDER BY fecha