"""
Análisis diagnóstico de Promedon en ChileCompra (Neo4j).
Ejecuta diagnósticos de integridad de datos y análisis comerciales
para identificar palancas de crecimiento de revenue.
"""
import os
import boto3
import pandas as pd
from dotenv import load_dotenv

import funcionesNeo4j as fn
import funcionesNeo4jEC2 as fne

# ─── CONFIG ──────────────────────────────────────────────────────────────
PROMEDON_RUT = "78.566.250-4"
INSTANCIA_EC2 = "Neo4j-EC2"
OUTPUT_DIR = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\analisis_promedon\\"

os.makedirs(OUTPUT_DIR, exist_ok=True)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

# ─── CONEXIÓN ────────────────────────────────────────────────────────────
load_dotenv("secrets.env")
ec2 = boto3.client("ec2", region_name="us-east-1")
_, public_ip, _ = fne.find_instance_by_name(ec2, INSTANCIA_EC2)

conn = fn.Neo4jConnection(
    uri=f"bolt://{public_ip}:7687",
    user=os.getenv("NEO4J_USER"),
    pwd=os.getenv("NEO4J_PASSWORD"),
    encrypted=False,
)


def q(cypher: str, **params) -> pd.DataFrame:
    """Ejecuta cypher con params por formato y devuelve DataFrame."""
    rendered = cypher.format(**params) if params else cypher
    return fn.neo4jToDataframe(rendered, conn)


def run_and_save(name: str, cypher: str, **params) -> pd.DataFrame:
    df = q(cypher, **params)
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"\n── {name}  (rows={len(df)})")
    print(df.head(20).to_string(index=False))
    print(f"   → {path}")
    return df


# ─── DIAGNÓSTICOS DE INTEGRIDAD ──────────────────────────────────────────
DIAG_1_ADJ_MIX = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(of:Oferta)
RETURN of.adjudicada AS adjudicada, count(*) AS n
"""

DIAG_2_ADJ_TYPE = """
MATCH (of:Oferta)
RETURN DISTINCT of.adjudicada AS valor
LIMIT 5
"""

DIAG_3_GAP_PAIRS = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(mia:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
WHERE mia.adjudicada = false
OPTIONAL MATCH (it)-[:PARA_ITEM]-(otra:Oferta)
WHERE otra <> mia AND otra.adjudicada = true
RETURN count(DISTINCT it)                                         AS items_promedon_perdio,
       count(DISTINCT CASE WHEN otra IS NOT NULL THEN it END)     AS items_con_ganadora_registrada
"""

DIAG_4_PRODUCT_PATH = """
MATCH (it:ItemLicitacion)-[r:ES_PRODUCTO_DE]-(n)
RETURN labels(n) AS destino, count(*) AS veces
ORDER BY veces DESC
"""


# ─── ANÁLISIS DE NEGOCIO ─────────────────────────────────────────────────
A1_EVOLUCION = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(of:Oferta)-[:PARA_ITEM]-(:ItemLicitacion)
      -[:REQUIERE_ITEM]-(lic:Licitacion)
WITH lic.year AS anio, lic.month AS mes,
     count(of)                                           AS n_ofertas,
     sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)      AS n_ganadas,
     sum(CASE WHEN of.adjudicada THEN of.precio_equiv_clp ELSE 0 END) AS revenue_clp
RETURN anio, mes, n_ofertas, n_ganadas,
       toFloat(n_ganadas)*100.0/n_ofertas AS win_rate,
       revenue_clp
ORDER BY anio, mes
"""

A2_COMPETIDORES = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
MATCH (competidor:Proveedor)-[:OFRECE]-(of_c:Oferta {{adjudicada:true}})-[:PARA_ITEM]-(it)
WHERE competidor.rut <> '{rut}'
RETURN competidor.rut           AS rut_competidor,
       competidor.nombre_legal  AS nombre_competidor,
       count(DISTINCT it)       AS veces_gano_en_mis_items,
       sum(of_c.precio_equiv_clp) AS revenue_capturado
ORDER BY revenue_capturado DESC
LIMIT 25
"""

A3_PORTAFOLIO = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(of:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
      -[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
WITH coalesce(pc.nombre_generico, pu.nombre_producto) AS producto, of
WITH producto,
     count(of)                                            AS ofertas,
     sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)       AS ganadas,
     sum(CASE WHEN of.adjudicada THEN of.precio_equiv_clp ELSE 0 END) AS revenue
RETURN producto, ofertas, ganadas,
       toFloat(ganadas)*100.0/ofertas AS win_rate,
       revenue
ORDER BY revenue DESC
"""

A4_COMPRADORES = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(of:Oferta)-[:PARA_ITEM]-(:ItemLicitacion)
      -[:REQUIERE_ITEM]-(:Licitacion)-[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
WITH org,
     count(of)                                           AS ofertas,
     sum(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)      AS ganadas,
     sum(CASE WHEN of.adjudicada THEN of.precio_equiv_clp ELSE 0 END) AS revenue
RETURN org.rut     AS rut_organismo,
       org.nombre  AS nombre_organismo,
       ofertas, ganadas,
       toFloat(ganadas)*100.0/ofertas AS win_rate,
       revenue
ORDER BY revenue DESC
"""

A5_WHITESPACE = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
WITH p, collect(DISTINCT pu) AS mis_productos_ungm

MATCH (pu:ProductoUngm)<-[:ES_PRODUCTO_DE]-(it:ItemLicitacion)
      <-[:REQUIERE_ITEM]-(lic:Licitacion)
      -[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
WHERE pu IN mis_productos_ungm
  AND NOT EXISTS {{
    MATCH (p)-[:OFRECE]-(:Oferta)-[:PARA_ITEM]-(:ItemLicitacion)
          -[:REQUIERE_ITEM]-(:Licitacion)-[:PUBLICA]-(:UnidadDeCompra)
          -[:TIENE_UNIDAD]-(org)
  }}
WITH org,
     count(DISTINCT lic)     AS licitaciones_relevantes,
     sum(lic.monto_estimado) AS monto_potencial_clp
RETURN org.rut     AS rut_organismo,
       org.nombre  AS nombre_organismo,
       licitaciones_relevantes,
       monto_potencial_clp
ORDER BY monto_potencial_clp DESC
LIMIT 40
"""

A6_RETENCION = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(of:Oferta {{adjudicada:true}})-[:PARA_ITEM]
      -(:ItemLicitacion)-[:REQUIERE_ITEM]-(:Licitacion)
      -[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
WITH org, count(DISTINCT of) AS adjudicaciones,
     sum(of.precio_equiv_clp) AS revenue
RETURN
  sum(CASE WHEN adjudicaciones = 1 THEN revenue ELSE 0 END)  AS revenue_one_shot,
  sum(CASE WHEN adjudicaciones > 1 THEN revenue ELSE 0 END)  AS revenue_recurrente,
  count(CASE WHEN adjudicaciones = 1 THEN org END)           AS clientes_one_shot,
  count(CASE WHEN adjudicaciones > 1 THEN org END)           AS clientes_recurrentes
"""


# ─── PRICING GAP vs TOP-3 COMPETIDORES ───────────────────────────────────
# ARENYS MED, COMERCIAL KENDALL, JOHNSON & JOHNSON
TOP3_RUTS = "['76.901.400-4', '77.237.150-0', '93.745.000-1']"

A7_GAP_POR_COMPETIDOR = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(mia:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
WHERE mia.adjudicada = false
MATCH (it)-[:PARA_ITEM]-(ganadora:Oferta {{adjudicada:true}})
MATCH (ganador:Proveedor)-[:OFRECE]-(ganadora)
WHERE ganador.rut IN {top3} AND ganadora.precio_unitario > 0
WITH ganador.rut           AS rut_competidor,
     ganador.nombre_legal  AS competidor,
     mia.precio_unitario   AS p_mia,
     ganadora.precio_unitario AS p_ganadora,
     ganadora.precio_equiv_clp AS revenue_perdido
RETURN rut_competidor, competidor,
       count(*)                                                               AS items_perdidos,
       avg((p_mia - p_ganadora) * 100.0 / p_ganadora)                         AS sobreprecio_pct_promedio,
       percentileCont((p_mia - p_ganadora) * 100.0 / p_ganadora, 0.5)         AS sobreprecio_pct_mediana,
       percentileCont((p_mia - p_ganadora) * 100.0 / p_ganadora, 0.25)        AS sobreprecio_pct_p25,
       percentileCont((p_mia - p_ganadora) * 100.0 / p_ganadora, 0.75)        AS sobreprecio_pct_p75,
       sum(revenue_perdido)                                                   AS revenue_perdido_clp
ORDER BY revenue_perdido_clp DESC
"""

A8_GAP_POR_COMPETIDOR_PRODUCTO = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(mia:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
WHERE mia.adjudicada = false
MATCH (it)-[:PARA_ITEM]-(ganadora:Oferta {{adjudicada:true}})
MATCH (ganador:Proveedor)-[:OFRECE]-(ganadora)
WHERE ganador.rut IN {top3} AND ganadora.precio_unitario > 0
OPTIONAL MATCH (it)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
WITH coalesce(pc.nombre_generico, pu.nombre_producto, 'Sin producto') AS producto,
     ganador.nombre_legal  AS competidor,
     mia, ganadora
WITH producto, competidor,
     count(*)                                                               AS items_perdidos,
     avg((mia.precio_unitario - ganadora.precio_unitario)
         * 100.0 / ganadora.precio_unitario)                                 AS sobreprecio_pct_promedio,
     percentileCont((mia.precio_unitario - ganadora.precio_unitario)
         * 100.0 / ganadora.precio_unitario, 0.5)                            AS sobreprecio_pct_mediana,
     sum(ganadora.precio_equiv_clp)                                          AS revenue_perdido_clp
WHERE items_perdidos >= 2
RETURN producto, competidor, items_perdidos,
       sobreprecio_pct_promedio, sobreprecio_pct_mediana, revenue_perdido_clp
ORDER BY revenue_perdido_clp DESC
LIMIT 60
"""

CENABAST_RUT = "61.608.700-2"

B1_CENABAST_PORTFOLIO_OVERLAP = """
// Productos que CENABAST demanda y Promedon también ofrece, con tamaño de la oportunidad
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu_mio:ProductoUngm)
WITH p, collect(DISTINCT pu_mio) AS mis_productos
MATCH (org:Organismo {{rut:'{cenabast}'}})
      -[:TIENE_UNIDAD]-(:UnidadDeCompra)-[:PUBLICA]-(lic:Licitacion)
      -[:REQUIERE_ITEM]-(it:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
WHERE pu IN mis_productos
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
WITH coalesce(pc.nombre_generico, pu.nombre_producto) AS producto,
     count(DISTINCT lic)                                      AS licitaciones,
     count(DISTINCT it)                                       AS items,
     sum(coalesce(lic.monto_estimado, 0))                     AS monto_estimado_clp,
     min(lic.fecha_cierre)                                    AS primera_fecha,
     max(lic.fecha_cierre)                                    AS ultima_fecha
RETURN producto, licitaciones, items, monto_estimado_clp,
       primera_fecha, ultima_fecha
ORDER BY monto_estimado_clp DESC
"""

B2_CENABAST_INCUMBENTES = """
// Quién le está ganando a Promedon en CENABAST (por producto)
MATCH (org:Organismo {{rut:'{cenabast}'}})
      -[:TIENE_UNIDAD]-(:UnidadDeCompra)-[:PUBLICA]-(:Licitacion)
      -[:REQUIERE_ITEM]-(it:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
MATCH (it)-[:PARA_ITEM]-(of:Oferta {{adjudicada:true}})
MATCH (ganador:Proveedor)-[:OFRECE]-(of)
// Solo productos que Promedon ofrece
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu)
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
WITH coalesce(pc.nombre_generico, pu.nombre_producto) AS producto,
     ganador.rut          AS rut_incumbente,
     ganador.nombre_legal AS incumbente,
     of
WITH producto, rut_incumbente, incumbente,
     count(of)                       AS items_ganados,
     sum(of.precio_equiv_clp)        AS revenue_capturado_clp
RETURN producto, rut_incumbente, incumbente, items_ganados, revenue_capturado_clp
ORDER BY revenue_capturado_clp DESC
LIMIT 80
"""

B3_CENABAST_PRECIOS_BENCHMARK = """
// Benchmark de precios CENABAST paga por producto (ofertas adjudicadas) vs. lo que Promedon cobra en otros compradores
MATCH (org:Organismo {{rut:'{cenabast}'}})
      -[:TIENE_UNIDAD]-(:UnidadDeCompra)-[:PUBLICA]-(:Licitacion)
      -[:REQUIERE_ITEM]-(it:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
MATCH (it)-[:PARA_ITEM]-(of_gana:Oferta {{adjudicada:true}})
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu)
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
WITH pu, pc, of_gana
OPTIONAL MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]
      -(mia:Oferta {{adjudicada:true}})-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu)
WITH coalesce(pc.nombre_generico, pu.nombre_producto) AS producto,
     collect(DISTINCT of_gana.precio_unitario) AS precios_cenabast,
     collect(DISTINCT mia.precio_unitario)     AS precios_promedon
RETURN producto,
       size(precios_cenabast)                           AS n_precios_cenabast,
       toInteger(apoc.coll.min(precios_cenabast))       AS cenabast_min,
       toInteger(apoc.coll.avg(precios_cenabast))       AS cenabast_avg,
       toInteger(apoc.coll.max(precios_cenabast))       AS cenabast_max,
       size(precios_promedon)                           AS n_precios_promedon,
       toInteger(apoc.coll.min(precios_promedon))       AS promedon_min,
       toInteger(apoc.coll.avg(precios_promedon))       AS promedon_avg,
       toInteger(apoc.coll.max(precios_promedon))       AS promedon_max
ORDER BY producto
"""

B4_CENABAST_PIPELINE_RECIENTE = """
// Licitaciones CENABAST con fecha de cierre en los últimos 18 meses (pipeline accionable)
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]
      -(:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu_mio:ProductoUngm)
WITH p, collect(DISTINCT pu_mio) AS mis_productos
MATCH (org:Organismo {{rut:'{cenabast}'}})
      -[:TIENE_UNIDAD]-(:UnidadDeCompra)-[:PUBLICA]-(lic:Licitacion)
      -[:REQUIERE_ITEM]-(it:ItemLicitacion)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
WHERE pu IN mis_productos
  AND lic.fecha_cierre >= '2025-01-01'
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
OPTIONAL MATCH (it)-[:PARA_ITEM]-(of:Oferta {{adjudicada:true}})
OPTIONAL MATCH (winner:Proveedor)-[:OFRECE]-(of)
WITH lic.id_licitacion                                   AS id_licitacion,
     lic.titulo                                          AS titulo,
     lic.fecha_cierre                                    AS fecha_cierre,
     lic.estado_licitacion                               AS estado,
     lic.monto_estimado                                  AS monto_estimado,
     coalesce(pc.nombre_generico, pu.nombre_producto)    AS producto,
     collect(DISTINCT winner.nombre_legal)               AS incumbentes
RETURN id_licitacion, titulo, fecha_cierre, estado,
       toInteger(monto_estimado) AS monto_estimado_clp,
       producto, incumbentes
ORDER BY fecha_cierre DESC
LIMIT 150
"""


A9_GAP_ITEMS_CRITICOS = """
MATCH (p:Proveedor {{rut:'{rut}'}})-[:OFRECE]-(mia:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
      -[:REQUIERE_ITEM]-(lic:Licitacion)
      -[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
WHERE mia.adjudicada = false
MATCH (it)-[:PARA_ITEM]-(ganadora:Oferta {{adjudicada:true}})
MATCH (ganador:Proveedor)-[:OFRECE]-(ganadora)
WHERE ganador.rut IN {top3} AND ganadora.precio_unitario > 0
OPTIONAL MATCH (it)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
OPTIONAL MATCH (pu)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)
RETURN lic.id_licitacion                                        AS id_licitacion,
       lic.fecha_cierre                                         AS fecha_cierre,
       org.nombre                                               AS comprador,
       coalesce(pc.nombre_generico, pu.nombre_producto)         AS producto,
       ganador.nombre_legal                                     AS competidor,
       toInteger(mia.precio_unitario)                           AS precio_promedon,
       toInteger(ganadora.precio_unitario)                      AS precio_ganador,
       toInteger((mia.precio_unitario - ganadora.precio_unitario)
                 * 100.0 / ganadora.precio_unitario)            AS sobreprecio_pct,
       toInteger(ganadora.precio_equiv_clp)                     AS revenue_perdido_clp
ORDER BY revenue_perdido_clp DESC
LIMIT 100
"""


# ─── PIPELINE ────────────────────────────────────────────────────────────
def main():
    print(f"=== ANÁLISIS PROMEDON ({PROMEDON_RUT}) ===")

    print("\n[1/4] DIAGNÓSTICOS DE INTEGRIDAD")
    diag1 = run_and_save("diag_1_adjudicada_mix",     DIAG_1_ADJ_MIX,     rut=PROMEDON_RUT)
    run_and_save("diag_2_adjudicada_type",            DIAG_2_ADJ_TYPE)
    diag3 = run_and_save("diag_3_pares_perdida_gana", DIAG_3_GAP_PAIRS,   rut=PROMEDON_RUT)
    run_and_save("diag_4_path_producto",              DIAG_4_PRODUCT_PATH)

    if not diag1.empty and set(diag1["adjudicada"].unique()) <= {True}:
        print("\n[!] Promedon solo tiene ofertas adjudicadas cargadas — pricing-gap no factible.")
    if not diag3.empty and diag3["items_con_ganadora_registrada"].iloc[0] == 0:
        print("[!] No hay ganadoras registradas en ítems perdidos — usar OrdenCompra como benchmark.")

    print("\n[2/4] ANÁLISIS COMERCIALES")
    run_and_save("a1_evolucion_mensual",      A1_EVOLUCION,    rut=PROMEDON_RUT)
    run_and_save("a2_competidores_top",       A2_COMPETIDORES, rut=PROMEDON_RUT)
    run_and_save("a3_portafolio_productos",   A3_PORTAFOLIO,   rut=PROMEDON_RUT)
    run_and_save("a4_compradores_top",        A4_COMPRADORES,  rut=PROMEDON_RUT)
    run_and_save("a5_whitespace_compradores", A5_WHITESPACE,   rut=PROMEDON_RUT)
    run_and_save("a6_retencion",              A6_RETENCION,    rut=PROMEDON_RUT)

    print("\n[3/4] PRICING GAP vs TOP-3 COMPETIDORES (ARENYS / KENDALL / J&J)")
    run_and_save("a7_gap_por_competidor",
                 A7_GAP_POR_COMPETIDOR,           rut=PROMEDON_RUT, top3=TOP3_RUTS)
    run_and_save("a8_gap_competidor_producto",
                 A8_GAP_POR_COMPETIDOR_PRODUCTO,  rut=PROMEDON_RUT, top3=TOP3_RUTS)
    run_and_save("a9_gap_items_criticos",
                 A9_GAP_ITEMS_CRITICOS,           rut=PROMEDON_RUT, top3=TOP3_RUTS)

    print("\n[4/4] PLAN CENABAST (whitespace #1)")
    run_and_save("b1_cenabast_portfolio_overlap",
                 B1_CENABAST_PORTFOLIO_OVERLAP,   rut=PROMEDON_RUT, cenabast=CENABAST_RUT)
    run_and_save("b2_cenabast_incumbentes",
                 B2_CENABAST_INCUMBENTES,         rut=PROMEDON_RUT, cenabast=CENABAST_RUT)
    run_and_save("b3_cenabast_precios_benchmark",
                 B3_CENABAST_PRECIOS_BENCHMARK,   rut=PROMEDON_RUT, cenabast=CENABAST_RUT)
    run_and_save("b4_cenabast_pipeline_reciente",
                 B4_CENABAST_PIPELINE_RECIENTE,   rut=PROMEDON_RUT, cenabast=CENABAST_RUT)

    print(f"\nOK. Resultados guardados en {OUTPUT_DIR}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
