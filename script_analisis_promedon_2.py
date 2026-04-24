"""
Matriz base A vs B (modelo U_AB) — Promedon en ChileCompra.

Genera UN SOLO CSV con granularidad (item × competidor B ≠ A).
Cada fila representa un ítem de U_AB para un competidor B:
    el ítem está en U_A (A participa) Y B también participa.

A partir de esta matriz se derivan en pandas todas las agregaciones
(producto, comprador, tiempo, tramo, etc.) sin volver a consultar Neo4j.
Toda comparación A vs B debe filtrarse por rut_B antes de agregar.
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
    rendered = cypher.format(**params) if params else cypher
    return fn.neo4jToDataframe(rendered, conn)


# ═════════════════════════════════════════════════════════════════════════
# MATRIZ BASE U_AB
# Una fila por (item, B). B = cualquier proveedor ≠ A que participa en el
# mismo item donde A también participa.
#
# Flags por item (idénticos para todos los B del mismo item):
#     A_gana, hubo_ganador, revenue_item_clp
# Flags específicos del par (item, B):
#     B_gana, precio_B, precio_B_ganador
#
# Para producir U_A (contexto): df.drop_duplicates("it_key")
# Para producir U_AB filtrado: df[df.rut_B == <rut>]
# ═════════════════════════════════════════════════════════════════════════
MATRIZ_BASE = """
MATCH (A:Proveedor {{rut:'{rut_A}'}})-[:OFRECE]-(of_a:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
MATCH (it)-[:PARA_ITEM]-(of_b:Oferta)-[:OFRECE]-(B:Proveedor)
WHERE B.rut <> '{rut_A}'

// Agregar ofertas al nivel (item, B): un ítem puede tener >1 oferta por proveedor
WITH it, B,
     avg(of_a.precio_unitario)                                            AS precio_A,
     max(CASE WHEN of_a.adjudicada THEN 1 ELSE 0 END)                     AS A_gana,
     avg(of_b.precio_unitario)                                            AS precio_B,
     max(CASE WHEN of_b.adjudicada THEN 1 ELSE 0 END)                     AS B_gana,
     max(CASE WHEN of_b.adjudicada THEN of_b.precio_unitario END)         AS precio_B_ganador

// Contexto del ítem
MATCH (it)-[:REQUIERE_ITEM]-(lic:Licitacion)
      -[:PUBLICA]-(:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
OPTIONAL MATCH (it)-[:ES_PRODUCTO_DE]-(pu:ProductoUngm)
OPTIONAL MATCH (it)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)

// Revenue del ítem: suma de precio_equiv_clp de ofertas adjudicadas
OPTIONAL MATCH (it)-[:PARA_ITEM]-(of_w:Oferta {{adjudicada:true}})
WITH it, B, precio_A, A_gana, precio_B, B_gana, precio_B_ganador,
     lic, org, pu, pc,
     sum(coalesce(of_w.precio_equiv_clp, 0))                AS revenue_item_clp,
     max(CASE WHEN of_w IS NOT NULL THEN 1 ELSE 0 END)      AS hubo_ganador

RETURN elementId(it)                AS it_key,
       lic.id_licitacion            AS id_licitacion,
       lic.fecha_cierre             AS fecha_cierre,
       lic.year                     AS anio,
       lic.month                    AS mes,
       org.rut                      AS rut_comprador,
       org.nombre                   AS nombre_comprador,
       pu.nombre_producto           AS producto_ungm,
       pc.nombre_generico           AS producto_canonico,
       coalesce(pc.nombre_generico, pu.nombre_producto) AS producto,
       B.rut                        AS rut_B,
       B.nombre_legal               AS nombre_B,
       toInteger(precio_A)          AS precio_A,
       toInteger(precio_B)          AS precio_B,
       toInteger(precio_B_ganador)  AS precio_B_ganador,
       A_gana,
       B_gana,
       CASE WHEN hubo_ganador = 1 AND A_gana = 0 AND B_gana = 0 THEN 1 ELSE 0 END AS otro_gana,
       toInteger(revenue_item_clp)  AS revenue_item_clp
"""


# ─── PIPELINE ────────────────────────────────────────────────────────────
def main():
    print(f"=== MATRIZ BASE A vs B — PROMEDON ({PROMEDON_RUT}) ===")

    df = q(MATRIZ_BASE, rut_A=PROMEDON_RUT)
    path = os.path.join(OUTPUT_DIR, "matriz_base_AB.csv")
    df.to_csv(path, index=False)

    n_items_UA = df["it_key"].nunique()
    n_pares    = len(df)
    n_B        = df["rut_B"].nunique()
    print(f"\nfilas (item×B)              : {n_pares:,}")
    print(f"items únicos en U_A         : {n_items_UA:,}")
    print(f"competidores B distintos    : {n_B:,}")
    print(f"\nhead:")
    print(df.head(10).to_string(index=False))
    print(f"\n→ {path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
