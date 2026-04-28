"""
Matriz base item × proveedor — Promedon en ChileCompra.

Genera UN SOLO CSV con granularidad (item × proveedor).
Universo: items donde Promedon (A) participa. Por cada item, una fila
por cada proveedor (incluido A) que ofertó en ese item.
"""
import os
from datetime import datetime

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
# MATRIZ BASE — granularidad (item × proveedor)
# Items donde Promedon (A) participa. Por cada item, una fila por cada
# proveedor (incluido A) que ofertó en ese item.
# ═════════════════════════════════════════════════════════════════════════
MATRIZ_BASE = """
// Items donde A participa
MATCH (A:Proveedor {{rut:'{rut_A}'}})-[:OFRECE]-(:Oferta)-[:PARA_ITEM]-(it:ItemLicitacion)
WITH DISTINCT it

// Por cada item, todos los proveedores que ofertaron
MATCH (it)-[:PARA_ITEM]-(of:Oferta)-[:OFRECE]-(p:Proveedor)

// Agregar ofertas al nivel (item, proveedor): puede haber >1 oferta por par
WITH it, p,
     sum(coalesce(of.cantidad, 0))                              AS num_unidades,
     avg(of.precio_unitario)                                    AS precio_unitario,
     max(CASE WHEN of.adjudicada THEN 1 ELSE 0 END)             AS flag_adjudicado

// Precio unitario ganador del item (igual para todas las filas del item)
OPTIONAL MATCH (it)-[:PARA_ITEM]-(of_w:Oferta {{adjudicada:true}})
WITH it, p, num_unidades, precio_unitario, flag_adjudicado,
     avg(of_w.precio_unitario)                                  AS precio_unitario_ganador

// Contexto del item (OPTIONAL para no perder ítems con cadena incompleta)
OPTIONAL MATCH (it)-[:REQUIERE_ITEM]-(lic:Licitacion)
OPTIONAL MATCH (lic)-[:PUBLICA]-(uc:UnidadDeCompra)-[:TIENE_UNIDAD]-(org:Organismo)
OPTIONAL MATCH (it)-[:ES_PRODUCTO_GENERICO]-(pc:ProductoCanonico)

RETURN lic.id_licitacion                   AS id_licitacion,
       lic.fecha_cierre                    AS fecha_cierre,
       lic.year                            AS anio,
       lic.month                           AS mes,
       org.rut                             AS rut_comprador,
       org.nombre                          AS nombre_comprador,
       pc.nombre_generico                  AS producto_canonico,
       p.rut                               AS rut_proveedor,
       p.nombre_legal                      AS nombre_proveedor,
       toInteger(num_unidades)             AS num_unidades,
       toInteger(precio_unitario)          AS precio_unitario,
       toInteger(precio_unitario_ganador)  AS precio_unitario_ganador,
       flag_adjudicado
"""


# ─── ANÁLISIS DERIVADOS (todo desde matriz_base) ─────────────────────────
ITEM_KEY = ["id_licitacion", "producto_canonico"]
SIN_PRODUCTO = "__SIN_PRODUCTO__"

# Partición de la tabla maestra por Rango WR a nivel producto.
WR_BINS = [
    ("40% <= WR",        0.40, 1.01),
    ("20% <= WR < 40%",  0.20, 0.40),
    ("5% <= WR < 20%",   0.05, 0.20),
    ("0% <= WR < 5%",    0.00, 0.05),
]
TOTAL_WR_LABEL = "Total"

# ─── UMBRALES PARA RECOMENDACIONES ───────────────────────────────────────
# Alineados con los bins de la tabla maestra (Total general — referencia):
# R2 = bin "5% <= WR < 20%", R3 ⊂ "0% <= WR < 5%".
WR_DILUCION_MIN          = 0.05   # bin "5% <= WR < 20%": cota inferior
WR_DILUCION_MAX          = 0.20   # bin "5% <= WR < 20%": cota superior
OFERTAS_ABANDONO_MIN     = 10
WR_ABANDONO_MAX          = 0.05   # bin "0% <= WR < 5%"
SOBREPRECIO_CARO_MIN     = 0.05   # +5% ⇒ frente PRECIO
SOBREPRECIO_BARATO_MAX   = -0.05  # −5% ⇒ frente NO-PRECIO
WR_CUENTA_MODELO_FACTOR  = 1.5    # WR > 1.5× WR agregado
OFERTAS_CUENTA_MODELO    = 20
OFERTAS_CUENTA_EXPANDIR  = 50
H2H_TOP_N                = 15
PRECIO_MIN_COMPETIDOR    = 10     # filtra ofertas ruido (precios $0/$1) en CLP
MIN_COMPETIDORES_HEADROOM = 3     # ≥3 competidores válidos ⇒ señal robusta para R9
HEADROOM_CAP_PCT         = 1.0    # cap a +100%: por encima es outlier o mismatch de producto


def derive_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["producto_canonico"] = df["producto_canonico"].fillna(SIN_PRODUCTO)
    df["revenue_estimado_clp"] = (
        df["num_unidades"].fillna(0).astype("int64")
        * df["precio_unitario"].fillna(0).astype("int64")
    )
    df["revenue_adj_clp"] = df["revenue_estimado_clp"] * df["flag_adjudicado"]
    return df


def a2_competidores_top(df_c: pd.DataFrame, top: int = 25) -> pd.DataFrame:
    df_w = df_c[df_c["flag_adjudicado"] == 1]
    g = df_w.groupby(["rut_proveedor", "nombre_proveedor"], dropna=False).agg(
        items_ganados=("flag_adjudicado", "size"),
        revenue_capturado_clp=("revenue_estimado_clp", "sum"),
    ).reset_index().sort_values("revenue_capturado_clp", ascending=False)
    return g.head(top)


def a3_portafolio_productos(df_p: pd.DataFrame) -> pd.DataFrame:
    g = df_p.groupby("producto_canonico", dropna=False).agg(
        ofertas=("flag_adjudicado", "size"),
        ganadas=("flag_adjudicado", "sum"),
        revenue_clp=("revenue_adj_clp", "sum"),
    ).reset_index()
    g["win_rate"] = (g["ganadas"] / g["ofertas"]).round(3)
    return g.sort_values("revenue_clp", ascending=False)


def a4_compradores_top(df_p: pd.DataFrame, top: int = 25) -> pd.DataFrame:
    g = df_p.groupby(["rut_comprador", "nombre_comprador"], dropna=False).agg(
        ofertas=("flag_adjudicado", "size"),
        ganadas=("flag_adjudicado", "sum"),
        revenue_clp=("revenue_adj_clp", "sum"),
    ).reset_index()
    g["win_rate"] = (g["ganadas"] / g["ofertas"]).round(3)
    return g.sort_values("revenue_clp", ascending=False).head(top)


def _winner_table(df: pd.DataFrame) -> pd.DataFrame:
    df_w = df[df["flag_adjudicado"] == 1][
        ITEM_KEY + ["rut_proveedor", "nombre_proveedor",
                    "precio_unitario", "num_unidades", "revenue_estimado_clp"]
    ].rename(columns={
        "rut_proveedor":        "rut_ganador",
        "nombre_proveedor":     "nombre_ganador",
        "precio_unitario":      "precio_ganador",
        "num_unidades":         "num_unidades_ganador",
        "revenue_estimado_clp": "revenue_ganador_clp",
    })
    return df_w.drop_duplicates(ITEM_KEY)


def _promedon_prices(df: pd.DataFrame) -> pd.DataFrame:
    df_p = df[df["rut_proveedor"] == PROMEDON_RUT]
    return df_p.groupby(ITEM_KEY, dropna=False, as_index=False).agg(
        precio_promedon=("precio_unitario", "mean"),
        promedon_gana=("flag_adjudicado", "max"),
    )


def a10_head_to_head(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Comparación directa Promedon vs sus principales competidores.
    Universo: items donde AMBOS (Promedon y el competidor) ofertaron.
    Para cada competidor calcula:
      - cuántas veces gana Promedon, el competidor, o un tercero
      - mediana sobreprecio Promedon vs competidor
      - cuántos items Promedon ofertó más caro / más barato que el competidor
    Top competidores se selecciona por revenue capturado en espacio Promedon.
    """
    df_c = df[df["rut_proveedor"] != PROMEDON_RUT]
    top_comp = a2_competidores_top(df_c, top=top_n)
    top_ruts = top_comp[["rut_proveedor", "nombre_proveedor"]].drop_duplicates()

    df_p_items = df[df["rut_proveedor"] == PROMEDON_RUT].groupby(
        ITEM_KEY, dropna=False, as_index=False
    ).agg(precio_promedon=("precio_unitario", "mean"))

    winners = _winner_table(df)[ITEM_KEY + ["rut_ganador"]]

    rows = []
    for _, comp in top_ruts.iterrows():
        rut_x, nombre_x = comp["rut_proveedor"], comp["nombre_proveedor"]
        df_x = df[df["rut_proveedor"] == rut_x].groupby(
            ITEM_KEY, dropna=False, as_index=False
        ).agg(precio_competidor=("precio_unitario", "mean"))

        h2h = df_p_items.merge(df_x, on=ITEM_KEY, how="inner").merge(
            winners, on=ITEM_KEY, how="left"
        )
        n = len(h2h)
        if n == 0:
            continue

        wins_p = int((h2h["rut_ganador"] == PROMEDON_RUT).sum())
        wins_x = int((h2h["rut_ganador"] == rut_x).sum())
        n_winner = int(h2h["rut_ganador"].notna().sum())
        wins_3 = n_winner - wins_p - wins_x
        sin_adj = n - n_winner

        h2h["sobreprecio_pct"] = (
            (h2h["precio_promedon"] - h2h["precio_competidor"])
            / h2h["precio_competidor"]
        )
        med_sobre = h2h["sobreprecio_pct"].median()
        p_caro = int((h2h["sobreprecio_pct"] > 0).sum())
        p_barato = int((h2h["sobreprecio_pct"] < 0).sum())

        rows.append({
            "rut_competidor":           rut_x,
            "nombre_competidor":        nombre_x,
            "items_h2h":                n,
            "items_con_adjudicacion":   n_winner,
            "promedon_gana":            wins_p,
            "promedon_gana_pct":        wins_p / n_winner if n_winner else 0,
            "competidor_gana":          wins_x,
            "competidor_gana_pct":      wins_x / n_winner if n_winner else 0,
            "tercero_gana":             wins_3,
            "tercero_gana_pct":         wins_3 / n_winner if n_winner else 0,
            "items_sin_adjudicar":      sin_adj,
            "mediana_sobreprecio_pct":  med_sobre,
            "items_promedon_mas_caro":  p_caro,
            "items_promedon_mas_barato": p_barato,
        })
    return pd.DataFrame(rows)


def a9_items_criticos(df: pd.DataFrame, top: int = 100) -> pd.DataFrame:
    df_pp = _promedon_prices(df)
    df_pp = df_pp[df_pp["promedon_gana"] == 0]
    df_w = _winner_table(df)
    df_w = df_w[df_w["rut_ganador"] != PROMEDON_RUT]
    df_j = df_w.merge(df_pp, on=ITEM_KEY, how="inner")
    df_j["sobreprecio_pct"] = (
        (df_j["precio_promedon"] - df_j["precio_ganador"]) / df_j["precio_ganador"]
    )
    cols = ITEM_KEY + [
        "rut_ganador", "nombre_ganador",
        "num_unidades_ganador", "precio_promedon", "precio_ganador",
        "sobreprecio_pct", "revenue_ganador_clp",
    ]
    return df_j.sort_values("revenue_ganador_clp", ascending=False)[cols].head(top)


def a11_pricing_headroom(df: pd.DataFrame) -> pd.DataFrame:
    """
    Items donde Promedon GANA sub-cotizando — pricing power desaprovechado.

    Universo R9 (con filtros de calidad explícitos):
      - Promedon adjudicado en el item.
      - ≥ MIN_COMPETIDORES_HEADROOM competidores con precio_unitario ≥
        PRECIO_MIN_COMPETIDOR (señal robusta; ≤ 2 competidores se descarta).
      - 0 < headroom_pct ≤ HEADROOM_CAP_PCT  (excluye outliers / mismatch de
        producto: > +100% típicamente indica que el "competidor" ofertó otro
        producto en la misma línea).

    Métricas:
      - precio_p25_competidor: percentil 25 de las ofertas de competidores
        válidas. Más robusto que el mínimo: tolera 1-2 outliers a la baja sin
        distorsionar.
      - headroom_pct = (precio_p25_competidor − precio_promedon) / precio_promedon.
        Cota superior conservadora del aumento de precio que aún habría ganado
        en un tender precio-puro contra el competidor del cuartil bajo.
      - dinero_dejado_clp = (precio_p25_competidor − precio_promedon) × num_unidades.
    """
    df_p_win = df[(df["rut_proveedor"] == PROMEDON_RUT) & (df["flag_adjudicado"] == 1)]
    df_p_win = df_p_win.groupby(ITEM_KEY, dropna=False, as_index=False).agg(
        precio_promedon=("precio_unitario", "mean"),
        num_unidades=("num_unidades", "sum"),
        revenue_promedon_clp=("revenue_estimado_clp", "sum"),
    )

    df_comp = df[
        (df["rut_proveedor"] != PROMEDON_RUT)
        & (df["precio_unitario"] >= PRECIO_MIN_COMPETIDOR)
    ]
    df_comp_agg = df_comp.groupby(ITEM_KEY, dropna=False, as_index=False).agg(
        precio_min_competidor=("precio_unitario", "min"),
        precio_p25_competidor=("precio_unitario", lambda s: s.quantile(0.25)),
        n_competidores=("rut_proveedor", "nunique"),
    )

    df_j = df_p_win.merge(df_comp_agg, on=ITEM_KEY, how="inner")
    df_j = df_j[df_j["precio_promedon"] > 0]
    df_j = df_j[df_j["n_competidores"] >= MIN_COMPETIDORES_HEADROOM]
    df_j["headroom_pct"] = (
        (df_j["precio_p25_competidor"] - df_j["precio_promedon"])
        / df_j["precio_promedon"]
    )
    df_j = df_j[
        (df_j["headroom_pct"] > 0) & (df_j["headroom_pct"] <= HEADROOM_CAP_PCT)
    ].copy()
    df_j["dinero_dejado_clp"] = (
        (df_j["precio_p25_competidor"] - df_j["precio_promedon"])
        * df_j["num_unidades"].fillna(0)
    ).astype("int64")
    return df_j


def data_quality_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resumen de calidad de datos: qué proporción de la matriz cumple los
    requisitos para sostener cada análisis. Se materializa como sección
    explícita en el reporte para que las decisiones de pricing no se
    tomen sobre un universo sucio.
    """
    n_filas = len(df)
    n_items = df.groupby(ITEM_KEY, dropna=False).ngroups

    df_items = df.drop_duplicates(ITEM_KEY)
    sin_prod_mask = (
        (df_items["producto_canonico"] == SIN_PRODUCTO)
        | df_items["producto_canonico"].isna()
    )
    n_items_sin_prod = int(sin_prod_mask.sum())
    pct_sin_prod = n_items_sin_prod / n_items if n_items else 0

    mask_comp = df["rut_proveedor"] != PROMEDON_RUT
    n_ofertas_comp = int(mask_comp.sum())
    n_ofertas_comp_ruido = int(
        (mask_comp & (df["precio_unitario"] < PRECIO_MIN_COMPETIDOR)).sum()
    )
    pct_ruido = n_ofertas_comp_ruido / n_ofertas_comp if n_ofertas_comp else 0

    p_wins = df[(df["rut_proveedor"] == PROMEDON_RUT) & (df["flag_adjudicado"] == 1)]
    p_wins_items = p_wins.drop_duplicates(ITEM_KEY)
    n_p_wins = len(p_wins_items)

    df_comp_ok = df[
        (df["rut_proveedor"] != PROMEDON_RUT)
        & (df["precio_unitario"] >= PRECIO_MIN_COMPETIDOR)
    ]
    comp_count = (
        df_comp_ok.groupby(ITEM_KEY, dropna=False)["rut_proveedor"]
        .nunique()
        .rename("n_comp_validos")
        .reset_index()
    )
    p_with_comp = p_wins_items.merge(comp_count, on=ITEM_KEY, how="left")
    p_with_comp["n_comp_validos"] = p_with_comp["n_comp_validos"].fillna(0)
    n_p_con_1 = int((p_with_comp["n_comp_validos"] >= 1).sum())
    n_p_con_3 = int((p_with_comp["n_comp_validos"] >= MIN_COMPETIDORES_HEADROOM).sum())

    a11 = a11_pricing_headroom(df)
    n_r9 = len(a11)
    n_r9_excluidos_outlier = max(n_p_con_3 - n_r9, 0)

    rows = [
        ("Filas matriz_base (item × proveedor)",            n_filas,                ""),
        ("Items distintos (licitación × producto)",         n_items,                ""),
        ("Items SIN producto canónico identificado",        n_items_sin_prod,       f"{pct_sin_prod:.1%}"),
        ("Ofertas de competidores",                          n_ofertas_comp,         ""),
        (f"  → con precio < ${PRECIO_MIN_COMPETIDOR} (excluidas como ruido)",
                                                             n_ofertas_comp_ruido,   f"{pct_ruido:.1%}"),
        ("Items ganados por Promedon",                       n_p_wins,               ""),
        ("  → con ≥ 1 competidor válido",                    n_p_con_1,              f"{(n_p_con_1/n_p_wins) if n_p_wins else 0:.1%}"),
        (f"  → con ≥ {MIN_COMPETIDORES_HEADROOM} competidores válidos (universo bruto R9)",
                                                             n_p_con_3,              f"{(n_p_con_3/n_p_wins) if n_p_wins else 0:.1%}"),
        (f"  → excluidos por headroom > {HEADROOM_CAP_PCT:.0%} (outlier / mismatch)",
                                                             n_r9_excluidos_outlier, ""),
        ("**Universo final R9 (pricing headroom)**",         n_r9,                   f"{(n_r9/n_p_wins) if n_p_wins else 0:.1%}"),
    ]
    return pd.DataFrame(rows, columns=["Métrica", "Valor", "% sobre base"])


# ─── RECOMENDACIONES PRESCRIPTIVAS ───────────────────────────────────────
def build_recomendaciones(df: pd.DataFrame, df_p: pd.DataFrame, df_c: pd.DataFrame) -> dict:
    """Identifica acciones priorizadas con sus criterios cuantitativos y evidencia."""
    a3 = a3_portafolio_productos(df_p)

    dilucion_full = a3[
        (a3["win_rate"] >= WR_DILUCION_MIN)
        & (a3["win_rate"] < WR_DILUCION_MAX)
        & (a3["producto_canonico"] != SIN_PRODUCTO)
    ].sort_values("ofertas", ascending=False)
    dilucion = dilucion_full.head(20)

    cero_wr_full = a3[
        (a3["ofertas"] >= OFERTAS_ABANDONO_MIN)
        & (a3["win_rate"] < WR_ABANDONO_MAX)
        & (a3["producto_canonico"] != SIN_PRODUCTO)
    ].sort_values("ofertas", ascending=False)
    cero_wr = cero_wr_full.head(20)

    h2h = a10_head_to_head(df, top_n=H2H_TOP_N)
    cols_h2h = [
        "nombre_competidor", "items_h2h", "items_con_adjudicacion",
        "promedon_gana", "promedon_gana_pct",
        "competidor_gana", "competidor_gana_pct",
        "tercero_gana", "tercero_gana_pct",
        "mediana_sobreprecio_pct",
        "items_promedon_mas_caro", "items_promedon_mas_barato",
    ]
    h2h_caro = h2h[h2h["mediana_sobreprecio_pct"] > SOBREPRECIO_CARO_MIN].sort_values(
        "competidor_gana", ascending=False
    )[cols_h2h]
    h2h_barato = h2h[h2h["mediana_sobreprecio_pct"] < SOBREPRECIO_BARATO_MAX].sort_values(
        "competidor_gana", ascending=False
    )[cols_h2h]

    items_top = a9_items_criticos(df, top=20)

    a4 = a4_compradores_top(df_p, top=100)
    bm = (df_p["flag_adjudicado"].sum() / len(df_p)) if len(df_p) else 0
    cuentas_modelo = a4[
        (a4["ofertas"] >= OFERTAS_CUENTA_MODELO)
        & (a4["win_rate"] > bm * WR_CUENTA_MODELO_FACTOR)
    ].sort_values("revenue_clp", ascending=False).head(15)
    cuentas_expandir = a4[
        (a4["ofertas"] >= OFERTAS_CUENTA_EXPANDIR)
        & (a4["win_rate"] < bm)
    ].sort_values("ofertas", ascending=False).head(15)

    a11_full = a11_pricing_headroom(df)
    a11_resumen = pd.DataFrame([{
        "items_subcotizados":      len(a11_full),
        "productos_afectados":     a11_full["producto_canonico"].nunique() if len(a11_full) else 0,
        "mediana_headroom_pct":    a11_full["headroom_pct"].median() if len(a11_full) else 0,
        "promedio_headroom_pct":   a11_full["headroom_pct"].mean()   if len(a11_full) else 0,
        "dinero_dejado_total_clp": int(a11_full["dinero_dejado_clp"].sum()) if len(a11_full) else 0,
        "revenue_total_clp":       int(a11_full["revenue_promedon_clp"].sum()) if len(a11_full) else 0,
    }])

    if len(a11_full):
        a11_productos = a11_full.groupby("producto_canonico", dropna=False).agg(
            items_subcotizados=("headroom_pct", "size"),
            mediana_headroom_pct=("headroom_pct", "median"),
            mediana_precio_promedon=("precio_promedon", "median"),
            mediana_precio_p25_competidor=("precio_p25_competidor", "median"),
            dinero_dejado_clp=("dinero_dejado_clp", "sum"),
            revenue_capturado_clp=("revenue_promedon_clp", "sum"),
        ).reset_index().sort_values("dinero_dejado_clp", ascending=False).head(20)
        a11_productos = a11_productos[
            a11_productos["producto_canonico"] != SIN_PRODUCTO
        ]
    else:
        a11_productos = pd.DataFrame()

    a11_top_items = a11_full.sort_values("dinero_dejado_clp", ascending=False).head(15)[
        ITEM_KEY + [
            "precio_promedon", "precio_p25_competidor", "precio_min_competidor",
            "headroom_pct", "num_unidades", "n_competidores",
            "dinero_dejado_clp", "revenue_promedon_clp",
        ]
    ]

    return {
        "dilucion":         dilucion,
        "dilucion_full":    dilucion_full,
        "cero_wr":          cero_wr,
        "cero_wr_full":     cero_wr_full,
        "h2h_caro":         h2h_caro,
        "h2h_barato":       h2h_barato,
        "items_top":        items_top,
        "cuentas_modelo":   cuentas_modelo,
        "cuentas_expandir": cuentas_expandir,
        "benchmark_wr":     bm,
        "a11_resumen":      a11_resumen,
        "a11_productos":    a11_productos,
        "a11_top_items":    a11_top_items,
    }


def render_executive_summary(rec: dict, df_p: pd.DataFrame) -> list:
    total_revenue = int(df_p["revenue_adj_clp"].sum())
    total_ofertas = len(df_p)

    of_dilucion  = int(rec["dilucion_full"]["ofertas"].sum()) if len(rec["dilucion_full"]) else 0
    rev_dilucion = int(rec["dilucion_full"]["revenue_clp"].sum()) if len(rec["dilucion_full"]) else 0
    of_cero      = int(rec["cero_wr_full"]["ofertas"].sum())  if len(rec["cero_wr_full"])  else 0
    rev_items    = int(rec["items_top"]["revenue_ganador_clp"].sum()) if len(rec["items_top"]) else 0
    bm = rec["benchmark_wr"]

    a11r = rec["a11_resumen"].iloc[0] if len(rec["a11_resumen"]) else None
    if a11r is not None and int(a11r["items_subcotizados"]) > 0:
        n_items   = int(a11r["items_subcotizados"])
        n_prods   = int(a11r["productos_afectados"])
        med_hr    = a11r["mediana_headroom_pct"]
        dinero    = int(a11r["dinero_dejado_total_clp"])
        top_prods = rec["a11_productos"].head(3)["producto_canonico"].tolist()
        prods_str = ", ".join(top_prods) if top_prods else "—"
        r9_line = (
            f"- **R9 Subir precio donde sobra headroom** — en {n_items:,} items "
            f"({n_prods:,} productos) con ≥ {MIN_COMPETIDORES_HEADROOM} competidores "
            f"válidos, Promedon ganó **por debajo del P25 competitivo**. Headroom mediano: "
            f"{med_hr:.1%} (capeado a {HEADROOM_CAP_PCT:.0%} para excluir outliers). "
            f"Dinero dejado en mesa: **${dinero:,} CLP** "
            f"({_pct_of(dinero, total_revenue)} del revenue Promedon — upside de revisar lista). "
            f"Top productos donde subir lista: {prods_str}."
        )
    else:
        r9_line = (
            f"- **R9 Subir precio donde sobra headroom** — sin items con ≥ "
            f"{MIN_COMPETIDORES_HEADROOM} competidores y sub-cotización válida tras "
            "filtros de calidad."
        )

    bullets = [
        f"- **R2 Recortar zona de dilución** (bin `{WR_DILUCION_MIN:.0%} <= WR < {WR_DILUCION_MAX:.0%}`) — {len(rec['dilucion_full'])} productos absorben **{of_dilucion:,} ofertas** ({_pct_of(of_dilucion, total_ofertas)} del total Promedon) y devuelven sólo ${rev_dilucion:,} CLP ({_pct_of(rev_dilucion, total_revenue)} del total). La zona de dilución es **cola larga**, no productos puntuales: capacidad comercial dispersa con baja conversión.",
        f"- **R3 Abandono** (bin `0% <= WR < {WR_ABANDONO_MAX:.0%}`) — {len(rec['cero_wr_full'])} productos con WR < {WR_ABANDONO_MAX:.0%} y {of_cero:,} ofertas ({_pct_of(of_cero, total_ofertas)} de las ofertas Promedon): casi cero conversión, sin evidencia de ventaja competitiva.",
        f"- **R4 Frente PRECIO** — {len(rec['h2h_caro'])} competidores donde Promedon es ≥ {SOBREPRECIO_CARO_MIN:.0%} más caro y pierde. Recuperable con ajuste tarifario selectivo.",
        f"- **R5 Frente NO-PRECIO** — {len(rec['h2h_barato'])} competidores donde Promedon es ≤ {SOBREPRECIO_BARATO_MAX:.0%} más barato y aun así pierde. Decisión binaria: invertir en capacidades no-precio o salir.",
        f"- **R6 Defender top licitaciones** — {len(rec['items_top'])} items concentran ${rev_items:,} CLP capturados por terceros ({_pct_of(rev_items, total_revenue)} del total Promedon, como referencia de magnitud). Due diligence cuenta por cuenta.",
        f"- **R7 Replicar cuentas modelo** — {len(rec['cuentas_modelo'])} compradores con WR > {bm * WR_CUENTA_MODELO_FACTOR:.0%} (1.5× del agregado {bm:.1%}). Documentar diferencial.",
        f"- **R8 Expandir cuentas con conversión baja** — {len(rec['cuentas_expandir'])} cuentas con ≥ {OFERTAS_CUENTA_EXPANDIR} ofertas y WR < {bm:.1%}. Alta exposición sin retorno.",
        r9_line,
    ]
    return ["## Executive Summary", "", *bullets, ""]


def render_recomendaciones(rec: dict) -> list:
    out = ["## Recomendaciones priorizadas", ""]
    out.append(
        "Cada recomendación se deriva automáticamente de `df` con un criterio "
        "cuantitativo explícito. La evidencia lista los productos / competidores "
        "/ items / compradores exactos que disparan la recomendación."
    )
    out.append("")

    f_port = {"revenue_clp": _fmt_clp, "win_rate": _fmt_pct}
    f_h2h = {
        "promedon_gana_pct":       _fmt_pct,
        "competidor_gana_pct":     _fmt_pct,
        "tercero_gana_pct":        _fmt_pct,
        "mediana_sobreprecio_pct": _fmt_pct,
    }
    f_items = {
        "revenue_ganador_clp": _fmt_clp,
        "sobreprecio_pct":     _fmt_pct,
        "precio_promedon":     _fmt_clp,
        "precio_ganador":      _fmt_clp,
    }

    # R2 — vista agregada del bin completo + cola larga
    of_total_dil  = int(rec["dilucion_full"]["ofertas"].sum())     if len(rec["dilucion_full"]) else 0
    rev_total_dil = int(rec["dilucion_full"]["revenue_clp"].sum()) if len(rec["dilucion_full"]) else 0
    n_prods_dil   = len(rec["dilucion_full"])
    out += [
        f"### R2 — Recortar zona de dilución ({n_prods_dil} productos en el bin)",
        "",
        f"**Bin de referencia:** `{WR_DILUCION_MIN:.0%} <= WR < {WR_DILUCION_MAX:.0%}` "
        "en la tabla maestra (excluyendo `Sin producto canónico`). R2 cubre el bin "
        "**completo**, no un subconjunto: la dilución no es un fenómeno de pocos "
        "productos con muchas ofertas, sino una **cola larga** de productos con "
        "conversión baja que en agregado consumen capacidad comercial relevante.",
        "",
        f"**Criterio:** {WR_DILUCION_MIN:.0%} ≤ WR < {WR_DILUCION_MAX:.0%}, sin "
        "umbral mínimo de ofertas (la dilución se mide en el agregado del bin, "
        "no producto a producto).",
        "",
        f"**Diagnóstico (agregado del bin):** {n_prods_dil} productos absorben "
        f"**{of_total_dil:,} ofertas** y devuelven ${rev_total_dil:,} CLP. ROI "
        f"agregado ≈ ${(rev_total_dil // of_total_dil if of_total_dil else 0):,} "
        "CLP/oferta, bien por debajo del promedio Promedon.",
        "",
        "**Acción:** revisar el mix del bin como bloque — qué líneas / categorías "
        "concentran la cola larga, dónde tiene sentido seguir cotizando y dónde "
        "salir. La evidencia muestra los **top 20 productos por ofertas absorbidas** "
        "(cabeza de la cola larga, donde está la mayor parte de la capacidad).",
        "",
        "**Evidencia (top 20 productos del bin por ofertas absorbidas):**",
        "",
        _df_to_md(rec["dilucion"], fmt=f_port),
        "",
    ]

    # R3
    of = int(rec["cero_wr"]["ofertas"].sum()) if len(rec["cero_wr"]) else 0
    out += [
        f"### R3 — Abandono: WR < {WR_ABANDONO_MAX:.0%} con volumen ({len(rec['cero_wr'])} productos)",
        "",
        f"**Bin de referencia:** `0% <= WR < {WR_ABANDONO_MAX:.0%}` en la tabla "
        "maestra. R3 es el subconjunto del bin con volumen de ofertas relevante.",
        "",
        f"**Criterio:** ofertas ≥ {OFERTAS_ABANDONO_MIN} y WR < {WR_ABANDONO_MAX:.0%}.",
        "",
        f"**Diagnóstico:** {of:,} ofertas con conversión menor a 1 en 20. La "
        "señal es estructural, no de muestra: no hay ventaja competitiva visible "
        "y se invierte capacidad comercial sin retorno proporcional.",
        "",
        "**Acción:** salida inmediata o restructuración del enfoque (mix, "
        "especificación) antes de seguir cotizando.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["cero_wr"], fmt=f_port),
        "",
    ]

    # R4
    out += [
        f"### R4 — Frente PRECIO contra competidores donde Promedon cobra de más ({len(rec['h2h_caro'])} competidores)",
        "",
        f"**Criterio:** mediana_sobreprecio_pct > +{SOBREPRECIO_CARO_MIN:.0%} en items "
        "donde ambos competimos. Significa que Promedon es sistemáticamente más caro.",
        "",
        "**Diagnóstico:** la pérdida es atribuible a precio. Es **recuperable**: ajuste "
        "tarifario selectivo recupera revenue sin tocar producto.",
        "",
        "**Acción:** evaluar reducción de lista de precios 10–20% en las categorías "
        "donde estos competidores concentran sus victorias.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["h2h_caro"], fmt=f_h2h),
        "",
    ]

    # R5
    out += [
        f"### R5 — Frente NO-PRECIO ({len(rec['h2h_barato'])} competidores)",
        "",
        f"**Criterio:** mediana_sobreprecio_pct < {SOBREPRECIO_BARATO_MAX:.0%} en items "
        "donde ambos competimos. Significa que Promedon **es más barato** y aun así pierde.",
        "",
        "**Diagnóstico:** bajar precio NO recupera estos items. La pérdida es por "
        "marca, dossier técnico, especificación en bases o relación con el comprador.",
        "",
        "**Acción:** decisión binaria por competidor — (a) invertir en capacidades "
        "no-precio (medical-science-liaison, dossier, especificación temprana en "
        "bases) o (b) **salir conscientemente** de las categorías afectadas. "
        "Reinvertir esa capacidad comercial donde Promedon sí tiene ventaja.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["h2h_barato"], fmt=f_h2h),
        "",
    ]

    # R6
    rev = int(rec["items_top"]["revenue_ganador_clp"].sum()) if len(rec["items_top"]) else 0
    out += [
        f"### R6 — Licitaciones individuales de mayor daño ({len(rec['items_top'])} items, ${rev:,} CLP)",
        "",
        "**Criterio:** items donde Promedon ofertó y perdió, ordenados por revenue "
        "capturado por el ganador.",
        "",
        "**Diagnóstico:** la pérdida está **concentrada**: pocos items explican gran "
        "parte del revenue ido a competidores. Atención focalizada por cliente tiene "
        "alto leverage.",
        "",
        "**Acción:** due diligence cuenta por cuenta — decisor clínico, ciclo de "
        "renovación, condiciones del contrato vigente del incumbente. Preparar bid "
        "ganador para próxima iteración.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["items_top"], fmt=f_items),
        "",
    ]

    # R7
    bm = rec["benchmark_wr"]
    out += [
        f"### R7 — Cuentas modelo a replicar ({len(rec['cuentas_modelo'])})",
        "",
        f"**Criterio:** ofertas ≥ {OFERTAS_CUENTA_MODELO} y WR > {bm * WR_CUENTA_MODELO_FACTOR:.1%} "
        f"(≥ {WR_CUENTA_MODELO_FACTOR:.1f}× del agregado, que es {bm:.1%}).",
        "",
        "**Diagnóstico:** estos compradores adjudican a Promedon a tasa "
        "atípicamente alta. Debe haber un diferencial replicable: KAM, especificación "
        "técnica, relación clínica, mix de producto.",
        "",
        "**Acción:** documentar el diferencial en cada cuenta modelo. Convertirlo en "
        "playbook y aplicarlo a las cuentas de R8.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["cuentas_modelo"], fmt=f_port),
        "",
    ]

    # R8
    out += [
        f"### R8 — Cuentas a expandir ({len(rec['cuentas_expandir'])})",
        "",
        f"**Criterio:** ofertas ≥ {OFERTAS_CUENTA_EXPANDIR} y WR < {bm:.1%} "
        "(debajo del agregado).",
        "",
        "**Diagnóstico:** alta exposición comercial sin retorno proporcional. La "
        "relación existe (cotizamos mucho) pero la conversión es baja. Hay un "
        "problema específico de cuenta: mix producto, precio, especificación, o "
        "fit con el incumbente.",
        "",
        "**Acción:** investigar la causa por cuenta antes de invertir más. Aplicar "
        "el playbook de R7. Si la causa es estructural (specs incompatibles), "
        "replantear el mix de productos cotizados a esa cuenta.",
        "",
        "**Evidencia:**",
        "",
        _df_to_md(rec["cuentas_expandir"], fmt=f_port),
        "",
    ]

    # R9 — Subir precio donde sobra headroom (productos sub-cotizados)
    f_a11_resumen = {
        "mediana_headroom_pct":    _fmt_pct,
        "promedio_headroom_pct":   _fmt_pct,
        "dinero_dejado_total_clp": _fmt_clp,
        "revenue_total_clp":       _fmt_clp,
    }
    f_a11_productos = {
        "mediana_headroom_pct":          _fmt_pct,
        "mediana_precio_promedon":       _fmt_clp,
        "mediana_precio_p25_competidor": _fmt_clp,
        "dinero_dejado_clp":             _fmt_clp,
        "revenue_capturado_clp":         _fmt_clp,
    }
    f_a11_items = {
        "precio_promedon":       _fmt_clp,
        "precio_p25_competidor": _fmt_clp,
        "precio_min_competidor": _fmt_clp,
        "headroom_pct":          _fmt_pct,
        "dinero_dejado_clp":     _fmt_clp,
        "revenue_promedon_clp":  _fmt_clp,
    }
    n_prods = len(rec["a11_productos"])
    dinero_total = int(rec["a11_resumen"].iloc[0]["dinero_dejado_total_clp"]) if len(rec["a11_resumen"]) else 0
    out += [
        f"### R9 — Subir precio donde sobra headroom ({n_prods} productos top)",
        "",
        f"**Criterio:** items donde Promedon **gana** y existen ≥ {MIN_COMPETIDORES_HEADROOM} "
        f"competidores con precio_unitario ≥ ${PRECIO_MIN_COMPETIDOR}. El benchmark "
        "competitivo es el **percentil 25** de las ofertas de competidores válidas (no "
        "el mínimo, que se vuelve sensible a un solo outlier). "
        "`headroom_pct = (precio_p25_competidor − precio_promedon) / precio_promedon`, "
        f"capeado a {HEADROOM_CAP_PCT:.0%} (por encima se considera mismatch de producto). "
        "`dinero_dejado_clp = (precio_p25_competidor − precio_promedon) × num_unidades`.",
        "",
        "**Diagnóstico:** Promedon ganó cobrando **menos** que el cuartil bajo de los "
        "competidores que sí pueden servir el ítem. La oferta fue innecesariamente "
        "agresiva: pudo subir hasta el P25 competitivo y, en un tender precio-puro, "
        f"habría ganado igual. En agregado, **${dinero_total:,} CLP** de revenue "
        "quedaron en la mesa por sub-cotizar.",
        "",
        "**Por qué P25 y no el mínimo:** el mínimo se contamina con un solo outlier "
        "(otro proveedor que ofertó un producto distinto en la misma línea, o erró el "
        "precio). El P25 exige que **al menos 2 de cada 4 competidores** estén por "
        "encima — señal estructural, no anecdótica.",
        "",
        "**Acción:** (a) revisar lista de precios de los productos del top — el "
        "headroom mediano indica el aumento porcentual que la data soporta sin "
        "comprometer la victoria contra un cuartil completo de competidores; (b) en "
        "tenders ponderados (técnico + precio) donde Promedon gana técnico, el alza "
        "puede ser aún mayor; (c) priorizar productos por `dinero_dejado_clp`, no por "
        "`headroom_pct` (importa la magnitud absoluta, no el porcentaje).",
        "",
        "**Resumen agregado:**",
        "",
        _df_to_md(rec["a11_resumen"], fmt=f_a11_resumen),
        "",
        "**Productos top por dinero dejado en mesa:**",
        "",
        _df_to_md(rec["a11_productos"], fmt=f_a11_productos),
        "",
        "**Items individuales con mayor sub-cotización:**",
        "",
        _df_to_md(rec["a11_top_items"], fmt=f_a11_items),
        "",
    ]
    return out


# ─── REPORTE MARKDOWN ────────────────────────────────────────────────────
def _fmt_clp(v):
    return "" if pd.isna(v) else f"${int(v):,}"


def _fmt_pct(v):
    return "" if pd.isna(v) else f"{v:.1%}"


def _df_to_md(df: pd.DataFrame, fmt: dict | None = None) -> str:
    fmt = fmt or {}
    headers = list(df.columns)
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    for _, r in df.iterrows():
        cells = []
        for c in headers:
            v = r[c]
            if c in fmt:
                cells.append(fmt[c](v))
            elif pd.isna(v):
                cells.append("")
            elif isinstance(v, float):
                cells.append(f"{v:,.2f}")
            elif isinstance(v, (int,)) or hasattr(v, "item"):
                cells.append(f"{int(v):,}")
            else:
                cells.append(str(v).replace("|", "\\|"))
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def _pct_of(value: float, total: float) -> str:
    return "0%" if not total else f"{value / total:.1%}"


def _per_producto_universo_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stats por producto para TODOS los proveedores, restringido a items donde
    Promedon ofertó. Distingue 'mercado' (sin sufijo) de 'Promedon' (sólo
    nuestras ofertas) en la tabla maestra.
    """
    p_items = (
        df[df["rut_proveedor"] == PROMEDON_RUT][ITEM_KEY].drop_duplicates()
    )
    df_universe = df.merge(p_items, on=ITEM_KEY, how="inner")
    return df_universe.groupby(
        "producto_canonico", dropna=False, as_index=False
    ).agg(
        ofertas_total=("flag_adjudicado", "size"),
        ganadas_total=("flag_adjudicado", "sum"),
        revenue_total_clp=("revenue_adj_clp", "sum"),
    )


def _label_wr(wr: float):
    for label, lo, hi in WR_BINS:
        if lo <= wr < hi:
            return label
    return None


def _cell_row(wr_lab: str, sub: pd.DataFrame) -> dict:
    of_p      = int(sub["ofertas"].sum())
    adj_p     = int(sub["ganadas"].sum())
    rev_p     = int(sub["revenue_clp"].sum())
    rev_total = int(sub["revenue_total_clp"].sum())
    return {
        "Rango WR Promedon x Producto":          wr_lab,
        "Num Ofertas Presentadas":               int(sub["ofertas_total"].sum()),
        "Num Ofertas Presentadas Promedon":      of_p,
        "Num Ofertas Adjudicadas":               int(sub["ganadas_total"].sum()),
        "Num Ofertas Adjudicadas Promedon":      adj_p,
        "Monto Adjudicado Total":                rev_total,
        "Monto Adjudicado Promedon":             rev_p,
        "Tasa Adjudicación Promedon":            (adj_p / of_p) if of_p else 0,
        "Tasa Monto Adjudicación Promedon":      (rev_p / rev_total) if rev_total else 0,
    }


def build_tabla_maestra(df: pd.DataFrame, df_p: pd.DataFrame) -> pd.DataFrame:
    """
    Tabla maestra: partición exhaustiva de productos por Rango WR (a nivel
    producto). Cada producto cae en una única fila; las filas suman exacto
    al Total Promedon (última fila).
    Columnas sin sufijo = mercado total (todos los proveedores en items
    donde Promedon ofertó); con sufijo 'Promedon' = sólo Promedon.
    """
    a3 = a3_portafolio_productos(df_p)
    universo = _per_producto_universo_mercado(df)
    prod = a3.merge(universo, on="producto_canonico", how="left")
    for col in ("ofertas_total", "ganadas_total", "revenue_total_clp"):
        prod[col] = prod[col].fillna(0).astype("int64")

    prod["wr_label"] = prod["win_rate"].apply(_label_wr)

    rows = []
    for wr_lab, _, _ in WR_BINS:
        rows.append(_cell_row(wr_lab, prod[prod["wr_label"] == wr_lab]))
    rows.append(_cell_row(TOTAL_WR_LABEL, prod))
    return pd.DataFrame(rows)


def build_diagnostico_md(df: pd.DataFrame, df_p: pd.DataFrame, df_c: pd.DataFrame) -> str:
    n_ofertas = len(df_p)
    n_adj     = int(df_p["flag_adjudicado"].sum())
    rev_total = int(df_p["revenue_adj_clp"].sum())
    wr        = n_adj / n_ofertas if n_ofertas else 0

    rec = build_recomendaciones(df, df_p, df_c)
    dq = data_quality_summary(df)

    header = [
        "# Diagnóstico competitivo Promedon — ChileCompra",
        "",
        f"**Proveedor analizado:** PROMEDON · RUT {PROMEDON_RUT}",
        "**Fuente:** Grafo Neo4j ChileCompra (`matriz_base` item × proveedor)",
        f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "| Métrica baseline | Valor |",
        "|---|---:|",
        f"| Filas matriz_base (item × proveedor) | {len(df):,} |",
        f"| Licitaciones distintas | {df['id_licitacion'].nunique():,} |",
        f"| Proveedores distintos | {df['rut_proveedor'].nunique():,} |",
        f"| Ofertas Promedon | {n_ofertas:,} |",
        f"| Ofertas adjudicadas | {n_adj:,} |",
        f"| **Win rate agregado** | **{wr:.1%}** |",
        f"| Revenue estimado adjudicado | ${rev_total:,} CLP |",
        "",
        "---",
        "",
        "## Data Quality",
        "",
        "Reglas explícitas de exclusión que se aplican antes de los análisis "
        "sensibles a precio (especialmente R9). Sin esta capa, decisiones de "
        "pricing se contaminan con outliers y mismatches de producto.",
        "",
        "**Reglas aplicadas:**",
        "",
        f"- Ofertas de competidores con `precio_unitario < ${PRECIO_MIN_COMPETIDOR}` se "
        "consideran ruido (precios $0/$1 que aparecen por errores de carga) y se "
        "excluyen al calcular el benchmark competitivo.",
        f"- Para R9 se exigen ≥ {MIN_COMPETIDORES_HEADROOM} competidores válidos por "
        "ítem — con 1-2 competidores la señal de pricing no es estructural.",
        f"- El benchmark competitivo de R9 es el **percentil 25** de las ofertas de "
        "competidores válidos (no el mínimo, que se contamina con un solo outlier).",
        f"- `headroom_pct > {HEADROOM_CAP_PCT:.0%}` se descarta como outlier o mismatch "
        "de producto: precios > 2× el de Promedon en la misma línea típicamente "
        "indican que el competidor ofertó un producto distinto.",
        "",
        "**Cobertura del dataset:**",
        "",
        _df_to_md(dq),
        "",
        "---",
        "",
        "## Total general — referencia",
        "",
        "Partición exhaustiva del portafolio por **Rango WR** a nivel producto. "
        "Cada producto pertenece a **una única fila**; las filas suman exacto al "
        "Total Promedon (última fila).",
        "",
        "**Convención de columnas:** sin sufijo ⇒ mercado total (todos los "
        "proveedores en items donde Promedon ofertó); con sufijo *Promedon* ⇒ "
        "solo Promedon. `Tasa Adjudicación Promedon` = "
        "`Σ adj_Promedon / Σ presentadas_Promedon` (WR agregado de la fila, "
        "ponderado por volumen). `Tasa Monto Adjudicación Promedon` = "
        "`Σ Monto Adjudicado Promedon / Σ Monto Adjudicado Total` (share de "
        "Promedon sobre el monto total adjudicado del mercado en la fila).",
        "",
        _df_to_md(
            build_tabla_maestra(df, df_p),
            fmt={
                "Num Ofertas Presentadas":          lambda v: f"{int(v):,}",
                "Num Ofertas Presentadas Promedon": lambda v: f"{int(v):,}",
                "Num Ofertas Adjudicadas":          lambda v: f"{int(v):,}",
                "Num Ofertas Adjudicadas Promedon": lambda v: f"{int(v):,}",
                "Monto Adjudicado Total":           _fmt_clp,
                "Monto Adjudicado Promedon":        _fmt_clp,
                "Tasa Adjudicación Promedon":       _fmt_pct,
                "Tasa Monto Adjudicación Promedon": _fmt_pct,
            },
        ),
        "",
        "---",
        "",
    ]

    body = (
        render_executive_summary(rec, df_p)
        + ["---", ""]
        + render_recomendaciones(rec)
    )

    return "\n".join(header + body)


# ─── PIPELINE ────────────────────────────────────────────────────────────
def main():
    print(f"=== DIAGNÓSTICO PROMEDON ({PROMEDON_RUT}) ===")

    df = q(MATRIZ_BASE, rut_A=PROMEDON_RUT)
    df.to_csv(os.path.join(OUTPUT_DIR, "matriz_base.csv"), index=False)

    df = derive_revenue(df)
    df_p = df[df["rut_proveedor"] == PROMEDON_RUT].copy()
    df_c = df[df["rut_proveedor"] != PROMEDON_RUT].copy()

    md = build_diagnostico_md(df, df_p, df_c)
    md_path = os.path.join(OUTPUT_DIR, "diagnostico_promedon.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\nfilas (item×proveedor)      : {len(df):,}")
    print(f"licitaciones distintas      : {df['id_licitacion'].nunique():,}")
    print(f"proveedores distintos       : {df['rut_proveedor'].nunique():,}")

    n_ofertas = len(df_p)
    n_adj     = int(df_p["flag_adjudicado"].sum())
    rev_total = int(df_p["revenue_adj_clp"].sum())
    wr        = n_adj / n_ofertas if n_ofertas else 0
    print("\n--- Baseline Promedon ---")
    print(f"  ofertas presentadas : {n_ofertas:,}")
    print(f"  ofertas adjudicadas : {n_adj:,}")
    print(f"  win rate            : {wr:.1%}")
    print(f"  revenue estimado    : ${rev_total:,} CLP")

    print(f"\n→ {md_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
