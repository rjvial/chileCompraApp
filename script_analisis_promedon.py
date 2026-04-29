"""
Matriz base item × proveedor — Promedon en ChileCompra.

Genera UN SOLO CSV con granularidad (item × proveedor).
Universo: items donde Promedon (A) participa. Por cada item, una fila
por cada proveedor (incluido A) que ofertó en ese item.

═══════════════════════════════════════════════════════════════════════════
MODELO ANALÍTICO
═══════════════════════════════════════════════════════════════════════════

1) Definir el modelo analítico (antes de cualquier tabla)
---------------------------------------------------------
Unidad base única:
    item × proveedor (licitación–producto–oferta)

Y declarar explícitamente las 4 dimensiones independientes:
    - Producto     → desempeño estructural (WR)
    - Precio       → posicionamiento relativo
    - Competencia  → dinámica head-to-head
    - Comprador    → comportamiento de demanda

Todo el análisis cuelga de esto. No hay eje dominante.


2) Construir 4 "tablas maestras" (una por dimensión)
----------------------------------------------------
(A) Tabla Producto (portafolio)
    Agregación: producto_canonico
    Columnas mínimas:
        - ofertas
        - adjudicaciones
        - revenue
        - win_rate
        - ticket promedio
        - nº competidores promedio
        - varianza de precios
    👉 Esta reemplaza la tabla WR como base real.

(B) Tabla Precio (nivel item)
    Columnas:
        - precio_promedon
        - p25_competidores
        - p50, p75
        - ranking de precio
        - gap (precio_promedon vs p25 competidores)

(C) Tabla Competencia (head-to-head)
    Agregación: competidor
        - items compartidos
        - % victorias Promedon
        - % victorias competidor
        - mediana sobreprecio
        - veces más caro / más barato

(D) Tabla Comprador (cuentas)
    Agregación: comprador
        - ofertas
        - win rate
        - revenue
        - ticket promedio
        - mix de productos


3) Generar diagnósticos por dimensión (sin mezclar)
---------------------------------------------------
3.1 Portafolio (Producto)
    Aquí sí usas WR, pero bien definido.
    Segmentación:
        - Core:        WR ≥ 40%
        - Competitivo: 20–40%
        - Débil:       5–20%
        - Crítico:     < 5%
    Y agregas volumen (ofertas) como segunda variable.
    👉 Esto produce:
        P1: escalar core
        P2: optimizar débil
        P3: eliminar crítico
    (esto reemplaza R2 y R3, pero más limpio)

3.2 Precio (Pricing power)
    No depende del WR.
    Segmentos:
        - Subcotizado:           ganamos con gap < 0
        - Sobreprecio perdedor:  perdemos con gap > 0
        - Barato y pierde:       perdemos con gap ≤ 0
    Esto produce:
        PR1: subir precios
        PR2: bajar precios selectivamente
        PR3: no tocar precio

3.3 Competencia
    Clasificación por cuadrantes:

                |Ganas           |Pierdes
    Más caro    |premium real    |pricing problema
    Más barato  |eficiencia      |problema no-precio

    👉 Esto genera directamente R4 / R5 pero sin ambigüedad.

3.4 Compradores
    Segmentación 2D:
        eje X: volumen (ofertas)
        eje Y: win rate
    Cuadrantes:
        - Alta conv + alto volumen → replicar
        - Alta conv + bajo volumen → expandir
        - Baja conv + alto volumen → intervenir
        - Baja conv + bajo volumen → ignorar
    👉 Esto reemplaza R7 / R8 de forma más clara.


4) Recién aquí generas recomendaciones
--------------------------------------
No defines R1–R9 antes. Las construyes desde los cortes.

Ejemplo limpio:
    Portafolio
        - Salir de X productos (WR < 5% + volumen alto)
        - Reducir foco en Y productos (5–20%)
    Pricing
        - Subir precios en N items (gap negativo validado)
        - Ajustar a la baja en M frentes competitivos
    Competencia
        - Estrategia precio contra A, B, C
        - Estrategia no-precio contra D, E
    Cuentas
        - Defender top 20 licitaciones
        - Replicar 15 cuentas
        - Intervenir 13 cuentas


5) Regla clave de diseño
------------------------
Cada output debe cumplir:
    "puedo reconstruir este resultado con un query claro desde una tabla base"
Si no puedes, está mal definido.


6) Qué cambia respecto a la versión previa
------------------------------------------
Antes:
    - WR como eje implícito
    - Rs mezclando niveles
    - narrativa fuerte, trazabilidad débil
Después:
    - 4 ejes explícitos
    - cada insight vive en su dimensión
    - recomendaciones = combinación de cortes


7) Insight final
----------------
El error original no es técnico, es de modelado:
    se trató de forzar un problema multidimensional en una sola tabla (WR)

La versión ordenada reconoce que:
    - no hay una vista única correcta
    - hay que construir varias vistas coherentes y luego combinarlas
═══════════════════════════════════════════════════════════════════════════
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

RETURN elementId(it)                       AS item_key,
       lic.id_licitacion                   AS id_licitacion,
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



# ═════════════════════════════════════════════════════════════════════════
# (A) TABLA PRODUCTO — agregación por producto_canonico
# ═════════════════════════════════════════════════════════════════════════
def build_tabla_producto(df_base: pd.DataFrame, rut_promedon: str) -> pd.DataFrame:
    """
    Métricas a nivel producto_canonico (sólo ofertas de Promedon):
        ofertas, adjudicaciones, revenue, win_rate, ticket_promedio,
        n_competidores_promedio, var_precios_promedio.
    Segmentación: Core (≥40%) / Competitivo (20–40%) / Débil (5–20%) / Crítico (<5%).
    Diagnóstico: P1 escalar core / P2 optimizar débil / P3 eliminar crítico.
    """
    base = df_base.copy()
    base["revenue"] = (base["flag_adjudicado"].fillna(0)
                       * base["num_unidades"].fillna(0)
                       * base["precio_unitario"].fillna(0))
    base["ticket"] = base["num_unidades"].fillna(0) * base["precio_unitario"].fillna(0)

    item_stats = (base.groupby("item_key")
                  .agg(n_competidores=("rut_proveedor", lambda s: s.nunique() - 1),
                       var_precio=("precio_unitario", "var"))
                  .reset_index())

    promedon = (base[base["rut_proveedor"] == rut_promedon]
                .merge(item_stats, on="item_key", how="left"))

    out = (promedon.groupby("producto_canonico", dropna=False)
           .agg(ofertas=("flag_adjudicado", "size"),
                adjudicaciones=("flag_adjudicado", "sum"),
                revenue=("revenue", "sum"),
                win_rate=("flag_adjudicado", "mean"),
                ticket_promedio=("ticket", "mean"),
                n_competidores_promedio=("n_competidores", "mean"),
                var_precios_promedio=("var_precio", "mean"))
           .reset_index())

    bins = [-0.01, 0.05, 0.20, 0.40, 1.01]
    labels = ["Crítico", "Débil", "Competitivo", "Core"]
    out["segmento"] = pd.cut(out["win_rate"], bins=bins, labels=labels)

    diag = {"Core": "P1: escalar core",
            "Débil": "P2: optimizar débil",
            "Crítico": "P3: eliminar crítico",
            "Competitivo": "mantener"}
    out["diagnostico"] = out["segmento"].map(diag)

    return out.sort_values("revenue", ascending=False)


# ═════════════════════════════════════════════════════════════════════════
# (B) TABLA PRECIO — nivel item
# ═════════════════════════════════════════════════════════════════════════
def build_tabla_precio(df_base: pd.DataFrame, rut_promedon: str) -> pd.DataFrame:
    """
    Métricas a nivel item (sólo items donde Promedon ofertó):
        precio_promedon, p25/p50/p75 competidores, ranking_precio,
        gap = (precio_promedon - p25_comp) / p25_comp.
    Diagnóstico: PR1 subir / PR2 bajar / PR3 barato y pierde / OK.
    """
    base = df_base.copy()

    promedon = (base[base["rut_proveedor"] == rut_promedon]
                [["item_key", "id_licitacion", "producto_canonico", "rut_comprador",
                  "precio_unitario", "num_unidades", "flag_adjudicado",
                  "precio_unitario_ganador"]]
                .rename(columns={"precio_unitario": "precio_promedon",
                                 "flag_adjudicado": "promedon_gano"}))

    competidores = base[base["rut_proveedor"] != rut_promedon]
    stats = (competidores.groupby("item_key")["precio_unitario"]
             .agg(p25=lambda s: s.quantile(0.25),
                  p50="median",
                  p75=lambda s: s.quantile(0.75))
             .reset_index())

    out = promedon.merge(stats, on="item_key", how="left")

    base["precio_rank"] = base.groupby("item_key")["precio_unitario"].rank(method="min")
    rank = base.loc[base["rut_proveedor"] == rut_promedon, ["item_key", "precio_rank"]]
    out = out.merge(rank, on="item_key", how="left")

    out["gap"] = (out["precio_promedon"] - out["p25"]) / out["p25"]

    def diag_precio(r):
        if r["promedon_gano"] and pd.notna(r["gap"]) and r["gap"] < 0:
            return "PR1: subir precio (subcotizado)"
        if not r["promedon_gano"] and pd.notna(r["gap"]) and r["gap"] > 0:
            return "PR2: bajar precio (sobreprecio perdedor)"
        if not r["promedon_gano"] and pd.notna(r["gap"]) and r["gap"] <= 0:
            return "PR3: barato y pierde (señal no-precio)"
        return "OK"
    out["diagnostico"] = out.apply(diag_precio, axis=1)

    return out


# ═════════════════════════════════════════════════════════════════════════
# (C) TABLA COMPETENCIA — head-to-head por competidor
# ═════════════════════════════════════════════════════════════════════════
def build_tabla_competencia(df_base: pd.DataFrame, rut_promedon: str) -> pd.DataFrame:
    """
    Para cada competidor que coincidió con Promedon en algún item:
        items_compartidos, wr_promedon, wr_competidor,
        mediana_sobreprecio (precio_promedon vs precio_competidor),
        veces_mas_caro, veces_mas_barato.
    Cuadrantes: premium real / eficiencia / pricing problema / problema no-precio.
    """
    base = df_base.copy()

    promedon = (base[base["rut_proveedor"] == rut_promedon]
                [["item_key", "precio_unitario", "flag_adjudicado"]]
                .rename(columns={"precio_unitario": "precio_promedon",
                                 "flag_adjudicado": "gano_promedon"}))
    competidores = (base[base["rut_proveedor"] != rut_promedon]
                    [["item_key", "rut_proveedor", "nombre_proveedor",
                      "precio_unitario", "flag_adjudicado"]]
                    .rename(columns={"precio_unitario": "precio_competidor",
                                     "flag_adjudicado": "gano_competidor"}))

    h2h = competidores.merge(promedon, on="item_key", how="inner")
    h2h["sobreprecio"] = ((h2h["precio_promedon"] - h2h["precio_competidor"])
                          / h2h["precio_competidor"])
    h2h["mas_caro"] = (h2h["precio_promedon"] > h2h["precio_competidor"]).astype(int)
    h2h["mas_barato"] = (h2h["precio_promedon"] < h2h["precio_competidor"]).astype(int)

    out = (h2h.groupby(["rut_proveedor", "nombre_proveedor"], dropna=False)
           .agg(items_compartidos=("item_key", "nunique"),
                wr_promedon=("gano_promedon", "mean"),
                wr_competidor=("gano_competidor", "mean"),
                mediana_sobreprecio=("sobreprecio", "median"),
                veces_mas_caro=("mas_caro", "sum"),
                veces_mas_barato=("mas_barato", "sum"))
           .reset_index())

    def cuad(r):
        gana = r["wr_promedon"] >= 0.5
        caro = pd.notna(r["mediana_sobreprecio"]) and r["mediana_sobreprecio"] > 0
        if gana and caro: return "premium real"
        if gana and not caro: return "eficiencia"
        if not gana and caro: return "pricing problema"
        return "problema no-precio"
    out["cuadrante"] = out.apply(cuad, axis=1)

    return out.sort_values("items_compartidos", ascending=False)


# ═════════════════════════════════════════════════════════════════════════
# (D) TABLA COMPRADOR — agregación por organismo
# ═════════════════════════════════════════════════════════════════════════
def build_tabla_comprador(df_base: pd.DataFrame, rut_promedon: str) -> pd.DataFrame:
    """
    Métricas Promedon por comprador:
        ofertas, win_rate, revenue, ticket_promedio, mix_productos.
    Segmentación 2D (volumen × win_rate, umbrales en mediana):
        Replicar / Expandir / Intervenir / Ignorar.
    """
    promedon = df_base[df_base["rut_proveedor"] == rut_promedon].copy()
    promedon["revenue"] = (promedon["flag_adjudicado"].fillna(0)
                           * promedon["num_unidades"].fillna(0)
                           * promedon["precio_unitario"].fillna(0))
    promedon["ticket"] = (promedon["num_unidades"].fillna(0)
                          * promedon["precio_unitario"].fillna(0))

    out = (promedon.groupby(["rut_comprador", "nombre_comprador"], dropna=False)
           .agg(ofertas=("flag_adjudicado", "size"),
                win_rate=("flag_adjudicado", "mean"),
                revenue=("revenue", "sum"),
                ticket_promedio=("ticket", "mean"),
                mix_productos=("producto_canonico", "nunique"))
           .reset_index())

    vol_med = out["ofertas"].median()
    wr_med = out["win_rate"].median()

    def seg(r):
        alto_vol = r["ofertas"] >= vol_med
        alta_wr = r["win_rate"] >= wr_med
        if alta_wr and alto_vol: return "Replicar (alta conv + alto vol)"
        if alta_wr and not alto_vol: return "Expandir (alta conv + bajo vol)"
        if not alta_wr and alto_vol: return "Intervenir (baja conv + alto vol)"
        return "Ignorar (baja conv + bajo vol)"
    out["segmento"] = out.apply(seg, axis=1)

    return out.sort_values("revenue", ascending=False)


# ═════════════════════════════════════════════════════════════════════════
# REPORTE — diagnóstico Promedon en Markdown
# ═════════════════════════════════════════════════════════════════════════
def _fmt_money(x) -> str:
    if pd.isna(x): return "-"
    return f"${x:,.0f}".replace(",", ".")


def _fmt_pct(x, decimals: int = 1) -> str:
    if pd.isna(x): return "-"
    return f"{x*100:.{decimals}f}%"


def _md_table(df: pd.DataFrame, cols: dict, max_rows: int = 10,
              totals: dict | None = None) -> str:
    """
    Render df como tabla markdown. cols = {col_origen: (header, formatter)}.
    totals: dict opcional {col_origen: valor_total} para una fila final "**Total**".
    """
    if df.empty:
        return "_(sin datos)_\n"
    df = df.head(max_rows)
    headers = [h for _, (h, _) in cols.items()]
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join(["---"] * len(headers)) + " |"]
    for _, row in df.iterrows():
        cells = []
        for src, (_, fmt) in cols.items():
            val = row.get(src)
            cells.append(fmt(val) if fmt else ("-" if pd.isna(val) else str(val)))
        lines.append("| " + " | ".join(cells) + " |")
    if totals is not None:
        cells = []
        for i, (src, (_, fmt)) in enumerate(cols.items()):
            val = totals.get(src)
            if i == 0 and val is None:
                cells.append("**Total**")
            elif val is None:
                cells.append("-")
            else:
                rendered = fmt(val) if fmt else str(val)
                cells.append(f"**{rendered}**")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def _wavg(s_val: pd.Series, s_w: pd.Series):
    s_val = s_val.fillna(0)
    w = s_w.fillna(0)
    total_w = float(w.sum())
    return float((s_val * w).sum() / total_w) if total_w else None


def _segmentar_productos(t_prod: pd.DataFrame) -> tuple[dict, float]:
    """
    Clasifica cada producto_canonico en:
      - "A" (alta competencia / commodity)   si n_competidores_promedio ≥ mediana
      - "B" (baja competencia / especializado) si n_competidores_promedio < mediana
    NaN → "B". Retorna (mapping, threshold).
    """
    valid = t_prod.dropna(subset=["n_competidores_promedio"])
    threshold = float(valid["n_competidores_promedio"].median()) if not valid.empty else 0.0
    mapping = {}
    for _, row in t_prod.iterrows():
        ncomp = row["n_competidores_promedio"]
        prod = row["producto_canonico"]
        if pd.isna(ncomp) or ncomp < threshold:
            mapping[prod] = "B"
        else:
            mapping[prod] = "A"
    return mapping, threshold


def _seccion_producto(t_prod_dict: dict, prefix: str = "1") -> list[str]:
    md = []
    md.append(f"## {prefix} Portafolio (Producto)\n")
    md.append(
        "Mide el **desempeño estructural** de Promedon por categoría de producto. "
        "La unidad de análisis es `producto_canonico` y la métrica clave es el "
        "win rate (adjudicaciones / ofertas), complementada por revenue y volumen.\n"
    )
    md.append(
        "Notas de columnas:\n"
        "- **Seg.**: A = alta competencia (commodities), B = baja competencia (especializados).\n"
        "- **Ofertas / Adjudicaciones / Revenue / WR**: solo Promedon en items del producto.\n"
        "- **Ticket prom.**: monto medio por oferta Promedon (`num_unidades × precio_unitario`).\n"
        "- **Comp/item**: competidores promedio por item del producto.\n"
    )
    md.append(
        "Los productos se clasifican en cuatro segmentos según win rate, lo que "
        "permite separar la conversación \"dónde escalar\" de \"dónde salir\":\n"
        "- **Core** (WR ≥ 40%): fortaleza estructural, foco de crecimiento.\n"
        "- **Competitivo** (20–40%): zona de palancas tácticas (precio, comprador).\n"
        "- **Débil** (5–20%): exige diagnóstico fino antes de invertir más.\n"
        "- **Crítico** (< 5%): candidatos a desinversión salvo razón estratégica.\n"
    )

    # Distribución por segmento — long format (Seg. × Segmento WR)
    rows_dist = []
    for seg_p in ["A", "B"]:
        agg = (t_prod_dict[seg_p].groupby("segmento", observed=True)
               .agg(n_productos=("producto_canonico", "count"),
                    ofertas=("ofertas", "sum"),
                    adjudicaciones=("adjudicaciones", "sum"),
                    revenue=("revenue", "sum"))
               .reset_index())
        agg["seg_producto"] = seg_p
        rows_dist.append(agg)
    df_dist = pd.concat(rows_dist, ignore_index=True)[
        ["seg_producto", "segmento", "n_productos", "ofertas",
         "adjudicaciones", "revenue"]
    ]
    md.append("### Distribución por segmento\n")
    md.append(_md_table(df_dist, {
        "seg_producto": ("Seg.", None),
        "segmento": ("Segmento", None),
        "n_productos": ("# productos", lambda v: f"{int(v):,}"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "adjudicaciones": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, max_rows=len(df_dist), totals={
        "seg_producto": "A+B",
        "n_productos": int(df_dist["n_productos"].sum()),
        "ofertas": int(df_dist["ofertas"].sum()),
        "adjudicaciones": int(df_dist["adjudicaciones"].sum()),
        "revenue": float(df_dist["revenue"].sum()),
    }))

    p_explicaciones = {
        "Core": (
            "P1 — escalar core",
            "Productos donde Promedon ya gana de forma sistemática (WR ≥ 40%). "
            "Top 10 por revenue, en cada segmento de competencia (A y B):",
        ),
        "Débil": (
            "P2 — optimizar débil",
            "Productos con tracción pero baja conversión (WR 5–20%). "
            "Top 10 por revenue, en cada segmento (A y B):",
        ),
        "Crítico": (
            "P3 — eliminar crítico",
            "Productos donde Promedon ofertó pero casi nunca gana (WR < 5%). "
            "Top 10 por revenue, en cada segmento (A y B):",
        ),
    }
    cols = {
        "seg_producto": ("Seg.", None),
        "producto_canonico": ("Producto", None),
        "ofertas": ("Ofertas", lambda v: "-" if pd.isna(v) else f"{int(v):,}"),
        "win_rate": ("WR", _fmt_pct),
        "revenue": ("Revenue", _fmt_money),
        "ticket_promedio": ("Ticket prom.", _fmt_money),
        "n_competidores_promedio": ("Comp/item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
    }
    for seg in ["Core", "Débil", "Crítico"]:
        titulo, descripcion = p_explicaciones[seg]
        md.append(f"### {titulo}\n")
        md.append(descripcion + "\n")
        combined = []
        all_full = []
        for seg_p in ["A", "B"]:
            full = (t_prod_dict[seg_p][t_prod_dict[seg_p]["segmento"] == seg]
                    .sort_values("revenue", ascending=False))
            if full.empty:
                continue
            top = full.head(10).copy()
            top["seg_producto"] = seg_p
            combined.append(top)
            rest = full.iloc[10:]
            if not rest.empty:
                r_of = int(rest["ofertas"].sum())
                r_adj = int(rest["adjudicaciones"].sum())
                combined.append(pd.DataFrame([{
                    "seg_producto": seg_p,
                    "producto_canonico": f"Otros ({len(rest)} productos)",
                    "ofertas": r_of,
                    "adjudicaciones": r_adj,
                    "revenue": float(rest["revenue"].sum()),
                    "win_rate": (r_adj / r_of) if r_of else 0,
                    "ticket_promedio": _wavg(rest["ticket_promedio"], rest["ofertas"]),
                    "n_competidores_promedio": _wavg(rest["n_competidores_promedio"], rest["ofertas"]),
                }]))
            all_full.append(full)
        if not combined:
            md.append("_(sin productos en este segmento)_\n")
            continue
        df_combined = pd.concat(combined, ignore_index=True)
        all_df = pd.concat(all_full, ignore_index=True)
        a_of = int(all_df["ofertas"].sum())
        a_adj = int(all_df["adjudicaciones"].sum())
        totals = {
            "seg_producto": "A+B",
            "producto_canonico": f"Total {seg}",
            "ofertas": a_of,
            "win_rate": (a_adj / a_of) if a_of else 0,
            "revenue": float(all_df["revenue"].sum()),
            "ticket_promedio": _wavg(all_df["ticket_promedio"], all_df["ofertas"]),
            "n_competidores_promedio": _wavg(all_df["n_competidores_promedio"], all_df["ofertas"]),
        }
        md.append(_md_table(df_combined, cols, max_rows=len(df_combined), totals=totals))
    return md


def _seccion_precio(t_prec_dict: dict, prefix: str = "2") -> list[str]:
    md = []
    md.append(f"## {prefix} Precio (Pricing power)\n")
    md.append(
        "Analiza el **posicionamiento relativo** del precio Promedon dentro de "
        "cada item respecto a sus competidores. La unidad es el item; cada fila "
        "compara el precio Promedon contra el percentil 25 de los competidores "
        "vía `gap = (precio_promedon − p25_comp) / p25_comp`.\n"
    )
    md.append(
        "Esto permite separar problemas de precio de problemas no-precio:\n"
        "- **PR1 — Subcotizado** (ganamos con `gap < 0`): margen para subir precio.\n"
        "- **PR2 — Sobreprecio perdedor** (perdimos con `gap > 0`): bajar selectivamente.\n"
        "- **PR3 — Barato y pierde** (perdimos con `gap ≤ 0`): el problema no es precio.\n"
    )
    diag_order = [
        "PR1: subir precio (subcotizado)",
        "PR2: bajar precio (sobreprecio perdedor)",
        "PR3: barato y pierde (señal no-precio)",
        "OK",
    ]

    # Distribución por diagnóstico — long (Seg. × diagnóstico)
    rows_dist = []
    for seg_p in ["A", "B"]:
        agg = (t_prec_dict[seg_p].groupby("diagnostico")
               .agg(n_items=("item_key", "nunique"))
               .reset_index())
        agg["seg_producto"] = seg_p
        agg["__order"] = agg["diagnostico"].map({d: i for i, d in enumerate(diag_order)})
        agg = agg.sort_values("__order").drop(columns="__order").reset_index(drop=True)
        rows_dist.append(agg)
    df_dist = pd.concat(rows_dist, ignore_index=True)[
        ["seg_producto", "diagnostico", "n_items"]
    ]
    md.append("### Distribución por diagnóstico\n")
    md.append(_md_table(df_dist, {
        "seg_producto": ("Seg.", None),
        "diagnostico": ("Diagnóstico", None),
        "n_items": ("# items", lambda v: f"{int(v):,}"),
    }, max_rows=len(df_dist), totals={
        "seg_producto": "A+B",
        "n_items": int(df_dist["n_items"].sum()),
    }))

    pr_cols = {
        "seg_producto": ("Seg.", None),
        "id_licitacion": ("Licitación", None),
        "producto_canonico": ("Producto", None),
        "precio_promedon": ("Precio Promedon", _fmt_money),
        "p25": ("P25 comp.", _fmt_money),
        "p50": ("P50 comp.", _fmt_money),
        "gap": ("Gap", _fmt_pct),
    }
    pr_config = {
        "PR1": {"ascending": True,  "ext_label": "precio < p25/5",
                "ext_mask": lambda s: s["precio_promedon"] < s["p25"] / 5},
        "PR2": {"ascending": False, "ext_label": "precio > 5·p25",
                "ext_mask": lambda s: s["precio_promedon"] > s["p25"] * 5},
        "PR3": {"ascending": True,  "ext_label": "precio < p25/5",
                "ext_mask": lambda s: s["precio_promedon"] < s["p25"] / 5},
    }
    for diag, titulo in [("PR1: subir precio (subcotizado)", "PR1 — subir precios (gap más negativo)"),
                         ("PR2: bajar precio (sobreprecio perdedor)", "PR2 — bajar precios (gap más positivo)"),
                         ("PR3: barato y pierde (señal no-precio)", "PR3 — barato y pierde (señal no-precio)")]:
        cfg = pr_config[diag[:3]]
        md.append(f"### {titulo} (top 10 por segmento)\n")

        # Tabla de uplift potencial — sólo PR1 (Promedon ganó subcotizado)
        if diag.startswith("PR1"):
            rows_up = []
            for seg_p in ["A", "B"]:
                sub_pr1 = t_prec_dict[seg_p][t_prec_dict[seg_p]["diagnostico"] == diag].copy()
                if sub_pr1.empty:
                    continue
                qty = sub_pr1["num_unidades"].fillna(0)
                rev_act = float((qty * sub_pr1["precio_promedon"].fillna(0)).sum())
                rev_p25 = float((qty * sub_pr1["p25"].fillna(0)).sum())
                rows_up.append({
                    "seg_producto": seg_p,
                    "n_items": len(sub_pr1),
                    "rev_actual": rev_act,
                    "rev_p25": rev_p25,
                    "uplift_abs": rev_p25 - rev_act,
                    "uplift_pct": (rev_p25 / rev_act - 1) if rev_act else None,
                })
            if rows_up:
                df_up = pd.DataFrame(rows_up)
                tot_act = float(df_up["rev_actual"].sum())
                tot_p25 = float(df_up["rev_p25"].sum())
                md.append(
                    "**Uplift potencial** asumiendo que Promedon hubiera cobrado el "
                    "P25 de los competidores en estos items (cota superior; ignora "
                    "elasticidad y posible pérdida de la adjudicación):\n"
                )
                md.append(_md_table(df_up, {
                    "seg_producto": ("Seg.", None),
                    "n_items": ("# items", lambda v: f"{int(v):,}"),
                    "rev_actual": ("Revenue Promedon", _fmt_money),
                    "rev_p25": ("Revenue @ P25", _fmt_money),
                    "uplift_abs": ("Uplift $", _fmt_money),
                    "uplift_pct": ("Uplift %", _fmt_pct),
                }, max_rows=len(df_up), totals={
                    "seg_producto": "A+B",
                    "n_items": int(df_up["n_items"].sum()),
                    "rev_actual": tot_act,
                    "rev_p25": tot_p25,
                    "uplift_abs": tot_p25 - tot_act,
                    "uplift_pct": (tot_p25 / tot_act - 1) if tot_act else None,
                }))

        # Tabla de recuperación potencial — sólo PR2 (Promedon perdió por sobreprecio)
        if diag.startswith("PR2"):
            discounts = [0.10, 0.20, 0.30]
            rows_dn = []
            for seg_p in ["A", "B"]:
                sub_pr2 = t_prec_dict[seg_p][t_prec_dict[seg_p]["diagnostico"] == diag].copy()
                if sub_pr2.empty:
                    continue
                qty = sub_pr2["num_unidades"].fillna(0)
                price = sub_pr2["precio_promedon"].fillna(0)
                winner = sub_pr2["precio_unitario_ganador"]
                row = {"seg_producto": seg_p, "n_items": len(sub_pr2)}
                for d in discounts:
                    hyp = price * (1 - d)
                    won_mask = winner.notna() & (hyp < winner)
                    row[f"recup_{int(d*100)}"] = int(won_mask.sum())
                    row[f"rev_{int(d*100)}"] = float((qty * hyp * won_mask.astype(int)).sum())
                rows_dn.append(row)
            if rows_dn:
                df_dn = pd.DataFrame(rows_dn)
                md.append(
                    "**Recuperación potencial** asumiendo que Promedon hubiera bajado "
                    "su precio -10%, -20% o -30%. Un item se considera recuperado si el "
                    "precio hipotético cae por debajo del precio adjudicado en ese item "
                    "(`precio_promedon × (1−d) < precio_unitario_ganador`); el revenue "
                    "se calcula como `num_unidades × precio_hipotético`. Promedon "
                    "actualmente no factura nada por estos items (los perdió).\n"
                )
                md.append(_md_table(df_dn, {
                    "seg_producto": ("Seg.", None),
                    "n_items": ("# items PR2", lambda v: f"{int(v):,}"),
                    "recup_10": ("Rec. -10%", lambda v: f"{int(v):,}"),
                    "rev_10": ("Revenue -10%", _fmt_money),
                    "recup_20": ("Rec. -20%", lambda v: f"{int(v):,}"),
                    "rev_20": ("Revenue -20%", _fmt_money),
                    "recup_30": ("Rec. -30%", lambda v: f"{int(v):,}"),
                    "rev_30": ("Revenue -30%", _fmt_money),
                }, max_rows=len(df_dn), totals={
                    "seg_producto": "A+B",
                    "n_items": int(df_dn["n_items"].sum()),
                    "recup_10": int(df_dn["recup_10"].sum()),
                    "rev_10": float(df_dn["rev_10"].sum()),
                    "recup_20": int(df_dn["recup_20"].sum()),
                    "rev_20": float(df_dn["rev_20"].sum()),
                    "recup_30": int(df_dn["recup_30"].sum()),
                    "rev_30": float(df_dn["rev_30"].sum()),
                }))

        combined = []
        all_sub = []
        for seg_p in ["A", "B"]:
            sub = t_prec_dict[seg_p][t_prec_dict[seg_p]["diagnostico"] == diag].copy()
            if sub.empty:
                continue
            mask_ext = cfg["ext_mask"](sub)
            sub_extreme = sub[mask_ext]
            sub_show = sub[~mask_ext].sort_values("gap", ascending=cfg["ascending"])
            top = sub_show.head(10).copy()
            top["seg_producto"] = seg_p
            combined.append(top)
            rest = pd.concat([sub_show.iloc[10:], sub_extreme], ignore_index=True)
            n_extreme = int(mask_ext.sum())
            if not rest.empty:
                combined.append(pd.DataFrame([{
                    "seg_producto": seg_p,
                    "id_licitacion": f"Otros ({len(rest)} items, {n_extreme} con {cfg['ext_label']})",
                    "producto_canonico": "—",
                    "gap": rest["gap"].median(),
                }]))
            all_sub.append(sub)
        if not combined:
            md.append("_(sin items en este diagnóstico)_\n")
            continue
        df_combined = pd.concat(combined, ignore_index=True)
        all_df = pd.concat(all_sub, ignore_index=True)
        totals = {
            "seg_producto": "A+B",
            "id_licitacion": f"Total: {len(all_df):,} items",
            "gap": all_df["gap"].median(),
        }
        md.append(_md_table(df_combined, pr_cols, max_rows=len(df_combined), totals=totals))
    return md


def _seccion_competencia(t_comp_dict: dict, n_ofertas_promedon_dict: dict,
                         prefix: str = "3") -> list[str]:
    md = []
    md.append(f"## {prefix} Competencia (head-to-head)\n")
    md.append(
        "Mide la **dinámica directa** contra cada competidor en los items donde "
        "ambos ofertaron. Para cada par (Promedon, competidor) se calculan "
        "win rates mutuos, mediana de sobreprecio y conteos de cuándo Promedon "
        "fue más caro o más barato.\n"
    )
    md.append(
        "Los competidores se ubican en cuatro cuadrantes precio × resultado:\n"
        "- **premium real** (ganamos siendo más caros): mantener narrativa de valor.\n"
        "- **eficiencia** (ganamos siendo más baratos): defender posición de costo.\n"
        "- **pricing problema** (perdemos por más caros): ajustar precio.\n"
        "- **problema no-precio** (perdemos siendo más baratos): atacar palancas no-precio.\n"
    )
    md.append("Cuadrantes precio × resultado:\n")
    md.append("| | Ganas | Pierdes |")
    md.append("| --- | --- | --- |")
    md.append("| Más caro | premium real | pricing problema |")
    md.append("| Más barato | eficiencia | problema no-precio |\n")

    # Distribución por cuadrante — long
    rows_dist = []
    for seg_p in ["A", "B"]:
        agg = (t_comp_dict[seg_p].groupby("cuadrante")
               .agg(n_competidores=("rut_proveedor", "count"),
                    ofertas=("items_compartidos", "sum"))
               .reset_index()
               .sort_values("ofertas", ascending=False))
        agg["seg_producto"] = seg_p
        rows_dist.append(agg)
        rows_dist.append(pd.DataFrame([{
            "seg_producto": seg_p,
            "cuadrante": "Promedon (referencia)",
            "n_competidores": pd.NA,
            "ofertas": int(n_ofertas_promedon_dict[seg_p]),
        }]))
    df_dist = pd.concat(rows_dist, ignore_index=True)[
        ["seg_producto", "cuadrante", "n_competidores", "ofertas"]
    ]
    md.append("### Distribución por cuadrante\n")
    md.append(_md_table(df_dist, {
        "seg_producto": ("Seg.", None),
        "cuadrante": ("Cuadrante", None),
        "n_competidores": ("# competidores", lambda v: f"{int(v):,}" if pd.notna(v) else "—"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
    }, max_rows=len(df_dist), totals={
        "seg_producto": "A+B",
        "n_competidores": int(df_dist["n_competidores"].dropna().sum()),
        "ofertas": int(df_dist["ofertas"].sum()),
    }))

    cuad_explicaciones = {
        "pricing problema": "Perdemos siendo más caros — palanca de precio.",
        "problema no-precio": "Perdemos siendo más baratos — palanca no-precio (servicio, marca, especificación técnica).",
        "premium real": "Ganamos siendo más caros — proteger narrativa de valor.",
        "eficiencia": "Ganamos siendo más baratos — defender posición de costo.",
    }
    cuad_cols = {
        "seg_producto": ("Seg.", None),
        "nombre_proveedor": ("Competidor", None),
        "items_compartidos": ("Items", lambda v: f"{int(v):,}"),
        "wr_promedon": ("WR Promedon", _fmt_pct),
        "wr_competidor": ("WR comp.", _fmt_pct),
        "mediana_sobreprecio": ("Mediana sobreprecio", _fmt_pct),
    }
    for cuad in ["pricing problema", "problema no-precio", "premium real", "eficiencia"]:
        md.append(f"### Top en \"{cuad}\" (top 10 por segmento)\n")
        md.append(cuad_explicaciones[cuad] + "\n")
        combined = []
        all_sub = []
        for seg_p in ["A", "B"]:
            sub = (t_comp_dict[seg_p][t_comp_dict[seg_p]["cuadrante"] == cuad]
                   .sort_values("items_compartidos", ascending=False))
            if sub.empty:
                continue
            top = sub.head(10).copy()
            top["seg_producto"] = seg_p
            combined.append(top)
            rest = sub.iloc[10:]
            if not rest.empty:
                combined.append(pd.DataFrame([{
                    "seg_producto": seg_p,
                    "nombre_proveedor": f"Otros ({len(rest)} competidores)",
                    "items_compartidos": int(rest["items_compartidos"].sum()),
                }]))
            all_sub.append(sub)
        if not combined:
            md.append("_(sin competidores en este cuadrante)_\n")
            continue
        df_combined = pd.concat(combined, ignore_index=True)
        all_df = pd.concat(all_sub, ignore_index=True)
        totals = {
            "seg_producto": "A+B",
            "nombre_proveedor": f"Total {cuad}: {len(all_df):,} competidores",
            "items_compartidos": int(all_df["items_compartidos"].sum()),
        }
        md.append(_md_table(df_combined, cuad_cols, max_rows=len(df_combined), totals=totals))
    return md


def _seccion_comprador(t_compr_dict: dict, prefix: str = "4") -> list[str]:
    md = []
    md.append(f"## {prefix} Compradores (cuentas)\n")
    md.append(
        "Mide el **comportamiento de demanda** por organismo comprador. La "
        "unidad es el `rut_comprador`; se agrega ofertas, win rate, revenue, "
        "ticket promedio y mix de productos vendidos a cada cuenta.\n"
    )
    md.append(
        "Cada comprador se ubica en un cuadrante 2D según volumen × win rate "
        "(umbrales = mediana, recalculados independientemente para A y B):\n"
        "- **Replicar** (alta conv + alto vol): cuentas a defender y modelar.\n"
        "- **Expandir** (alta conv + bajo vol): potencial de crecimiento orgánico.\n"
        "- **Intervenir** (baja conv + alto vol): demanda existente que no se captura.\n"
        "- **Ignorar** (baja conv + bajo vol): bajo retorno por esfuerzo comercial.\n"
    )

    # Distribución por segmento — long
    rows_dist = []
    for seg_p in ["A", "B"]:
        agg = (t_compr_dict[seg_p].groupby("segmento")
               .agg(n_compradores=("rut_comprador", "count"),
                    ofertas=("ofertas", "sum"),
                    revenue=("revenue", "sum"))
               .reset_index()
               .sort_values("revenue", ascending=False))
        agg["seg_producto"] = seg_p
        rows_dist.append(agg)
    df_dist = pd.concat(rows_dist, ignore_index=True)[
        ["seg_producto", "segmento", "n_compradores", "ofertas", "revenue"]
    ]
    md.append("### Distribución por segmento\n")
    md.append(_md_table(df_dist, {
        "seg_producto": ("Seg.", None),
        "segmento": ("Segmento", None),
        "n_compradores": ("# compradores", lambda v: f"{int(v):,}"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, max_rows=len(df_dist), totals={
        "seg_producto": "A+B",
        "n_compradores": int(df_dist["n_compradores"].sum()),
        "ofertas": int(df_dist["ofertas"].sum()),
        "revenue": float(df_dist["revenue"].sum()),
    }))

    _fmt_int = lambda v: "-" if pd.isna(v) else f"{int(v):,}"
    seg_cols = {
        "seg_producto": ("Seg.", None),
        "nombre_comprador": ("Comprador", None),
        "ofertas": ("Ofertas", _fmt_int),
        "win_rate": ("WR", _fmt_pct),
        "revenue": ("Revenue", _fmt_money),
        "ticket_promedio": ("Ticket prom.", _fmt_money),
        "mix_productos": ("Mix prod.", _fmt_int),
    }
    for seg in ["Replicar (alta conv + alto vol)",
                "Expandir (alta conv + bajo vol)",
                "Intervenir (baja conv + alto vol)"]:
        md.append(f"### {seg} (top 10 por segmento)\n")
        combined = []
        all_sub = []
        for seg_p in ["A", "B"]:
            sub = (t_compr_dict[seg_p][t_compr_dict[seg_p]["segmento"] == seg]
                   .sort_values("revenue", ascending=False))
            if sub.empty:
                continue
            top = sub.head(10).copy()
            top["seg_producto"] = seg_p
            combined.append(top)
            rest = sub.iloc[10:]
            if not rest.empty:
                r_of = int(rest["ofertas"].sum())
                r_won = float((rest["ofertas"] * rest["win_rate"]).sum())
                n_rest = int(rest["rut_comprador"].count())
                combined.append(pd.DataFrame([{
                    "seg_producto": seg_p,
                    "nombre_comprador": f"Otros ({n_rest} compradores)",
                    "ofertas": r_of,
                    "win_rate": (r_won / r_of) if r_of else float("nan"),
                    "revenue": float(rest["revenue"].sum()),
                    "ticket_promedio": float("nan"),
                    "mix_productos": pd.NA,
                }]))
            all_sub.append(sub)
        if not combined:
            md.append("_(sin compradores en este segmento)_\n")
            continue
        df_combined = pd.concat(combined, ignore_index=True)
        all_df = pd.concat(all_sub, ignore_index=True)
        a_of = int(all_df["ofertas"].sum())
        a_won = float((all_df["ofertas"] * all_df["win_rate"]).sum())
        a_n = int(all_df["rut_comprador"].count())
        totals = {
            "seg_producto": "A+B",
            "nombre_comprador": f"Total: {a_n:,} compradores",
            "ofertas": a_of,
            "win_rate": (a_won / a_of) if a_of else float("nan"),
            "revenue": float(all_df["revenue"].sum()),
        }
        md.append(_md_table(df_combined, seg_cols, max_rows=len(df_combined), totals=totals))
    return md


def _seccion_recomendaciones(t_prod_dict: dict, t_prec_dict: dict,
                             t_comp_dict: dict, t_compr_dict: dict,
                             prefix: str = "5") -> list[str]:
    md = []
    md.append(f"## {prefix} Recomendaciones consolidadas\n")
    md.append(
        "Síntesis cuantitativa de los cortes anteriores: un conteo por acción "
        "en cada dimensión, separado por segmento de producto (A vs B). **Las "
        "recomendaciones no son nuevas conclusiones**, sino la combinación "
        "trazable de los segmentos de las secciones previas.\n"
    )

    def _row(label, key_a, key_b):
        return [label, key_a, key_b, key_a + key_b]

    # Portafolio
    rows = []
    for label_seg, key in [("Escalar Core", "Core"),
                           ("Optimizar Débil", "Débil"),
                           ("Evaluar salida Crítico", "Crítico")]:
        a = int((t_prod_dict["A"]["segmento"] == key).sum())
        b = int((t_prod_dict["B"]["segmento"] == key).sum())
        rows.append({"accion": label_seg, "A": a, "B": b, "Total": a + b})
    md.append("**Portafolio** (# productos)\n")
    md.append(_md_table(pd.DataFrame(rows), {
        "accion": ("Acción", None),
        "A": ("A", lambda v: f"{int(v):,}"),
        "B": ("B", lambda v: f"{int(v):,}"),
        "Total": ("Total", lambda v: f"{int(v):,}"),
    }, max_rows=len(rows)))

    # Pricing
    rows = []
    for label_seg, key in [("Subir precio (PR1)", "PR1: subir precio (subcotizado)"),
                           ("Bajar precio (PR2)", "PR2: bajar precio (sobreprecio perdedor)"),
                           ("Atacar PR3 (no-precio)", "PR3: barato y pierde (señal no-precio)")]:
        a = int((t_prec_dict["A"]["diagnostico"] == key).sum())
        b = int((t_prec_dict["B"]["diagnostico"] == key).sum())
        rows.append({"accion": label_seg, "A": a, "B": b, "Total": a + b})
    md.append("**Pricing** (# items)\n")
    md.append(_md_table(pd.DataFrame(rows), {
        "accion": ("Acción", None),
        "A": ("A", lambda v: f"{int(v):,}"),
        "B": ("B", lambda v: f"{int(v):,}"),
        "Total": ("Total", lambda v: f"{int(v):,}"),
    }, max_rows=len(rows)))

    # Competencia
    rows = []
    for cuad in ["pricing problema", "problema no-precio", "premium real", "eficiencia"]:
        a = int((t_comp_dict["A"]["cuadrante"] == cuad).sum())
        b = int((t_comp_dict["B"]["cuadrante"] == cuad).sum())
        rows.append({"cuadrante": cuad, "A": a, "B": b, "Total": a + b})
    md.append("**Competencia** (# competidores)\n")
    md.append(_md_table(pd.DataFrame(rows), {
        "cuadrante": ("Cuadrante", None),
        "A": ("A", lambda v: f"{int(v):,}"),
        "B": ("B", lambda v: f"{int(v):,}"),
        "Total": ("Total", lambda v: f"{int(v):,}"),
    }, max_rows=len(rows)))

    # Cuentas
    rows = []
    for label_seg, key in [("Defender Replicar", "Replicar (alta conv + alto vol)"),
                           ("Expandir", "Expandir (alta conv + bajo vol)"),
                           ("Intervenir", "Intervenir (baja conv + alto vol)")]:
        a = int((t_compr_dict["A"]["segmento"] == key).sum())
        b = int((t_compr_dict["B"]["segmento"] == key).sum())
        rows.append({"accion": label_seg, "A": a, "B": b, "Total": a + b})
    md.append("**Cuentas** (# compradores)\n")
    md.append(_md_table(pd.DataFrame(rows), {
        "accion": ("Acción", None),
        "A": ("A", lambda v: f"{int(v):,}"),
        "B": ("B", lambda v: f"{int(v):,}"),
        "Total": ("Total", lambda v: f"{int(v):,}"),
    }, max_rows=len(rows)))
    return md


def build_reporte_md(df_base: pd.DataFrame,
                     rut_promedon: str) -> str:
    promedon = df_base[df_base["rut_proveedor"] == rut_promedon]
    n_ofertas_totales = len(df_base)
    n_ofertas_promedon = len(promedon)
    n_adjudicadas = int(promedon["flag_adjudicado"].sum())
    wr_global = n_adjudicadas / n_ofertas_promedon if n_ofertas_promedon else 0
    revenue_total = float((promedon["flag_adjudicado"].fillna(0)
                           * promedon["num_unidades"].fillna(0)
                           * promedon["precio_unitario"].fillna(0)).sum())
    n_competidores = df_base.loc[df_base["rut_proveedor"] != rut_promedon, "rut_proveedor"].nunique()
    n_compradores = df_base["rut_comprador"].nunique()
    fecha = datetime.now().strftime("%Y-%m-%d")

    md = []
    md.append(f"# Diagnóstico Promedon — ChileCompra\n")
    md.append(f"**RUT:** {rut_promedon}  ")
    md.append(f"**Fecha:** {fecha}  ")
    md.append(f"**Modelo:** item × proveedor (4 dimensiones × 2 segmentos de producto)\n")

    # ── Resumen ejecutivo ────────────────────────────────────────────────
    md.append("## Resumen ejecutivo\n")
    md.append(
        "Foto agregada del universo competitivo donde Promedon participa. "
        "El universo es el conjunto de items (licitación–producto) en los que "
        "Promedon presentó al menos una oferta; cada item se observa con todas "
        "las ofertas competidoras. Las métricas a continuación dimensionan la "
        "actividad y sirven de referencia (denominador) para los cortes posteriores.\n"
    )
    md.append("| Métrica | Valor |")
    md.append("| --- | --- |")
    md.append(f"| Ofertas Totales (Promedon + competencia) | {n_ofertas_totales:,} |")
    md.append(f"| Ofertas Promedon | {n_ofertas_promedon:,} |")
    md.append(f"| Adjudicaciones | {n_adjudicadas:,} |")
    md.append(f"| Win rate global | {_fmt_pct(wr_global)} |")
    md.append(f"| Revenue total | {_fmt_money(revenue_total)} |")
    md.append(f"| Competidores distintos | {n_competidores:,} |")
    md.append(f"| Compradores distintos | {n_compradores:,} |\n")

    # ── Segmentación de productos por intensidad competitiva ────────────
    t_prod_global = build_tabla_producto(df_base, rut_promedon)
    seg_map, threshold = _segmentar_productos(t_prod_global)
    df_seg = df_base.copy()
    df_seg["producto_segmento"] = df_seg["producto_canonico"].map(seg_map)

    md.append("## Segmentación por intensidad competitiva\n")
    md.append(
        "Cada producto se etiqueta como **A) Alta competencia (commodities)** o "
        "**B) Baja competencia (especializados)** según cuántos competidores "
        "enfrenta Promedon en promedio por item:\n"
        f"- **A — Alta competencia (commodities):** `n_competidores_promedio ≥ {threshold:.2f}` (mediana del universo).\n"
        f"- **B — Baja competencia (especializados):** `n_competidores_promedio < {threshold:.2f}`.\n\n"
        "Productos sin valor de competidores se asignan a B. En las secciones "
        "siguientes cada tabla muestra A y B juntos en una sola vista; los "
        "thresholds locales (mediana de volumen/win rate de compradores, "
        "percentil 25 de precio, cuadrantes precio×resultado) se calculan "
        "independientemente dentro de A y dentro de B.\n"
    )

    rows_split = []
    sub_dict = {}
    sub_p_dict = {}
    for label, label_titulo in [("A", "A — Alta competencia (commodities)"),
                                ("B", "B — Baja competencia (especializados)")]:
        sub = df_seg[df_seg["producto_segmento"] == label]
        sub_p = sub[sub["rut_proveedor"] == rut_promedon]
        sub_dict[label] = sub
        sub_p_dict[label] = sub_p
        rev = float((sub_p["flag_adjudicado"].fillna(0)
                     * sub_p["num_unidades"].fillna(0)
                     * sub_p["precio_unitario"].fillna(0)).sum())
        rows_split.append({
            "segmento": label_titulo,
            "n_productos": sum(1 for v in seg_map.values() if v == label),
            "ofertas_promedon": int(len(sub_p)),
            "revenue": rev,
        })
    df_split = pd.DataFrame(rows_split)
    md.append("### Distribución de productos A vs B\n")
    md.append(_md_table(df_split, {
        "segmento": ("Segmento", None),
        "n_productos": ("# productos", lambda v: f"{int(v):,}"),
        "ofertas_promedon": ("Ofertas Promedon", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, max_rows=len(df_split), totals={
        "n_productos": int(df_split["n_productos"].sum()),
        "ofertas_promedon": int(df_split["ofertas_promedon"].sum()),
        "revenue": float(df_split["revenue"].sum()),
    }))

    # Construir tablas maestras por segmento
    t_prod_dict = {}
    t_prec_dict = {}
    t_comp_dict = {}
    t_compr_dict = {}
    n_ofertas_dict = {}
    for label in ["A", "B"]:
        sub_base = sub_dict[label]
        if sub_base.empty:
            continue
        t_prod_dict[label] = build_tabla_producto(sub_base, rut_promedon)
        t_prec_dict[label] = build_tabla_precio(sub_base, rut_promedon)
        t_comp_dict[label] = build_tabla_competencia(sub_base, rut_promedon)
        t_compr_dict[label] = build_tabla_comprador(sub_base, rut_promedon)
        n_ofertas_dict[label] = int(len(sub_p_dict[label]))

    md.append("---\n")
    md.extend(_seccion_producto(t_prod_dict, prefix="1"))
    md.extend(_seccion_precio(t_prec_dict, prefix="2"))
    md.extend(_seccion_competencia(t_comp_dict, n_ofertas_dict, prefix="3"))
    md.extend(_seccion_comprador(t_compr_dict, prefix="4"))
    md.extend(_seccion_recomendaciones(t_prod_dict, t_prec_dict, t_comp_dict,
                                       t_compr_dict, prefix="5"))

    return "\n".join(md)


# ─── PIPELINE ────────────────────────────────────────────────────────────
def main():
    print(f"=== DIAGNÓSTICO PROMEDON ({PROMEDON_RUT}) ===")

    df = q(MATRIZ_BASE, rut_A=PROMEDON_RUT)
    df.to_csv(os.path.join(OUTPUT_DIR, "matriz_base.csv"), index=False)
    print(f"  matriz_base: {len(df):,} filas (item × proveedor)")

    md = build_reporte_md(df, PROMEDON_RUT)
    md_path = os.path.join(OUTPUT_DIR, "diagnostico_promedon.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"  reporte: {md_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
