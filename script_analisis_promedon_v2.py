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
                  "precio_unitario", "flag_adjudicado", "precio_unitario_ganador"]]
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


def _seccion_producto(t_prod: pd.DataFrame, prefix: str = "1") -> list[str]:
    md = []
    md.append(f"## {prefix} Portafolio (Producto)\n")
    md.append(
        "Mide el **desempeño estructural** de Promedon por categoría de producto. "
        "La unidad de análisis es `producto_canonico` y la métrica clave es el "
        "win rate (adjudicaciones / ofertas), complementada por revenue y volumen.\n"
    )
    md.append(
        "Notas de columnas:\n"
        "- **Ofertas / Adjudicaciones / Revenue / WR**: solo Promedon en items del producto.\n"
        "- **Ticket prom.**: monto medio por oferta Promedon (`num_unidades × precio_unitario`).\n"
        "- **Competidores promedio por item**: para cada item del producto se cuenta "
        "cuántos otros proveedores (distintos a Promedon) ofertaron, y se promedia "
        "ese conteo entre todos los items. Proxy de intensidad competitiva: "
        "valor 0 → Promedon ofertó solo; valor 5 → en promedio compitió contra 5 "
        "proveedores por item.\n"
    )
    md.append(
        "Los productos se clasifican en cuatro segmentos según win rate, lo que "
        "permite separar la conversación \"dónde escalar\" de \"dónde salir\":\n"
        "- **Core** (WR ≥ 40%): fortaleza estructural, foco de crecimiento.\n"
        "- **Competitivo** (20–40%): zona de palancas tácticas (precio, comprador).\n"
        "- **Débil** (5–20%): exige diagnóstico fino antes de invertir más.\n"
        "- **Crítico** (< 5%): candidatos a desinversión salvo razón estratégica.\n"
    )
    seg_prod = (t_prod.groupby("segmento", observed=True)
                .agg(n_productos=("producto_canonico", "count"),
                     ofertas=("ofertas", "sum"),
                     adjudicaciones=("adjudicaciones", "sum"),
                     revenue=("revenue", "sum"))
                .reset_index())
    md.append("### Distribución por segmento\n")
    md.append(_md_table(seg_prod, {
        "segmento": ("Segmento", None),
        "n_productos": ("# productos", lambda v: f"{int(v):,}"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "adjudicaciones": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, totals={
        "n_productos": int(seg_prod["n_productos"].sum()),
        "ofertas": int(seg_prod["ofertas"].sum()),
        "adjudicaciones": int(seg_prod["adjudicaciones"].sum()),
        "revenue": float(seg_prod["revenue"].sum()),
    }))

    p_explicaciones = {
        "Core": (
            "P1 — escalar core",
            "Productos donde Promedon ya gana de forma sistemática (WR ≥ 40%). "
            "Son la base estructural del negocio: la acción es **proteger y "
            "escalar** — invertir en disponibilidad, capacidad comercial y "
            "presencia en más compradores donde aún no se compite. "
            "Top 10 por revenue:",
        ),
        "Débil": (
            "P2 — optimizar débil",
            "Productos con tracción pero baja conversión (WR 5–20%). Aquí no se "
            "decide salir ni doblar la apuesta: la acción es **diagnosticar y "
            "corregir** — revisar precio (sección 2), competidores dominantes "
            "(sección 3) y compradores donde se concentra la pérdida (sección 4). "
            "Top 10 por revenue:",
        ),
        "Crítico": (
            "P3 — eliminar crítico",
            "Productos donde Promedon ofertó pero casi nunca gana (WR < 5%). "
            "Si el volumen de ofertas es alto, el costo de oportunidad comercial "
            "es relevante: la acción es **evaluar salida** — dejar de cotizar o "
            "rediseñar la propuesta de valor. Top 10 por revenue (cuando lo hay):",
        ),
    }

    for seg in ["Core", "Débil", "Crítico"]:
        titulo, descripcion = p_explicaciones[seg]
        full = t_prod[t_prod["segmento"] == seg].sort_values("revenue", ascending=False)
        md.append(f"### {titulo}\n")
        md.append(descripcion + "\n")
        if full.empty:
            md.append("_(sin productos en este segmento)_\n")
            continue
        top = full.head(10)
        rest = full.iloc[10:]
        rows = top.copy()
        if not rest.empty:
            otros_ofertas = int(rest["ofertas"].sum())
            otros_adj = int(rest["adjudicaciones"].sum())
            otros = pd.DataFrame([{
                "producto_canonico": f"Otros ({len(rest)} productos)",
                "ofertas": otros_ofertas,
                "adjudicaciones": otros_adj,
                "revenue": float(rest["revenue"].sum()),
                "win_rate": (otros_adj / otros_ofertas) if otros_ofertas else 0,
                "ticket_promedio": _wavg(rest["ticket_promedio"], rest["ofertas"]),
                "n_competidores_promedio": _wavg(rest["n_competidores_promedio"], rest["ofertas"]),
            }])
            rows = pd.concat([top, otros], ignore_index=True)

        total_ofertas = int(full["ofertas"].sum())
        total_adj = int(full["adjudicaciones"].sum())
        totals = {
            "producto_canonico": f"Total {seg}",
            "ofertas": total_ofertas,
            "win_rate": (total_adj / total_ofertas) if total_ofertas else 0,
            "revenue": float(full["revenue"].sum()),
            "ticket_promedio": _wavg(full["ticket_promedio"], full["ofertas"]),
            "n_competidores_promedio": _wavg(full["n_competidores_promedio"], full["ofertas"]),
        }
        md.append(_md_table(rows, {
            "producto_canonico": ("Producto", None),
            "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
            "win_rate": ("WR", _fmt_pct),
            "revenue": ("Revenue", _fmt_money),
            "ticket_promedio": ("Ticket prom.", _fmt_money),
            "n_competidores_promedio": ("Competidores promedio por item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        }, max_rows=len(rows), totals=totals))
    return md


def _seccion_precio(t_prec: pd.DataFrame, prefix: str = "2") -> list[str]:
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
    seg_prec = (t_prec.groupby("diagnostico")
                .agg(n_items=("item_key", "nunique"))
                .reset_index())
    seg_prec["__order"] = seg_prec["diagnostico"].map(
        {d: i for i, d in enumerate(diag_order)})
    seg_prec = (seg_prec.sort_values("__order")
                .drop(columns="__order")
                .reset_index(drop=True))
    md.append("### Distribución por diagnóstico\n")
    md.append(_md_table(seg_prec, {
        "diagnostico": ("Diagnóstico", None),
        "n_items": ("# items", lambda v: f"{int(v):,}"),
    }, max_rows=20, totals={
        "n_items": int(seg_prec["n_items"].sum()),
    }))

    pr_cols = {
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
        sub = t_prec[t_prec["diagnostico"] == diag].copy()
        md.append(f"### {titulo} (top 10)\n")
        if sub.empty:
            md.append("_(sin items en este diagnóstico)_\n")
            continue
        mask_ext = cfg["ext_mask"](sub)
        sub_extreme = sub[mask_ext]
        sub_show = sub[~mask_ext].sort_values("gap", ascending=cfg["ascending"])
        top = sub_show.head(10)
        rest = pd.concat([sub_show.iloc[10:], sub_extreme], ignore_index=True)
        n_extreme = int(mask_ext.sum())

        rows = top.copy()
        if not rest.empty:
            otros = pd.DataFrame([{
                "id_licitacion": f"Otros ({len(rest)} items, {n_extreme} con {cfg['ext_label']})",
                "producto_canonico": "—",
                "gap": rest["gap"].median(),
            }])
            rows = pd.concat([rows, otros], ignore_index=True)
        totals = {
            "id_licitacion": f"Total: {len(sub):,} items",
            "gap": sub["gap"].median(),
        }
        md.append(_md_table(rows, pr_cols, max_rows=len(rows), totals=totals))
    return md


def _seccion_competencia(t_comp: pd.DataFrame, n_ofertas_promedon: int,
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
        "Los competidores se ubican en cuatro cuadrantes precio × resultado, "
        "que dictan estrategia diferenciada por rival:\n"
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
    seg_comp = (t_comp.groupby("cuadrante")
                .agg(n_competidores=("rut_proveedor", "count"),
                     ofertas=("items_compartidos", "sum"))
                .reset_index()
                .sort_values("ofertas", ascending=False))
    seg_comp = pd.concat([seg_comp, pd.DataFrame([{
        "cuadrante": "Promedon (referencia)",
        "n_competidores": pd.NA,
        "ofertas": n_ofertas_promedon,
    }])], ignore_index=True)
    md.append("### Distribución por cuadrante\n")
    md.append(_md_table(seg_comp, {
        "cuadrante": ("Cuadrante", None),
        "n_competidores": ("# competidores", lambda v: f"{int(v):,}" if pd.notna(v) else "—"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
    }, max_rows=len(seg_comp), totals={
        "n_competidores": int(seg_comp["n_competidores"].dropna().sum()),
        "ofertas": int(seg_comp["ofertas"].sum()),
    }))
    cuad_explicaciones = {
        "pricing problema": "Perdemos siendo más caros — palanca de precio.",
        "problema no-precio": "Perdemos siendo más baratos — palanca no-precio (servicio, marca, especificación técnica).",
        "premium real": "Ganamos siendo más caros — proteger narrativa de valor.",
        "eficiencia": "Ganamos siendo más baratos — defender posición de costo.",
    }
    cuad_cols = {
        "nombre_proveedor": ("Competidor", None),
        "items_compartidos": ("Items", lambda v: f"{int(v):,}"),
        "wr_promedon": ("WR Promedon", _fmt_pct),
        "wr_competidor": ("WR comp.", _fmt_pct),
        "mediana_sobreprecio": ("Mediana sobreprecio", _fmt_pct),
    }
    cuad_orden = ["pricing problema", "problema no-precio", "premium real", "eficiencia"]
    for cuad in cuad_orden:
        sub = t_comp[t_comp["cuadrante"] == cuad].sort_values("items_compartidos", ascending=False)
        md.append(f"### Top en \"{cuad}\" (top 10)\n")
        md.append(cuad_explicaciones[cuad] + "\n")
        if sub.empty:
            md.append("_(sin competidores en este cuadrante)_\n")
            continue
        n_total = len(sub)
        items_total = int(sub["items_compartidos"].sum())
        top = sub.head(10)
        rest = sub.iloc[10:]
        rows = top.copy()
        if not rest.empty:
            otros = pd.DataFrame([{
                "nombre_proveedor": f"Otros ({len(rest)} competidores)",
                "items_compartidos": int(rest["items_compartidos"].sum()),
            }])
            rows = pd.concat([rows, otros], ignore_index=True)
        totals = {
            "nombre_proveedor": f"Total {cuad}: {n_total:,} competidores",
            "items_compartidos": items_total,
        }
        md.append(_md_table(rows, cuad_cols, max_rows=len(rows), totals=totals))
    return md


def _seccion_comprador(t_compr: pd.DataFrame, prefix: str = "4") -> list[str]:
    md = []
    md.append(f"## {prefix} Compradores (cuentas)\n")
    md.append(
        "Mide el **comportamiento de demanda** por organismo comprador. La "
        "unidad es el `rut_comprador`; se agrega ofertas, win rate, revenue, "
        "ticket promedio y mix de productos vendidos a cada cuenta.\n"
    )
    md.append(
        "Cada comprador se ubica en un cuadrante 2D según volumen (eje X) y "
        "win rate (eje Y), usando la mediana como umbral. La acción depende del "
        "cuadrante:\n"
        "- **Replicar** (alta conv + alto vol): cuentas a defender y modelar.\n"
        "- **Expandir** (alta conv + bajo vol): potencial de crecimiento orgánico.\n"
        "- **Intervenir** (baja conv + alto vol): demanda existente que no se captura.\n"
        "- **Ignorar** (baja conv + bajo vol): bajo retorno por esfuerzo comercial.\n"
    )
    md.append("Segmentación 2D (volumen × win rate, umbrales = mediana):\n")
    seg_compr = (t_compr.groupby("segmento")
                 .agg(n_compradores=("rut_comprador", "count"),
                      ofertas=("ofertas", "sum"),
                      revenue=("revenue", "sum"))
                 .reset_index()
                 .sort_values("revenue", ascending=False))
    md.append("### Distribución por segmento\n")
    md.append(_md_table(seg_compr, {
        "segmento": ("Segmento", None),
        "n_compradores": ("# compradores", lambda v: f"{int(v):,}"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, totals={
        "n_compradores": int(seg_compr["n_compradores"].sum()),
        "ofertas": int(seg_compr["ofertas"].sum()),
        "revenue": float(seg_compr["revenue"].sum()),
    }))

    _fmt_int = lambda v: "-" if pd.isna(v) else f"{int(v):,}"
    seg_cols = {
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
        sub = t_compr[t_compr["segmento"] == seg].sort_values("revenue", ascending=False)
        md.append(f"### {seg} (top 10)\n")
        if sub.empty:
            md.append("_(sin compradores en este segmento)_\n")
            continue
        n_total = int(sub["rut_comprador"].count())
        top = sub.head(10)
        rest = sub.iloc[10:]
        rows = top.copy()
        if not rest.empty:
            rest_ofertas = int(rest["ofertas"].sum())
            rest_ganadas = float((rest["ofertas"] * rest["win_rate"]).sum())
            n_rest = int(rest["rut_comprador"].count())
            otros = pd.DataFrame([{
                "nombre_comprador": f"Otros ({n_rest} compradores)",
                "ofertas": rest_ofertas,
                "win_rate": (rest_ganadas / rest_ofertas) if rest_ofertas else float("nan"),
                "revenue": float(rest["revenue"].sum()),
                "ticket_promedio": float("nan"),
                "mix_productos": pd.NA,
            }])
            rows = pd.concat([rows, otros], ignore_index=True)
        sub_ofertas = int(sub["ofertas"].sum())
        sub_ganadas = float((sub["ofertas"] * sub["win_rate"]).sum())
        totals = {
            "nombre_comprador": f"Total: {n_total:,} compradores",
            "ofertas": sub_ofertas,
            "win_rate": (sub_ganadas / sub_ofertas) if sub_ofertas else float("nan"),
            "revenue": float(sub["revenue"].sum()),
        }
        md.append(_md_table(rows, seg_cols, max_rows=len(rows), totals=totals))
    return md


def _seccion_recomendaciones(t_prod: pd.DataFrame, t_prec: pd.DataFrame,
                             t_comp: pd.DataFrame, t_compr: pd.DataFrame,
                             prefix: str = "5") -> list[str]:
    md = []
    md.append(f"## {prefix} Recomendaciones consolidadas\n")
    md.append(
        "Síntesis cuantitativa de los cortes anteriores: un conteo por acción "
        "en cada dimensión. **Las recomendaciones no son nuevas conclusiones**, "
        "sino la combinación trazable de los segmentos de las secciones previas. "
        "Cada item/producto/competidor/cuenta listado aquí puede reconstruirse "
        "filtrando la tabla maestra correspondiente.\n"
    )
    n_core = int((t_prod["segmento"] == "Core").sum())
    n_debil = int((t_prod["segmento"] == "Débil").sum())
    n_critico = int((t_prod["segmento"] == "Crítico").sum())
    n_pr1 = int((t_prec["diagnostico"] == "PR1: subir precio (subcotizado)").sum())
    n_pr2 = int((t_prec["diagnostico"] == "PR2: bajar precio (sobreprecio perdedor)").sum())
    n_pr3 = int((t_prec["diagnostico"] == "PR3: barato y pierde (señal no-precio)").sum())
    n_replicar = int((t_compr["segmento"] == "Replicar (alta conv + alto vol)").sum())
    n_expandir = int((t_compr["segmento"] == "Expandir (alta conv + bajo vol)").sum())
    n_intervenir = int((t_compr["segmento"] == "Intervenir (baja conv + alto vol)").sum())

    md.append("**Portafolio**")
    md.append(f"- Escalar {n_core} productos Core")
    md.append(f"- Optimizar {n_debil} productos Débiles")
    md.append(f"- Evaluar salida en {n_critico} productos Críticos\n")
    md.append("**Pricing**")
    md.append(f"- Subir precios en {n_pr1} items con gap negativo validado")
    md.append(f"- Bajar precios selectivamente en {n_pr2} items (sobreprecio perdedor)")
    md.append(f"- Atacar {n_pr3} items \"barato y pierde\" con palancas no-precio\n")
    md.append("**Competencia**")
    for cuad in ["pricing problema", "problema no-precio", "premium real", "eficiencia"]:
        n = int((t_comp["cuadrante"] == cuad).sum())
        md.append(f"- {cuad}: {n} competidores")
    md.append("")
    md.append("**Cuentas**")
    md.append(f"- Defender {n_replicar} cuentas Replicar")
    md.append(f"- Expandir {n_expandir} cuentas Expandir")
    md.append(f"- Intervenir {n_intervenir} cuentas Intervenir\n")
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
        "Antes de mirar las 4 dimensiones, cada producto se etiqueta como "
        "**A) Alta competencia (commodities)** o **B) Baja competencia "
        "(especializados)** según cuántos competidores enfrenta Promedon en "
        "promedio por item:\n"
        f"- **A — Alta competencia (commodities):** `n_competidores_promedio ≥ {threshold:.2f}` (mediana del universo).\n"
        f"- **B — Baja competencia (especializados):** `n_competidores_promedio < {threshold:.2f}`.\n\n"
        "Productos sin valor de competidores se asignan a B. Las 4 dimensiones se "
        "rehacen completas dentro de cada segmento — los thresholds locales "
        "(mediana de volumen/win rate de compradores, percentil 25 de precio) se "
        "recalculan independientemente para A y para B.\n"
    )

    rows_split = []
    for label, label_titulo in [("A", "A — Alta competencia (commodities)"),
                                ("B", "B — Baja competencia (especializados)")]:
        sub = df_seg[df_seg["producto_segmento"] == label]
        sub_p = sub[sub["rut_proveedor"] == rut_promedon]
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

    descripciones = {
        "A": (
            "A. Alta competencia (commodities)",
            "Productos donde Promedon enfrenta varios proveedores por item. El "
            "precio relativo es la palanca dominante: gaps pequeños suelen "
            "explicar las pérdidas y el margen se decide en pocos puntos "
            "porcentuales. La estrategia natural es eficiencia operacional + "
            "ajuste fino de pricing.",
        ),
        "B": (
            "B. Baja competencia (especializados)",
            "Productos donde Promedon es uno de pocos proveedores cualificados. "
            "El patrón típico es **PR3 dominante** (perdimos aun siendo más "
            "baratos), señal de que el problema no es precio sino especificación "
            "técnica, marca, evaluación clínica o relación con el comprador. La "
            "estrategia natural es palancas no-precio: portafolio, certificaciones, "
            "foco en cuentas con afinidad.",
        ),
    }

    for seg_label in ["A", "B"]:
        sub_base = df_seg[df_seg["producto_segmento"] == seg_label]
        if sub_base.empty:
            continue
        titulo, descripcion = descripciones[seg_label]
        sub_promedon = sub_base[sub_base["rut_proveedor"] == rut_promedon]

        t_prod = build_tabla_producto(sub_base, rut_promedon)
        t_prec = build_tabla_precio(sub_base, rut_promedon)
        t_comp = build_tabla_competencia(sub_base, rut_promedon)
        t_compr = build_tabla_comprador(sub_base, rut_promedon)

        md.append("---\n")
        md.append(f"# {titulo}\n")
        md.append(descripcion + "\n")
        md.extend(_seccion_producto(t_prod, prefix=f"{seg_label}.1"))
        md.extend(_seccion_precio(t_prec, prefix=f"{seg_label}.2"))
        md.extend(_seccion_competencia(t_comp, len(sub_promedon),
                                       prefix=f"{seg_label}.3"))
        md.extend(_seccion_comprador(t_compr, prefix=f"{seg_label}.4"))
        md.extend(_seccion_recomendaciones(t_prod, t_prec, t_comp, t_compr,
                                           prefix=f"{seg_label}.5"))

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
