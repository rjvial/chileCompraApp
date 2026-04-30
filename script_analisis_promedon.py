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
        - Eliminable:  < 5%
    Y agregas volumen (ofertas) como segunda variable.
    👉 Esto produce:
        P1 Segmento Core
        P2 Segmento Débil
        P3 Segmento Eliminable
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
    Más caro    |premium real    |problema precio
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

HTML_STYLE = """
<style>
    :root {
        --primary: #1d4ed8;
        --primary-soft: #eff6ff;
        --accent: #0ea5e9;
        --secondary: #64748b;
        --success: #16a34a;
        --danger: #dc2626;
        --warning: #d97706;
        --bg: #f1f5f9;
        --card-bg: #ffffff;
        --border: #e2e8f0;
        --border-strong: #cbd5e1;
        --text: #0f172a;
        --text-muted: #475569;
        --text-soft: #64748b;
    }
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, system-ui, sans-serif;
        font-feature-settings: 'cv11', 'ss01', 'tnum';
        font-size: 14px;
        line-height: 1.55;
        color: var(--text);
        background: var(--bg);
        margin: 0;
        padding: 0;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }
    .container { max-width: 1280px; margin: 0 auto; padding: 32px 28px 64px; }

    /* ─── HEADER ───────────────────────────────────────────── */
    .report-header {
        background: linear-gradient(135deg, #1e3a8a 0%, #1d4ed8 60%, #0ea5e9 100%);
        color: #fff;
        border-radius: 14px;
        padding: 28px 32px;
        margin-bottom: 28px;
        box-shadow: 0 6px 20px -8px rgba(29, 78, 216, 0.4);
    }
    .report-header h1 {
        margin: 0 0 6px 0;
        font-size: 1.75em;
        font-weight: 700;
        color: #fff;
        letter-spacing: -0.02em;
        border: none;
        padding: 0;
    }
    .report-header .subtitle {
        opacity: 0.85;
        font-size: 0.95em;
        margin: 0;
    }
    .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 18px;
        margin-top: 14px;
        font-size: 0.85em;
    }
    .meta-row .meta {
        background: rgba(255,255,255,0.12);
        border: 1px solid rgba(255,255,255,0.18);
        padding: 4px 10px;
        border-radius: 6px;
        backdrop-filter: blur(4px);
    }
    .meta-row .meta b { font-weight: 600; opacity: 0.95; }

    /* ─── HEADINGS ─────────────────────────────────────────── */
    h1, h2, h3, h4 { color: var(--text); letter-spacing: -0.01em; }
    h2 {
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 1.3em;
        font-weight: 700;
        margin: 0 0 14px 0;
        padding: 0 0 12px 0;
        border-bottom: 1px solid var(--border);
        color: var(--text);
    }
    .sec-num {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 30px;
        height: 30px;
        background: var(--primary);
        color: #fff;
        border-radius: 8px;
        font-size: 0.85em;
        font-weight: 700;
    }
    h3 {
        font-size: 1.02em;
        font-weight: 600;
        color: var(--text);
        margin: 22px 0 8px 0;
        padding-left: 10px;
        border-left: 3px solid var(--accent);
    }
    h3 + p { color: var(--text-muted); margin-top: 4px; }

    /* ─── CARDS ────────────────────────────────────────────── */
    .card {
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 12px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        padding: 24px 28px;
        margin-bottom: 22px;
    }
    .card > p:first-of-type { color: var(--text-muted); margin-top: 4px; }
    .card ul { color: var(--text-muted); padding-left: 20px; margin: 8px 0 12px; }
    .card ul li { margin: 3px 0; }

    /* ─── TABLES ───────────────────────────────────────────── */
    table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        margin: 12px 0 18px;
        font-size: 0.875em;
        background: white;
        border: 1px solid var(--border);
        border-radius: 8px;
        overflow: hidden;
    }
    thead th {
        background: #f8fafc;
        color: var(--text-muted);
        font-weight: 600;
        text-align: left;
        padding: 10px 12px;
        border-bottom: 1px solid var(--border-strong);
        font-size: 0.78em;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        white-space: nowrap;
    }
    tbody td {
        padding: 8px 12px;
        border-bottom: 1px solid #f1f5f9;
        vertical-align: middle;
    }
    tbody tr:last-child td { border-bottom: none; }
    tbody tr:nth-child(even) { background: #fafbfc; }
    tbody tr:hover { background: var(--primary-soft); }
    .total-row td {
        background: #f8fafc !important;
        font-weight: 700;
        color: var(--text);
        border-top: 2px solid var(--border-strong);
        border-bottom: none;
    }
    .total-row:hover td { background: #f8fafc !important; }

    /* ─── BADGES ───────────────────────────────────────────── */
    .badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 0.78em;
        font-weight: 600;
        letter-spacing: 0.01em;
        line-height: 1.5;
        border: 1px solid transparent;
    }
    .badge-core { background: #dcfce7; color: #14532d; border-color: #bbf7d0; }
    .badge-weak { background: #fef9c3; color: #713f12; border-color: #fde68a; }
    .badge-crit { background: #fee2e2; color: #7f1d1d; border-color: #fecaca; }

    /* ─── NUMERIC / EMPHASIS ───────────────────────────────── */
    .numeric {
        text-align: right;
        font-variant-numeric: tabular-nums;
        font-feature-settings: 'tnum';
        white-space: nowrap;
    }
    .positive { color: var(--success); font-weight: 600; }
    .negative { color: var(--danger); font-weight: 600; }

    .center {
        text-align: center;
    }

    code, .mono {
        font-family: 'JetBrains Mono', 'SF Mono', Consolas, 'Courier New', monospace;
        font-size: 0.92em;
        background: #f1f5f9;
        padding: 1px 5px;
        border-radius: 4px;
        color: var(--text-muted);
    }

    /* ─── SUMMARY KPI GRID ─────────────────────────────────── */
    .summary-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
        gap: 14px;
        margin-bottom: 24px;
    }
    .summary-stat {
        background: white;
        padding: 16px 18px;
        border: 1px solid var(--border);
        border-radius: 10px;
        position: relative;
        overflow: hidden;
        transition: transform 0.15s, box-shadow 0.15s;
    }
    .summary-stat::before {
        content: '';
        position: absolute;
        left: 0; top: 0; bottom: 0;
        width: 3px;
        background: linear-gradient(180deg, var(--primary), var(--accent));
    }
    .summary-stat:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px -4px rgba(15, 23, 42, 0.08);
    }
    .stat-label {
        font-size: 0.72em;
        color: var(--text-soft);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-weight: 600;
    }
    .stat-value {
        font-size: 1.5em;
        font-weight: 700;
        display: block;
        color: var(--text);
        margin-top: 4px;
        font-variant-numeric: tabular-nums;
        letter-spacing: -0.01em;
    }

    /* ─── PRINT ────────────────────────────────────────────── */
    @media print {
        body { background: white; padding: 0; font-size: 10pt; }
        .container { max-width: 100%; padding: 0; }
        .report-header { box-shadow: none; }
        .card { break-inside: avoid; box-shadow: none; border: 1px solid #ccc; }
        table { break-inside: auto; font-size: 9pt; }
        tr { break-inside: avoid; }
        thead { display: table-header-group; }
    }
</style>
"""

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
    Segmentación: Core (≥40%) / Competitivo (20–40%) / Débil (5–20%) / Eliminable (<5%).
    Diagnóstico: P1 Segmento Core / P2 Segmento Débil / P3 Segmento Eliminable.
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
    labels = ["Eliminable", "Débil", "Competitivo", "Core"]
    out["segmento"] = pd.cut(out["win_rate"], bins=bins, labels=labels)

    diag = {"Core": "P1 Segmento Core",
            "Débil": "P2 Segmento Débil",
            "Eliminable": "P3 Segmento Eliminable",
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
    stats = (competidores.groupby("item_key")
             .agg(p25=("precio_unitario", lambda s: s.quantile(0.25)),
                  p50=("precio_unitario", "median"),
                  p75=("precio_unitario", lambda s: s.quantile(0.75)),
                  precio_min_competidor=("precio_unitario", "min"),
                  n_competidores=("rut_proveedor", "nunique"))
             .reset_index())

    out = promedon.merge(stats, on="item_key", how="left")

    base["precio_rank"] = base.groupby("item_key")["precio_unitario"].rank(method="min")
    rank = base.loc[base["rut_proveedor"] == rut_promedon, ["item_key", "precio_rank"]]
    out = out.merge(rank, on="item_key", how="left")

    out["gap"] = (out["precio_promedon"] - out["p25"]) / out["p25"]
    out["gap_to_win"] = (out["precio_promedon"] - out["precio_unitario_ganador"]) / out["precio_promedon"]
    out["gap_to_lose"] = (out["precio_min_competidor"] - out["precio_promedon"]) / out["precio_promedon"]

    # Segmentación mecanismo-consistente (distancia al boundary de adjudicación):
    #   Ganador  → delta_precio_win = (p_min_comp − p_promedon) / p_promedon  (headroom)
    #   Perdedor → delta_precio_lose = (p_promedon − p_ganador) / p_promedon   (required discount)
    def diag_precio(r):
        won = bool(r["promedon_gano"])
        delta_precio_win = r["gap_to_lose"]
        delta_precio_lose = r["gap_to_win"]
        if won:
            if pd.isna(delta_precio_win):
                return "W5 — Monopoly"
            if delta_precio_win < 0:
                return "W4 — Loyal"
            if delta_precio_win <= 0.05:
                return "W1 — Tight"
            if delta_precio_win <= 0.20:
                return "W2 — Slack"
            return "W3 — High"
        else:
            if pd.isna(delta_precio_lose):
                return "L3 — Outsider"
            if delta_precio_lose <= 0:
                return "L3 — Outsider"
            if delta_precio_lose <= 0.05:
                return "L1 — Close"
            return "L2 — Steep"
    out["diagnostico"] = out.apply(diag_precio, axis=1)

    def comp_bucket(n):
        if pd.isna(n) or n <= 2:
            return "Low (≤2)"
        if n <= 4:
            return "Mid (3-4)"
        return "High (≥5)"
    out["competition_bucket"] = out["n_competidores"].apply(comp_bucket)

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
    Cuadrantes: premium real / eficiencia / problema precio / problema no-precio.
    """
    base = df_base.copy()

    promedon = (base[base["rut_proveedor"] == rut_promedon]
                [["item_key", "precio_unitario", "flag_adjudicado", "num_unidades"]]
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
    h2h["revenue_promedon"] = (h2h["gano_promedon"].fillna(0)
                               * h2h["num_unidades"].fillna(0)
                               * h2h["precio_promedon"].fillna(0))
    h2h["ticket_promedon"] = (h2h["num_unidades"].fillna(0)
                              * h2h["precio_promedon"].fillna(0))

    out = (h2h.groupby(["rut_proveedor", "nombre_proveedor"], dropna=False)
           .agg(items_compartidos=("item_key", "nunique"),
                wr_promedon=("gano_promedon", "mean"),
                wr_competidor=("gano_competidor", "mean"),
                adjudicadas=("gano_promedon", "sum"),
                revenue=("revenue_promedon", "sum"),
                ticket_promedio=("ticket_promedon", "mean"),
                mediana_sobreprecio=("sobreprecio", "median"),
                veces_mas_caro=("mas_caro", "sum"),
                veces_mas_barato=("mas_barato", "sum"))
           .reset_index())
    out["adjudicadas"] = out["adjudicadas"].astype(int)

    def cuad(r):
        gana = r["wr_promedon"] >= 0.5
        caro = pd.notna(r["mediana_sobreprecio"]) and r["mediana_sobreprecio"] > 0
        if gana and caro: return "premium real"
        if gana and not caro: return "eficiencia"
        if not gana and caro: return "problema precio"
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
    item_stats = (df_base.groupby("item_key")
                  .agg(n_competidores=("rut_proveedor", lambda s: s.nunique() - 1))
                  .reset_index())

    promedon = df_base[df_base["rut_proveedor"] == rut_promedon].copy()
    promedon = promedon.merge(item_stats, on="item_key", how="left")

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
                n_competidores_promedio=("n_competidores", "mean"),
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
# REPORTE — diagnóstico Promedon en HTML
# ═════════════════════════════════════════════════════════════════════════
def _fmt_money(x) -> str:
    if pd.isna(x): return "-"
    return f"${x:,.0f}".replace(",", ".")


def _fmt_pct(x, decimals: int = 1) -> str:
    if pd.isna(x): return "-"
    return f"{x*100:.{decimals}f}%"


def _generate_table(df: pd.DataFrame, cols: dict, max_rows: int = 10,
                   totals: dict | None = None) -> str:
    """
    Genera una tabla HTML. cols = {col_origen: (header, formatter, [alignment])}.
    """
    if df.empty:
        return "<p><i>(sin datos)</i></p>"
    
    # Pre-identificar columnas numéricas para alineación consistente
    numeric_cols = {}
    for src in cols:
        if src in df.columns:
            numeric_cols[src] = pd.api.types.is_numeric_dtype(df[src])
        else:
            numeric_cols[src] = False

    df_show = df.head(max_rows)
    html = ["<table><thead><tr>"]
    for src, col_info in cols.items():
        header = col_info[0]
        alignment = col_info[2] if len(col_info) > 2 else None
        
        classes = []
        if numeric_cols.get(src): classes.append("numeric")
        if alignment == "center": classes.append("center")
        
        class_str = f" class='{' '.join(classes)}'" if classes else ""
        html.append(f"<th{class_str}>{header}</th>")
    html.append("</tr></thead><tbody>")
    
    for _, row in df_show.iterrows():
        html.append("<tr>")
        for src, col_info in cols.items():
            fmt = col_info[1]
            alignment = col_info[2] if len(col_info) > 2 else None
            val = row.get(src)
            content = fmt(val) if fmt else ("-" if pd.isna(val) else str(val))
            
            classes = []
            if numeric_cols.get(src): classes.append("numeric")
            if alignment == "center": classes.append("center")
            
            class_str = f" class='{' '.join(classes)}'" if classes else ""
            html.append(f"<td{class_str}>{content}</td>")
        html.append("</tr>")
        
    if totals is not None:
        html.append("<tr class='total-row'>")
        for i, (src, col_info) in enumerate(cols.items()):
            fmt = col_info[1]
            alignment = col_info[2] if len(col_info) > 2 else None
            val = totals.get(src)
            label = "**Total**" if i == 0 and val is None else ""
            if pd.isna(val):
                content = "-"
            else:
                content = fmt(val) if fmt else str(val)
            if label: content = label
            
            classes = []
            if numeric_cols.get(src): classes.append("numeric")
            if alignment == "center": classes.append("center")
            
            class_str = f" class='{' '.join(classes)}'" if classes else ""
            html.append(f"<td{class_str}>{content}</td>")
        html.append("</tr>")
        
    html.append("</tbody></table>")
    return "".join(html)


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


def _calculate_optimal_discount(df_seg: pd.DataFrame) -> dict | None:
    """
    Descuento único que maximiza revenue recuperado en un segmento de perdedores
    (L1/L2). Recuperamos un item si: precio_promedon * (1 − d) < precio_unitario_ganador
    (equivalente: d > gap_to_win).
    """
    sub = df_seg.dropna(subset=["gap_to_win", "num_unidades", "precio_promedon"])
    if sub.empty:
        return None
    results = []
    for d in range(1, 81):  # 1% a 80%
        d_rate = d / 100.0
        won_mask = d_rate > sub["gap_to_win"]
        rev = (sub["num_unidades"] * sub["precio_promedon"] * (1 - d_rate) * won_mask).sum()
        results.append({"discount": d_rate, "revenue": rev, "n_items": int(won_mask.sum())})
    best = max(results, key=lambda x: x["revenue"])
    if best["revenue"] == 0:
        return None
    return best


def _calculate_optimal_uplift(df_seg: pd.DataFrame) -> dict | None:
    """
    Uplift único que maximiza revenue total en un segmento de ganadores con
    headroom positivo (W2/W3). Un item mantiene la adjudicación si:
        precio_promedon * (1 + u) < precio_min_competidor
    """
    sub = df_seg.dropna(subset=["num_unidades", "precio_promedon"])
    if sub.empty:
        return None
    results = []
    for u in range(0, 81):  # 0% a 80%
        u_rate = u / 100.0
        new_price = sub["precio_promedon"] * (1 + u_rate)
        mantiene = sub["precio_min_competidor"].isna() | (new_price < sub["precio_min_competidor"])
        rev = (sub["num_unidades"] * new_price * mantiene).sum()
        results.append({"uplift": u_rate, "revenue": rev, "n_items": int(mantiene.sum())})
    best = max(results, key=lambda x: x["revenue"])
    if best["revenue"] == 0:
        return None
    return best


SEGMENT_ORDER = [
    "W1 — Tight",
    "W2 — Slack",
    "W3 — High",
    "W4 — Loyal",
    "W5 — Monopoly",
    "L1 — Close",
    "L2 — Steep",
    "L3 — Outsider",
]
SEGMENT_LABEL = {
    "W1 — Tight": "W1 — Tight",
    "W2 — Slack": "W2 — Slack",
    "W3 — High": "W3 — High",
    "W4 — Loyal": "W4 — Loyal",
    "W5 — Monopoly": "W5 — Monopoly",
    "L1 — Close": "L1 — Close",
    "L2 — Steep": "L2 — Steep",
    "L3 — Outsider": "L3 — Outsider",
}
def _seccion_precio(t_prec_dict: dict, prefix: str = "2") -> list[str]:
    html = []
    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>Precio (Pricing power)</h2>")
    html.append(
        "<p>Cada item se clasifica midiendo qué tan cerca estuvo Promedon de cambiar su resultado "
        "en la licitación. Según el resultado se calcula una de estas dos métricas:</p>"
        "<ul>"
        "<li>Si <b>ganamos</b>: <code>delta_precio_win = (precio_min_competidor − precio_promedon) / precio_promedon</code>. "
        "Mide cuánto podríamos haber subido el precio antes de perder la adjudicación.</li>"
        "<li>Si <b>perdimos</b>: <code>delta_precio_lose = (precio_promedon − precio_ganador) / precio_promedon</code>. "
        "Mide cuánto descuento habríamos necesitado para ganar la adjudicación.</li>"
        "</ul>"
        "<ul>"
        "<li><b>W1 — Tight</b> (delta_precio_win ≤ 5%): precio eficiente.</li>"
        "<li><b>W2 — Slack</b> (5% < delta_precio_win ≤ 20%): subir selectivo.</li>"
        "<li><b>W3 — High</b> (delta_precio_win > 20%): subir sistemático.</li>"
        "<li><b>W4 — Loyal</b> (delta_precio_win &lt; 0): ganamos sin ser el más barato — señal de relación con el comprador.</li>"
        "<li><b>W5 — Monopoly</b>: ganamos sin competencia.</li>"
        "<li><b>L1 — Close</b> (delta_precio_lose ≤ 5%): ajustar pricing.</li>"
        "<li><b>L2 — Steep</b> (delta_precio_lose > 5%): evaluar margen vs volumen.</li>"
        "<li><b>L3 — Outsider</b>: perdimos siendo más baratos que el ganador — señal de relación del comprador con otro proveedor.</li>"
        "</ul>"
    )

    df_all_prec = pd.concat([t_prec_dict[p] for p in ["A", "B"] if p in t_prec_dict], ignore_index=True).copy()
    df_all_prec["revenue"] = (df_all_prec["promedon_gano"].fillna(0)
                              * df_all_prec["num_unidades"].fillna(0)
                              * df_all_prec["precio_promedon"].fillna(0))
    df_all_prec["ticket"] = (df_all_prec["num_unidades"].fillna(0)
                             * df_all_prec["precio_promedon"].fillna(0))

    # ─── Pricing óptimo por segmento (consolidado) ────────────────────────
    html.append("<h3>Pricing óptimo por segmento</h3>")
    html.append(
        "<p style='font-size:12px;color:var(--text-muted)'>"
        "Acción de pricing recomendada y cómo se calcula el revenue al óptimo en cada segmento:"
        "<ul style='font-size:12px;color:var(--text-muted)'>"
        "<li><b>W1, W2, W3</b> — <i>subir precio</i>. Se busca el uplift <code>u</code> que maximiza "
        "revenue manteniendo la adjudicación: el item se mantiene mientras "
        "<code>precio_promedon × (1 + u) &lt; precio_min_competidor</code>.</li>"
        "<li><b>W4 — Loyal</b> — <i>no se simula</i>. Ya ganamos siendo más caros, así que "
        "el boundary de adjudicación no es informativo (el comprador no decidió por precio).</li>"
        "<li><b>W5 — Monopoly</b> — <i>no se simula</i>. Sin competidor no hay precio de referencia "
        "para fijar el uplift; se reporta <code>+0%</code> y revenue al óptimo = revenue actual.</li>"
        "<li><b>L1, L2</b> — <i>bajar precio</i>. Se busca el descuento <code>d</code> que maximiza "
        "revenue recuperando adjudicaciones: el item se gana cuando "
        "<code>precio_promedon × (1 − d) &lt; precio_ganador</code>.</li>"
        "<li><b>L3 — Outsider</b> — <i>no se simula</i>. Perdimos siendo más baratos, así que la "
        "causa no es precio (probable relación del comprador con otro proveedor).</li>"
        "</ul></p>"
    )

    UPLIFT_SEGS = {"W1 — Tight", "W2 — Slack", "W3 — High"}
    DISCOUNT_SEGS = {"L1 — Close", "L2 — Steep"}

    rows_cons = []
    tot_rev_actual = 0.0
    tot_rev_opt = 0.0
    tot_n_opt = 0
    for seg in SEGMENT_ORDER:
        seg_df = df_all_prec[df_all_prec["diagnostico"] == seg].copy()
        n_seg = len(seg_df)
        if n_seg == 0:
            continue
        qty = seg_df["num_unidades"].fillna(0)
        price = seg_df["precio_promedon"].fillna(0)
        rev_actual = float((seg_df["promedon_gano"].fillna(0) * qty * price).sum())
        accion = "—"
        n_opt = None
        rev_opt = None
        if seg == "W5 — Monopoly":
            accion = "+0%"
            n_opt = int(seg_df["promedon_gano"].fillna(0).sum())
            rev_opt = rev_actual
        elif seg in UPLIFT_SEGS:
            opt = _calculate_optimal_uplift(seg_df)
            if opt:
                accion = f"+{opt['uplift']*100:.0f}%"
                n_opt = int(opt["n_items"])
                rev_opt = float(opt["revenue"])
                if opt["uplift"] == 0:
                    n_opt = int(seg_df["promedon_gano"].fillna(0).sum())
                    rev_opt = rev_actual
        elif seg in DISCOUNT_SEGS:
            opt = _calculate_optimal_discount(seg_df)
            if opt:
                accion = f"−{opt['discount']*100:.0f}%"
                n_opt = int(opt["n_items"])
                rev_opt = float(opt["revenue"])
                if opt["discount"] == 0:
                    n_opt = int(seg_df["promedon_gano"].fillna(0).sum())
                    rev_opt = rev_actual
        rev_opt_show = rev_opt if rev_opt is not None else rev_actual
        rows_cons.append({
            "segmento": SEGMENT_LABEL[seg],
            "competidores_unicos": float(seg_df["n_competidores"].fillna(0).mean()),
            "ofertas": n_seg,
            "adjudicadas": int(seg_df["promedon_gano"].fillna(0).sum()),
            "delta_precio_win_med": (float(seg_df["gap_to_lose"].median())
                          if seg_df["gap_to_lose"].notna().any() else None),
            "delta_precio_lose_med": (float(seg_df["gap_to_win"].median())
                          if seg_df["gap_to_win"].notna().any() else None),
            "rev_actual": rev_actual,
            "accion": accion,
            "rev_adic": rev_opt_show - rev_actual,
            "rev_opt": rev_opt_show,
        })
        tot_rev_actual += rev_actual
        tot_rev_opt += rev_opt_show

    df_cons = pd.DataFrame(rows_cons)
    tot_ofertas_cons = int(df_cons["ofertas"].sum()) if len(df_cons) else 0
    tot_adj_cons = int(df_cons["adjudicadas"].sum()) if len(df_cons) else 0
    tot_comp_unicos = (float(df_all_prec["n_competidores"].fillna(0).mean())
                       if len(df_all_prec) else 0.0)
    tot_umax_cons = (float(df_all_prec["gap_to_lose"].median())
                     if df_all_prec["gap_to_lose"].notna().any() else None)
    tot_dmin_cons = (float(df_all_prec["gap_to_win"].median())
                     if df_all_prec["gap_to_win"].notna().any() else None)
    html.append(_generate_table(df_cons, {
        "segmento": ("Segmento", None),
        "competidores_unicos": ("Comp./Item", lambda v: f"{v:.1f}"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "adjudicadas": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "rev_actual": ("Revenue actual", _fmt_money),
        "accion": ("Var. Precio Optimo", None, "center"),
        "rev_adic": ("Revenue Adic. al Óptimo", _fmt_money),
        "rev_opt": ("Revenue al óptimo", _fmt_money),
    }, max_rows=len(df_cons), totals={
        "segmento": "Total",
        "competidores_unicos": tot_comp_unicos,
        "ofertas": tot_ofertas_cons,
        "adjudicadas": tot_adj_cons,
        "rev_actual": tot_rev_actual,
        "accion": "—",
        "rev_adic": tot_rev_opt - tot_rev_actual,
        "rev_opt": tot_rev_opt,
    }))

    html.append("</div>")
    return html


def _seccion_competencia(t_prec_dict: dict, sub_dict: dict, rut_promedon: str,
                          prefix: str = "3") -> list[str]:
    html = []
    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>Competencia por diagnóstico</h2>")
    html.append(
        "<p>Para cada categoría de diagnóstico del capítulo 1 (Pricing), identifica los "
        "competidores que aparecen en los items de esa categoría y mide su comportamiento "
        "frente a Promedon.</p>"
    )

    all_prec = pd.concat([t_prec_dict[s] for s in ["A", "B"] if s in t_prec_dict],
                         ignore_index=True)
    all_base = pd.concat([sub_dict[s] for s in ["A", "B"] if s in sub_dict], ignore_index=True)

    item_to_diag = dict(zip(all_prec["item_key"], all_prec["diagnostico"]))
    prom_offers = all_base[all_base["rut_proveedor"] == rut_promedon].copy()
    prom_offers["diagnostico"] = prom_offers["item_key"].map(item_to_diag)
    competidores = all_base[all_base["rut_proveedor"] != rut_promedon].copy()
    competidores["diagnostico"] = competidores["item_key"].map(item_to_diag)
    prom_price = prom_offers.set_index("item_key")["precio_unitario"].rename("precio_promedon_item")
    competidores = competidores.join(prom_price, on="item_key")
    competidores["sobreprecio"] = ((competidores["precio_promedon_item"] - competidores["precio_unitario"])
                                   / competidores["precio_unitario"])

    diag_titulos = [(seg, SEGMENT_LABEL[seg]) for seg in SEGMENT_ORDER]

    # Nota sobre multi-adjudicación
    html.append(
        "<p><i>Nota: un mismo item puede tener más de un proveedor adjudicado. "
        "Por eso las columnas <b>Adjudicada Comp</b> / <b>Promedon</b> / <b>Tercero</b> cuentan items "
        "de forma independiente y pueden sumar más que <b>Items Compart.</b></i></p>"
    )

    # Top competidores por diagnóstico
    prom_won = prom_offers.set_index("item_key")["flag_adjudicado"].fillna(0).astype(int).rename("gano_promedon")
    adj_count_per_item = all_base.groupby("item_key")["flag_adjudicado"].sum().rename("adj_count_item")
    for diag, titulo in diag_titulos:
        items_diag = prom_offers.loc[prom_offers["diagnostico"] == diag, "item_key"].unique()
        comp_d = competidores[competidores["item_key"].isin(items_diag)].copy()
        if comp_d.empty:
            html.append(f"<h3>{titulo} — top competidores</h3><p><i>(sin competencia)</i></p>")
            continue
        comp_d = comp_d.join(prom_won, on="item_key")
        comp_d["gano_promedon"] = comp_d["gano_promedon"].fillna(0).astype(int)
        comp_d = comp_d.join(adj_count_per_item, on="item_key")
        comp_d["adj_count_item"] = comp_d["adj_count_item"].fillna(0).astype(int)
        comp_d["adj_terc_row"] = ((comp_d["adj_count_item"]
                                   - comp_d["flag_adjudicado"]
                                   - comp_d["gano_promedon"]) > 0).astype(int)
        agg = (comp_d.groupby(["rut_proveedor", "nombre_proveedor"], dropna=False)
               .agg(items_compartidos=("item_key", "nunique"),
                    adj_comp=("flag_adjudicado", "sum"),
                    adj_prom=("gano_promedon", "sum"),
                    adj_terc=("adj_terc_row", "sum"),
                    sobreprecio_med=("sobreprecio", "median"))
               .reset_index())
        agg["adj_comp"] = agg["adj_comp"].astype(int)
        agg["adj_prom"] = agg["adj_prom"].astype(int)
        agg["adj_terc"] = agg["adj_terc"].astype(int)
        agg["wr_promedon"] = agg["adj_prom"] / agg["items_compartidos"]
        agg = agg.sort_values("items_compartidos", ascending=False)

        top = agg.head(10).copy()
        top_ruts = set(top["rut_proveedor"])
        rows_to_show = top.copy()

        # Items donde algún competidor del top-10 fue adjudicado (para Otros: ese top-10 es "tercero")
        items_top_adj = set(competidores.loc[
            (competidores["rut_proveedor"].isin(top_ruts))
            & (competidores["flag_adjudicado"] == 1)
            & (competidores["item_key"].isin(items_diag)),
            "item_key"
        ].unique())

        otros_d = comp_d[~comp_d["rut_proveedor"].isin(top_ruts)]
        if not otros_d.empty:
            otros_items = set(otros_d["item_key"].unique())
            items_otros = len(otros_items)
            adj_comp_otros = int(otros_d.loc[otros_d["flag_adjudicado"] == 1, "item_key"].nunique())
            adj_prom_otros = int(otros_d.loc[otros_d["gano_promedon"] == 1, "item_key"].nunique())
            adj_terc_otros = len(otros_items & items_top_adj)
            sob_otros = float(otros_d["sobreprecio"].median()) if otros_d["sobreprecio"].notna().any() else None
            otros_row = pd.DataFrame([{
                "rut_proveedor": "",
                "nombre_proveedor": "Otros",
                "items_compartidos": items_otros,
                "adj_comp": adj_comp_otros,
                "adj_prom": adj_prom_otros,
                "adj_terc": adj_terc_otros,
                "wr_promedon": (adj_prom_otros / items_otros) if items_otros else None,
                "sobreprecio_med": sob_otros,
            }])
            rows_to_show = pd.concat([rows_to_show, otros_row], ignore_index=True)

        # Total: grupo = todos los competidores → "tercero" no aplica (todo no-Promedon es competidor)
        items_total = int(comp_d["item_key"].nunique())
        adj_comp_total = int(comp_d.loc[comp_d["flag_adjudicado"] == 1, "item_key"].nunique())
        adj_prom_total = int(comp_d.loc[comp_d["gano_promedon"] == 1, "item_key"].nunique())
        adj_terc_total = 0
        sob_total = float(comp_d["sobreprecio"].median()) if comp_d["sobreprecio"].notna().any() else None

        html.append(f"<h3>{titulo} — top competidores</h3>")
        html.append(_generate_table(rows_to_show, {
            "nombre_proveedor": ("Competidor", None),
            "items_compartidos": ("Items Compart.", lambda v: f"{int(v):,}"),
            "adj_comp": ("Adjudicada<br>Comp", lambda v: f"{int(v):,}"),
            "adj_prom": ("Adjudicada<br>Promedon", lambda v: f"{int(v):,}"),
            "adj_terc": ("Adjudicada<br>Tercero", lambda v: f"{int(v):,}"),
            "sobreprecio_med": ("Sobreprecio Med.", _fmt_pct),
        }, max_rows=len(rows_to_show), totals={
            "nombre_proveedor": "Total",
            "items_compartidos": None,
            "adj_comp": None,
            "adj_prom": None,
            "adj_terc": None,
            "sobreprecio_med": sob_total,
        }))
    html.append("</div>")
    return html


def _seccion_comprador(t_compr_dict: dict, prefix: str = "4") -> list[str]:
    html = []
    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>Compradores (cuentas)</h2>")
    html.append("<p>Mide el comportamiento de demanda por organismo comprador.</p>")
    
    df_all_compr = pd.concat([t_compr_dict[p] for p in ["A", "B"] if p in t_compr_dict], ignore_index=True)
    df_dist = (df_all_compr.groupby("segmento")
               .apply(lambda g: pd.Series({
                   "n_compradores": int(g["rut_comprador"].count()),
                   "win_rate": _wavg(g["win_rate"], g["ofertas"]),
                   "comp_item": _wavg(g["n_competidores_promedio"], g["ofertas"]),
                   "revenue": float(g["revenue"].sum()),
                   "ticket_promedio": _wavg(g["ticket_promedio"], g["ofertas"]),
               }), include_groups=False)
               .reset_index())
    html.append("<h3>Distribución por segmento</h3>")
    html.append(_generate_table(df_dist, {
        "segmento": ("Segmento", None),
        "win_rate": ("WR", _fmt_pct),
        "comp_item": ("Comp./Item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        "n_compradores": ("Compradores", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
        "ticket_promedio": ("Ticket Prom.", _fmt_money),
    }, max_rows=len(df_dist), totals={
        "segmento": "Total",
        "win_rate": _wavg(df_all_compr["win_rate"], df_all_compr["ofertas"]),
        "comp_item": _wavg(df_all_compr["n_competidores_promedio"], df_all_compr["ofertas"]),
        "n_compradores": int(df_dist["n_compradores"].sum()),
        "revenue": float(df_dist["revenue"].sum()),
        "ticket_promedio": _wavg(df_all_compr["ticket_promedio"], df_all_compr["ofertas"]),
    }))

    cols = {
        "seg_producto": ("Cat.", None),
        "nombre_comprador": ("Comprador", None),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "win_rate": ("Win Rate", _fmt_pct),
        "revenue": ("Revenue", _fmt_money),
    }
    for seg in ["Replicar (alta conv + alto vol)", "Expandir (alta conv + bajo vol)", "Intervenir (baja conv + alto vol)"]:
        html.append(f"<h3>{seg}</h3>")
        combined = []
        for seg_p in ["A", "B"]:
            sub = t_compr_dict[seg_p][t_compr_dict[seg_p]["segmento"] == seg].sort_values("revenue", ascending=False).head(10).copy()
            sub["seg_producto"] = seg_p
            combined.append(sub)
        if combined:
            html.append(_generate_table(pd.concat(combined, ignore_index=True), cols))
    html.append("</div>")
    return html


def _seccion_recomendaciones(t_prod_dict: dict, t_prec_dict: dict, t_comp_dict: dict, t_compr_dict: dict, prefix: str = "4") -> list[str]:
    html = []
    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>Recomendaciones consolidadas</h2>")
    
    for label, d_a, d_b, col, val_key in [
        ("Pricing (Items)", t_prec_dict["A"], t_prec_dict["B"], "accion", "diagnostico"),
        ("Competencia (Competidores)", t_comp_dict["A"], t_comp_dict["B"], "cuadrante", "cuadrante")
    ]:
        html.append(f"<h3>{label}</h3>")
        # Agregación manual para el resumen
        keys = set(d_a[val_key].unique()) | set(d_b[val_key].unique())
        summary_rows = []
        for k in sorted(list(keys)):
            a = int((d_a[val_key] == k).sum())
            b = int((d_b[val_key] == k).sum())
            summary_rows.append({col: k, "A": a, "B": b, "Total": a + b})
        html.append(_generate_table(pd.DataFrame(summary_rows), {col: ("Elemento", None), "A": ("Cat. A", None), "B": ("Cat. B", None), "Total": ("Total", None)}, max_rows=100))
    
    html.append("</div>")
    return html


def build_reporte_html(df_base: pd.DataFrame, rut_promedon: str) -> str:
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
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = ["<!DOCTYPE html><html lang='es'><head><meta charset='utf-8'>"]
    html.append("<title>Diagnóstico Promedon</title>")
    html.append("<link rel='preconnect' href='https://fonts.googleapis.com'>")
    html.append("<link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>")
    html.append("<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap' rel='stylesheet'>")
    html.append(HTML_STYLE)
    html.append("</head><body><div class='container'>")
    html.append("<div class='report-header'>")
    html.append("<h1>Diagnóstico Promedon — ChileCompra</h1>")
    html.append("<p class='subtitle'>Análisis estructural item × proveedor: portafolio, pricing, competencia y compradores.</p>")
    html.append(f"<div class='meta-row'><span class='meta'><b>RUT:</b> {rut_promedon}</span>"
                f"<span class='meta'><b>Generado:</b> {fecha}</span></div>")
    html.append("</div>")

    # Resumen Ejecutivo
    html.append("<div class='summary-grid'>")
    for label, val in [("Ofertas Totales", f"{n_ofertas_totales:,}"),
                       ("Ofertas Promedon", f"{n_ofertas_promedon:,}"),
                       ("Adjudicaciones", f"{n_adjudicadas:,}"),
                       ("Win Rate", _fmt_pct(wr_global)),
                       ("Revenue Total", _fmt_money(revenue_total))]:
        html.append(f"<div class='summary-stat'><span class='stat-label'>{label}</span><span class='stat-value'>{val}</span></div>")
    html.append("</div>")

    # Segmentación
    t_prod_global = build_tabla_producto(df_base, rut_promedon)
    seg_map, threshold = _segmentar_productos(t_prod_global)
    df_seg = df_base.copy()
    df_seg["producto_segmento"] = df_seg["producto_canonico"].map(seg_map)

    html.append("<div class='card'><h2>Segmentación por Intensidad Competitiva</h2>")
    html.append(f"<p>Umbral de mediana: <b>{threshold:.2f} competidores/item</b>.</p>")
    
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
        item_comp = sub.groupby("item_key")["rut_proveedor"].nunique() - 1
        comp_item = (float(item_comp[item_comp.index.isin(sub_p["item_key"].unique())].mean())
                     if len(sub_p) else None)
        rows_split.append({
            "segmento": label_titulo,
            "n_productos": sum(1 for v in seg_map.values() if v == label),
            "ofertas_promedon": int(len(sub_p)),
            "adjudicaciones": int(sub_p["flag_adjudicado"].sum()),
            "comp_item": comp_item,
            "revenue": rev,
        })

    df_rows_split = pd.DataFrame(rows_split)
    all_promedon_items = df_seg.loc[df_seg["rut_proveedor"] == rut_promedon, "item_key"].unique()
    total_item_comp = df_seg.groupby("item_key")["rut_proveedor"].nunique() - 1
    tot_comp_item = (float(total_item_comp[total_item_comp.index.isin(all_promedon_items)].mean())
                     if len(all_promedon_items) else None)
    html.append(_generate_table(df_rows_split, {
        "segmento": ("Cat.", None),
        "comp_item": ("Comp./Item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        "n_productos": ("Productos", lambda v: f"{int(v):,}"),
        "ofertas_promedon": ("Ofertas", lambda v: f"{int(v):,}"),
        "adjudicaciones": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "revenue": ("Revenue", _fmt_money),
    }, totals={
        "segmento": "Total",
        "comp_item": tot_comp_item,
        "n_productos": int(df_rows_split["n_productos"].sum()),
        "ofertas_promedon": int(df_rows_split["ofertas_promedon"].sum()),
        "adjudicaciones": int(df_rows_split["adjudicaciones"].sum()),
        "revenue": float(df_rows_split["revenue"].sum()),
    }))
    html.append("</div>")

    # Secciones
    t_prod_dict = {}
    t_prec_dict = {}
    t_comp_dict = {}
    t_compr_dict = {}
    n_ofertas_dict = {}
    for label in ["A", "B"]:
        sub_base = sub_dict[label]
        if sub_base.empty: continue
        t_prod_dict[label] = build_tabla_producto(sub_base, rut_promedon)
        t_prec_dict[label] = build_tabla_precio(sub_base, rut_promedon)
        t_comp_dict[label] = build_tabla_competencia(sub_base, rut_promedon)
        t_compr_dict[label] = build_tabla_comprador(sub_base, rut_promedon)
        n_ofertas_dict[label] = int(len(sub_p_dict[label]))

    html.extend(_seccion_precio(t_prec_dict, prefix="1"))
    html.extend(_seccion_competencia(t_prec_dict, sub_dict, rut_promedon, prefix="2"))
    html.extend(_seccion_comprador(t_compr_dict, prefix="3"))
    html.extend(_seccion_recomendaciones(t_prod_dict, t_prec_dict, t_comp_dict, t_compr_dict, prefix="4"))

    html.append("</div></body></html>")
    return "".join(html)


# ─── PIPELINE ────────────────────────────────────────────────────────────
def main():
    print(f"=== DIAGNÓSTICO PROMEDON ({PROMEDON_RUT}) ===")

    df = q(MATRIZ_BASE, rut_A=PROMEDON_RUT)
    df.to_csv(os.path.join(OUTPUT_DIR, "matriz_base.csv"), index=False)
    print(f"  matriz_base: {len(df):,} filas (item × proveedor)")

    html_content = build_reporte_html(df, PROMEDON_RUT)
    html_path = os.path.join(OUTPUT_DIR, "diagnostico_promedon.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"  reporte: {html_path}")


if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
