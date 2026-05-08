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

Y declarar explícitamente las 2 dimensiones independientes:
    - Producto     → desempeño estructural (WR)
    - Precio       → posicionamiento relativo

Todo el análisis cuelga de esto. No hay eje dominante.


2) Construir 2 "tablas maestras" (una por dimensión)
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

    /* ─── HEATMAP ──────────────────────────────────────────── */
    .heatmap { width: 100%; border-collapse: collapse; margin: 8px 0 16px; }
    .heatmap th, .heatmap td {
        border: 1px solid var(--border);
        padding: 6px 8px;
        font-size: 0.88em;
    }
    .heatmap th { background: var(--primary-soft); font-weight: 600; color: var(--text); }
    .heat-cell {
        text-align: center;
        font-variant-numeric: tabular-nums;
        font-weight: 600;
    }

    /* ─── RIVAL CARDS ──────────────────────────────────────── */
    .rival-cards {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
        gap: 14px;
        margin: 12px 0 18px;
    }
    .rival-card {
        background: #f8fafc;
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 14px 16px;
    }
    .rival-card h4 {
        margin: 0;
        font-size: 0.98em;
        font-weight: 700;
        color: var(--text);
    }
    .rival-card .rut {
        font-family: 'JetBrains Mono', 'SF Mono', Consolas, monospace;
        font-size: 0.78em;
        color: var(--text-soft);
        margin-bottom: 8px;
    }
    .rival-card .stat-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 6px;
        margin: 8px 0;
    }
    .rival-card .stat-grid > div {
        font-size: 0.72em;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .rival-card .stat-grid > div b {
        display: block;
        font-size: 1.35em;
        color: var(--text);
        font-weight: 700;
        font-variant-numeric: tabular-nums;
        text-transform: none;
        letter-spacing: 0;
        margin-top: 2px;
    }
    .rival-card .seg-list {
        font-size: 0.84em;
        color: var(--text-muted);
        margin: 5px 0 0 0;
        line-height: 1.5;
    }
    .rival-card .seg-list b { color: var(--text); }

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
                  precio_siguiente_ganador=("precio_unitario", "min"),
                  n_competidores=("rut_proveedor", "nunique"))
             .reset_index())

    out = promedon.merge(stats, on="item_key", how="left")

    base["precio_rank"] = base.groupby("item_key")["precio_unitario"].rank(method="min")
    rank = base.loc[base["rut_proveedor"] == rut_promedon, ["item_key", "precio_rank"]]
    out = out.merge(rank, on="item_key", how="left")

    out["gap"] = (out["precio_promedon"] - out["p25"]) / out["p25"]
    out["gap_to_win"] = (out["precio_unitario_ganador"] - out["precio_promedon"]) / out["precio_promedon"]
    out["gap_to_lose"] = (out["precio_siguiente_ganador"] - out["precio_promedon"]) / out["precio_promedon"]

    # Segmentación mecanismo-consistente (distancia al boundary de adjudicación):
    #   Definimos delta(p) = (p − precio_promedon) / precio_promedon
    #   Ganador  → delta(win)  = delta(p_sig_ganador)  (headroom)
    #   Perdedor → delta(lose) = delta(p_ganador)      (required discount)
    def diag_precio(r):
        won = bool(r["promedon_gano"])
        delta_win = r["gap_to_lose"]
        delta_lose = r["gap_to_win"]
        if won:
            if pd.isna(delta_win):
                return "W5 — Monopoly"
            if delta_win < 0:
                return "W4 — Lock-In"
            if delta_win <= 0.05:
                return "W1 — Aligned"
            if delta_win <= 0.15:
                return "W2 — Improvable"
            return "W3 — Comp-Overshot"
        else:
            if pd.isna(delta_lose):
                return "D — Unawarded"
            if delta_lose >= 0:
                return "L3 — Lock-Out"
            if delta_lose >= -0.15:
                return "L1 — Recoverable"
            return "L2 — Overshot"
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
# REPORTE — diagnóstico Promedon en HTML
# ═════════════════════════════════════════════════════════════════════════
def _fmt_money(x) -> str:
    if pd.isna(x): return "-"
    sign = "-" if x < 0 else ""
    millions = abs(x) / 1_000_000
    int_part, dec_part = f"{millions:.1f}".split(".")
    int_with_sep = f"{int(int_part):,}".replace(",", ".")
    return f"{sign}MM$ {int_with_sep},{dec_part}"


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
    (L1). Recuperamos un item si: precio_promedon * (1 − d) < precio_unitario_ganador
    (equivalente: d > -delta_lose).
    """
    sub = df_seg.dropna(subset=["gap_to_win", "num_unidades", "precio_promedon"])
    if sub.empty:
        return None
    results = []
    for d in range(1, 81):  # 1% a 80%
        d_rate = d / 100.0
        won_mask = d_rate > -sub["gap_to_win"]
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
        precio_promedon * (1 + u) < precio_siguiente_ganador
    """
    sub = df_seg.dropna(subset=["num_unidades", "precio_promedon"])
    if sub.empty:
        return None
    results = []
    for u in range(0, 81):  # 0% a 80%
        u_rate = u / 100.0
        new_price = sub["precio_promedon"] * (1 + u_rate)
        mantiene = sub["precio_siguiente_ganador"].isna() | (new_price < sub["precio_siguiente_ganador"])
        rev = (sub["num_unidades"] * new_price * mantiene).sum()
        results.append({"uplift": u_rate, "revenue": rev, "n_items": int(mantiene.sum())})
    best = max(results, key=lambda x: x["revenue"])
    if best["revenue"] == 0:
        return None
    return best


SEGMENT_ORDER = [
    "W1 — Aligned",
    "W2 — Improvable",
    "W3 — Comp-Overshot",
    "W4 — Lock-In",
    "W5 — Monopoly",
    "L1 — Recoverable",
    "L2 — Overshot",
    "L3 — Lock-Out",
    "D — Unawarded",
]
SEGMENT_LABEL = {
    "W1 — Aligned": "W1 — Aligned",
    "W2 — Improvable": "W2 — Improvable",
    "W3 — Comp-Overshot": "W3 — Comp-Overshot",
    "W4 — Lock-In": "W4 — Lock-In",
    "W5 — Monopoly": "W5 — Monopoly",
    "L1 — Recoverable": "L1 — Recoverable",
    "L2 — Overshot": "L2 — Overshot",
    "L3 — Lock-Out": "L3 — Lock-Out",
    "D — Unawarded": "D — Unawarded",
}
def _seccion_anual(df_universo: pd.DataFrame, rut_promedon: str) -> list[str]:
    """Resumen agregado por año: ofertas, adjudicaciones (Promedon vs. competencia)
    y revenue por lado, sobre los items donde Promedon ofertó. Recibe el universo
    ya filtrado (mismo set de items que el resto de las tablas)."""
    html = []
    promedon = df_universo[df_universo["rut_proveedor"] == rut_promedon].copy()
    if "anio" not in promedon.columns or promedon.empty:
        return html

    # Competidores por item (excluye a Promedon)
    item_comp = (df_universo.groupby("item_key")["rut_proveedor"].nunique() - 1).rename("n_comp")
    promedon = promedon.merge(item_comp, on="item_key", how="left")

    rows = []
    for anio, grp in promedon.groupby("anio", dropna=False):
        comp_mask = ((grp["precio_unitario_ganador"].notna())
                     & (grp["flag_adjudicado"].fillna(0) == 0))
        rev_promedon = float((grp["flag_adjudicado"].fillna(0)
                              * grp["num_unidades"].fillna(0)
                              * grp["precio_unitario"].fillna(0)).sum())
        rev_comp = float((grp.loc[comp_mask, "num_unidades"].fillna(0)
                          * grp.loc[comp_mask, "precio_unitario_ganador"].fillna(0)).sum())
        n_ofertas = len(grp)
        n_adj = int(grp["flag_adjudicado"].fillna(0).sum())
        rows.append({
            "anio": str(int(anio)) if pd.notna(anio) else "(s/a)",
            "comp_item": float(grp["n_comp"].fillna(0).mean()) if n_ofertas else 0.0,
            "ofertas": n_ofertas,
            "adj_comp": int(comp_mask.sum()),
            "adjudicadas": n_adj,
            "wr_promedon": (n_adj / n_ofertas) if n_ofertas else 0.0,
            "rev_comp": rev_comp,
            "rev_promedon": rev_promedon,
        })

    df_y = pd.DataFrame(rows).sort_values("anio").reset_index(drop=True)
    tot_ofertas = int(df_y["ofertas"].sum())
    tot_comp_item = (float((df_y["comp_item"] * df_y["ofertas"]).sum() / tot_ofertas)
                     if tot_ofertas > 0 else 0.0)

    html.append("<div class='card'><h2>Resumen anual</h2>")
    html.append(
        "<p>Evolución anual de la posición de Promedon frente a la competencia. "
        "<i>Revenue Comp. considera solo items en los que Promedon ofertó.</i></p>"
    )
    tot_adj = int(df_y["adjudicadas"].sum())
    html.append(_generate_table(df_y, {
        "anio": ("Año", None, "center"),
        "comp_item": ("Comp./Item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "adj_comp": ("Adjudicadas Comp.", lambda v: f"{int(v):,}"),
        "adjudicadas": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "wr_promedon": ("WR Promedon", _fmt_pct),
        "rev_comp": ("Revenue Comp.", _fmt_money),
        "rev_promedon": ("Revenue Promedon", _fmt_money),
    }, max_rows=len(df_y), totals={
        "anio": "Total",
        "comp_item": tot_comp_item,
        "ofertas": tot_ofertas,
        "adj_comp": int(df_y["adj_comp"].sum()),
        "adjudicadas": tot_adj,
        "wr_promedon": (tot_adj / tot_ofertas) if tot_ofertas else 0.0,
        "rev_comp": float(df_y["rev_comp"].sum()),
        "rev_promedon": float(df_y["rev_promedon"].sum()),
    }))
    tot_rev_comp_y = float(df_y["rev_comp"].sum())
    tot_rev_promedon_y = float(df_y["rev_promedon"].sum())
    tot_rev_total_y = tot_rev_comp_y + tot_rev_promedon_y
    share_promedon = (tot_rev_promedon_y / tot_rev_total_y) if tot_rev_total_y > 0 else 0.0
    html.append(
        f"<p><b>Share de Promedon sobre Revenue Total:</b> {_fmt_pct(share_promedon)} "
        f"({_fmt_money(tot_rev_promedon_y)} de {_fmt_money(tot_rev_total_y)}). "
        f"<i>Revenue Total = Revenue Promedon + Revenue Comp., en items donde "
        f"Promedon participó.</i></p>"
    )
    html.append("</div>")
    return html


def _seccion_precio(t_prec_dict: dict, prefix: str = "2") -> list[str]:
    html = []
    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>Diagnóstico Posicionamiento de Precio</h2>")
    html.append(
        "<p>Cada item se clasifica midiendo qué tan cerca estuvo Promedon de cambiar su resultado "
        "en la licitación. Se define <code>delta(p) = (p − precio_promedon) / precio_promedon</code>. "
        "Según el resultado se calcula una de estas dos métricas:</p>"
        "<ul>"
        "<li>Si Promedon <b>gana</b>: <code>delta(win) = delta(precio_siguiente_ganador)</code>. "
        "Mide cuánto podría haberse subido el precio antes de perder la adjudicación.</li>"
        "<li>Si Promedon <b>pierde</b>: <code>delta(lose) = delta(precio_ganador)</code>. "
        "Mide cuánto descuento se habría necesitado para ganar la adjudicación.</li>"
        "</ul>"
        "<ul>"
        "<li><b>W1 — Aligned</b> (0 < delta(win) ≤ 5%): precio eficiente.</li>"
        "<li><b>W2 — Improvable</b> (5% < delta(win) ≤ 15%): subir moderadamente.</li>"
        "<li><b>W3 — Comp-Overshot</b> (delta(win) > 15%): el competidor sobre-especificó; el producto de Promedon, de menor calidad, cumple el requerimiento.</li>"
        "<li><b>W4 — Lock-In</b> (delta(win) < 0): Promedon gana sin ser el más barato — señal de relación del comprador con Promedon.</li>"
        "<li><b>W5 — Monopoly</b>: Promedon gana sin competencia.</li>"
        "<li><b>L1 — Recoverable</b> (-15% ≤ delta(lose) < 0): reducir moderadamente.</li>"
        "<li><b>L2 — Overshot</b> (delta(lose) < -15%): Promedon ofreció un producto de mayor calidad que la requerida.</li>"
        "<li><b>L3 — Lock-Out</b>: Promedon pierde siendo más barato que el ganador — la causa no es precio.</li>"
        "<li><b>D — Unawarded</b>: el item quedó sin adjudicación (ningún proveedor ganó). Pricing no aplica.</li>"
        "</ul>"
    )

    df_all_prec = pd.concat([t_prec_dict[p] for p in ["A", "B"] if p in t_prec_dict], ignore_index=True).copy()
    df_all_prec["revenue"] = (df_all_prec["promedon_gano"].fillna(0)
                              * df_all_prec["num_unidades"].fillna(0)
                              * df_all_prec["precio_promedon"].fillna(0))
    df_all_prec["ticket"] = (df_all_prec["num_unidades"].fillna(0)
                             * df_all_prec["precio_promedon"].fillna(0))

    # ─── Pricing óptimo por segmento (consolidado) ────────────────────────
    html.append("<h3>Posicionamiento de Precio por Segmento</h3>")
    html.append(
        "<p>Para cada segmento se reporta cuántas ofertas hubo, quién se las "
        "quedó (Promedon vs. competencia) y el revenue capturado por cada lado. "
        "<i>Revenue Comp. corresponde solo a items donde Promedon participó y "
        "la adjudicación se la quedó un competidor — no incluye items en los que "
        "Promedon no ofertó.</i></p>"
    )

    UPLIFT_SEGS = {"W2 — Improvable", "W3 — Comp-Overshot"}
    DISCOUNT_SEGS = {"L1 — Recoverable", "L2 — Overshot"}

    rows_cons = []
    tot_rev_actual = 0.0
    tot_rev_opt = 0.0
    tot_adj_adic = 0
    for seg in SEGMENT_ORDER:
        seg_df = df_all_prec[df_all_prec["diagnostico"] == seg].copy()
        n_seg = len(seg_df)
        if n_seg == 0:
            continue
        qty = seg_df["num_unidades"].fillna(0)
        price = seg_df["precio_promedon"].fillna(0)
        rev_actual = float((seg_df["promedon_gano"].fillna(0) * qty * price).sum())
        adj_actual = int(seg_df["promedon_gano"].fillna(0).sum())
        accion = "—"
        n_opt = None
        rev_opt = None
        if seg in {"W4 — Lock-In", "W5 — Monopoly"}:
            accion = "No Calculable"
            n_opt = adj_actual
            rev_opt = rev_actual
        elif seg == "W1 — Aligned" or seg in UPLIFT_SEGS:
            opt = _calculate_optimal_uplift(seg_df)
            if opt:
                accion = f"+{opt['uplift']*100:.0f}%"
                n_opt = int(opt["n_items"])
                rev_opt = float(opt["revenue"])
                if opt["uplift"] == 0:
                    n_opt = adj_actual
                    rev_opt = rev_actual
        elif seg in DISCOUNT_SEGS:
            opt = _calculate_optimal_discount(seg_df)
            if opt:
                accion = f"−{opt['discount']*100:.0f}%"
                n_opt = int(opt["n_items"])
                rev_opt = float(opt["revenue"])
                if opt["discount"] == 0:
                    n_opt = adj_actual
                    rev_opt = rev_actual
        rev_opt_show = rev_opt if rev_opt is not None else rev_actual
        adj_adic = (n_opt - adj_actual) if n_opt is not None else 0
        comp_mask = ((seg_df["precio_unitario_ganador"].notna())
                     & (seg_df["promedon_gano"].fillna(0) == 0))
        adj_comp = int(comp_mask.sum())
        rev_comp = float((seg_df.loc[comp_mask, "num_unidades"].fillna(0)
                          * seg_df.loc[comp_mask, "precio_unitario_ganador"].fillna(0)).sum())
        rows_cons.append({
            "segmento": SEGMENT_LABEL[seg],
            "competidores_unicos": float(seg_df["n_competidores"].fillna(0).mean()),
            "ofertas": n_seg,
            "adjudicadas_comp": adj_comp,
            "adjudicadas": adj_actual,
            "delta_win_med": (float(seg_df["gap_to_lose"].median())
                          if seg_df["gap_to_lose"].notna().any() else None),
            "delta_lose_med": (float(seg_df["gap_to_win"].median())
                          if seg_df["gap_to_win"].notna().any() else None),
            "rev_comp": rev_comp,
            "rev_actual": rev_actual,
            "accion": accion,
            "adj_adic": adj_adic,
            "rev_adic": rev_opt_show - rev_actual,
            "rev_opt": rev_opt_show,
        })
        tot_rev_actual += rev_actual
        tot_rev_opt += rev_opt_show
        tot_adj_adic += adj_adic

    def _fmt_signed_int(v):
        if pd.isna(v):
            return "-"
        n = int(v)
        if n > 0:
            return f"+{n:,}"
        if n < 0:
            return f"−{abs(n):,}"
        return "0"

    df_cons = pd.DataFrame(rows_cons)
    tot_ofertas_cons = int(df_cons["ofertas"].sum()) if len(df_cons) else 0
    tot_adj_cons = int(df_cons["adjudicadas"].sum()) if len(df_cons) else 0
    tot_adj_comp = int(df_cons["adjudicadas_comp"].sum()) if len(df_cons) else 0
    tot_rev_comp = float(df_cons["rev_comp"].sum()) if len(df_cons) else 0.0
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
        "adjudicadas_comp": ("Adjudicadas Comp.", lambda v: f"{int(v):,}"),
        "adjudicadas": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "rev_comp": ("Revenue Comp.", _fmt_money),
        "rev_actual": ("Revenue Promedon", _fmt_money),
        "accion": ("Brecha con Competencia", None, "center"),
    }, max_rows=len(df_cons), totals={
        "segmento": "Total",
        "competidores_unicos": tot_comp_unicos,
        "ofertas": tot_ofertas_cons,
        "adjudicadas_comp": tot_adj_comp,
        "adjudicadas": tot_adj_cons,
        "rev_comp": tot_rev_comp,
        "rev_actual": tot_rev_actual,
        "accion": "—",
    }))

    html.append(
        "<p><b>Cómo leer Brecha con Competencia:</b> mide <b>cuán cerca está el precio de "
        "Promedon del precio del competidor relevante</b> (siguiente oferente en segmentos W, "
        "ganador en segmentos L). Cerca de 0% indica productos comparables (oferta "
        "sustituible y competencia real); valores altos en términos absolutos sugieren que se "
        "están comparando productos de calidad o categoría distinta — no debe interpretarse "
        "como señal de subir o bajar precio en esa magnitud.</p>"
        "<p><b>Acciones a seguir por segmento:</b>"
        "<ul>"
        "<li><b>W1 — Aligned</b>: oferta alineada con la competencia — productos "
        "comparables. <i>Acción:</i> mantener precio y vigilar competidores agresivos.</li>"
        "<li><b>W2 — Improvable</b>: brecha moderada con el siguiente "
        "oferente; productos probablemente comparables con leve ventaja para Promedon. "
        "<i>Acción:</i> evaluar uplift acotado en items donde se confirme sustituibilidad — "
        "no aplicar el porcentaje en bloque al segmento.</li>"
        "<li><b>W3 — Comp-Overshot</b>: brecha grande — el competidor "
        "sobre-especificó la oferta; el producto de Promedon, de menor calidad, cumple el "
        "requerimiento y por eso gana. La brecha no es headroom capturable. "
        "<i>Acción:</i> validar que la oferta ajusta al requerimiento con muestreo "
        "(especificaciones, marcas, categorías); si en algún subgrupo aparece un "
        "competidor genuinamente comparable y la brecha persiste, evaluar uplift acotado.</li>"
        "<li><b>W4 — Lock-In</b> (No Calculable): Promedon gana pese a no ser el más barato — "
        "el comprador no decidió por precio. <i>Acción:</i> proteger la cuenta con "
        "cross-sell, renovaciones y servicio; evaluar alza moderada caso a caso.</li>"
        "<li><b>W5 — Monopoly</b> (No Calculable): sin competidor de referencia. "
        "<i>Acción:</i> revisar caso a caso si el comprador toleraría un precio mayor en "
        "futuras licitaciones.</li>"
        "<li><b>L1 — Recoverable</b>: brecha moderada respecto al ganador "
        "— productos probablemente comparables. <i>Acción:</i> usar el deep dive (productos, "
        "compradores, competidores) para focalizar rebajas en items donde la brecha sea "
        "consistente, no aplicar el descuento a todo el segmento.</li>"
        "<li><b>L2 — Overshot</b>: brecha grande — indicio de que "
        "Promedon ofreció un producto de mayor calidad que la requerida; el comprador eligió una "
        "alternativa más básica/barata. El descuento mostrado no es aplicable. "
        "<i>Acción:</i> revisar especificaciones del item para evaluar si conviene ofrecer "
        "una versión más ajustada al requerimiento, o si el segmento sencillamente no "
        "encaja con el portafolio de Promedon.</li>"
        "<li><b>L3 — Lock-Out</b> (no se simula): Promedon pierde siendo más barato — la causa no "
        "es precio. <i>Acción:</i> investigar atributos no-precio (especificación, plazo, "
        "garantía, marca, relación del comprador con el ganador).</li>"
        "<li><b>D — Unawarded</b> (no se simula): sin adjudicación, no hay benchmark. "
        "<i>Acción:</i> revisar si conviene reformular la oferta o esperar relanzamiento.</li>"
        "</ul></p>"
    )

    html.append("</div>")
    return html


def _build_df_l1(df_seg: pd.DataFrame, t_prec_dict: dict) -> pd.DataFrame:
    """Construye el df enriquecido de items L1 — Recoverable (Promedon perdió y un
    descuento moderado habría revertido la adjudicación). Devuelve df vacío si no
    hay datos."""
    parts = []
    for label in ["A", "B"]:
        if label in t_prec_dict:
            tmp = t_prec_dict[label].copy()
            tmp["producto_segmento"] = label
            parts.append(tmp)
    if not parts:
        return pd.DataFrame()
    df_all = pd.concat(parts, ignore_index=True)
    df_l = df_all[df_all["diagnostico"] == "L1 — Recoverable"].copy()
    if df_l.empty:
        return df_l

    winners = (df_seg[df_seg["flag_adjudicado"].fillna(0).astype(int) == 1]
               .drop_duplicates(subset=["item_key"])
               [["item_key", "rut_proveedor", "nombre_proveedor"]]
               .rename(columns={"rut_proveedor": "rut_ganador",
                                "nombre_proveedor": "nombre_ganador"}))
    df_l = df_l.merge(winners, on="item_key", how="left")

    buyers = df_seg.drop_duplicates(subset=["item_key"])[["item_key", "nombre_comprador"]]
    df_l = df_l.merge(buyers, on="item_key", how="left")

    PRODUCTO_EXCLUIDO = "producto no identificable - especificación insuficiente"
    df_l = df_l[
        (df_l["producto_canonico"] != PRODUCTO_EXCLUIDO)
        & df_l["nombre_comprador"].notna()
    ].copy()

    if df_l.empty:
        return df_l

    df_l["descuento_requerido"] = -df_l["gap_to_win"].astype(float)
    df_l["unidades"] = df_l["num_unidades"].fillna(0).astype(float)
    df_l["revenue_recuperable"] = (df_l["unidades"] *
                                   df_l["precio_promedon"].fillna(0).astype(float) *
                                   (1 - df_l["descuento_requerido"]))
    return df_l


def _seccion_l1_deep_dive(df_seg: pd.DataFrame, t_prec_dict: dict,
                          prefix: str = "2") -> list[str]:
    """
    Análisis profundo del segmento L1 — Recoverable: items donde Promedon perdió
    y un descuento moderado (≤15%) habría revertido la adjudicación. L2 — Overshot
    queda fuera porque la brecha de precio es estructural y L3 porque la causa no
    es precio.
    """
    html = []
    df_l = _build_df_l1(df_seg, t_prec_dict)
    if df_l.empty:
        html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>"
                    "Deep dive — Segmento Recuperable (L1)</h2>")
        html.append("<p><i>(sin items en L1)</i></p></div>")
        return html

    n_l1 = int((df_l["diagnostico"] == "L1 — Recoverable").sum())
    rev_total = float(df_l["revenue_recuperable"].sum())

    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>"
                "Deep dive — Segmento Recuperable (L1)</h2>")

    # Helper: métricas de un subgrupo dentro de L1 (todos perdedores).
    # Devuelve la misma estructura que "Pricing óptimo por segmento": ofertas,
    # comp_item, accion, adj_adic, rev_adic.
    def _grp_row(sub: pd.DataFrame) -> dict:
        n = len(sub)
        comp_item = float(sub["n_competidores"].fillna(0).mean()) if n else 0.0
        opt_g = _calculate_optimal_discount(sub)
        if opt_g:
            accion = f"−{opt_g['discount']*100:.0f}%"
            rev_opt_g = float(opt_g["revenue"])
            n_items_opt = int(opt_g["n_items"])
        else:
            accion = "—"
            rev_opt_g = 0.0
            n_items_opt = 0
        return {
            "comp_item": comp_item,
            "ofertas": n,
            "accion": accion,
            "adj_adic": n_items_opt,
            "rev_adic": rev_opt_g,
            "rev_opt": rev_opt_g, # Mantenemos para sorting interno
        }

    cols_optimo = {
        "comp_item": ("Comp./Item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        "ofertas": ("Ofertas", lambda v: f"{int(v):,}"),
        "accion": ("Var. Precio Óptimo", None, "center"),
        "adj_adic": ("Adjudicadas Adic.", lambda v: f"{int(v):,}"),
        "rev_adic": ("Revenue Adic. al Óptimo", _fmt_money),
    }

    def _totals_optimo(df_rows: pd.DataFrame, key: str, label: str) -> dict:
        return {
            key: label,
            "comp_item": float(df_l["n_competidores"].fillna(0).mean()) if len(df_l) else 0.0,
            "ofertas": int(df_rows["ofertas"].sum()) if len(df_rows) else 0,
            "accion": "—",
            "adj_adic": int(df_rows["adj_adic"].sum()) if len(df_rows) else 0,
            "rev_adic": float(df_rows["rev_adic"].sum()) if len(df_rows) else 0.0,
        }

    def _totals_de_filas(df_show: pd.DataFrame, key: str, label: str) -> dict:
        """Total = suma directa de las filas mostradas."""
        if len(df_show) == 0:
            return {key: label, "comp_item": 0.0, "ofertas": 0,
                    "accion": "—", "adj_adic": 0, "rev_adic": 0.0}
        ofertas = float(df_show["ofertas"].sum())
        comp_item = (float((df_show["comp_item"].fillna(0) * df_show["ofertas"]).sum() / ofertas)
                     if ofertas > 0 else 0.0)
        return {
            key: label,
            "comp_item": comp_item,
            "ofertas": int(ofertas),
            "accion": "—",
            "adj_adic": int(df_show["adj_adic"].sum()),
            "rev_adic": float(df_show["rev_adic"].sum()),
        }

    # ─── 2.1 Top productos perdidos ───────────────────────────────────────
    html.append(f"<h3>{prefix}.1 Top productos perdidos por revenue recuperable</h3>")
    html.append(
        "<p>Por cada producto se busca el descuento único que, aplicado a todos sus items L1, "
        "recupera más revenue. <b>Var. Precio Óptimo</b> es ese descuento; "
        "<b>Adjudicadas Adic.</b> y <b>Revenue Adic. al Óptimo</b> son las adjudicaciones y el "
        "revenue que devolvería.</p>"
        "<p><b>Cómo leerla:</b> los productos del top concentran la oportunidad. Si pocos "
        "productos acumulan la mayoría del revenue adicional, la estrategia es focalizada "
        "— no hace falta una rebaja general en todo el portafolio.</p>"
    )
    rows_p = []
    for prod, sub in df_l.groupby("producto_canonico", dropna=False):
        row = _grp_row(sub)
        row["producto_canonico"] = prod if pd.notna(prod) else "(sin producto)"
        rows_p.append(row)
    df_p_show = (pd.DataFrame(rows_p)
                 .sort_values("rev_opt", ascending=False)
                 .head(15)
                 .reset_index(drop=True))
    html.append(_generate_table(df_p_show, {
        "producto_canonico": ("Producto", None),
        **cols_optimo,
    }, max_rows=len(df_p_show),
       totals=_totals_de_filas(df_p_show, "producto_canonico", "Total")))

    # ─── 2.2 Top compradores ──────────────────────────────────────────────
    html.append(f"<h3>{prefix}.2 Top compradores donde Promedon pierde</h3>")
    html.append(
        "<p>Agrupando por comprador: el descuento único que rescataría más revenue si "
        "se aplicara a todos los items L1 que ese comprador adjudicó a otro proveedor.</p>"
        "<p><b>Cómo leerla:</b> si pocos compradores concentran la mayor parte del revenue "
        "adicional, la palanca es comercial — negociación cuenta a cuenta. Si está repartido "
        "entre muchos, la palanca es de pricing transversal.</p>"
    )
    rows_buy = []
    for (rut, nombre), sub in df_l.groupby(["rut_comprador", "nombre_comprador"], dropna=False):
        row = _grp_row(sub)
        row["rut_comprador"] = rut if pd.notna(rut) else "—"
        row["nombre_comprador"] = nombre if pd.notna(nombre) else "(sin comprador)"
        rows_buy.append(row)
    df_buy_show = (pd.DataFrame(rows_buy)
                   .sort_values("rev_opt", ascending=False)
                   .head(15)
                   .reset_index(drop=True))
    html.append(_generate_table(df_buy_show, {
        "nombre_comprador": ("Comprador", None),
        **cols_optimo,
    }, max_rows=len(df_buy_show),
       totals=_totals_de_filas(df_buy_show, "nombre_comprador", "Total")))

    # ─── 2.3 Competidores que ganan ───────────────────────────────────────
    html.append(f"<h3>{prefix}.3 Competidores que le ganan a Promedon en L1</h3>")
    html.append(
        "<p>Para cada proveedor que se quedó con adjudicaciones que Promedon perdió en L1, "
        "se calcula el descuento que habría neutralizado a ese rival en bloque y el revenue "
        "que se habría recuperado. La columna <i>Ofertas</i> indica cuántos items de Promedon "
        "ganó ese competidor.</p>"
        "<p><b>Cómo leerla:</b> un rival con muchas <i>Ofertas</i> y <i>Var. Precio Óptimo</i> "
        "baja es competencia sistemática y barata de neutralizar — vale revisar estrategia "
        "frente a él. Un rival con pocos items, o que requiere descuentos altos, pesa menos "
        "en la decisión.</p>"
    )
    rows_c = []
    for (rut, nombre), sub in df_l.dropna(subset=["rut_ganador"]).groupby(
            ["rut_ganador", "nombre_ganador"]):
        row = _grp_row(sub)
        row["rut_ganador"] = rut
        row["nombre_ganador"] = nombre if pd.notna(nombre) else "(sin nombre)"
        rows_c.append(row)
    df_c_show = (pd.DataFrame(rows_c)
                 .sort_values("rev_opt", ascending=False)
                 .head(15)
                 .reset_index(drop=True))
    html.append(_generate_table(df_c_show, {
        "nombre_ganador": ("Competidor", None),
        **cols_optimo,
    }, max_rows=len(df_c_show),
       totals=_totals_de_filas(df_c_show, "nombre_ganador", "Total")))

    html.append("</div>")
    return html


def _seccion_quick_wins(df_seg: pd.DataFrame, t_prec_dict: dict,
                        prefix: str = "3") -> list[str]:
    """Top 20 items L1 por ROI (revenue recuperable / descuento requerido)."""
    html = []
    df_l = _build_df_l1(df_seg, t_prec_dict)
    title = "Quick wins — top 20 items por ROI"
    if df_l.empty:
        html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>{title}</h2>")
        html.append("<p><i>(sin items en L1)</i></p></div>")
        return html

    html.append(f"<div class='card'><h2><span class='sec-num'>{prefix}</span>{title}</h2>")
    html.append(
        "<p>Lista item por item, ordenada por <b>ROI</b> = "
        "<code>revenue recuperable ÷ descuento requerido</code>: máximo revenue rescatado por "
        "punto porcentual de margen sacrificado. En lo alto aparecen los items donde una "
        "rebaja milimétrica devuelve mucho ingreso — accionables uno a uno sin necesidad de "
        "cambiar la política general de precios.</p>"
    )
    df_qw = df_l[df_l["descuento_requerido"] > 0].copy()
    df_qw["roi"] = df_qw["revenue_recuperable"] / df_qw["descuento_requerido"]
    df_qw = (df_qw.sort_values("roi", ascending=False)
                  .head(20)
                  .reset_index(drop=True))
    rev_qw = float(df_qw["revenue_recuperable"].sum())
    html.append(_generate_table(df_qw, {
        "diagnostico": ("Seg.", None),
        "producto_canonico": ("Producto", None),
        "nombre_comprador": ("Comprador", None),
        "nombre_ganador": ("Ganador", None),
        "unidades": ("Unidades", lambda v: f"{int(v):,}"),
        "precio_promedon": ("Precio Promedon", _fmt_money),
        "descuento_requerido": ("Var. Precio Óptimo",
                                 lambda v: f"−{v*100:.1f}%" if pd.notna(v) else "-"),
        "precio_unitario_ganador": ("Precio ganador", _fmt_money),
        "revenue_recuperable": ("Revenue recup.", _fmt_money),
    }, max_rows=20, totals={
        "revenue_recuperable": rev_qw,
    }))
    html.append("</div>")
    return html


def build_reporte_html(df_base: pd.DataFrame, rut_promedon: str) -> str:
    # Segmentación primero: el universo común de todo el reporte son los items
    # con producto_canonico mapeable a A o B. Items sin producto identificable
    # quedan fuera para que los totales calcen entre tablas.
    t_prod_global = build_tabla_producto(df_base, rut_promedon)
    seg_map, threshold = _segmentar_productos(t_prod_global)
    df_seg = df_base.copy()
    df_seg["producto_segmento"] = df_seg["producto_canonico"].map(seg_map)
    df_universo = df_seg[df_seg["producto_segmento"].isin(["A", "B"])].copy()

    promedon = df_universo[df_universo["rut_proveedor"] == rut_promedon]
    n_ofertas_totales = len(df_universo)
    n_ofertas_promedon = len(promedon)
    n_adjudicadas = int(promedon["flag_adjudicado"].sum())
    wr_global = n_adjudicadas / n_ofertas_promedon if n_ofertas_promedon else 0
    revenue_total = float((promedon["flag_adjudicado"].fillna(0)
                           * promedon["num_unidades"].fillna(0)
                           * promedon["precio_unitario"].fillna(0)).sum())
    comp_mask_global = ((promedon["precio_unitario_ganador"].notna())
                        & (promedon["flag_adjudicado"].fillna(0) == 0))
    revenue_comp_global = float((promedon.loc[comp_mask_global, "num_unidades"].fillna(0)
                                 * promedon.loc[comp_mask_global, "precio_unitario_ganador"].fillna(0)).sum())
    revenue_universo = revenue_total + revenue_comp_global
    share_promedon = (revenue_total / revenue_universo) if revenue_universo > 0 else 0.0
    n_competidores = df_universo.loc[df_universo["rut_proveedor"] != rut_promedon,
                                     "rut_proveedor"].nunique()
    n_compradores = df_universo["rut_comprador"].nunique()
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
                       ("Share Promedon", _fmt_pct(share_promedon)),
                       ("Revenue Promedon", _fmt_money(revenue_total))]:
        html.append(f"<div class='summary-stat'><span class='stat-label'>{label}</span><span class='stat-value'>{val}</span></div>")
    html.append("</div>")

    # Resumen anual
    html.extend(_seccion_anual(df_universo, rut_promedon))

    # Segmentación
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
        n_ofertas_p = int(len(sub_p))
        n_adj_p = int(sub_p["flag_adjudicado"].sum())
        rows_split.append({
            "segmento": label_titulo,
            "n_productos": sum(1 for v in seg_map.values() if v == label),
            "ofertas_promedon": n_ofertas_p,
            "adjudicaciones": n_adj_p,
            "wr_promedon": (n_adj_p / n_ofertas_p) if n_ofertas_p else 0.0,
            "comp_item": comp_item,
            "revenue": rev,
        })

    df_rows_split = pd.DataFrame(rows_split)
    all_promedon_items = df_seg.loc[df_seg["rut_proveedor"] == rut_promedon, "item_key"].unique()
    total_item_comp = df_seg.groupby("item_key")["rut_proveedor"].nunique() - 1
    tot_comp_item = (float(total_item_comp[total_item_comp.index.isin(all_promedon_items)].mean())
                     if len(all_promedon_items) else None)
    tot_ofertas_p = int(df_rows_split["ofertas_promedon"].sum())
    tot_adj_p = int(df_rows_split["adjudicaciones"].sum())
    html.append(_generate_table(df_rows_split, {
        "segmento": ("Cat.", None),
        "comp_item": ("Comp./Item", lambda v: f"{v:.1f}" if pd.notna(v) else "-"),
        "n_productos": ("Productos", lambda v: f"{int(v):,}"),
        "ofertas_promedon": ("Ofertas", lambda v: f"{int(v):,}"),
        "adjudicaciones": ("Adjudicadas", lambda v: f"{int(v):,}"),
        "wr_promedon": ("WR Promedon", _fmt_pct),
        "revenue": ("Revenue Promedon", _fmt_money),
    }, totals={
        "segmento": "Total",
        "comp_item": tot_comp_item,
        "n_productos": int(df_rows_split["n_productos"].sum()),
        "ofertas_promedon": tot_ofertas_p,
        "adjudicaciones": tot_adj_p,
        "wr_promedon": (tot_adj_p / tot_ofertas_p) if tot_ofertas_p else 0.0,
        "revenue": float(df_rows_split["revenue"].sum()),
    }))
    html.append("</div>")

    # Secciones
    t_prod_dict = {}
    t_prec_dict = {}
    n_ofertas_dict = {}
    for label in ["A", "B"]:
        sub_base = sub_dict[label]
        if sub_base.empty: continue
        t_prod_dict[label] = build_tabla_producto(sub_base, rut_promedon)
        t_prec_dict[label] = build_tabla_precio(sub_base, rut_promedon)
        n_ofertas_dict[label] = int(len(sub_p_dict[label]))

    html.extend(_seccion_precio(t_prec_dict, prefix="1"))
    html.extend(_seccion_l1_deep_dive(df_seg, t_prec_dict, prefix="2"))
    html.extend(_seccion_quick_wins(df_seg, t_prec_dict, prefix="3"))

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
