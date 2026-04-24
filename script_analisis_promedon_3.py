"""
Análisis competitivo A vs B — reporte único en Markdown.

Consume matriz_base_AB.csv (generada por script_analisis_promedon_2.py)
y produce un solo archivo: analisis_competitivo_AvsB.md con todas las
agregaciones (global, producto, comprador, tiempo, precio) y la
interpretación estratégica por competidor (§6 del modelo).
"""
import os
import numpy as np
import pandas as pd

# ─── CONFIG ──────────────────────────────────────────────────────────────
OUTPUT_DIR  = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\analisis_promedon\\"
MATRIZ_PATH = os.path.join(OUTPUT_DIR, "matriz_base_AB.csv")
MD_PATH     = os.path.join(OUTPUT_DIR, "analisis_competitivo_AvsB.md")

TOP_N_DETALLE        = 5      # competidores que reciben análisis por dimensión
TOP_ROWS_PRODUCTO    = 15
TOP_ROWS_COMPRADOR   = 15
MIN_ITEMS_PRODUCTO   = 3
MIN_ITEMS_COMPRADOR  = 3
UMBRAL_ALTO, UMBRAL_BAJO = 0.30, 0.15


# ─── Formateo ────────────────────────────────────────────────────────────
def fmt_clp(x):
    if pd.isna(x) or x == 0:
        return "-"
    x = float(x)
    if abs(x) >= 1e9:  return f"${x/1e9:,.2f}B"
    if abs(x) >= 1e6:  return f"${x/1e6:,.1f}M"
    if abs(x) >= 1e3:  return f"${x/1e3:,.0f}K"
    return f"${x:,.0f}"


def fmt_pct(x):
    return "-" if pd.isna(x) else f"{x*100:,.1f}%"


def fmt_pct_raw(x):
    return "-" if pd.isna(x) else f"{x:,.1f}%"


def to_md(df, money=(), pct=(), pct_raw=(), int_cols=()):
    if df.empty:
        return "_(sin datos)_"
    out = df.copy()
    for c in money:   out[c] = out[c].apply(fmt_clp)
    for c in pct:     out[c] = out[c].apply(fmt_pct)
    for c in pct_raw: out[c] = out[c].apply(fmt_pct_raw)
    for c in int_cols:
        out[c] = out[c].fillna(0).astype(int)
    return out.to_markdown(index=False)


# ─── Métricas ────────────────────────────────────────────────────────────
def wr_metrics(df_ab, by=None):
    def agg(g):
        n = len(g)
        a = int(g.A_gana.sum())
        b = int(g.B_gana.sum())
        o = int(g.otro_gana.sum())
        return pd.Series({
            "items_U_AB":       n,
            "A_gana_AB":        a,
            "B_gana_AB":        b,
            "otros_gana_AB":    o,
            "WR_A_AB":          a / n if n else np.nan,
            "WR_B_AB":          b / n if n else np.nan,
            "WR_otros_AB":      o / n if n else np.nan,
            "rev_perdido_vs_B": g.loc[(g.A_gana == 0) & (g.B_gana == 1), "revenue_item_clp"].sum(),
        })
    if by is None:
        return agg(df_ab).to_frame().T
    return df_ab.groupby(by, dropna=False).apply(agg).reset_index()


def gap_stats(df_ab):
    m = (df_ab.B_gana == 1) & (df_ab.A_gana == 0) & (df_ab.precio_A > 0) & (df_ab.precio_B_ganador > 0)
    sub = df_ab.loc[m]
    if sub.empty:
        return {
            "n_gap": 0, "gap_avg": np.nan, "gap_p50": np.nan,
            "items_mas_caro": 0, "items_mas_barato": 0,
            "rev_mas_caro": 0, "rev_mas_barato": 0,
        }
    gap = (sub.precio_A - sub.precio_B_ganador) * 100.0 / sub.precio_B_ganador
    return {
        "n_gap":            len(sub),
        "gap_avg":          gap.mean(),
        "gap_p50":          gap.median(),
        "items_mas_caro":   int((gap > 0).sum()),
        "items_mas_barato": int((gap < 0).sum()),
        "rev_mas_caro":     sub.loc[gap > 0, "revenue_item_clp"].sum(),
        "rev_mas_barato":   sub.loc[gap < 0, "revenue_item_clp"].sum(),
    }


def caso_estrategico(r):
    if r.WR_A_AB >= UMBRAL_ALTO:
        return "3 — B irrelevante"
    if r.WR_B_AB >= UMBRAL_ALTO and r.WR_A_AB < UMBRAL_BAJO:
        return "1 — B es el problema"
    if r.WR_A_AB < UMBRAL_BAJO and r.WR_B_AB < UMBRAL_BAJO and r.WR_otros_AB >= UMBRAL_ALTO:
        return "2 — ambos débiles (otros dominan)"
    return "mixto"


# ─── Secciones del reporte ───────────────────────────────────────────────
def seccion_contexto(df):
    dfua = df.drop_duplicates("it_key")
    n = len(dfua)
    a = int(dfua.A_gana.sum())
    rev = dfua.loc[dfua.A_gana == 1, "revenue_item_clp"].sum()
    tbl = pd.DataFrame([{
        "items_U_A":  n,
        "A_gana":     a,
        "WR_A":       a / n,
        "revenue_A":  rev,
    }])
    return (
        "## 1. Contexto — Universo U_A\n\n"
        "Desempeño global de A (Promedon) sobre todos los ítems donde participa. "
        "Este bloque no se mezcla con ningún WR_A_AB.\n\n"
        + to_md(tbl, money=["revenue_A"], pct=["WR_A"], int_cols=["items_U_A", "A_gana"])
    )


def seccion_output_estandar(g1):
    tbl = g1[[
        "nombre_B", "items_U_AB", "WR_A_AB", "WR_B_AB", "WR_otros_AB",
        "rev_perdido_vs_B", "gap_avg", "caso_estrategico",
    ]].rename(columns={
        "nombre_B": "Competidor B", "gap_avg": "gap precio avg",
    })
    return (
        "## 2. Output estándar por competidor (§9)\n\n"
        "Una fila por B, todo calculado sobre U_AB (ítems donde A y B compiten). "
        "`gap precio avg` positivo = A tiende a ser más caro cuando B gana.\n\n"
        + to_md(
            tbl,
            money=["rev_perdido_vs_B"],
            pct=["WR_A_AB", "WR_B_AB", "WR_otros_AB"],
            pct_raw=["gap precio avg"],
            int_cols=["items_U_AB"],
        )
    )


def seccion_interpretacion(g1):
    conteo = g1.caso_estrategico.value_counts().rename_axis("caso").reset_index(name="n_competidores")
    return (
        "## 3. Interpretación estratégica (§6)\n\n"
        f"Umbrales: WR alto ≥ {UMBRAL_ALTO:.0%}, WR bajo < {UMBRAL_BAJO:.0%}.\n\n"
        "- **Caso 1** — B es el problema: competir directo.\n"
        "- **Caso 2** — ambos débiles: el problema es otro competidor o el segmento.\n"
        "- **Caso 3** — B irrelevante: no priorizar.\n\n"
        + to_md(conteo, int_cols=["n_competidores"])
    )


def seccion_detalle_B(df, rut_B, nombre_B):
    df_ab = df[df.rut_B == rut_B]
    n_items = len(df_ab)

    # Resumen agregado
    g = wr_metrics(df_ab).iloc[0]
    gs = gap_stats(df_ab)
    resumen = pd.DataFrame([{
        "items_U_AB":       int(g.items_U_AB),
        "WR_A_AB":          g.WR_A_AB,
        "WR_B_AB":          g.WR_B_AB,
        "WR_otros_AB":      g.WR_otros_AB,
        "rev_perdido_vs_B": g.rev_perdido_vs_B,
        "gap_avg_pct":      gs["gap_avg"],
        "gap_p50_pct":      gs["gap_p50"],
        "items_más_caro":   gs["items_mas_caro"],
        "items_más_barato": gs["items_mas_barato"],
        "rev_más_caro":     gs["rev_mas_caro"],
        "rev_más_barato":   gs["rev_mas_barato"],
    }])

    # Producto
    prod = wr_metrics(df_ab, by="producto")
    prod = prod[prod.items_U_AB >= MIN_ITEMS_PRODUCTO] \
             .sort_values("rev_perdido_vs_B", ascending=False) \
             .head(TOP_ROWS_PRODUCTO)

    # Comprador
    comp = wr_metrics(df_ab, by=["rut_comprador", "nombre_comprador"])
    comp = comp[comp.items_U_AB >= MIN_ITEMS_COMPRADOR] \
             .sort_values("rev_perdido_vs_B", ascending=False) \
             .head(TOP_ROWS_COMPRADOR)[[
                 "nombre_comprador", "items_U_AB",
                 "WR_A_AB", "WR_B_AB", "WR_otros_AB", "rev_perdido_vs_B",
             ]]

    # Tiempo
    tiempo = wr_metrics(df_ab, by=["anio", "mes"]).sort_values(["anio", "mes"])

    partes = [
        f"## 4.{nombre_B}\n",
        f"**RUT:** {rut_B}  ·  **Ítems en U_AB:** {n_items:,}\n",
        "### Resumen agregado\n",
        to_md(
            resumen,
            money=["rev_perdido_vs_B", "rev_más_caro", "rev_más_barato"],
            pct=["WR_A_AB", "WR_B_AB", "WR_otros_AB"],
            pct_raw=["gap_avg_pct", "gap_p50_pct"],
            int_cols=["items_U_AB", "items_más_caro", "items_más_barato"],
        ),
        "\n### Por producto (top por revenue perdido)\n",
        to_md(
            prod[["producto", "items_U_AB", "WR_A_AB", "WR_B_AB", "WR_otros_AB", "rev_perdido_vs_B"]],
            money=["rev_perdido_vs_B"],
            pct=["WR_A_AB", "WR_B_AB", "WR_otros_AB"],
            int_cols=["items_U_AB"],
        ),
        "\n### Por comprador (top por revenue perdido)\n",
        to_md(
            comp,
            money=["rev_perdido_vs_B"],
            pct=["WR_A_AB", "WR_B_AB", "WR_otros_AB"],
            int_cols=["items_U_AB"],
        ),
        "\n### Evolución mensual\n",
        to_md(
            tiempo[["anio", "mes", "items_U_AB", "WR_A_AB", "WR_B_AB", "rev_perdido_vs_B"]],
            money=["rev_perdido_vs_B"],
            pct=["WR_A_AB", "WR_B_AB"],
            int_cols=["anio", "mes", "items_U_AB"],
        ),
    ]
    return "\n".join(partes)


# ─── Main ────────────────────────────────────────────────────────────────
def main():
    print(f"Cargando matriz: {MATRIZ_PATH}")
    df = pd.read_csv(MATRIZ_PATH)
    print(f"  filas (item×B): {len(df):,}  ·  items: {df.it_key.nunique():,}  ·  B: {df.rut_B.nunique():,}")

    # Output estándar + gap + caso estratégico
    g1 = wr_metrics(df, by=["rut_B", "nombre_B"])
    gaps = df.groupby(["rut_B", "nombre_B"]).apply(
        lambda g: pd.Series(gap_stats(g))
    ).reset_index()
    g1 = g1.merge(gaps, on=["rut_B", "nombre_B"], how="left")
    g1["caso_estrategico"] = g1.apply(caso_estrategico, axis=1)
    g1 = g1.sort_values("rev_perdido_vs_B", ascending=False)

    top_Bs = g1.head(TOP_N_DETALLE)[["rut_B", "nombre_B"]].values.tolist()

    bloques = [
        "# Análisis competitivo A vs B — Promedon (ChileCompra)\n",
        f"_Generado desde `matriz_base_AB.csv` — {len(df):,} pares (item×B), "
        f"{df.it_key.nunique():,} ítems, {df.rut_B.nunique():,} competidores._\n",
        "**Modelo:** toda métrica A vs B se calcula sobre U_AB (ítems donde A y B compiten). "
        "U_A es sólo contexto y nunca se mezcla en la misma métrica.\n",
        seccion_contexto(df),
        seccion_output_estandar(g1),
        seccion_interpretacion(g1),
        "## 4. Detalle por competidor\n",
    ]
    for rut_B, nombre_B in top_Bs:
        bloques.append(seccion_detalle_B(df, rut_B, nombre_B))

    md = "\n\n".join(bloques)
    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"\nOK. Reporte: {MD_PATH}")


if __name__ == "__main__":
    main()
