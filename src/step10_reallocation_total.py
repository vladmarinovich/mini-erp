# src/step11_report_plots.py
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# ========= utilidades =========


def add_bar_labels(ax, fmt="{:,.0f}", rotation=0, va="bottom", ha="center"):
    """
    Pone etiquetas numéricas encima (o adentro si negativo) de cada barra.
    Detecta barras negativas y ajusta la posición.
    """
    for p in ax.patches:
        h = p.get_height()
        if pd.isna(h):
            continue
        x = p.get_x() + p.get_width() / 2
        if h >= 0:
            ax.text(x, h, fmt.format(h), ha=ha, va=va, rotation=rotation)
        else:
            ax.text(x, h, fmt.format(h), ha=ha, va="top", rotation=rotation)


def add_point_labels(ax, xs, ys, fmt="{:,.0f}", va="bottom", ha="center", rotate=0):
    for x, y in zip(xs, ys):
        ax.text(x, y, fmt.format(y), ha=ha, va=va, rotation=rotate)


def ensure_out_dirs():
    Path("out/plots").mkdir(parents=True, exist_ok=True)


# ========= carga =========
ensure_out_dirs()
df = pd.read_csv("out/plan_reallocation_total_detailed.csv")

need_cols = {
    "campaign_id", "adset_name",
    "total_budget", "new_budget_adset",
    "rev_expected_old", "rev_expected_new",
    "mar_expected_old", "mar_expected_new",
    "delta_budget_abs", "delta_budget_pct"
}
missing = need_cols - set(df.columns)
if missing:
    raise SystemExit(
        f"Faltan columnas en plan_reallocation_total_detailed.csv: {missing}")

# ========= resúmenes =========
sum_campaign = (df.groupby("campaign_id", as_index=False)
                .agg(budget_old=("total_budget", "sum"),
                     budget_new=("new_budget_adset", "sum"),
                     revenue_old=("rev_expected_old", "sum"),
                     revenue_new=("rev_expected_new", "sum"),
                     margin_old=("mar_expected_old", "sum"),
                     margin_new=("mar_expected_new", "sum"))
                )
sum_campaign["budget_delta"] = sum_campaign["budget_new"] - \
    sum_campaign["budget_old"]
sum_campaign["revenue_delta"] = sum_campaign["revenue_new"] - \
    sum_campaign["revenue_old"]
sum_campaign["margin_delta"] = sum_campaign["margin_new"] - \
    sum_campaign["margin_old"]
sum_campaign.to_csv("out/summary_by_campaign.csv", index=False)

kpi_global = {
    "budget_old":  df["total_budget"].sum(),
    "budget_new":  df["new_budget_adset"].sum(),
    "revenue_old": df["rev_expected_old"].sum(),
    "revenue_new": df["rev_expected_new"].sum(),
    "margin_old":  df["mar_expected_old"].sum(),
    "margin_new":  df["mar_expected_new"].sum(),
}
kpi_global["budget_delta"] = kpi_global["budget_new"] - \
    kpi_global["budget_old"]
kpi_global["revenue_delta"] = kpi_global["revenue_new"] - \
    kpi_global["revenue_old"]
kpi_global["margin_delta"] = kpi_global["margin_new"] - \
    kpi_global["margin_old"]
pd.DataFrame([kpi_global]).to_csv("out/kpi_global.csv", index=False)

# ========= 1) Presupuesto por campaña (antes vs después) =========
plt.figure()
x = sum_campaign["campaign_id"].astype(str)
y_before = sum_campaign["budget_old"].values
y_after = sum_campaign["budget_new"].values
plt.plot(x, y_before, marker="o", label="Antes")
plt.plot(x, y_after,  marker="o", label="Después")
add_point_labels(plt.gca(), x, y_before, fmt="{:,.0f}")
add_point_labels(plt.gca(), x, y_after,  fmt="{:,.0f}")
plt.title("Presupuesto por campaña (antes vs después)")
plt.xlabel("campaign_id")
plt.ylabel("COP")
plt.legend()
plt.tight_layout()
plt.savefig("out/plots/budget_by_campaign_before_after.png", dpi=160)
plt.close()

# ========= 2) Top adsets por ajuste de presupuesto =========
topN = 12
df_top = df.sort_values("delta_budget_abs", ascending=False).head(topN)
plt.figure(figsize=(10, 6))
ax = plt.gca()
ax.barh(df_top["adset_name"], df_top["delta_budget_abs"])
ax.invert_yaxis()
add_bar_labels(ax, fmt="{:,.0f}", ha="left")
plt.title(f"Top {topN} adsets por ajuste de presupuesto (COP)")
plt.xlabel("Δ presupuesto (COP)")
plt.ylabel("adset")
plt.tight_layout()
plt.savefig("out/plots/top_adsets_delta_budget.png", dpi=160)
plt.close()

# ========= 3) Δ Ingresos por campaña =========
plt.figure()
ax = plt.gca()
bars = ax.bar(sum_campaign["campaign_id"].astype(
    str), sum_campaign["revenue_delta"])
add_bar_labels(ax, fmt="{:,.0f}")
plt.title("Diferencia de ingresos por campaña (nuevo - actual)")
plt.xlabel("campaign_id")
plt.ylabel("Δ ingresos (COP)")
plt.tight_layout()
plt.savefig("out/plots/revenue_diff_by_campaign.png", dpi=160)
plt.close()

# ========= 4) KPIs globales (ingresos y margen: antes vs después) =========
plt.figure()
labels = ["Ingresos", "Margen"]
before = [kpi_global["revenue_old"], kpi_global["margin_old"]]
after = [kpi_global["revenue_new"], kpi_global["margin_new"]]
pos = range(len(labels))
ax = plt.gca()
ax.bar([p - 0.2 for p in pos], before, width=0.4, label="Antes")
ax.bar([p + 0.2 for p in pos], after,  width=0.4, label="Después")
# etiquetas
for i, v in enumerate(before):
    ax.text(i - 0.2, v, f"{v:,.0f}", ha="center", va="bottom")
for i, v in enumerate(after):
    ax.text(i + 0.2, v, f"{v:,.0f}", ha="center", va="bottom")
plt.xticks(list(pos), labels)
plt.title("KPIs globales (antes vs después)")
plt.ylabel("COP")
plt.legend()
plt.tight_layout()
plt.savefig("out/plots/global_kpis_before_after.png", dpi=160)
plt.close()

# ========= 5) Ingresos por producto (actual vs estimado, doble barra) =========
df_prod = df.groupby("adset_name", as_index=False).agg(
    revenue_old=("rev_expected_old", "sum"),
    revenue_new=("rev_expected_new", "sum"),
    margin_old=("mar_expected_old", "sum"),
    margin_new=("mar_expected_new", "sum")
)
df_prod["revenue_diff"] = df_prod["revenue_new"] - df_prod["revenue_old"]
df_prod["margin_diff"] = df_prod["margin_new"] - df_prod["margin_old"]
df_prod.to_csv("out/revenue_by_product.csv", index=False)

df_top_prod = df_prod.sort_values("revenue_old", ascending=False).head(15)
plt.figure(figsize=(11, 6))
pos = range(len(df_top_prod))
ax = plt.gca()
ax.bar([p - 0.2 for p in pos], df_top_prod["revenue_old"],
       width=0.4, label="Actual")
ax.bar([p + 0.2 for p in pos], df_top_prod["revenue_new"],
       width=0.4, label="Estimado")
# etiquetas
for i, v in enumerate(df_top_prod["revenue_old"]):
    ax.text(i - 0.2, v, f"{v:,.0f}", ha="center", va="bottom", rotation=90)
for i, v in enumerate(df_top_prod["revenue_new"]):
    ax.text(i + 0.2, v, f"{v:,.0f}", ha="center", va="bottom", rotation=90)
plt.xticks(list(pos), df_top_prod["adset_name"], rotation=90)
plt.title("Ingresos por producto (actual vs estimado)")
plt.ylabel("COP")
plt.legend()
plt.tight_layout()
plt.savefig("out/plots/revenue_by_product_before_after.png", dpi=160)
plt.close()

# ========= 6) Δ Ingresos por producto (positivo/negativo) =========
df_top_diff = df_prod.sort_values("revenue_diff", ascending=False).head(20)
plt.figure(figsize=(11, 6))
ax = plt.gca()
bars = ax.bar(df_top_diff["adset_name"], df_top_diff["revenue_diff"])
# etiquetas encima/abajo
for rect, val in zip(bars, df_top_diff["revenue_diff"]):
    h = rect.get_height()
    x = rect.get_x() + rect.get_width()/2
    if val >= 0:
        ax.text(x, h, f"{val:,.0f}", ha="center", va="bottom", rotation=90)
    else:
        ax.text(x, h, f"{val:,.0f}", ha="center", va="top", rotation=90)
plt.xticks(rotation=90)
plt.title("Ingresos extra por producto (positivo o negativo)")
plt.ylabel("Δ ingresos (COP)")
plt.tight_layout()
plt.savefig("out/plots/revenue_diff_by_product.png", dpi=160)
plt.close()

print("✅ Listo. Revisa:")
print(" - out/summary_by_campaign.csv")
print(" - out/kpi_global.csv")
print(" - out/revenue_by_product.csv")
print(" - out/plots/budget_by_campaign_before_after.png")
print(" - out/plots/top_adsets_delta_budget.png")
print(" - out/plots/revenue_diff_by_campaign.png")
print(" - out/plots/global_kpis_before_after.png")
print(" - out/plots/revenue_by_product_before_after.png")
print(" - out/plots/revenue_diff_by_product.png")


df = pd.read_csv("out/plan_reallocation_total_detailed.csv")
df.to_excel("out/plan_reallocation_total_detailed.xlsx", index=False)

# Repite para los otros datasets que vas a visualizar:
# sum_campaign.to_excel("out/summary_by_campaign.xlsx", index=False)
# df_prod.to_excel("out/revenue_by_product.xlsx", index=False)
