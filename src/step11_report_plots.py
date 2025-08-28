import pandas as pd
import duckdb
from pathlib import Path
import matplotlib.pyplot as plt

# === paths ===
Path("out/plots").mkdir(parents=True, exist_ok=True)

# === carga de insumos ===
df = pd.read_csv("out/plan_reallocation_total_detailed.csv")

# --- aseguremos columnas core ---
need_cols = {"campaign_id", "adset_name", "total_budget", "new_budget_adset",
             "delta_budget_abs", "delta_budget_pct",
             "rev_expected_old", "rev_expected_new",
             "mar_expected_old", "mar_expected_new"}
missing = need_cols - set(df.columns)
if missing:
    raise SystemExit(
        f"Faltan columnas en plan_reallocation_total_detailed.csv: {missing}")

# === 1) Resúmenes por campaña ===
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

# === 2) KPIs globales ===
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

# === 3) Gráficos ===
# Regla: matplotlib, 1 gráfico por figura, sin estilos ni colores forzados.

# 3.1 Presupuesto antes vs después por campaña
plt.figure()
x = sum_campaign["campaign_id"].astype(str)
plt.plot(x, sum_campaign["budget_old"], marker="o", label="Antes")
plt.plot(x, sum_campaign["budget_new"], marker="o", label="Después")
plt.title("Presupuesto por campaña (antes vs después)")
plt.xlabel("campaign_id")
plt.ylabel("COP")
plt.legend()
plt.tight_layout()
plt.savefig("out/plots/budget_by_campaign_before_after.png", dpi=160)
plt.close()

# 3.2 Top adsets por ajuste de presupuesto (abs)
topN = 12
df_top = df.sort_values("delta_budget_abs", ascending=False).head(topN)
plt.figure()
plt.barh(df_top["adset_name"], df_top["delta_budget_abs"])
plt.title(f"Top {topN} adsets por ajuste de presupuesto (COP)")
plt.xlabel("Δ presupuesto (COP)")
plt.ylabel("adset")
plt.tight_layout()
plt.savefig("out/plots/top_adsets_delta_budget.png", dpi=160)
plt.close()

# 3.3 Dif. de ingresos por campaña
plt.figure()
plt.bar(sum_campaign["campaign_id"].astype(str), sum_campaign["revenue_delta"])
plt.title("Diferencia de ingresos por campaña (nuevo - actual)")
plt.xlabel("campaign_id")
plt.ylabel("Δ ingresos (COP)")
plt.tight_layout()
plt.savefig("out/plots/revenue_diff_by_campaign.png", dpi=160)
plt.close()

# 3.4 KPIs globales antes vs después (ingresos y margen)
plt.figure()
labels = ["Ingresos", "Margen"]
before = [kpi_global["revenue_old"], kpi_global["margin_old"]]
after = [kpi_global["revenue_new"], kpi_global["margin_new"]]
pos = range(len(labels))
plt.bar([p - 0.2 for p in pos], before, width=0.4, label="Antes")
plt.bar([p + 0.2 for p in pos], after,  width=0.4, label="Después")
plt.xticks(list(pos), labels)
plt.title("KPIs globales (antes vs después)")
plt.ylabel("COP")
plt.legend()
plt.tight_layout()
plt.savefig("out/plots/global_kpis_before_after.png", dpi=160)
plt.close()

print("✅ Listo. Revisa:")
print(" - out/summary_by_campaign.csv")
print(" - out/kpi_global.csv")
print(" - out/plots/budget_by_campaign_before_after.png")
print(" - out/plots/top_adsets_delta_budget.png")
print(" - out/plots/revenue_diff_by_campaign.png")
print(" - out/plots/global_kpis_before_after.png")
