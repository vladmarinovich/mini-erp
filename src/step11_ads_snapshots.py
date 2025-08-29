# src/step11_ads_snapshots.py
from utils.supa_client import get_client
import os
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from utils.supa_client import get_client  # tu helper existente

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


# -----------------------
# Helpers
# -----------------------


def ensure_out(p):
    Path(p).parent.mkdir(parents=True, exist_ok=True)


def to_csv(df, path):
    ensure_out(path)
    df = df.copy()
    for c in df.select_dtypes(include="number").columns:
        df[c] = df[c].astype(float).round(2)
    # separador decimal punto (Sheets lo detecta bien)
    df.to_csv(path, index=False)


# -----------------------
# 1) Cargar últimos anuncios simulados
# -----------------------
sb = get_client()
last = (
    sb.table("ad_simulation")
      .select("run_ts")
      .order("run_ts", desc=True)
      .limit(1)
      .execute()
)
if not last.data:
    raise SystemExit("No hay datos en ad_simulation.")
run_ts = last.data[0]["run_ts"]

rows = (
    sb.table("ad_simulation")
      .select("ad_id, run_ts, campaign_id, adset_name, budget, cpc, aov, clicks, conversions, category_id, sku, impresions")
      .eq("run_ts", run_ts)
      .execute()
).data

ads = pd.DataFrame(rows)
if ads.empty:
    raise SystemExit("Último run vacío en ad_simulation.")

# Normaliza y llena nulos razonables
num_cols = ["budget", "cpc", "aov", "clicks", "conversions", "impresions"]
for c in num_cols:
    ads[c] = pd.to_numeric(ads[c], errors="coerce").fillna(0.0)

# -----------------------
# 2) KPIs por anuncio
# -----------------------
ads["ctr"] = np.where(ads["impresions"] > 0,
                      ads["clicks"]/ads["impresions"], 0.0)
ads["revenue"] = ads["conversions"] * ads["aov"]
ads["cpa"] = np.where(ads["conversions"] > 0,
                      ads["budget"]/ads["conversions"], np.nan)
ads["roas"] = np.where(ads["budget"] > 0, ads["revenue"]/ads["budget"], np.nan)
# margen “bruto” vs inversión
ads["margin"] = ads["revenue"] - ads["budget"]
ads["margin_pct"] = np.where(
    ads["revenue"] > 0, ads["margin"]/ads["revenue"], np.nan)

# Clasificaciones
ads["class_profit"] = np.select(
    [
        ads["margin"] <= 0,
        (ads["margin"].abs()/ads["revenue"].replace(0, np.nan)
         ) <= 0.01,   # ±1% ~ break-even
        ads["margin_pct"] >= 0.10
    ],
    ["no_rentable", "break_even", "rentable_10p"],
    default="mixto"
)

# -----------------------
# 3) Top 5 por rentabilidad (margen)
# -----------------------
top5 = ads.sort_values("margin", ascending=False).head(5)

# -----------------------
# 4) Reasignación de presupuesto a quienes explican 75% del revenue
#    (Pareto simple, proporcional a su revenue dentro del grupo)
# -----------------------
TOTAL_BUDGET = float(ads["budget"].sum())
ads_sorted_rev = ads.sort_values("revenue", ascending=False)
cum_share = ads_sorted_rev["revenue"].cumsum(
) / ads_sorted_rev["revenue"].sum().replace(0, np.nan)
pareto_cut = ads_sorted_rev.loc[cum_share <= 0.75]
if pareto_cut.empty:
    # si no cumple, al menos el top 1
    pareto_cut = ads_sorted_rev.head(1)

# Nuevo presupuesto solo para el grupo Pareto, proporcional a su revenue
weights = pareto_cut["revenue"] / \
    pareto_cut["revenue"].sum().replace(0, np.nan)
new_budget = weights * TOTAL_BUDGET

plan = pareto_cut[["ad_id", "campaign_id", "adset_name", "sku",
                   "budget", "revenue", "margin", "cpa", "roas", "ctr"]].copy()
plan["new_budget"] = new_budget.values
plan["delta_budget"] = plan["new_budget"] - plan["budget"]
plan["delta_budget_pct"] = np.where(
    plan["budget"] > 0, plan["delta_budget"]/plan["budget"], np.nan)

# Nota: proyecciones simples manteniendo CPC/CTR/CVR constantes:
# (si quieres, puedes afinar luego con elasticidades)
plan["clicks_new"] = np.where(ads.set_index("ad_id").loc[plan["ad_id"], "cpc"].values > 0,
                              plan["new_budget"]/ads.set_index("ad_id").loc[plan["ad_id"], "cpc"].values, 0.0)
plan["conversions_new"] = plan["clicks_new"] * ads.set_index(
    # ~ clicks / (budget/conversions)
    "ad_id").loc[plan["ad_id"], "cpa"].rpow(-1).values
plan["revenue_new"] = plan["conversions_new"] * \
    ads.set_index("ad_id").loc[plan["ad_id"], "aov"].values
plan["margin_new"] = plan["revenue_new"] - plan["new_budget"]
plan["delta_revenue"] = plan["revenue_new"] - plan["revenue"]
plan["delta_margin"] = plan["margin_new"] - plan["margin"]

# -----------------------
# 5) Guardar snapshots en DuckDB + CSV para Looker
# -----------------------
Path("out").mkdir(exist_ok=True, parents=True)
con = duckdb.connect("warehouse.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS mart;")

# Materializamos
con.register("ads_df", ads)
con.register("top5_df", top5)
con.register("plan_df", plan)

con.execute("CREATE OR REPLACE TABLE mart.snap_ads_kpis AS SELECT * FROM ads_df;")
con.execute(
    "CREATE OR REPLACE TABLE mart.snap_ads_top5 AS SELECT * FROM top5_df;")
con.execute(
    "CREATE OR REPLACE TABLE mart.snap_budget_plan AS SELECT * FROM plan_df;")

# Parquet (rápido) y CSV (para Looker/GSheets)
con.execute(
    "COPY mart.snap_ads_kpis   TO 'out/snap_ads_kpis.parquet'   (FORMAT PARQUET);")
con.execute(
    "COPY mart.snap_ads_top5   TO 'out/snap_ads_top5.parquet'   (FORMAT PARQUET);")
con.execute(
    "COPY mart.snap_budget_plan TO 'out/snap_budget_plan.parquet' (FORMAT PARQUET);")

to_csv(ads,  "out/snap_ads_kpis.csv")
to_csv(top5, "out/snap_ads_top5.csv")
to_csv(plan, "out/snap_budget_plan.csv")

print("✅ Snapshots listos:")
print(" - mart.snap_ads_kpis / out/snap_ads_kpis.(parquet|csv)")
print(" - mart.snap_ads_top5 / out/snap_ads_top5.(parquet|csv)")
print(" - mart.snap_budget_plan / out/snap_budget_plan.(parquet|csv)")
