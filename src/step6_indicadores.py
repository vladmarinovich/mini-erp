# src/step6_indicadores.py
import pandas as pd
import numpy as np
from supa_client import get_client

supabase = get_client()

# 1) Descubrir el último run_ts
last = (supabase.table("adset_simulation")
        .select("run_ts")
        .order("run_ts", desc=True)
        .limit(1)
        .execute())
if not last.data:
    raise SystemExit("No hay datos en adset_simulation.")
run_ts = last.data[0]["run_ts"]

# 2) Traer todas las filas del último run
res = (supabase.table("adset_simulation")
       .select("campaign_id, adset_name, product_id, sku, budget, cpc, cvr, aov, clicks, conversions, revenue, margin")
       .eq("run_ts", run_ts)
       .execute())
df = pd.DataFrame(res.data or [])
if df.empty:
    raise SystemExit("Último run vacío.")

# Si tu tabla no guarda clicks/conversions/revenue/margin, calcúlalo al vuelo:
if "clicks" not in df or df["clicks"].isna().any():
    df["clicks"] = df["budget"] / df["cpc"]
if "conversions" not in df or df["conversions"].isna().any():
    df["conversions"] = df["clicks"] * df["cvr"]
if "revenue" not in df or df["revenue"].isna().any():
    df["revenue"] = df["conversions"] * df["aov"]

# 3) Indicadores por CAMPAÑA
camp = (df.groupby("campaign_id", as_index=False)
          .agg(total_budget=("budget", "sum"),
               total_clicks=("clicks", "sum"),
               total_conv=("conversions", "sum"),
               total_rev=("revenue", "sum"),
               total_margin=("margin", "sum") if "margin" in df.columns else ("revenue", "sum")))
camp["CPA"] = camp["total_budget"] / camp["total_conv"].replace({0: np.nan})
camp["ROAS"] = camp["total_rev"] / camp["total_budget"].replace({0: np.nan})
camp["ConvRate"] = camp["total_conv"] / \
    camp["total_clicks"].replace({0: np.nan})

print("\n=== KPIs por CAMPAÑA (último run) ===")
print(camp.round(2))

# 4) Indicadores por PRODUCTO (sumando campañas)
prod = (df.groupby(["product_id", "sku"], as_index=False)
          .agg(total_budget=("budget", "sum"),
               total_conv=("conversions", "sum"),
               total_rev=("revenue", "sum"),
               total_margin=("margin", "sum") if "margin" in df.columns else ("revenue", "sum")))
prod["CPA"] = prod["total_budget"] / prod["total_conv"].replace({0: np.nan})
prod = prod.sort_values(
    "total_margin" if "margin" in prod.columns else "total_rev", ascending=False)

print("\n=== Top 10 PRODUCTOS por margen/revenue (último run) ===")
print(prod.head(10).round(2))

# 5) (Opcional) Guardar csvs para revisar
camp.to_csv("out/kpis_campana.csv", index=False)
prod.to_csv("out/kpis_producto.csv", index=False)
print("\nArchivos guardados en out/: kpis_campana.csv, kpis_producto.csv")


df = pd.read_csv("out/plan_reallocation_total_detailed.csv")
df.to_excel("out/plan_reallocation_total_detailed.xlsx", index=False)

# Repite para los otros datasets que vas a visualizar:
# sum_campaign.to_excel("out/summary_by_campaign.xlsx", index=False)
# df_prod.to_excel("out/revenue_by_product.xlsx", index=False)
