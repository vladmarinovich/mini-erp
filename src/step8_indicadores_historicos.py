# src/step8_indicadores_historicos.py
import pandas as pd
from pathlib import Path

Path("out").mkdir(parents=True, exist_ok=True)

# --- 1) Cargar vistas materializadas (parquet) ---
camp_p = "out/kpis_campana.parquet"
adset_p = "out/kpis_adset.parquet"
adset_prod_p = "out/kpis_adset_producto.parquet"
prod_p = "out/kpis_producto.parquet"

df_camp = pd.read_parquet(camp_p)
df_adset = pd.read_parquet(adset_p)
df_ap = pd.read_parquet(adset_prod_p)
df_prod = pd.read_parquet(prod_p)

# --- 2) Normalizaciones rápidas (por si vienen nulos) ---
for df in (df_camp, df_adset, df_ap, df_prod):
    for col in ("total_budget", "total_clicks", "total_conversions", "total_revenue", "total_margin"):
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

# --- 3) KPIs por CAMPAÑA (baseline) ---
camp = df_camp.copy()
camp["CPA"] = camp["total_budget"] / \
    camp["total_conversions"].replace({0: pd.NA})
camp["ROAS"] = camp["total_revenue"] / camp["total_budget"].replace({0: pd.NA})
camp["ConvRate"] = camp["total_conversions"] / \
    camp["total_clicks"].replace({0: pd.NA})
camp_out = camp[["campaign_id", "total_budget", "total_clicks", "total_conversions",
                 "total_revenue", "total_margin", "CPA", "ROAS", "ConvRate"]].sort_values("campaign_id")

# --- 4) KPIs por ADSET ---
adset = df_adset.copy()
adset["CPA"] = adset["total_budget"] / \
    adset["total_conversions"].replace({0: pd.NA})
adset["ROAS"] = adset["total_revenue"] / \
    adset["total_budget"].replace({0: pd.NA})
adset["ConvRate"] = adset["total_conversions"] / \
    adset["total_clicks"].replace({0: pd.NA})
adset_out = adset[["campaign_id", "adset_name", "total_budget", "total_clicks",
                   "total_conversions", "total_revenue", "total_margin", "CPA", "ROAS", "ConvRate"]]\
    .sort_values(["campaign_id", "adset_name"])

# --- 5) KPIs por PRODUCTO (sumando campañas y adsets) ---
prod = df_ap.groupby(["product_id", "sku"], as_index=False).agg(
    total_budget=("total_budget", "sum"),
    total_clicks=("total_clicks", "sum"),
    total_conversions=("total_conversions", "sum"),
    total_revenue=("total_revenue", "sum"),
    total_margin=("total_margin", "sum"),
)
prod["CPA"] = prod["total_budget"] / \
    prod["total_conversions"].replace({0: pd.NA})
prod["ROAS"] = prod["total_revenue"] / prod["total_budget"].replace({0: pd.NA})
prod_out = prod.sort_values("total_margin", ascending=False)

# --- 6) Guardar snapshots ---
camp_out.to_csv("out/indicadores_campana.csv", index=False)
adset_out.to_csv("out/indicadores_adset.csv", index=False)
prod_out.to_csv("out/indicadores_producto.csv", index=False)

# Opcional: también en parquet
camp_out.to_parquet("out/indicadores_campana.parquet", index=False)
adset_out.to_parquet("out/indicadores_adset.parquet", index=False)
prod_out.to_parquet("out/indicadores_producto.parquet", index=False)

# --- 7) Prints cortos para validar ---
print("\n=== KPIs por CAMPAÑA ===")
print(camp_out.round(2))

print("\n=== Top 10 ADSETS por ROAS ===")
print(adset_out.sort_values("ROAS", ascending=False).head(10).round(2))

print("\n=== Top 10 PRODUCTOS por margen ===")
print(prod_out.head(10).round(2))


df = pd.read_csv("out/plan_reallocation_total_detailed.csv")
df.to_excel("out/plan_reallocation_total_detailed.xlsx", index=False)

# Repite para los otros datasets que vas a visualizar:
# sum_campaign.to_excel("out/summary_by_campaign.xlsx", index=False)
# df_prod.to_excel("out/revenue_by_product.xlsx", index=False)
