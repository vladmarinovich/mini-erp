import numpy as np
import pandas as pd
import random
from datetime import datetime
from supa_client import get_client  # usamos el cliente correcto

# =======================
# Configuración
# =======================
TOP10_SKUS = [
    "SKU017", "SKU001", "SKU006", "SKU018", "SKU002",
    "SKU005", "SKU016", "SKU020", "SKU015", "SKU019"
]

CAMPAIGNS = {
    1: {"name": "PMAX",     "budget": 3_000_000},
    2: {"name": "Video",    "budget": 2_000_000},
    3: {"name": "Busqueda", "budget": 2_000_000},
}

# Rangos de simulación pedidos
CPC_MIN, CPC_MAX = 0.1, 2.8      # costo por clic
CVR_MIN, CVR_MAX = 0.005, 0.05   # 0.5% a 5%

# =======================
# Carga de productos (DB)
# =======================
supabase = get_client()

res = (
    supabase.table("products")
    .select("product_id, sku, product_name, sale_price, unit_cost")
    .in_("sku", TOP10_SKUS)
    .execute()
)
prods = pd.DataFrame(res.data or [])
if prods.empty or len(prods) != 10:
    raise SystemExit(
        "No pude cargar los 10 SKUs desde 'products'. Revisa TOP10_SKUS.")

prods = prods.sort_values("sku").reset_index(drop=True)

# =======================
# Simulación
# =======================
results = []
run_ts = datetime.utcnow().isoformat()

for campaign_id, camp in CAMPAIGNS.items():
    budget_total = camp["budget"]
    # reparto igual dentro de la campaña
    budget_share = budget_total / len(prods)

    for _, p in prods.iterrows():
        # AOV = sale_price del producto
        sale_price = float(p["sale_price"])
        unit_cost = float(p["unit_cost"])

        cpc = round(random.uniform(CPC_MIN, CPC_MAX), 2)
        cvr = round(random.uniform(CVR_MIN, CVR_MAX), 4)

        clicks = budget_share / cpc if cpc > 0 else 0.0
        conversions = clicks * cvr
        revenue = conversions * sale_price
        margin = conversions * (sale_price - unit_cost)

        results.append({
            "run_ts": run_ts,
            "campaign_id": int(campaign_id),
            "adset_name": f"{p['product_name']} - {p['sku']}",
            "product_id": int(p["product_id"]),
            "sku": p["sku"],
            "budget": float(budget_share),
            "cpc": float(cpc),
            "cvr": float(cvr),
            # AOV = sale_price (clave del cambio)
            "aov": float(sale_price),
            "clicks": float(clicks),
            "conversions": float(conversions),
            "revenue": float(revenue),
            "margin": float(margin),
            "assumptions": {
                "budget_total_campaign": budget_total,
                "cpc_range": [CPC_MIN, CPC_MAX],
                "cvr_range": [CVR_MIN, CVR_MAX],
                "aov_source": "sale_price_from_products"
            },
        })

df = pd.DataFrame(results)
print("\n=== Preview ===")
print(df[["campaign_id", "sku", "budget", "cpc", "cvr", "aov",
      "clicks", "conversions", "revenue", "margin"]].head(12))

# =======================
# Guardar en Supabase
# =======================
ins = supabase.table("adset_simulation").insert(results).execute()
print(f"\n✅ {len(results)} filas guardadas en adset_simulation.")


df = pd.read_csv("out/plan_reallocation_total_detailed.csv")
df.to_excel("out/plan_reallocation_total_detailed.xlsx", index=False)

# Repite para los otros datasets que vas a visualizar:
# sum_campaign.to_excel("out/summary_by_campaign.xlsx", index=False)
# df_prod.to_excel("out/revenue_by_product.xlsx", index=False)
