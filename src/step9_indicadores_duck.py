import pandas as pd
import duckdb

# Abrir la base local
con = duckdb.connect("warehouse.duckdb")

# Registrar los CSV/Parquet como tablas temporales
con.execute(
    "CREATE OR REPLACE TABLE adset AS SELECT * FROM read_csv_auto('out/kpis_adset.csv');")
con.execute(
    "CREATE OR REPLACE TABLE adset_prod AS SELECT * FROM read_csv_auto('out/kpis_adset_producto.csv');")
con.execute(
    "CREATE OR REPLACE TABLE camp AS SELECT * FROM read_csv_auto('out/kpis_campana.csv');")
con.execute(
    "CREATE OR REPLACE TABLE prod AS SELECT * FROM read_csv_auto('out/kpis_producto.csv');")


df = pd.read_csv("out/plan_reallocation_total_detailed.csv")
df.to_excel("out/plan_reallocation_total_detailed.xlsx", index=False)

# Repite para los otros datasets que vas a visualizar:
# sum_campaign.to_excel("out/summary_by_campaign.xlsx", index=False)
# df_prod.to_excel("out/revenue_by_product.xlsx", index=False)
