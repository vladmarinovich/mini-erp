# src/step7_duckdb_kpis.py
import duckdb
import pandas as pd
from pathlib import Path
from supa_client import get_client
import os


def save_csv(df, path):
    """
    Guarda un DataFrame en formato CSV, usando coma (,) como separador decimal
    y redondeando los números a 2 decimales. Ideal para Google Sheets.
    """
    # Asegura que el directorio de salida exista
    os.makedirs(os.path.dirname(path), exist_ok=True)

    df_copy = df.copy()

    # Itera sobre las columnas que son numéricas (enteros o flotantes)
    for col in df_copy.select_dtypes(include=['number']).columns:
        # Redondea la columna a 2 decimales
        df_copy[col] = df_copy[col].round(2)

    # Exporta a CSV especificando la coma como separador decimal
    # Se elimina float_format para evitar conflictos con el parámetro 'decimal'
    df_copy.to_csv(path, index=False, decimal=',')
    print(f"Archivo guardado en: {path}")


# --- INICIO DE TU CÓDIGO ORIGINAL (SIN CAMBIOS) ---

# 0) Prep carpetas/DB
Path("out").mkdir(parents=True, exist_ok=True)
con = duckdb.connect("warehouse.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS mart;")

# 1) Trae el último run desde Supabase
sb = get_client()
last = (sb.table("adset_simulation")
          .select("run_ts")
          .order("run_ts", desc=True)
          .limit(1)
          .execute())
if not last.data:
    raise SystemExit("No hay datos en adset_simulation.")
run_ts = last.data[0]["run_ts"]

rows = (sb.table("adset_simulation")
          .select("campaign_id, adset_name, product_id, sku, budget, cpc, cvr, aov, clicks, conversions, revenue, margin, run_ts")
          .eq("run_ts", run_ts)
          .execute()).data
df = pd.DataFrame(rows)
if df.empty:
    raise SystemExit("Último run vacío.")

# 2) Registra el DataFrame en DuckDB
con.register("adset_sim_df", df)

# 3) Vistas de KPIs
con.execute("""
CREATE OR REPLACE VIEW mart.kpi_campaign AS
WITH base AS (
  SELECT
    campaign_id,
    SUM(budget)      AS total_budget,
    SUM(clicks)      AS total_clicks,
    SUM(conversions) AS total_conversions,
    SUM(revenue)     AS total_revenue,
    SUM(COALESCE(margin, revenue)) AS total_margin
  FROM adset_sim_df
  GROUP BY campaign_id
)
SELECT
  campaign_id,
  total_budget,
  total_clicks,
  total_conversions,
  total_revenue,
  total_margin,
  total_budget / NULLIF(total_conversions, 0) AS cpa,
  total_revenue / NULLIF(total_budget, 0)     AS roas,
  total_conversions / NULLIF(total_clicks, 0) AS conv_rate
FROM base;
""")

con.execute("""
CREATE OR REPLACE VIEW mart.kpi_product AS
WITH base AS (
  SELECT
    product_id, sku,
    SUM(budget)      AS total_budget,
    SUM(conversions) AS total_conversions,
    SUM(revenue)     AS total_revenue,
    SUM(COALESCE(margin, revenue)) AS total_margin
  FROM adset_sim_df
  GROUP BY product_id, sku
)
SELECT
  product_id, sku,
  total_budget,
  total_conversions,
  total_revenue,
  total_margin,
  total_budget / NULLIF(total_conversions, 0) AS cpa
FROM base
ORDER BY total_margin DESC NULLS LAST;
""")

con.execute("""
CREATE OR REPLACE VIEW mart.kpi_adset AS
WITH base AS (
  SELECT
    campaign_id,
    adset_name,
    SUM(budget)      AS total_budget,
    SUM(clicks)      AS total_clicks,
    SUM(conversions) AS total_conversions,
    SUM(revenue)     AS total_revenue,
    SUM(COALESCE(margin, revenue)) AS total_margin
  FROM adset_sim_df
  GROUP BY campaign_id, adset_name
)
SELECT
  campaign_id,
  adset_name,
  total_budget,
  total_clicks,
  total_conversions,
  total_revenue,
  total_margin,
  total_budget / NULLIF(total_conversions, 0) AS cpa,
  total_revenue / NULLIF(total_budget, 0)     AS roas,
  total_conversions / NULLIF(total_clicks, 0) AS conv_rate
FROM base
ORDER BY campaign_id, adset_name;
""")

con.execute("""
CREATE OR REPLACE VIEW mart.kpi_adset_product AS
SELECT
  campaign_id,
  adset_name,
  product_id,
  sku,
  SUM(budget)      AS total_budget,
  SUM(clicks)      AS total_clicks,
  SUM(conversions) AS total_conversions,
  SUM(revenue)     AS total_revenue,
  SUM(COALESCE(margin, revenue)) AS total_margin,
  SUM(budget) / NULLIF(SUM(conversions), 0) AS cpa
FROM adset_sim_df
GROUP BY campaign_id, adset_name, product_id, sku
ORDER BY campaign_id, adset_name, total_margin DESC NULLS LAST;
""")

# 4) Materializa snapshots
con.execute(
    "CREATE OR REPLACE TABLE mart.kpi_campaign_snap AS SELECT * FROM mart.kpi_campaign;")
con.execute(
    "CREATE OR REPLACE TABLE mart.kpi_product_snap  AS SELECT * FROM mart.kpi_product;")
con.execute(
    "CREATE OR REPLACE TABLE mart.kpi_adset_snap    AS SELECT * FROM mart.kpi_adset;")
con.execute(
    "CREATE OR REPLACE TABLE mart.kpi_adset_product_snap AS SELECT * FROM mart.kpi_adset_product;")

# Exporta PARQUET (ideal para consultas)
con.execute(
    "COPY mart.kpi_campaign_snap        TO 'out/kpis_campana.parquet'        (FORMAT PARQUET);")
con.execute(
    "COPY mart.kpi_product_snap         TO 'out/kpis_producto.parquet'       (FORMAT PARQUET);")
con.execute(
    "COPY mart.kpi_adset_snap           TO 'out/kpis_adset.parquet'          (FORMAT PARQUET);")
con.execute(
    "COPY mart.kpi_adset_product_snap   TO 'out/kpis_adset_producto.parquet' (FORMAT PARQUET);")

# --- FIN DE TU CÓDIGO ORIGINAL ---


# --- NUEVA SECCIÓN: Exporta CSV con formato para Google Sheets ---
print("\nExportando KPIs a formato CSV para Google Sheets...")

# Carga las tablas materializadas de DuckDB a DataFrames de pandas
df_campaign = con.table('mart.kpi_campaign_snap').df()
df_product = con.table('mart.kpi_product_snap').df()
df_adset = con.table('mart.kpi_adset_snap').df()
df_adset_product = con.table('mart.kpi_adset_product_snap').df()

# Llama a la función save_csv para cada DataFrame
save_csv(df_campaign, 'out/kpis_campana.csv')
save_csv(df_product, 'out/kpis_producto.csv')
save_csv(df_adset, 'out/kpis_adset.csv')
save_csv(df_adset_product, 'out/kpis_adset_producto.csv')

print("\nProceso completado exitosamente.")

# Cierra la conexión con la base de datos
con.close()
