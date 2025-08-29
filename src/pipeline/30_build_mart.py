# src/pipeline/30_build_mart.py
import os
import pickle
import pandas as pd
import numpy as np
import duckdb

pd.options.display.float_format = '{:,.2f}'.format


def build_marts_from_snapshots(warehouse_path: str):
    """
    Se conecta a DuckDB, lee los snapshots y construye los marts de KPIs.
    """
    # Se conecta en modo de solo lectura, una buena práctica para análisis
    con = duckdb.connect(warehouse_path, read_only=True)

    # 1. Unir snapshots para crear una vista analítica completa
    query = """
    WITH merged_data AS (
        SELECT
            sim.*,
            c.campaign_name,
            p.product_name
        FROM snapshot_adset_simulation AS sim
        LEFT JOIN snapshot_campaigns AS c ON sim.campaign_id = c.campaign_id
        LEFT JOIN snapshot_products AS p ON sim.product_id = p.product_id
    ),
    
    -- 2. Simular impresiones para poder calcular el CTR
    enriched_data AS (
        SELECT
            *,
            -- Genera un CTR aleatorio entre 1% y 10% para calcular las impresiones
            (clicks / (random() * 0.09 + 0.01))::INTEGER AS simulated_impressions
        FROM merged_data
    )
    
    -- 3. Seleccionar todos los datos enriquecidos para el análisis
    SELECT * FROM enriched_data;
    """
    merged_df = con.execute(query).fetch_df()
    con.close()

    # 4. Calcular KPIs agregados usando Pandas
    kpi_adset = merged_df.groupby(['adset_name', 'campaign_name']).agg(
        total_budget=('budget', 'sum'), total_clicks=('clicks', 'sum'),
        total_impressions=('simulated_impressions', 'sum'), total_conversions=('conversions', 'sum'),
        total_revenue=('revenue', 'sum'), total_margin=('margin', 'sum')
    ).reset_index()

    kpi_campaign = kpi_adset.groupby('campaign_name').agg(
        total_budget=('total_budget', 'sum'), total_clicks=('total_clicks', 'sum'),
        total_impressions=('total_impressions', 'sum'), total_conversions=('total_conversions', 'sum'),
        total_revenue=('total_revenue', 'sum'), total_margin=('total_margin', 'sum')
    ).reset_index()

    top_10_products = merged_df.groupby('product_name').agg(
        total_revenue=('revenue', 'sum'), total_conversions=('conversions', 'sum')
    ).sort_values(by='total_revenue', ascending=False).head(10).reset_index()

    kpi_global = {
        'total_budget': kpi_campaign['total_budget'].sum(),
        'total_revenue': kpi_campaign['total_revenue'].sum(),
        'total_conversions': kpi_campaign['total_conversions'].sum(),
        'total_clicks': kpi_campaign['total_clicks'].sum(),
        'total_impressions': kpi_campaign['total_impressions'].sum()
    }
    # Se añade una condición para evitar división por cero
    kpi_global['cpa'] = kpi_global['total_budget'] / \
        kpi_global['total_conversions'] if kpi_global['total_conversions'] else 0
    kpi_global['roas'] = kpi_global['total_revenue'] / \
        kpi_global['total_budget'] if kpi_global['total_budget'] else 0
    kpi_global['cpc'] = kpi_global['total_budget'] / \
        kpi_global['total_clicks'] if kpi_global['total_clicks'] else 0
    kpi_global['ctr'] = kpi_global['total_clicks'] / \
        kpi_global['total_impressions'] if kpi_global['total_impressions'] else 0

    return kpi_global, kpi_campaign, kpi_adset, top_10_products


if __name__ == "__main__":
    print("\n--- [Paso 30] Iniciando la construcción de Snapshots y KPIs ---")
    input_path = "out/raw_data.pkl"
    warehouse_path = "warehouse.duckdb"

    # --- Carga de datos crudos ---
    try:
        with open(input_path, "rb") as f:
            raw_data = pickle.load(f)
        print(f"✅ Datos crudos cargados desde: {input_path}")
    except FileNotFoundError:
        print(
            f"🔥 Error: No se encontró el archivo de datos en '{input_path}'.")
        print("➡️ Asegúrate de ejecutar primero el script '10_extract_supabase.py'.")
        exit()

    # --- Creación de Snapshots (con verificación de datos) ---
    con = duckdb.connect(warehouse_path)
    valid_tables = []
    for name, df in raw_data.items():
        if df.empty:
            print(
                f"⚠️ ¡Atención! La tabla '{name}' está vacía y será ignorada.")
            continue

        table_name = f"snapshot_{name}"
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        print(f"✅ Snapshot '{table_name}' creado en DuckDB.")
        valid_tables.append(name)
    con.close()

    # Comprobar si tenemos los datos necesarios para continuar
    if 'adset_simulation' not in valid_tables:
        print("\n🔥 La tabla 'adset_simulation' está vacía. No se pueden calcular los KPIs.")
        print("➡️ Por favor, añade datos a la tabla 'adset_simulation' en Supabase y vuelve a ejecutar el pipeline.")
        exit()

    print("\n--- Construyendo KPIs desde los Snapshots ---")
    global_kpis, campaign_kpis, adset_kpis, top_products = build_marts_from_snapshots(
        warehouse_path)

    print("\n\n--- 📊 RESULTADOS DE INDICADORES ---")
    print("\n--- Indicadores Globales ---")
    print(pd.Series(global_kpis))
    print("\n\n--- Top 10 Productos más Vendidos (por Ingresos) ---")
    print(top_products)
    print("\n\n--- KPI por Campaña ---")
    print(campaign_kpis.sort_values(by='total_margin', ascending=False))
    print("\n\n--- KPI por Adset ---")
    print(adset_kpis.sort_values(by='total_margin', ascending=False))
    print("\n\n--- [Paso 30] Cálculo de KPIs completado. ---")
