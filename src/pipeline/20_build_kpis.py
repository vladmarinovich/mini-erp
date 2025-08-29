# src/pipeline/20_build_kpis.py
import pandas as pd
import numpy as np
import os
from ..utils.supa_client import supabase

pd.options.display.float_format = '{:,.2f}'.format

if __name__ == "__main__":
    print("\n--- Iniciando script: Análisis y Exportación de KPIs ---")

    # 1. Extraer la tabla adset_simulation
    try:
        response = supabase.from_('adset_simulation').select("*").execute()
        df = pd.DataFrame(response.data)
        print(f"✅ Tabla 'adset_simulation' cargada ({len(df)} filas).")
    except Exception as e:
        print(f"🔥 Error al cargar la tabla 'adset_simulation': {e}")
        exit()

    if df.empty:
        print("\n🔥 La tabla 'adset_simulation' está vacía. No se puede continuar.")
        exit()

    # 2. Limpiar y preparar datos
    df['clicks'] = pd.to_numeric(
        df['clicks'], errors='coerce').fillna(0).astype(int)
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce').fillna(0)
    df['conversions'] = pd.to_numeric(
        df['conversions'], errors='coerce').fillna(0).astype(int)
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    df['margin'] = pd.to_numeric(df['margin'], errors='coerce').fillna(0)

    df['simulated_ctr'] = np.random.uniform(0.01, 0.10, size=len(df))
    df['simulated_impressions'] = (
        df['clicks'] / df['simulated_ctr']).replace(np.inf, 0).astype(int)

    # 3. Calcular los KPIs
    # KPI por Adset
    kpi_adset = df.groupby(['campaign_id', 'adset_name']).agg(
        total_budget=('budget', 'sum'),
        total_clicks=('clicks', 'sum'),
        total_impressions=('simulated_impressions', 'sum'),
        total_conversions=('conversions', 'sum'),
        total_revenue=('revenue', 'sum'),
        total_margin=('margin', 'sum')
    ).reset_index()

    # KPI por Campaña
    kpi_campaign = kpi_adset.groupby('campaign_id').agg(
        total_budget=('total_budget', 'sum'),
        total_clicks=('total_clicks', 'sum'),
        total_impressions=('total_impressions', 'sum'),
        total_conversions=('total_conversions', 'sum'),
        total_revenue=('total_revenue', 'sum'),
        total_margin=('total_margin', 'sum')
    ).reset_index()

    # Top 10 Productos
    top_10_products = df.groupby('product_id').agg(
        total_revenue=('revenue', 'sum'),
        total_conversions=('conversions', 'sum')
    ).sort_values(by='total_revenue', ascending=False).head(10).reset_index()

    # KPI Global
    kpi_global = {
        'total_budget': kpi_campaign['total_budget'].sum(),
        'total_revenue': kpi_campaign['total_revenue'].sum(),
        'total_conversions': kpi_campaign['total_conversions'].sum(),
        'total_clicks': kpi_campaign['total_clicks'].sum(),
        'total_impressions': kpi_campaign['total_impressions'].sum()
    }
    kpi_global['cpa'] = kpi_global['total_budget'] / \
        kpi_global['total_conversions'] if kpi_global['total_conversions'] else 0
    kpi_global['roas'] = kpi_global['total_revenue'] / \
        kpi_global['total_budget'] if kpi_global['total_budget'] else 0
    kpi_global['cpc'] = kpi_global['total_budget'] / \
        kpi_global['total_clicks'] if kpi_global['total_clicks'] else 0
    kpi_global['ctr'] = kpi_global['total_clicks'] / \
        kpi_global['total_impressions'] if kpi_global['total_impressions'] else 0

    # Convertir el diccionario de KPI Global a DataFrame para guardarlo
    kpi_global_df = pd.DataFrame([kpi_global])

    # 4. Imprimir resultados en la terminal
    print("\n\n--- 📊 RESULTADOS DE INDICADORES ---")
    print("\n--- Indicadores Globales ---")
    print(kpi_global_df)
    print("\n\n--- Top 10 Productos (por ID) ---")
    print(top_10_products)
    print("\n\n--- KPI por Campaña (por ID) ---")
    print(kpi_campaign.sort_values(by='total_margin', ascending=False))
    print("\n\n--- KPI por Adset ---")
    print(kpi_adset.sort_values(by='total_margin', ascending=False))

    # --- 5. EXPORTAR RESULTADOS A CSV ---
    output_dir = "out"
    # Crea la carpeta 'out' si no existe
    os.makedirs(output_dir, exist_ok=True)

    try:
        print(f"\n\n--- 💾 Exportando KPIs a la carpeta '{output_dir}' ---")
        kpi_global_df.to_csv(os.path.join(
            output_dir, "kpi_global.csv"), index=False)
        kpi_campaign.to_csv(os.path.join(
            output_dir, "kpi_campaign.csv"), index=False)
        kpi_adset.to_csv(os.path.join(
            output_dir, "kpi_adset.csv"), index=False)
        top_10_products.to_csv(os.path.join(
            output_dir, "top_10_products.csv"), index=False)
        print("✅ Exportación completada exitosamente.")
    except Exception as e:
        print(f"🔥 Error durante la exportación: {e}")

    print("\n\n--- ✅ Pipeline de análisis completado. ---")
