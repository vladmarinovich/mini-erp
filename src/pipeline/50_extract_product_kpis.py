# src/pipeline/50_extract_product_kpis.py
import pandas as pd
from ..utils.supa_client import supabase

pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.width', 1000)


def fetch_table_as_df(table_name: str):
    """Lee una tabla de Supabase y la convierte a DataFrame."""
    try:
        response = supabase.from_(table_name).select("*").execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        print(f"🔥 Error al cargar la tabla '{table_name}': {e}")
        return None


if __name__ == "__main__":
    print("\n--- Extrayendo KPIs de Productos para Análisis ---")

    sales_df = fetch_table_as_df('sales')
    products_df = fetch_table_as_df('products')

    # --- CÓDIGO DE DEPURACIÓN ---
    if sales_df is not None:
        print("\n--- Columnas disponibles en la tabla 'sales' ---")
        print(list(sales_df.columns))
        print("--------------------------------------------------\n")
    # --- FIN DEL CÓDIGO DE DEPURACIÓN ---

    if sales_df is None or products_df is None or sales_df.empty:
        print("🔥 Faltan datos de ventas o productos. Abortando.")
        exit()

    # Asegúrate de que los nombres de columna aquí coincidan con los de tu tabla
    # MODIFICA 'qty' y 'unit_price' si es necesario
    sales_df['revenue'] = sales_df['qty'] * sales_df['unit_price']

    product_kpis = sales_df.groupby('product_id').agg(
        sales_frequency=('sale_id', 'count'),
        sales_volume=('qty', 'sum'),  # <- TAMBIÉN AQUÍ
        total_revenue=('revenue', 'sum')
    ).reset_index()

    product_kpis = pd.merge(product_kpis, products_df,
                            on='product_id', how='left')

    product_kpis['profit_margin_pct'] = (
        (product_kpis['sale_price'] - product_kpis['unit_cost']) / product_kpis['sale_price']).fillna(0) * 100

    output_df = product_kpis[[
        'product_id', 'product_name', 'total_revenue',
        'sales_frequency', 'sales_volume', 'profit_margin_pct'
    ]].sort_values(by='total_revenue', ascending=False)

    print("\n\n--- COPIA Y PEGA LA SIGUIENTE TABLA EN CHATGPT ---\n")
    print("Por favor, clasifica los siguientes productos en 4 segmentos (ej. 'Estrella', 'Nicho', 'Volumen', 'Durmiente') basándote en estas métricas:\n")
    print(output_df.to_string(index=False))

    print("\n\n--- Extracción de KPIs completada. ---")
