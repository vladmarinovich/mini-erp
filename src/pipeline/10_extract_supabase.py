# src/pipeline/10_extract_supabase.py

import os
import pickle
import pandas as pd
# Importamos el cliente usando una importación relativa
from ..utils.supa_client import supabase

# --- ESTA ES LA FUNCIÓN QUE FALTABA ---


def fetch_table_as_df(table_name: str, limit: int = None):
    """
    Función genérica para leer una tabla completa de Supabase y convertirla a DataFrame.
    """
    if supabase is None:
        print(
            f"🔥 No hay conexión a Supabase. Abortando la carga de '{table_name}'.")
        return None

    try:
        print(f"🚀 Realizando consulta a la tabla '{table_name}'...")
        query = supabase.from_(table_name).select("*")

        if limit:
            query = query.limit(limit)

        response = query.execute()
        data = response.data
        df = pd.DataFrame(data)
        print(
            f"✅ Tabla '{table_name}' cargada exitosamente ({len(df)} filas).")
        return df

    except Exception as e:
        print(f"🔥 Error al consultar la tabla '{table_name}': {e}")
        return None


# --- Punto de entrada principal del script ---
if __name__ == "__main__":
    print("\n--- [Paso 10] Iniciando el script de extracción desde Supabase ---")

    output_dir = "out"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "raw_data.pkl")

    tables_to_extract = [
        "products", "suppliers", "campaigns",
        "adsets", "adset_product"
    ]

    dataframes = {}
    for table in tables_to_extract:
        # Aquí es donde se llama a la función
        dataframes[table] = fetch_table_as_df(table)

    try:
        with open(output_path, "wb") as f:
            pickle.dump(dataframes, f)
        print(f"\n✅ Datos guardados exitosamente en: {output_path}")
    except Exception as e:
        print(f"🔥 Error al guardar los datos: {e}")

    print("\n--- [Paso 10] Extracción completada. ---")
