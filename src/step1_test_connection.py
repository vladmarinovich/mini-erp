import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# 1) Cargar .env
load_dotenv()
URL = os.environ["SUPABASE_URL"]
KEY = os.environ["SUPABASE_SERVICE_ROLE"]  # solo backend/local

# 2) Cliente
supabase = create_client(URL, KEY)

# 3) Probar: leer 5 productos
resp = supabase.table("products").select(
    "product_id, sku, product_name, sale_price").limit(5).execute()
rows = resp.data or []
print("✅ Conectado. Filas leídas (products):", len(rows))
print(pd.DataFrame(rows))
