import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Esta línea busca el archivo .env en tu carpeta
load_dotenv()

# Estas líneas leen la URL y la llave desde ese archivo
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Y finalmente, las usa para crear la conexión
try:
    supabase: Client = create_client(url, key)
    print("✅ Conexión a Supabase exitosa.")
except Exception as e:
    print(f"🔥 Error conectando a Supabase: {e}")
    supabase = None
