import os
from supabase import create_client
from dotenv import load_dotenv


def get_client(service_role: bool = True):
    load_dotenv()
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE"] if service_role else os.environ["SUPABASE_ANON_KEY"]
    return create_client(url, key)
