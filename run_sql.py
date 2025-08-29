# src/sql/run_sql.py
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")
if not DB_URL:
    raise SystemExit("Falta SUPABASE_DB_URL en .env")

SQL_DIR = Path(__file__).resolve().parent  # src/sql
FILES = [
    SQL_DIR / "00_schema.sql",
    SQL_DIR / "01_seed_core.sql",
    SQL_DIR / "02_seed_demo.sql",  # opcional, si existe
]


def run_sql_file(cur, path: Path):
    sql = path.read_text(encoding="utf-8")
    # separa por ';' de forma simple (scripts nuestros)
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        cur.execute(stmt + ';')


def main():
    with psycopg2.connect(DB_URL) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for f in FILES:
                if not f.exists():
                    print(f"[WARN] No existe {f}, se omite.")
                    continue
                print(f"[APPLY] {f.name}")
                run_sql_file(cur, f)
        print("✅ SQL aplicado (DDL + seeds).")


if __name__ == "__main__":
    main()
