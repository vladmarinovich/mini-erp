import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("SUPA_HOST")
PORT = os.getenv("SUPA_PORT", "6543")
DB = os.getenv("SUPA_DB", "postgres")
PWD = os.getenv("SUPA_PASSWORD")
PRJ = os.getenv("SUPA_PROJECT")

USER_A = os.getenv("SUPA_USER_A")  # postgres.<ref>
USER_B = os.getenv("SUPA_USER_B", "postgres")

DSN_A = f"user={USER_A} password='{PWD}' host={HOST} port={PORT} dbname={DB} sslmode=require"
DSN_B = f"user={USER_B} password='{PWD}' host={HOST} port={PORT} dbname={DB} sslmode=require options='project={PRJ}'"


def try_dsn(label, dsn):
    print(f"\n=== Probar {label} ===")
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select current_user, current_database(), inet_server_addr(), inet_server_port();")
                print("✅ OK:", cur.fetchone())
    except Exception as e:
        print("❌", repr(e))


try_dsn("A (usuario con .ref)", DSN_A)
try_dsn("B (usuario simple + options)", DSN_B)
print("\n✅ Conexión OK")
