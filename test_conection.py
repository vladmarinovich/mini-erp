import os
import psycopg2
from dotenv import load_dotenv

# Carga las variables del archivo .env en el entorno
load_dotenv()

print("Iniciando prueba de conexión a la base de datos...")

try:
    # Lee las credenciales del entorno
    db_password = os.getenv("DB_PASSWORD")
    db_user = os.getenv("DB_USER")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # Intenta establecer la conexión
    print(f"Conectando al host '{db_host}'...")
    with psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    ) as conn:
        # Si la conexión es exitosa, crea un cursor para ejecutar comandos
        with conn.cursor() as cur:
            # Ejecuta una consulta simple para obtener la versión de PostgreSQL
            cur.execute("SELECT version();")

            # Recupera el resultado de la consulta
            db_version = cur.fetchone()

            print("\n" + "="*40)
            print("✅ ¡Conexión exitosa!")
            print(f"Versión de PostgreSQL: {db_version[0]}")
            print("="*40)

except Exception as e:
    print("\n" + "!"*40)
    print("❌ Error al conectar a la base de datos.")
    print(f"Detalle del error: {e}")
    print("!"*40)
    print("\nRevisa los siguientes puntos:")
    print("1. ¿La contraseña en el archivo .env es correcta?")
    print("2. ¿Tu red permite conexiones salientes en el puerto 5432?")
    print("3. ¿Has añadido tu IP a la lista de permitidos en Supabase si tienes alguna restricción?")
