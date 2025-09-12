from sqlalchemy import create_engine, text
import pymysql
import socket

DB_HOST = "bancorep.ctb9mrigxgco.us-east-1.rds.amazonaws.com"
DB_PORT = 3306
DB_NAME = "bancorep"
DB_USER = "admin"
DB_PASSWORD = "admin123"

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW() AS fecha;"))
        print("✅ Conexión exitosa:", result.fetchone())
except Exception as e:
    print("❌ Error de conexión:", e)


print("Probando DNS…")
try:
    ip = socket.gethostbyname(DB_HOST)
    print("✔ DNS resuelto:", ip)
except Exception as e:
    print("❌ Error resolviendo DNS:", e)

print("Probando conexión MySQL…")
try:
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        connect_timeout=5
    )
    print("✅ Conexión exitosa a MySQL RDS")
    conn.close()
except Exception as e:
    print("❌ Error de conexión:", e)

