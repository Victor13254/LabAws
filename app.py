import json
import boto3
from datetime import datetime, timezone
import pymysql

s3 = boto3.client("s3", "us-east-1")

DB_HOST = "bancorep.ctb9mrigxgco.us-east-1.rds.amazonaws.com" 
DB_PORT = 3306 
DB_NAME = "bancorep" 
DB_USER = "admin" 
DB_PASSWORD = "admin123"

def _pairs_to_rows(pairs):
    rows = []
    for i, p in enumerate(pairs):
        try:
            ts_ms = int(p[0])
            valor = float(p[1])
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
            rows.append((dt, valor))
        except Exception as e:
            print(f"Fila inválida en índice {i}: {p} -> {e}")
    return rows
    
def _connect_mysql_server():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        connect_timeout=10,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.Cursor 
    )
    
def _ensure_database_and_table(conn):
    with conn.cursor() as cur:
        cur.execute(
        f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
        )
        cur.execute(f"USE `{DB_NAME}`;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dolar (
            fecha DATETIME PRIMARY KEY,
            valor DECIMAL(12,6) NOT NULL
        );
    """)
        conn.commit()
        
def h(event, context):
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    print(f"Nuevo archivo: s3://{bucket}/{key}")
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj['Body'].read()
    
    try:
        data = json.loads(raw)
    except Exception as e:
        print("Error parseando JSON:", e)
        return {"statusCode": 200, "body": "Archivo no es JSON válido"}
    
    rows = _pairs_to_rows(data)
    if not rows:
        print("No se encontraron datos válidos en el JSON.")
        return {"statusCode": 200, "body": "Sin pares (fecha, valor)"}
        
    conn = _connect_mysql_server()
    try:
        _ensure_database_and_table(conn)
        
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dolar (fecha, valor)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE valor = VALUES(valor)
                """,
                rows
            )
    
        conn.commit()
        msg = f"Insertados/actualizados: {len(rows)}"
        print(msg)
        return {"statusCode": 200, "body": msg}
    
    finally:
        conn.close()