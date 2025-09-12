# test_lambda.py
import json
from io import BytesIO
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import app as lf

# --------------------------
# Utilidades
# --------------------------
import io

class S3Mock:
    """Simula el cliente S3 devolviendo un objeto con Body tipo archivo."""
    def __init__(self, raw_bytes: bytes):
        self._raw = raw_bytes

    def get_object(self, Bucket, Key):
        return {
            "Body": io.BytesIO(self._raw),
            "ContentType": "application/json",
        }


class CursorMock:
    """Cursor mínimo para registrar las consultas ejecutadas en memoria."""
    def __init__(self, log: dict):
        self.log = log

    def execute(self, sql, *args, **kwargs):
        self.log.setdefault("execute_sql", []).append(str(sql))

    def executemany(self, sql, seq_of_params):
        self.log["executemany_sql"] = str(sql)
        self.log["executemany_params"] = list(seq_of_params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False  # no ocultar excepciones


class ConnMock:
    """Conexión fake para capturar cursor/commit/cierre."""
    def __init__(self):
        self.log = {}
        self._closed = False

    def cursor(self):
        return CursorMock(self.log)

    def commit(self):
        self.log["commit_called"] = True

    def close(self):
        self._closed = True

    @property
    def closed(self):
        return self._closed


def build_s3_event(bucket="test-bucket", key="path/to/file.json"):
    """Evento S3 (ObjectCreated) mínimo para disparar la Lambda."""
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key}
            }
        }]
    }

# ------------
# Pruebas
# ------------

def test_pairs_to_rows_ok_y_malos(capsys):
    datos = [
        ["1726012800000", "4015.2"],  # válido (ms desde epoch)
        ["oops", "x"],                # inválido
    ]
    filas = lf._pairs_to_rows(datos)

    # Solo debe quedar la fila válida
    assert len(filas) == 1
    dt, val = filas[0]
    # 1726012800000 ms -> 2024-09-11 00:00:00 UTC (naive en tu código)
    esperado = datetime.fromtimestamp(1726012800000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    assert dt == esperado
    assert val == 4015.2

    # Verifica que se reporta la fila inválida (mismo texto que emite tu app)
    out = capsys.readouterr().out
    assert "Fila inválida en índice 1" in out


def test_h_json_invalido_no_llama_bd(monkeypatch):
    # S3 entrega bytes que no son JSON
    monkeypatch.setattr(lf, "s3", S3Mock(b"{esto no es json}"))

    connect_spy = MagicMock()
    monkeypatch.setattr(lf, "_connect_mysql_server", connect_spy)

    event = build_s3_event()
    resp = lf.h(event, context=None)

    assert resp["statusCode"] == 200
    assert "Archivo no es JSON válido" in resp["body"]
    # No debe intentar abrir conexión a la BD
    connect_spy.assert_not_called()


def test_h_sin_pares(monkeypatch):
    # JSON válido pero vacío
    monkeypatch.setattr(lf, "s3", S3Mock(b"[]"))

    connect_spy = MagicMock()
    monkeypatch.setattr(lf, "_connect_mysql_server", connect_spy)

    event = build_s3_event()
    resp = lf.h(event, context=None)

    assert resp["statusCode"] == 200
    assert "Sin pares (fecha, valor)" in resp["body"]
    connect_spy.assert_not_called()


def test_h_flujo_feliz_inserta_y_commit(monkeypatch):
    # JSON con dos puntos válidos
    pares = [
        ["1726012800000", "4015.2"],
        ["1726099200000", "4020.7"],
    ]
    contenido = json.dumps(pares).encode("utf-8")
    monkeypatch.setattr(lf, "s3", S3Mock(contenido))

    # Conexión fake para ver qué queries y commits pasan
    conn_fake = ConnMock()
    monkeypatch.setattr(lf, "_connect_mysql_server", lambda: conn_fake)

    # Ejecutar handler
    event = build_s3_event(bucket="dolar-raw-2025", key="dolar-20240911T000000.json")
    resp = lf.h(event, context=None)

    # Respuesta OK
    assert resp["statusCode"] == 200
    assert "Insertados/actualizados: 2" in resp["body"]

    # Debe haberse creado/seleccionado DB y tabla (verificación general)
    ejecutadas = "\n".join(conn_fake.log.get("execute_sql", []))
    assert "CREATE DATABASE IF NOT EXISTS" in ejecutadas
    assert "USE `bancorep`" in ejecutadas  # ajusta si cambias DB_NAME
    assert "CREATE TABLE IF NOT EXISTS dolar" in ejecutadas

    # Verificar INSERT ... ON DUPLICATE
    assert "INSERT INTO dolar (fecha, valor)" in conn_fake.log.get("executemany_sql", "")

    # Parámetros de inserción (fechas y floats)
    params = conn_fake.log.get("executemany_params", [])
    assert len(params) == 2
    esperado1 = datetime.fromtimestamp(1726012800000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    esperado2 = datetime.fromtimestamp(1726099200000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    assert params[0][0] == esperado1 and isinstance(params[0][1], float)
    assert params[1][0] == esperado2 and isinstance(params[1][1], float)

    # Commit y cierre de conexión
    assert conn_fake.log.get("commit_called", False) is True
    assert conn_fake.closed is True
