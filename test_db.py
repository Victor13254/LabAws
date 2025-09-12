# test_lambda.py
import json
from io import BytesIO
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Ajusta este import al nombre real de tu archivo donde está la función h
# Por ejemplo, si el archivo es app.py, entonces: import app as lf
import app as lf


# --------------------------
# Helpers / Fakes para tests
# --------------------------
import io

class FakeS3:
    def __init__(self, payload: bytes):
        self.payload = payload

    def get_object(self, Bucket, Key):
        # boto3 retorna un dict con 'Body' = StreamingBody (file-like)
        return {
            "Body": io.BytesIO(self.payload),
            "ContentType": "application/json",
        }


class FakeCursor:
    def __init__(self, recorder: dict):
        self.recorder = recorder

    def execute(self, sql, *args, **kwargs):
        # Guardamos cualquier CREATE/USE que se ejecute
        self.recorder.setdefault("execute_sql", []).append(str(sql))

    def executemany(self, sql, seq_of_params):
        self.recorder["executemany_sql"] = str(sql)
        self.recorder["executemany_params"] = list(seq_of_params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False  # no suprime excepciones


class FakeConn:
    def __init__(self):
        self.recorder = {}
        self._closed = False

    def cursor(self):
        return FakeCursor(self.recorder)

    def commit(self):
        self.recorder["commit_called"] = True

    def close(self):
        self._closed = True

    @property
    def closed(self):
        return self._closed


def make_s3_event(bucket="test-bucket", key="path/to/file.json"):
    """Evento de S3 (ObjectCreated) mínimo válido para Lambda."""
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key}
            }
        }]
    }


# ------------
# Tests unitarios
# ------------

def test_pairs_to_rows_ok_y_malos(capsys):
    pairs = [
        ["1726012800000", "4015.2"],  # válido (ms desde epoch)
        ["oops", "x"],                # inválido
    ]
    rows = lf._pairs_to_rows(pairs)

    # Debe convertir la fila válida
    assert len(rows) == 1
    dt, val = rows[0]
    # 1726012800000 ms -> 2024-09-11 00:00:00 UTC (naive en tu código)
    expected_dt = datetime.fromtimestamp(1726012800000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    assert dt == expected_dt
    assert val == 4015.2

    # Debe reportar la fila inválida por stdout (no es estricto, pero verificamos que menciona índice)
    out = capsys.readouterr().out
    assert "Fila inválida en índice 1" in out


def test_h_json_invalido_no_llama_bd(monkeypatch):
    # Inyectamos S3 que devuelve bytes no JSON
    monkeypatch.setattr(lf, "s3", FakeS3(b"{esto no es json}"))

    connect_spy = MagicMock()
    monkeypatch.setattr(lf, "_connect_mysql_server", connect_spy)

    event = make_s3_event()
    resp = lf.h(event, context=None)

    assert resp["statusCode"] == 200
    assert "Archivo no es JSON válido" in resp["body"]
    # No debe intentar conectarse a la BD
    connect_spy.assert_not_called()


def test_h_sin_pares(monkeypatch):
    # JSON válido pero vacío
    monkeypatch.setattr(lf, "s3", FakeS3(b"[]"))

    connect_spy = MagicMock()
    monkeypatch.setattr(lf, "_connect_mysql_server", connect_spy)

    event = make_s3_event()
    resp = lf.h(event, context=None)

    assert resp["statusCode"] == 200
    assert "Sin pares (fecha, valor)" in resp["body"]
    connect_spy.assert_not_called()


def test_h_flujo_feliz_inserta_y_commit(monkeypatch):
    # Preparamos un JSON con dos pares válidos
    pairs = [
        ["1726012800000", "4015.2"],
        ["1726099200000", "4020.7"],
    ]
    payload = json.dumps(pairs).encode("utf-8")
    monkeypatch.setattr(lf, "s3", FakeS3(payload))

    # Conexión fake para capturar queries y commits
    fake_conn = FakeConn()
    monkeypatch.setattr(lf, "_connect_mysql_server", lambda: fake_conn)

    # Ejecutamos
    event = make_s3_event(bucket="dolar-raw-2025", key="dolar-20240911T000000.json")
    resp = lf.h(event, context=None)

    # Respuesta
    assert resp["statusCode"] == 200
    assert "Insertados/actualizados: 2" in resp["body"]

    # Validar que se llamaron CREATE/USE/CREATE TABLE (de forma general)
    exec_sql = "\n".join(fake_conn.recorder.get("execute_sql", []))
    assert "CREATE DATABASE IF NOT EXISTS" in exec_sql
    assert "USE `bancorep`" in exec_sql  # Ajusta si cambiaste DB_NAME
    assert "CREATE TABLE IF NOT EXISTS dolar" in exec_sql

    # Validar INSERT ... ON DUPLICATE
    assert "INSERT INTO dolar (fecha, valor)" in fake_conn.recorder.get("executemany_sql", "")

    # Validar parámetros de inserción (fechas correctas y floats)
    params = fake_conn.recorder.get("executemany_params", [])
    assert len(params) == 2
    # Comprobamos que las fechas sean las esperadas
    expected_dt1 = datetime.fromtimestamp(1726012800000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    expected_dt2 = datetime.fromtimestamp(1726099200000 / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    assert params[0][0] == expected_dt1 and isinstance(params[0][1], float)
    assert params[1][0] == expected_dt2 and isinstance(params[1][1], float)

    # Debe haberse hecho commit y cerrado la conexión
    assert fake_conn.recorder.get("commit_called", False) is True
    assert fake_conn.closed is True
