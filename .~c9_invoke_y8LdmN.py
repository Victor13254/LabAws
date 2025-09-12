import json
import io
import pytest
from unittest.mock import patch
from app import f   # importa tu lambda (ajusta si tu archivo no se llama app.py)

@pytest.fixture
def evento_contexto():
    """Event/context mínimo"""
    return {}, {}

def test_f_guarda_en_s3(evento_contexto):
    event, context = evento_contexto

    datos_falsos = b'{"dolar": 4000.55}'
    respuesta_falsa = io.BytesIO(datos_falsos)

    # urlopen y put_object
    with patch("app.urllib.request.urlopen", return_value=respuesta_falsa):
        with patch("app.s3.put_object") as s3_simulado:
            
            resultado = f(event, context)

            # La lambda debe responder OK
            assert resultado["statusCode"] == 200
            assert json.loads(resultado["body"]) == "Guardado Correcto"

            # Y debe haber escrito el objeto en S3
            s3_simulado.assert_called_once()
            args, kwargs = s3_simulado.call_args

            assert kwargs["Bucket"] == "dolar-raw-2025"
            assert kwargs["ContentType"] == "application/json"
            assert kwargs["Body"] == datos_falsos
            assert kwargs["Key"].startswith("dolar-")
