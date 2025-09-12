import json
import io
import pytest
from unittest.mock import patch, MagicMock
from app import f   # importa tu lambda (ajusta si el archivo se llama distinto)

@pytest.fixture
def mock_event_context():
    return {}, {}

def test_f_guardado_correcto(mock_event_context):
    event, context = mock_event_context

    # Simular respuesta de la URL
    fake_data = b'{"dolar": 4000.55}'
    mock_response = io.BytesIO(fake_data)

    with patch("app.urllib.request.urlopen", return_value=mock_response):
        with patch("app.s3.put_object") as mock_put_object:
            
            result = f(event, context)

            # Verifica que devuelve 200
            assert result["statusCode"] == 200
            assert json.loads(result["body"]) == "Guardado Correcto"

            # Verifica que se llamó put_object en S3
            mock_put_object.assert_called_once()
            args, kwargs = mock_put_object.call_args

            assert kwargs["Bucket"] == "dolar-raw-2025"
            assert kwargs["ContentType"] == "application/json"
            assert kwargs["Body"] == fake_data
            assert kwargs["Key"].startswith("dolar-")
