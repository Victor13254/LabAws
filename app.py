import json
import boto3
import urllib.request
from datetime import datetime, timezone

#bd
import os
import pymysql

s3 = boto3.client("s3", "us-east-1")

def f(event, context):

    BUCKET = "dolar-raw-2025"
    
    now = datetime.now(timezone.utc)
    ft = now.strftime("%Y%m%dT%H%M%S")
    
    name = "dolar-" + ft + ".json"

    respuesta = urllib.request.urlopen("https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario")
    raw = respuesta.read()

    s3.put_object(
        Bucket=BUCKET,
        Key=name,
        Body=raw,
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Guardado Correcto')
    }

