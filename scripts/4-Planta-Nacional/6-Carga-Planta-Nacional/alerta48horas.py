import boto3
from datetime import datetime, timezone, timedelta

BUCKET     = "pbx-cdr-view"
PREFIX     = "input_emails/"
REGION     = "us-east-1"
SNS_ARN    = "arn:aws:sns:us-east-1:926162397524:alerta-reporte-webex-cdR"
HORAS      = 48


def get_ultimo_reporte(s3) -> tuple:
    """
    Retorna (fecha_str, ultimo_modificado) del ZIP más reciente en input_emails/
    """
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    
    zips = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(".zip")
    ]
    
    if not zips:
        return None, None
    
    ultimo = max(zips, key=lambda x: x["LastModified"])
    filename = ultimo["Key"].split("/")[-1]
    ultimo_modificado = ultimo["LastModified"]
    
    # Extraer fecha del nombre del archivo
    import re
    match = re.search(r'(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}\.zip$', filename)
    if match:
        fecha_str = match.group(1)
    else:
        fecha_str = ultimo_modificado.strftime("%Y-%m-%d")
    
    return fecha_str, ultimo_modificado


def lambda_handler(event, context):
    s3  = boto3.client("s3", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)
    
    fecha_str, ultimo_modificado = get_ultimo_reporte(s3)
    ahora = datetime.now(timezone.utc)
    
    if ultimo_modificado is None:
        # No hay ningún reporte
        mensaje = (
            "Alerta — Reporte de llamadas Movistar Webex no recibido\n\n"
            "No se ha recibido ningún reporte de llamadas Movistar Webex.\n\n"
            "Por favor verificar con Movistar el envío del reporte."
        )
        enviar = True
    else:
        horas_transcurridas = (ahora - ultimo_modificado).total_seconds() / 3600
        enviar = horas_transcurridas >= HORAS
        
        mensaje = (
            f"Alerta — Reporte de llamadas Movistar Webex no recibido\n\n"
            f"Han transcurrido 48 horas sin recibir un nuevo reporte de llamadas Movistar Webex.\n\n"
            f"Último reporte recibido: {fecha_str}\n\n"
            f"Por favor verificar con Movistar el envío del reporte."
        )
    
    if enviar:
        print("Enviando alerta SNS...")
        sns.publish(
            TopicArn=SNS_ARN,
            Subject="Alerta — Reporte de llamadas Movistar Webex no recibido",
            Message=mensaje
        )
        print("Alerta enviada.")
    else:
        print(f"Reporte recibido hace {horas_transcurridas:.1f} horas. No se envía alerta.")
    
    return {"statusCode": 200, "body": "OK"}