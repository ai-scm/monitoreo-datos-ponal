#!/usr/bin/env python3
"""
Validador de predicciones para AWS Lambda.

Adaptación del script original para ejecutarse en AWS Lambda con:
- Parámetros desde event dict (no CLI arguments)
- Almacenamiento de resultados en S3 (no filesystem local)
- Logging estructurado para CloudWatch
- Alertas automáticas por SNS
"""

import csv
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Configuración
BUCKET = os.environ.get("S3_BUCKET", "pon-prod-ml-iapol")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ROOT_PREFIX = "PrediccionMdP"
HORAS_ESPERADAS = {f"{hora:02d}00" for hora in range(24)}
UTC_MENOS_5 = timezone(timedelta(hours=-5))
ALERT_THRESHOLD = int(os.environ.get("ALERT_THRESHOLD", "50"))
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

# Configurar logging para CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Handler para CloudWatch (JSON estructurado)
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.now(UTC_MENOS_5).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "function": record.funcName,
        }
        # Incluir campos extra si existen
        if hasattr(record, "extra_data"):
            log_obj.update(record.extra_data)
        return json.dumps(log_obj)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)


# ============================================================================
# Funciones Utilitarias
# ============================================================================


def semana_actual():
    """Retorna el lunes actual en UTC-5, en formato YYYYMMDD."""
    hoy = datetime.now(UTC_MENOS_5).date()
    lunes = hoy - timedelta(days=hoy.weekday())
    return lunes.strftime("%Y%m%d")


def semana_pasada():
    """Retorna el lunes de hace 7 días en UTC-5, en formato YYYYMMDD."""
    hace_semana = datetime.now(UTC_MENOS_5).date() - timedelta(days=7)
    lunes_pasado = hace_semana - timedelta(days=hace_semana.weekday())
    return lunes_pasado.strftime("%Y%m%d")


def hoy():
    """Retorna hoy en formato YYYYMMDD."""
    return datetime.now(UTC_MENOS_5).date().strftime("%Y%m%d")


def validar_fecha(fecha_str):
    """Valida formato de fecha YYYYMMDD."""
    try:
        return datetime.strptime(fecha_str, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {fecha_str}. Use YYYYMMDD.") from exc


def conectar_s3():
    """Conecta a S3 y valida acceso al bucket."""
    logger.info("conectando_a_s3", extra={"extra_data": {"bucket": BUCKET, "region": REGION}})
    try:
        cliente = boto3.client("s3", region_name=REGION)
        cliente.head_bucket(Bucket=BUCKET)
        logger.info("s3_conexion_exitosa")
        return cliente
    except ClientError as exc:
        logger.error(
            "s3_conexion_fallida",
            extra={"extra_data": {"error": str(exc), "bucket": BUCKET}}
        )
        raise


def listar_carpetas(s3, prefix):
    """Lista solamente las carpetas hijas directas de un prefijo."""
    carpetas = []
    paginator = s3.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(
            Bucket=BUCKET,
            Prefix=prefix,
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                nombre = item["Prefix"].rstrip("/").split("/")[-1]
                if nombre:
                    carpetas.append(nombre)
    except ClientError as exc:
        logger.error(
            "error_listando_carpetas",
            extra={"extra_data": {"prefix": prefix, "error": str(exc)}}
        )
        raise

    return sorted(set(carpetas))


def listar_semanas(s3, fecha_desde, fecha_hasta=None):
    """Busca las semanas existentes dentro del intervalo solicitado."""
    anio = fecha_desde[:4]
    prefix = f"{ROOT_PREFIX}/{anio}/"
    semanas = []
    fecha_desde_obj = validar_fecha(fecha_desde)
    fecha_hasta_obj = validar_fecha(fecha_hasta) if fecha_hasta else None

    logger.info(
        "buscando_semanas",
        extra={"extra_data": {
            "desde": fecha_desde,
            "hasta": fecha_hasta or "ULTIMA_DISPONIBLE",
            "prefix": prefix
        }}
    )

    for nombre in listar_carpetas(s3, prefix):
        if len(nombre) != 8 or not nombre.isdigit():
            continue
        try:
            fecha_semana = validar_fecha(nombre)
            if (
                fecha_semana >= fecha_desde_obj
                and (fecha_hasta_obj is None or fecha_semana <= fecha_hasta_obj)
            ):
                semanas.append(nombre)
        except ValueError:
            continue

    logger.info("semanas_encontradas", extra={"extra_data": {"cantidad": len(semanas), "semanas": semanas}})
    return sorted(semanas)


def listar_siglas(s3, semana, sigla_filtro=None):
    """Lista SIGLAS para una semana específica."""
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/"
    siglas = listar_carpetas(s3, prefix)

    if sigla_filtro:
        sigla_filtro = sigla_filtro.upper()
        return [sigla_filtro] if sigla_filtro in siglas else []

    return siglas


def listar_unidades(s3, semana, sigla):
    """Lista unidades para una SIGLA específica."""
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/{sigla}/"
    return listar_carpetas(s3, prefix)


def patron_prediccion_unidad(unidad):
    """Genera patrón regex para archivos de predicción."""
    unidad_regex = re.escape(unidad)
    return re.compile(
        rf"^Prediccion_{unidad_regex}_"
        rf"(?P<fecha>\d{{8}})_(?P<hora>\d{{4}})\.geojson$"
    )


def validar_unidad(s3, semana, sigla, unidad):
    """
    Valida archivos de Predicción para una unidad.
    
    Retorna: (validos_count, [invalidos], [faltantes])
    """
    prefix = (
        f"{ROOT_PREFIX}/{semana[:4]}/{semana}/"
        f"{sigla}/{unidad}/Prediccion/"
    )
    patron_prediccion = patron_prediccion_unidad(unidad)

    validos_prediccion = 0
    invalidos = []
    cobertura = {"Prediccion": defaultdict(set)}
    paginator = s3.get_paginator("list_objects_v2")

    try:
        # Delimiter impide que S3 entregue objetos de Turno/ u otras subcarpetas
        for page in paginator.paginate(
            Bucket=BUCKET,
            Prefix=prefix,
            Delimiter="/",
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                ruta_relativa = key[len(prefix) :]

                # Defensa: garantizar que sea un hijo directo
                if not ruta_relativa or "/" in ruta_relativa:
                    continue

                filename = ruta_relativa

                # Ignorar archivos que no sean Prediccion
                if not filename.startswith("Prediccion_"):
                    continue

                match_prediccion = patron_prediccion.fullmatch(filename)
                match = match_prediccion

                if match:
                    fecha_archivo = match.group("fecha")
                    hora_archivo = match.group("hora")
                    tipo_archivo = "Prediccion"

                    try:
                        validar_fecha(fecha_archivo)
                        fecha_valida = True
                    except ValueError:
                        fecha_valida = False

                    if fecha_valida and hora_archivo in HORAS_ESPERADAS:
                        cobertura[tipo_archivo][fecha_archivo].add(hora_archivo)
                        validos_prediccion += 1
                        continue

                # Registrar como inválido
                if not match:
                    razon = (
                        "Nombre no cumple: "
                        f"Prediccion_{unidad}_"
                        "YYYYMMDD_HHMM.geojson"
                    )
                else:
                    razon = (
                        "La fecha no es válida o la hora no corresponde a "
                        "una hora completa entre 0000 y 2300"
                    )

                invalidos.append(
                    {
                        "semana": semana,
                        "sigla": sigla,
                        "unidad": unidad,
                        "archivo": filename,
                        "ruta_s3": key,
                        "tamaño_kb": round(obj["Size"] / 1024, 1),
                        "fecha_modificacion": obj["LastModified"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "tipo": "INVALIDO",
                        "razon": razon,
                    }
                )

    except ClientError as exc:
        logger.warning(
            "error_validando_unidad",
            extra={"extra_data": {
                "semana": semana,
                "sigla": sigla,
                "unidad": unidad,
                "error": str(exc)
            }}
        )
        raise

    # Verificar cobertura horaria (24 horas por día)
    inicio_semana = validar_fecha(semana).date()
    faltantes = []

    for desplazamiento in range(7):
        fecha = (inicio_semana + timedelta(days=desplazamiento)).strftime("%Y%m%d")

        for tipo_archivo in cobertura:
            horas_presentes = cobertura[tipo_archivo].get(fecha, set())

            if len(horas_presentes) < 24:
                horas_faltantes = sorted(
                    HORAS_ESPERADAS - horas_presentes
                )
                archivos_faltantes = [
                    f"Prediccion_{unidad}_{fecha}_{hora}.geojson"
                    for hora in horas_faltantes
                ]
                faltantes.append(
                    {
                        "semana": semana,
                        "sigla": sigla,
                        "unidad": unidad,
                        "fecha": fecha,
                        "tipo_archivo": tipo_archivo,
                        "horas_presentes": len(horas_presentes),
                        "cantidad_faltantes": len(horas_faltantes),
                        "horas_faltantes": ";".join(horas_faltantes),
                        "archivos_faltantes": ";".join(archivos_faltantes),
                        "ruta_s3": prefix,
                    }
                )

    return validos_prediccion, invalidos, faltantes


def guardar_csv_s3(s3, bucket, key, columnas, filas):
    """Guarda CSV en S3."""
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=columnas)
    writer.writeheader()
    writer.writerows(filas)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv; charset=utf-8-sig",
        )
        logger.info("csv_guardado_en_s3", extra={"extra_data": {"key": key, "filas": len(filas)}})
    except ClientError as exc:
        logger.error("error_guardando_csv", extra={"extra_data": {"key": key, "error": str(exc)}})
        raise


def etiqueta_rango(fecha_desde, fecha_hasta):
    """Construye etiqueta de rango de fechas."""
    inicio = validar_fecha(fecha_desde)
    fin = validar_fecha(fecha_hasta)

    if inicio.year == fin.year:
        return f"{inicio:%Y}_{inicio:%m%d}-{fin:%m%d}"

    return f"{inicio:%Y_%m%d}-{fin:%Y_%m%d}"


def enviar_alerta_sns(sns_client, topic_arn, subject, mensaje, severity="WARNING"):
    """Envía alerta por SNS."""
    if not topic_arn or not sns_client:
        logger.warning("SNS topic no configurado, alerta no enviada")
        return

    payload = {
        "severity": severity,
        "timestamp": datetime.now(UTC_MENOS_5).isoformat(),
        "subject": subject,
        "message": mensaje,
    }

    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=f"[{severity}] Validación Predicciones - {subject}",
            Message=json.dumps(payload, indent=2, ensure_ascii=False),
        )
        logger.info("alerta_sns_enviada", extra={"extra_data": {"severity": severity}})
    except ClientError as exc:
        logger.error("error_enviando_sns", extra={"extra_data": {"error": str(exc)}})


def lambda_handler(event, context):
    """
    Función principal de Lambda.

    Event expected format:
    {
        "desde": "20260901",    # Optional, default: semana_pasada()
        "hasta": "20260907",    # Optional
        "sigla": null,          # Optional, procesar todas si null
        "alert_threshold": 50   # Optional
    }
    """
    start_time = datetime.now(UTC_MENOS_5)
    execution_id = context.request_id if context else "manual-test"

    # Parsear parámetros de entrada
    desde = event.get("desde", semana_pasada())
    hasta = event.get("hasta")
    sigla_filtro = event.get("sigla")
    alert_threshold = int(event.get("alert_threshold", ALERT_THRESHOLD))

    logger.info(
        "lambda_iniciado",
        extra={"extra_data": {
            "execution_id": execution_id,
            "desde": desde,
            "hasta": hasta,
            "sigla_filtro": sigla_filtro,
            "alert_threshold": alert_threshold
        }}
    )

    # Validar fechas
    try:
        validar_fecha(desde)
        if hasta:
            validar_fecha(hasta)
            if validar_fecha(hasta) < validar_fecha(desde):
                raise ValueError("--hasta no puede ser anterior a --desde")
    except ValueError as exc:
        logger.error("fecha_invalida", extra={"extra_data": {"error": str(exc)}})
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(exc)})
        }

    # Conectar a S3
    try:
        s3 = conectar_s3()
        sns = boto3.client("sns", region_name=REGION) if SNS_TOPIC_ARN else None
    except Exception as exc:
        logger.error("init_fallido", extra={"extra_data": {"error": str(exc)}})
        # Enviar alerta CRITICAL
        mensaje = f"No se puede conectar a AWS: {str(exc)}"
        if sns and SNS_TOPIC_ARN:
            enviar_alerta_sns(sns, SNS_TOPIC_ARN, "CRITICAL: Init Failed", mensaje, "CRITICAL")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Conexión a servicios AWS falló"})
        }

    # Listar semanas
    try:
        semanas = listar_semanas(s3, desde, hasta)
        if not semanas:
            logger.warning("no_semanas_encontradas")
            return {
                "statusCode": 200,
                "body": json.dumps({"mensaje": "No se encontraron semanas para procesar"})
            }
    except Exception as exc:
        logger.error("listar_semanas_fallido", extra={"extra_data": {"error": str(exc)}})
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error listando semanas"})
        }

    # Preparar path en S3 para resultados
    fecha_reporte = datetime.now(UTC_MENOS_5)
    s3_prefix = f"ValidationReports/{fecha_reporte:%Y/%Y-%m-%d}/"
    fecha_final_efectiva = hasta or semanas[-1]
    rango = etiqueta_rango(desde, fecha_final_efectiva)

    # Validar todas las unidades
    todos_invalidos = []
    todos_faltantes = []
    unidades_con_error_formato = set()
    unidades_con_horas_faltantes = set()
    total_siglas = 0
    total_unidades = 0
    total_prediccion = 0

    logger.info(
        "iniciando_validacion",
        extra={"extra_data": {
            "semanas_a_procesar": len(semanas),
            "s3_prefix": s3_prefix
        }}
    )

    for semana in semanas:
        try:
            siglas = listar_siglas(s3, semana, sigla_filtro)
            logger.info("procesando_semana", extra={"extra_data": {
                "semana": semana,
                "siglas": len(siglas)
            }})

            for indice_sigla, sigla in enumerate(siglas, 1):
                try:
                    unidades = listar_unidades(s3, semana, sigla)
                    total_siglas += 1
                    invalidos_sigla = 0
                    faltantes_sigla = 0

                    for unidad in unidades:
                        try:
                            pred, invalidos, faltantes = validar_unidad(
                                s3, semana, sigla, unidad
                            )
                            total_unidades += 1
                            total_prediccion += pred
                            cantidad_invalidos = len(invalidos)
                            cantidad_faltantes = sum(
                                fila["cantidad_faltantes"] for fila in faltantes
                            )

                            if cantidad_invalidos or cantidad_faltantes:
                                invalidos_sigla += cantidad_invalidos
                                faltantes_sigla += cantidad_faltantes
                                todos_invalidos.extend(invalidos)
                                todos_faltantes.extend(faltantes)

                                clave = (semana, sigla, unidad)
                                if cantidad_invalidos:
                                    unidades_con_error_formato.add(clave)
                                if cantidad_faltantes:
                                    unidades_con_horas_faltantes.add(clave)

                        except Exception as exc:
                            logger.warning(
                                "error_validando_unidad",
                                extra={"extra_data": {
                                    "semana": semana,
                                    "sigla": sigla,
                                    "unidad": unidad,
                                    "error": str(exc)
                                }}
                            )
                            continue

                    logger.info(
                        "sigla_completada",
                        extra={"extra_data": {
                            "semana": semana,
                            "sigla": sigla,
                            "unidades": len(unidades),
                            "errores_formato": invalidos_sigla,
                            "horas_faltantes": faltantes_sigla
                        }}
                    )

                except Exception as exc:
                    logger.error(
                        "error_procesando_sigla",
                        extra={"extra_data": {
                            "semana": semana,
                            "sigla": sigla,
                            "error": str(exc)
                        }}
                    )
                    continue

        except Exception as exc:
            logger.error(
                "error_procesando_semana",
                extra={"extra_data": {
                    "semana": semana,
                    "error": str(exc)
                }}
            )
            continue

    # Guardar resultados en S3
    columnas_detalle = [
        "semana", "sigla", "unidad", "archivo", "ruta_s3",
        "tamaño_kb", "fecha_modificacion", "tipo", "razon",
    ]
    columnas_faltantes = [
        "semana", "sigla", "unidad", "fecha", "tipo_archivo",
        "horas_presentes", "cantidad_faltantes", "horas_faltantes",
        "archivos_faltantes", "ruta_s3",
    ]

    try:
        guardar_csv_s3(
            s3,
            BUCKET,
            f"{s3_prefix}errores_prediccion_rango_{rango}.csv",
            columnas_detalle,
            todos_invalidos
        )
        guardar_csv_s3(
            s3,
            BUCKET,
            f"{s3_prefix}horas_faltantes_rango_{rango}.csv",
            columnas_faltantes,
            todos_faltantes
        )
    except Exception as exc:
        logger.error("error_guardando_resultados", extra={"extra_data": {"error": str(exc)}})
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error guardando resultados en S3"})
        }

    # Construir resumen
    end_time = datetime.now(UTC_MENOS_5)
    execution_time = (end_time - start_time).total_seconds()

    summary = {
        "statusCode": 200,
        "timestamp": end_time.isoformat(),
        "execution_time_seconds": execution_time,
        "semanas_procesadas": len(semanas),
        "siglas_procesadas": total_siglas,
        "unidades_procesadas": total_unidades,
        "archivos_prediccion_validos": total_prediccion,
        "errores_formato": len(todos_invalidos),
        "unidades_con_error_formato": len(unidades_con_error_formato),
        "horas_prediccion_faltantes": sum(
            f["cantidad_faltantes"] for f in todos_faltantes
        ),
        "unidades_con_horas_faltantes": len(unidades_con_horas_faltantes),
        "s3_key_errores": f"{s3_prefix}errores_prediccion_rango_{rango}.csv",
        "s3_key_horas": f"{s3_prefix}horas_faltantes_rango_{rango}.csv",
    }

    logger.info(
        "validacion_completada",
        extra={"extra_data": summary}
    )

    # Enviar alertas si es necesario
    if len(todos_invalidos) > alert_threshold:
        mensaje = (
            f"Se detectaron {len(todos_invalidos)} archivos inválidos "
            f"(umbral: {alert_threshold})\n\n"
            f"Resumen:\n"
            f"- Semanas: {len(semanas)}\n"
            f"- SIGLAS: {total_siglas}\n"
            f"- Unidades: {total_unidades}\n"
            f"- Errores: {len(todos_invalidos)}\n"
            f"- Horas faltantes: {summary['horas_prediccion_faltantes']}\n\n"
            f"Reportes: {summary['s3_key_errores']}"
        )
        if sns and SNS_TOPIC_ARN:
            enviar_alerta_sns(
                sns,
                SNS_TOPIC_ARN,
                f"Observaciones detectadas ({len(todos_invalidos)} errores)",
                mensaje,
                "WARNING"
            )

    if len(todos_faltantes) > alert_threshold * 2:
        mensaje = (
            f"Se detectaron {summary['horas_prediccion_faltantes']} horas de predicción faltantes\n\n"
            f"Reportes: {summary['s3_key_horas']}"
        )
        if sns and SNS_TOPIC_ARN:
            enviar_alerta_sns(
                sns,
                SNS_TOPIC_ARN,
                f"Cobertura horaria afectada ({summary['horas_prediccion_faltantes']} horas)",
                mensaje,
                "WARNING"
            )

    return {
        "statusCode": 200,
        "body": json.dumps(summary, ensure_ascii=False, indent=2)
    }


if __name__ == "__main__":
    # Para testing local
    class MockContext:
        request_id = "local-test-123"

    event = {
        "desde": "20260901",
        "hasta": "20260907",
    }
    result = lambda_handler(event, MockContext())
    print(json.dumps(result, indent=2, ensure_ascii=False))
