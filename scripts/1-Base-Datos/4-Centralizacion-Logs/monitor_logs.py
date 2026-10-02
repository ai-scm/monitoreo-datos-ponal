#!/usr/bin/env python3
"""
monitor_logs.py — Higiene de los log groups del pipeline de datos (Glue/DMS/Lambda).
====================================================================================
Corre DIARIO. En esta cuenta NO existe hoy un pipeline de centralizacion de logs
(0 subscription filters en /aws-glue/*, 0 CloudWatch Logs destinations, ningun
bucket/Firehose que reciba logs de Glue/DMS). Por eso este script NO verifica un
destino cruzado: hace **higiene de los log groups** y reporta la ausencia de
centralizacion como un bloque INFORMATIVO (no como FALLA).

Flujo (mismo esquema que los demas scripts):
  1. Identidad AWS (con que rol se ejecuta).
  2. Descubre log groups por PREFIJO (describe_log_groups, paginado completo) + una
     lista fija de Lambdas del pipeline, y evalua cada uno:
     - Actividad: ultimo evento dentro de la ventana (--horas, 24 por defecto).
     - Retencion: null (nunca expira) o < umbral minimo (30 d por defecto).
     - Tamano sin retencion: storedBytes por encima del umbral (5 GB por defecto).
  3. Errores + localizacion: para los grupos candidatos (Glue jobs/python-jobs error,
     crawlers, sessions, DMS tasks, lambdas del pipeline) con actividad reciente, corre
     Logs Insights (24h). Si hay lineas de error -> FALLA e IDENTIFICA el recurso:
     servicio, tipo (Job/Crawler/Task/Function/Session), nombre, ARN y deeplink. Para
     los Glue jobs ademas CLASIFICA la causa por regex (tabla con evidencia real); para
     los demas servicios solo identifica el recurso, sin inventar la causa.
  4. Cobertura de logging DMS: tareas con Logging.EnableLogging == false (AVISO).
  5. Centralizacion: cuenta subscription filters (informativo, AVISO).

Severidad:
  - FALLA: un grupo candidato con actividad reciente Y lineas de error reales.
  - AVISO: retencion fuera de rango, tamano sin retencion, sin actividad reciente,
    testconnection agregado, cobertura DMS, centralizacion.
Los testconnection/* se RESUMEN en una sola fila (son ruido).

Es de SOLO LECTURA (logs:DescribeLogGroups/LogStreams/SubscriptionFilters,
logs:StartQuery/GetQueryResults, dms:DescribeReplicationTasks, sts). NO escribe nada.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV (con columna
extra tipo_error_detectado). Exit code: 1 si hay FALLA, 0 si todo bien, 2 args.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_logs.py
    python3 monitor_logs.py --con-avisos   # muestra todos los avisos de higiene
    python3 monitor_logs.py --horas 48
    python3 monitor_logs.py --sin-insights # omite la clasificacion de errores
    python3 monitor_logs.py --no-export
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_HORAS = float(os.environ.get("LOGS_ACTIVITY_WINDOW_HOURS", "24"))
RETENCION_MIN_DIAS = int(os.environ.get("LOGS_MIN_RETENTION_DAYS", "30"))
TAMANO_WARN_GB = float(os.environ.get("LOGS_SIZE_WARN_GB", "5"))

# Parametros de CloudWatch Logs Insights (clasificacion de errores de Glue).
INSIGHTS_MAX_ESPERAS = int(os.environ.get("INSIGHTS_MAX_ESPERAS", "40"))
INSIGHTS_POLL_SEG = float(os.environ.get("INSIGHTS_POLL_SEG", "1.0"))

# --------------------------------------------------------------------------------------
# Descubrimiento DINAMICO por prefijo (nunca se cablea el conteo ni los nombres de los
# grupos; solo la lista de prefijos, que cambia mucho menos). describe_log_groups
# pagina completo en cada corrida.
# --------------------------------------------------------------------------------------
PREFIJOS = [
    "/aws-glue/jobs/",            # incluye el suelto pon-prod-expertopol-conversation-audit-glue
    "/aws-glue/python-jobs/",
    "/aws-glue/sessions/",
    "/aws-glue/crawlers",
    "/aws-glue/column-statistics",
    "/aws-glue/testconnection/",  # se agregan, se resumen en una sola fila
    "dms-tasks-",
]

# Lambdas del pipeline que hoy quedan invisibles. Lista EXPLICITA (no /aws/lambda/*,
# que son cientos e irrelevantes). Mismo tratamiento de higiene que el resto.
LAMBDAS_PIPELINE = [
    "pon-prod-Monitor-Glue-Jobs-DPU",
    "pon-prod-disec-launch-extract-crawler",
    "pon-prod-dms-rows-dynamo",
    "pon-prod-dms-update-trigger",
    "resumeFailedTasksDms",
    "pon-prod-tablero-alerta-delitos-athena-update",
    "glue-usage",
    "athenafederatedcatalog_athena_ih2iqmeq",
    "athenafederatedcatalog_pon_prod_dynamo_tables",
]
LAMBDA_LOG_PREFIX = "/aws/lambda/"

# Prefijo cuyos grupos se resumen en una sola fila (ruido).
PREFIJO_RESUMIR = "/aws-glue/testconnection/"

# Grupos de error de Glue sobre los que se clasifica el error por regex.
GRUPOS_ERROR = ("/aws-glue/jobs/error", "/aws-glue/python-jobs/error")


def _es_candidato_a_error(log_group):
    """
    Grupos donde tiene sentido buscar lineas de error e identificar el recurso.
    Incluye los */error de Glue + crawlers + sessions + DMS + lambdas del pipeline.
    Se excluyen los /output y logs-v2 (mucho ruido, no son logs de error).
    """
    if log_group in GRUPOS_ERROR:
        return True
    if log_group.startswith("/aws-glue/crawlers"):
        return True
    if log_group.startswith("/aws-glue/sessions"):
        return True
    if log_group.startswith("dms-tasks-"):
        return True
    if log_group.startswith(LAMBDA_LOG_PREFIX):
        return True
    return False

# --------------------------------------------------------------------------------------
# Tabla de clasificacion de errores (regex -> causa + accion). Validada con evidencia
# real de la cuenta. El orden importa: los patrones mas especificos primero.
# --------------------------------------------------------------------------------------
PATRONES_ERROR = [
    (re.compile(r"ORA-17002"),
     ("Oracle cerro conexion en el handshake JDBC",
      "Reintentar con backoff; revisar listener/VPN Oracle.")),
    (re.compile(r"TASK_WRITE_FAILED"),
     ("Spark envuelve un fallo de escritura (normalmente AccessDenied de S3)",
      "Mirar el 'Caused by' anidado.")),
    (re.compile(r"s3:(PutObject|ListBucket).*AccessDenied"),
     ("Rol IAM sin permisos S3 sobre el bucket destino",
      "Revisar politica IAM del rol del job.")),
    (re.compile(r"athena:StartQueryExecution.*AccessDeniedException"),
     ("Rol IAM sin permiso de Athena",
      "Otorgar athena:StartQueryExecution al rol.")),
    (re.compile(r"InvalidRequestException.*mismatched input"),
     ("SQL mal formado hacia Athena",
      "Revisar generacion de query en el script origen.")),
    (re.compile(r"ConnectException: Connection refused.*GlueBootstrap"),
     ("Fallo de red executor->driver, infra transitoria",
      "Reintentar; vigilar si aumenta en frecuencia.")),
    (re.compile(r"argparse\.ArgumentError.*conflicting option"),
     ("Bug de codigo: argumento choca con uno que Glue inyecta",
      "Fix de codigo, renombrar argumento.")),
    (re.compile(r"RuntimeError: \d+ SIVICC table load\(s\) failed"),
     ("Contador agregado; la causa real no esta en esta linea",
      "Correlacionar por timestamp con /aws-glue/jobs/error.")),
    (re.compile(r"IndexError: index 0 is out of bounds"),
     ("DataFrame vacio sin guard",
      "Agregar validacion antes de .values[0].")),
    (re.compile(r"TypeError:.*missing 1 required positional argument: 'operation_name'"),
     ("Bug en el manejador de excepciones; enmascara el error real",
      "Revisar bloque 'During handling of...'.")),
    # Generico Oracle (menos especifico que ORA-17002) va al final.
    (re.compile(r"ORA-\d{5}"),
     ("Otro error Oracle",
      "Buscar codigo especifico; escalar a DBA si se repite.")),
]

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
ICON = {OK: "[ OK  ]", WARN: "[AVISO]", FAIL: "[FALLA]"}

RESULTS = []
CON_AVISOS = False
VERBOSE = False
SIN_INSIGHTS = False
ACCOUNT_ID = ""  # se rellena en check_identity(); se usa para armar ARNs/deeplinks

_SPIN = "|/-\\"
_spin_i = 0

_GB = 1024 ** 3


def paso(texto, fin=False):
    global _spin_i
    if VERBOSE:
        if fin:
            print(f"  [OK] {texto}")
        return
    if fin:
        print(f"\r  \033[32m✓\033[0m {texto}".ljust(72), flush=True)
    else:
        c = _SPIN[_spin_i % len(_SPIN)]
        _spin_i += 1
        print(f"\r  {c} {texto}".ljust(72), end="", flush=True)


def record(recurso, estado, mensaje="", ubicacion="", accion="", tipo_error="",
           origen="", servicio="", tipo_recurso="", recurso_nombre=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "mensaje": mensaje, "accion_sugerida": accion if estado != OK else "",
        "tipo_error_detectado": tipo_error, "servicio": servicio,
        "tipo_recurso": tipo_recurso, "recurso_nombre": recurso_nombre, "origen": origen,
    })
    if not VERBOSE:
        return
    print(f"  {ICON[estado]} {recurso}")
    if mensaje:
        print(f"           {mensaje}")


def _err(exc):
    if isinstance(exc, ClientError):
        e = exc.response.get("Error", {})
        return e.get("Code", "ClientError"), e.get("Message", "")
    return type(exc).__name__, str(exc)


def _err_detail(exc):
    code, msg = _err(exc)
    return f"{code}: {msg}" if msg else code


def _fmt_bytes(n):
    if not n:
        return "0 B"
    gb = n / _GB
    if gb >= 1:
        return f"{gb:.2f} GB"
    mb = n / (1024 ** 2)
    return f"{mb:.1f} MB"


def _limpiar(texto):
    return " ".join((texto or "").split())[:200]


# --------------------------------------------------------------------------------------
# Identidad
# --------------------------------------------------------------------------------------
def check_identity():
    global ACCOUNT_ID
    paso("Identidad AWS...")
    try:
        ident = aws_client("sts").get_caller_identity()
        ACCOUNT_ID = ident["Account"]
        record("Identidad AWS", OK,
               mensaje=f"Cuenta {ident['Account']}.",
               ubicacion=f"{ident['Arn']} (region {REGION})")
        paso(f"Identidad AWS  (cuenta {ident['Account']})", fin=True)
        return True
    except (ClientError, BotoCoreError) as exc:
        record("Identidad AWS", FAIL,
               mensaje=f"No se pudo obtener la identidad: {_err_detail(exc)}",
               ubicacion=f"STS region {REGION}",
               accion="Verifica credenciales de CloudShell.")
        paso("Identidad AWS  (FALLA)", fin=True)
        return False


# --------------------------------------------------------------------------------------
# Descubrimiento de log groups por prefijo + lambdas del pipeline
# --------------------------------------------------------------------------------------
def _listar_por_prefijo(logs, prefijo):
    grupos = []
    token = None
    while True:
        params = {"logGroupNamePrefix": prefijo, "limit": 50}
        if token:
            params["nextToken"] = token
        resp = logs.describe_log_groups(**params)
        grupos.extend(resp.get("logGroups", []))
        token = resp.get("nextToken")
        if not token:
            break
    return grupos


def descubrir_log_groups(logs):
    vistos = {}
    for pref in PREFIJOS:
        paso(f"Descubriendo log groups... {pref}")
        try:
            for g in _listar_por_prefijo(logs, pref):
                vistos[g["logGroupName"]] = g  # dedup por nombre
        except Exception as exc:  # noqa: BLE001
            code, _ = _err(exc)
            accion = ("Falta logs:DescribeLogGroups." if code in ("AccessDeniedException", "AccessDenied")
                      else "Revisa permisos/region de CloudWatch Logs.")
            record(f"Descubrimiento '{pref}'", FAIL,
                   mensaje=f"No se pudieron listar log groups: {_err_detail(exc)}",
                   ubicacion=f"CloudWatch Logs (region {REGION})", accion=accion)

    # Lambdas del pipeline: lista explicita. Cada una es un log group /aws/lambda/<nombre>.
    paso("Descubriendo log groups... lambdas del pipeline")
    for nombre_lambda in LAMBDAS_PIPELINE:
        lg_name = f"{LAMBDA_LOG_PREFIX}{nombre_lambda}"
        if lg_name in vistos:
            continue
        try:
            resp = logs.describe_log_groups(logGroupNamePrefix=lg_name, limit=5)
            encontrado = next((g for g in resp.get("logGroups", [])
                               if g["logGroupName"] == lg_name), None)
            if encontrado:
                vistos[lg_name] = encontrado
            else:
                # La lambda existe pero su log group aun no se creo (nunca invocada).
                record(f"{lg_name}", WARN,
                       mensaje="Lambda del pipeline sin log group (¿nunca invocada?).",
                       ubicacion=f"CloudWatch Logs '{lg_name}'",
                       accion="Verifica si la lambda deberia estar corriendo.")
        except Exception:  # noqa: BLE001
            continue
    return list(vistos.values())


# --------------------------------------------------------------------------------------
# Clasificacion de errores (Logs Insights sobre los grupos */error)
# --------------------------------------------------------------------------------------
def _run_insights(logs, log_group, query, start_ms, end_ms):
    resp = logs.start_query(
        logGroupName=log_group,
        startTime=int(start_ms / 1000), endTime=int(end_ms / 1000),
        queryString=query, limit=50,
    )
    qid = resp["queryId"]
    for _ in range(INSIGHTS_MAX_ESPERAS):
        res = logs.get_query_results(queryId=qid)
        st = res.get("status")
        if st in ("Complete", "Failed", "Cancelled", "Timeout"):
            if st != "Complete":
                return []
            return [{c["field"]: c["value"] for c in row} for row in res.get("results", [])]
        time.sleep(INSIGHTS_POLL_SEG)
    try:
        logs.stop_query(queryId=qid)
    except Exception:  # noqa: BLE001
        pass
    return []


# Extraccion de identificadores desde el log (JobName/JobRunId) para localizar la falla.
_JOBNAME_RE = re.compile(r'"JobName"\s*:\s*"([^"]+)"')
_JOBRUNID_RE = re.compile(r'"JobRunId"\s*:\s*"(jr_[0-9a-f]+)"')
_JR_STREAM_RE = re.compile(r"(jr_[0-9a-f]+)")

# Grupos cuyo error SI clasificamos por regex (solo donde hay evidencia real).
GRUPOS_CLASIFICABLES = ("/aws-glue/jobs/error", "/aws-glue/python-jobs/error")


def _identificar_recurso(log_group, stream, job_name="", job_run_id=""):
    """
    A partir del log group (+ stream + datos del payload) deduce de QUE recurso viene
    el error: servicio, tipo, nombre, ARN y deeplink a la consola.
    Soporta Glue (jobs/python-jobs/crawlers/sessions), DMS y Lambda. Para lo que no
    reconoce, devuelve un servicio generico sin inventar nombre.
    Devuelve dict: servicio, tipo, nombre, arn, deeplink, origen (texto legible).
    """
    servicio = "Desconocido"
    tipo = ""
    nombre = ""
    arn = ""
    deeplink = ""

    # ---- Glue jobs (ETL / Python Shell): nombre del payload, run del payload/stream.
    if log_group in ("/aws-glue/jobs/error", "/aws-glue/jobs/output", "/aws-glue/logs-v2"):
        servicio, tipo = "AWS Glue", "Job (ETL/Spark)"
        nombre = job_name
        if nombre and ACCOUNT_ID:
            arn = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/{nombre}"
        if nombre and job_run_id:
            deeplink = (f"https://{REGION}.console.aws.amazon.com/gluestudio/home?region={REGION}"
                        f"#/job/{nombre}/run/{job_run_id}")
    elif log_group == "/aws-glue/python-jobs/error":
        servicio, tipo = "AWS Glue", "Job (Python Shell)"
        nombre = job_name
        if nombre and ACCOUNT_ID:
            arn = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/{nombre}"
        if nombre and job_run_id:
            deeplink = (f"https://{REGION}.console.aws.amazon.com/gluestudio/home?region={REGION}"
                        f"#/job/{nombre}/run/{job_run_id}")

    # ---- Glue crawlers: el @logStream es el nombre del crawler.
    elif log_group.startswith("/aws-glue/crawlers"):
        servicio, tipo = "AWS Glue", "Crawler"
        nombre = stream or ""
        if nombre and ACCOUNT_ID:
            arn = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:crawler/{nombre}"
        if nombre:
            deeplink = (f"https://{REGION}.console.aws.amazon.com/glue/home?region={REGION}"
                        f"#/v2/data-catalog/crawlers/view/{nombre}")

    # ---- Glue interactive sessions: el stream suele ser el session id.
    elif log_group.startswith("/aws-glue/sessions"):
        servicio, tipo = "AWS Glue", "Interactive Session"
        nombre = stream or ""
        if nombre and ACCOUNT_ID:
            arn = f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:session/{nombre}"

    # ---- DMS: el nombre de la tarea esta en el log group dms-tasks-<...>.
    #      OJO: el ARN real de DMS usa un resource-id interno, no el nombre; aqui NO
    #      armamos ARN para no dar uno falso. El nombre del log group basta para ubicarla.
    elif log_group.startswith("dms-tasks-"):
        servicio, tipo = "AWS DMS", "Replication task (log group)"
        nombre = log_group[len("dms-tasks-"):] or stream or ""

    # ---- Lambda: el nombre esta en el propio log group /aws/lambda/<nombre>.
    elif log_group.startswith(LAMBDA_LOG_PREFIX):
        servicio, tipo = "AWS Lambda", "Function"
        nombre = log_group[len(LAMBDA_LOG_PREFIX):]
        if nombre and ACCOUNT_ID:
            arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{nombre}"
        if nombre:
            deeplink = (f"https://{REGION}.console.aws.amazon.com/lambda/home?region={REGION}"
                        f"#/functions/{nombre}")

    # Texto legible "origen".
    partes = [f"servicio {servicio}"]
    if tipo:
        partes.append(f"tipo {tipo}")
    if nombre:
        partes.append(f"nombre {nombre}")
    if job_run_id:
        partes.append(f"run {job_run_id}")
    if arn:
        partes.append(f"ARN {arn}")
    return {"servicio": servicio, "tipo_recurso": tipo, "nombre": nombre, "arn": arn,
            "deeplink": deeplink, "origen": " | ".join(partes)}


def clasificar_errores(logs, log_group, start_ms, end_ms):
    """
    Lee las lineas de error recientes de CUALQUIER grupo con actividad, identifica el
    recurso del que vienen (servicio/tipo/nombre/ARN) y, SOLO para los grupos de Glue
    jobs (GRUPOS_CLASIFICABLES, donde hay evidencia real), clasifica la causa por regex.
    Para los demas servicios identifica el recurso pero NO inventa causa.
    Devuelve dict (tipo_causa, accion, ejemplo, servicio, tipo, nombre, arn, deeplink,
    origen) o None si no hay lineas de error en la ventana.
    """
    query = (
        "fields @message, @logStream\n"
        "| filter @message like /ORA-/ or @message like /Error/ or @message like /Exception/ "
        "or @message like /AccessDenied/ or @message like /FAILED/ or @message like /Traceback/ "
        "or @message like /JobName/\n"
        "| sort @timestamp desc\n"
        "| limit 50"
    )
    try:
        filas = _run_insights(logs, log_group, query, start_ms, end_ms)
    except Exception:  # noqa: BLE001
        return None

    if not filas:
        return None

    # 1) Datos para identificar el recurso: JobName/JobRunId del payload y el stream.
    job_name = ""
    job_run_id = ""
    stream_rep = ""
    for f in filas:
        msg = f.get("@message", "")
        stream = f.get("@logStream", "")
        if not stream_rep and stream:
            stream_rep = stream
        if not job_name:
            m = _JOBNAME_RE.search(msg)
            if m:
                job_name = m.group(1)
        if not job_run_id:
            m = _JOBRUNID_RE.search(msg)
            if m:
                job_run_id = m.group(1)
        if not job_run_id and stream:
            m = _JR_STREAM_RE.search(stream)
            if m:
                job_run_id = m.group(1)
        if job_name and job_run_id:
            break

    loc = _identificar_recurso(log_group, stream_rep, job_name, job_run_id)
    clasificable = log_group in GRUPOS_CLASIFICABLES

    # 2) Si el grupo es clasificable, intentar casar un patron conocido.
    if clasificable:
        for f in filas:
            msg = f.get("@message", "")
            for rx, (causa, accion) in PATRONES_ERROR:
                if rx.search(msg):
                    return {"tipo": causa, "accion": accion, "ejemplo": _limpiar(msg), **loc}
        # Hay error pero ningun patron casa: no inventar causa.
        return {"tipo": "Error no catalogado — revisar manualmente",
                "accion": "Revisa el log crudo; agrega el patron a PATRONES_ERROR si se repite.",
                "ejemplo": _limpiar(filas[0].get("@message", "")), **loc}

    # 3) Grupo NO clasificable (crawlers/DMS/sessions/lambda): identificamos el recurso
    #    y marcamos actividad de error, pero sin clasificar la causa (no hay evidencia).
    return {"tipo": f"Error en {loc['servicio']} (sin clasificacion para este servicio)",
            "accion": "Identificado el recurso; revisa el log para la causa exacta.",
            "ejemplo": _limpiar(filas[0].get("@message", "")), **loc}


# --------------------------------------------------------------------------------------
# Higiene de cada log group
# --------------------------------------------------------------------------------------
def _evaluar_grupo(logs, g, ventana_ini_ms, ahora_ms):
    """
    Evalua higiene de un grupo. Para los grupos */error con actividad reciente, corre
    la clasificacion de errores: si detecta un patron real -> FALLA; si no, el estado
    sigue la logica de higiene (AVISO/OK).
    Devuelve el estado asignado.
    """
    nombre = g["logGroupName"]
    retencion = g.get("retentionInDays")          # None = nunca expira
    stored = g.get("storedBytes", 0)
    candidato_error = _es_candidato_a_error(nombre)

    # Actividad: ultimo evento del stream mas reciente.
    last_event_ms = None
    try:
        resp = logs.describe_log_streams(
            logGroupName=nombre, orderBy="LastEventTime", descending=True, limit=1)
        streams = resp.get("logStreams", [])
        if streams:
            last_event_ms = streams[0].get("lastEventTimestamp")
    except Exception:  # noqa: BLE001
        last_event_ms = None

    actividad_reciente = last_event_ms is not None and last_event_ms >= ventana_ini_ms

    problemas = []
    accion = []

    # 1) Actividad reciente (AVISO; un grupo sin actividad no es necesariamente roto).
    if last_event_ms is None:
        problemas.append("sin eventos / sin streams")
    elif last_event_ms < ventana_ini_ms:
        edad_h = (ahora_ms - last_event_ms) / 3_600_000
        problemas.append(f"ultimo evento hace {edad_h:.0f} h")
        accion.append("Verifica si la fuente dejo de escribir (puede que el job/tarea no corrio).")

    # 2) Retencion (AVISO).
    if retencion is None:
        problemas.append("retencion: nunca expira")
        accion.append(f"Configura retencion (>= {RETENCION_MIN_DIAS} d).")
    elif retencion < RETENCION_MIN_DIAS:
        problemas.append(f"retencion {retencion} d (< {RETENCION_MIN_DIAS})")
        accion.append(f"Sube la retencion a >= {RETENCION_MIN_DIAS} d.")

    # 3) Tamano sin retencion (AVISO).
    if retencion is None and stored > TAMANO_WARN_GB * _GB:
        problemas.append(f"tamano {_fmt_bytes(stored)} sin retencion")
        accion.append("Configura retencion para acotar el crecimiento/costo.")

    # 4) Candidatos a error con actividad reciente: identificar recurso + (Glue) clasificar.
    #    Solo marca FALLA si Insights encontro lineas de error (error real).
    if candidato_error and actividad_reciente and not SIN_INSIGHTS:
        info_err = clasificar_errores(logs, nombre, ventana_ini_ms, ahora_ms)
        if info_err:
            tipo = info_err["tipo"]
            ejemplo = info_err["ejemplo"]
            origen = info_err["origen"]
            deeplink = info_err["deeplink"]
            servicio = info_err["servicio"]
            tipo_rec = info_err.get("tipo_recurso", "")
            nombre_rec = info_err["nombre"]
            no_catalogado = tipo.startswith("Error no catalogado")

            # Ubicacion: si identificamos el recurso, apuntamos ahi; si no, al log group.
            ubicacion = origen if nombre_rec else f"CloudWatch Logs '{nombre}'"

            # Mensaje con servicio/tipo/nombre al frente para que salte a la vista.
            cab = "Error no catalogado" if no_catalogado else f"Tipo error: {tipo}"
            partes_msg = [f"{cab}."]
            svc_txt = f"{servicio} ({tipo_rec})" if tipo_rec else servicio
            partes_msg.append(f"Servicio: {svc_txt}.")
            if nombre_rec:
                partes_msg.append(f"Nombre: {nombre_rec}.")
            else:
                partes_msg.append("Nombre: no se pudo extraer del log.")
            if deeplink:
                partes_msg.append(f"Consola: {deeplink}")
            if ejemplo:
                partes_msg.append(f"Log: {ejemplo}")

            accion_final = info_err["accion"]
            if deeplink:
                accion_final = f"{accion_final} Abre el recurso: {deeplink}"

            record(nombre, FAIL, mensaje=" ".join(partes_msg),
                   ubicacion=ubicacion, accion=accion_final,
                   tipo_error=(tipo if not no_catalogado else "no catalogado"),
                   origen=origen, servicio=servicio, tipo_recurso=tipo_rec,
                   recurso_nombre=nombre_rec)
            return FAIL
        # Hay actividad pero Insights no devolvio lineas de error -> sigue higiene.

    if problemas:
        record(nombre, WARN,
               mensaje=" | ".join(problemas) + f" | tamano {_fmt_bytes(stored)}",
               ubicacion=f"CloudWatch Logs '{nombre}'",
               accion=" ".join(dict.fromkeys(accion)))
        return WARN
    record(nombre, OK,
           mensaje=f"Activo, retencion {retencion} d, tamano {_fmt_bytes(stored)}.",
           ubicacion=f"CloudWatch Logs '{nombre}'")
    return OK


def check_higiene(logs, grupos, horas):
    now = datetime.now(timezone.utc)
    ahora_ms = int(now.timestamp() * 1000)
    ventana_ini_ms = int((now - timedelta(hours=horas)).timestamp() * 1000)

    # Separar los testconnection (se resumen) del resto (uno por uno).
    testconn = [g for g in grupos if g["logGroupName"].startswith(PREFIJO_RESUMIR)]
    resto = [g for g in grupos if not g["logGroupName"].startswith(PREFIJO_RESUMIR)]

    total = len(resto)
    for i, g in enumerate(resto, start=1):
        paso(f"Higiene log groups... ({i}/{total}) {g['logGroupName']}")
        _evaluar_grupo(logs, g, ventana_ini_ms, ahora_ms)

    # Resumen de testconnection/* en una sola fila.
    if testconn:
        paso(f"Resumiendo testconnection ({len(testconn)})...")
        sin_ret = sum(1 for g in testconn if g.get("retentionInDays") is None)
        corta_ret = sum(1 for g in testconn
                        if g.get("retentionInDays") is not None and g["retentionInDays"] < RETENCION_MIN_DIAS)
        total_bytes = sum(g.get("storedBytes", 0) for g in testconn)
        fuera = sin_ret + corta_ret
        if fuera:
            record(f"testconnection ({len(testconn)} grupos)", WARN,
                   mensaje=(f"{sin_ret} sin retencion, {corta_ret} con retencion < {RETENCION_MIN_DIAS} d. "
                            f"Tamano total {_fmt_bytes(total_bytes)}."),
                   ubicacion=f"CloudWatch Logs '{PREFIJO_RESUMIR}*'",
                   accion=f"Aplica retencion (>= {RETENCION_MIN_DIAS} d) a los grupos testconnection.")
        else:
            record(f"testconnection ({len(testconn)} grupos)", OK,
                   mensaje=f"Todos con retencion >= {RETENCION_MIN_DIAS} d. Tamano total {_fmt_bytes(total_bytes)}.",
                   ubicacion=f"CloudWatch Logs '{PREFIJO_RESUMIR}*'")

    n_prob = sum(1 for r in RESULTS if r["estado"] != OK and r["recurso"] != "Identidad AWS")
    paso(f"Higiene  ({total} grupos + {len(testconn)} testconnection, {n_prob} aviso/falla)", fin=True)


# --------------------------------------------------------------------------------------
# Cobertura de logging DMS
# --------------------------------------------------------------------------------------
def check_dms_logging():
    dms = aws_client("dms")
    paso("Cobertura de logging DMS...")
    try:
        tasks = []
        marker = None
        while True:
            params = {"Marker": marker} if marker else {}
            resp = dms.describe_replication_tasks(**params)
            tasks.extend(resp.get("ReplicationTasks", []))
            marker = resp.get("Marker")
            if not marker:
                break
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        accion = ("Falta dms:DescribeReplicationTasks." if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa permisos/region de DMS.")
        record("Cobertura logging DMS", WARN,
               mensaje=f"No se pudieron listar tareas DMS: {_err_detail(exc)}",
               ubicacion=f"DMS (region {REGION})", accion=accion)
        paso("Cobertura logging DMS  (no evaluada)", fin=True)
        return

    total = len(tasks)
    sin_log = []
    for t in tasks:
        settings = t.get("ReplicationTaskSettings", "") or ""
        enabled = '"EnableLogging":true' in settings.replace(" ", "")
        if not enabled:
            sin_log.append(t.get("ReplicationTaskIdentifier", "?"))

    if total == 0:
        record("Cobertura logging DMS", OK,
               mensaje="No hay tareas DMS en la cuenta.", ubicacion=f"DMS (region {REGION})")
    elif sin_log:
        pct = 100.0 * len(sin_log) / total
        ejemplos = ", ".join(sin_log[:5]) + (" ..." if len(sin_log) > 5 else "")
        record("Cobertura logging DMS", WARN,
               mensaje=(f"{len(sin_log)}/{total} tareas ({pct:.0f}%) sin CloudWatch logging "
                        f"(EnableLogging=false). Ej: {ejemplos}"),
               ubicacion=f"DMS (region {REGION})",
               accion="Activa Logging.EnableLogging en esas tareas para tener trazabilidad.")
    else:
        record("Cobertura logging DMS", OK,
               mensaje=f"Las {total} tareas DMS tienen CloudWatch logging activado.",
               ubicacion=f"DMS (region {REGION})")
    paso(f"Cobertura logging DMS  ({total} tareas, {len(sin_log)} sin logging)", fin=True)


# --------------------------------------------------------------------------------------
# Estado de la centralizacion (informativo)
# --------------------------------------------------------------------------------------
def check_centralizacion(logs, grupos):
    paso("Centralizacion (subscription filters)...")
    con_filtro = 0
    revisados = 0
    for g in grupos[:60]:
        revisados += 1
        try:
            resp = logs.describe_subscription_filters(logGroupName=g["logGroupName"], limit=1)
            if resp.get("subscriptionFilters"):
                con_filtro += 1
        except Exception:  # noqa: BLE001
            continue

    if con_filtro == 0:
        record("Centralizacion de logs", WARN,
               mensaje=(f"No hay pipeline de centralizacion: 0 subscription filters en "
                        f"{revisados} grupos revisados. Los logs quedan solo en CloudWatch."),
               ubicacion=f"CloudWatch Logs (region {REGION})",
               accion=("Si el negocio espera centralizar (S3/OpenSearch/Firehose), es una "
                       "brecha de arquitectura a resolver aparte; este monitor solo la señala."))
    else:
        record("Centralizacion de logs", OK,
               mensaje=f"{con_filtro}/{revisados} grupos revisados tienen subscription filter.",
               ubicacion=f"CloudWatch Logs (region {REGION})")
    paso("Centralizacion  (informativo)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — HIGIENE DE LOGS (pipeline Glue/DMS/Lambda)")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso | {fail} falla")

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO (errores en logs):" if not CON_AVISOS else "Detalle:"
        print(f"\n  {titulo}")
        for nivel in niveles:
            for r in RESULTS:
                if r["estado"] == nivel:
                    print(f"\n  {ICON[nivel]} {r['recurso']}")
                    print(f"       que pasa : {r['mensaje']}")
                    if r.get("tipo_error_detectado"):
                        print(f"       tipo     : {r['tipo_error_detectado']}")
                    if r.get("servicio"):
                        svc = r["servicio"]
                        if r.get("tipo_recurso"):
                            svc += f" ({r['tipo_recurso']})"
                        print(f"       servicio : {svc}")
                    if r.get("recurso_nombre"):
                        print(f"       nombre   : {r['recurso_nombre']}")
                    if r.get("origen"):
                        print(f"       origen   : {r['origen']}")
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Log groups con buena higiene y sin errores detectados.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"logs_{ts}.csv")
    cols = ["recurso", "ubicacion", "estado", "mensaje", "accion_sugerida",
            "tipo_error_detectado", "servicio", "tipo_recurso", "recurso_nombre", "origen"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(RESULTS)
    print(f"\nReporte CSV: {csv_path}")
    home = os.path.expanduser("~")
    if os.path.commonpath([csv_path, home]) == home:
        print(f"Descarga (CloudShell Actions -> Download file): {os.path.relpath(csv_path, home)}")


def main():
    parser = argparse.ArgumentParser(
        description="Higiene de los log groups del pipeline Glue/DMS/Lambda + clasificacion de errores Glue.")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana de actividad en horas (default {DEFAULT_HORAS:g}; env LOGS_ACTIVITY_WINDOW_HOURS).")
    parser.add_argument("--sin-insights", action="store_true",
                        help="Omite la clasificacion de errores por Logs Insights (solo higiene).")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien los AVISOS de higiene (retencion, actividad, tamano, DMS, centralizacion).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE, SIN_INSIGHTS
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado
    SIN_INSIGHTS = args.sin_insights

    print("=" * 78)
    print("MONITOR DE CENTRALIZACION / HIGIENE DE LOGS (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana {args.horas:g} h  |  Retencion min {RETENCION_MIN_DIAS} d  "
          f"|  Tamano warn {TAMANO_WARN_GB:g} GB")
    print("  v1: no hay pipeline de centralizacion; se revisa higiene + errores Glue.\n")

    if not check_identity():
        print_summary()
        return 1

    logs = aws_client("logs")
    grupos = descubrir_log_groups(logs)
    paso(f"Log groups descubiertos: {len(grupos)}", fin=True)

    check_higiene(logs, grupos, args.horas)
    check_dms_logging()
    check_centralizacion(logs, grupos)

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
