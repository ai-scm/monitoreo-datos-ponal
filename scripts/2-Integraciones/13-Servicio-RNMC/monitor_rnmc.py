#!/usr/bin/env python3
"""
monitor_rnmc.py — Estado del "servicio" RNMC (pipeline de datos por lotes).
===========================================================================
Corre DIARIO. "Servicio RNMC" NO es una API: no hay API Gateway, ni endpoint HTTP.
Es un pipeline de datos por lotes con dos cadenas encadenadas + una tarea DMS puntual.
Por eso NO se verifica nada de HTTP/5xx; las señales son estado de jobs Glue,
metricas de la Lambda disparadora, estado de reglas EventBridge y de la DMS.

Recursos (naturaleza por recurso, igual criterio que SIVICC):
  1. pon-prod-raw-rnmc-incremental_etl   -> Glue, FRECUENTE (10+/dia): FAILED=FALLA,
     sin corrida en ventana = AVISO.
  2. pon-prod-homogenize-raw-rnmc-db     -> Glue, DISPARADO POR EVENTO S3 (no cron):
     "sin corrida en 24h" NO es AVISO (normal si no llego archivo). Solo FAILED=FALLA.
  3. RNMC-to-RDS                         -> Glue, DIARIO (trigger daily_rds 14:00 UTC):
     FAILED=FALLA, sin corrida del dia = AVISO. Escribe en Aurora Postgres.
  4. pon-prod-launch-homogenize-rnmc-job -> Lambda disparadora (evento S3). Se mira por
     METRICAS CloudWatch (no hay get_job_runs para Lambdas). Logica de CADENA ROTA:
       - invocada, 0 errores, job homogenize con run reciente -> OK
       - invocada, errores <=3, pero homogenize SI corrio despues -> AVISO (hiccup)
       - invocada, errores >3  O (con errores y homogenize sin run nuevo) -> FALLA
       - sin invocaciones en la ventana -> normal, solo dato (no llego archivo)
  5. Scheduling (DOS mecanismos distintos, NO asumir por el nombre):
     - EventBridge rules (prod-data-reload-rnmc1/rnmc2, prod-data-createview-rnmc):
       events.describe_rule -> State != ENABLED = AVISO.
     - Glue Triggers (daily_rds -> RNMC-to-RDS): glue.get_trigger -> State != ACTIVATED
       = AVISO. (daily_rds NO es EventBridge; es Glue nativo pese al estilo cron.)
  6. DMS rnmc-prod-source-to-rnmc-prod-target-fullload-2 -> detenida (stopped) = INFO
     (migracion puntual terminada), nunca FALLA.

Clasificacion de error (1 patron real de RNMC-to-RDS): schema drift id_hechos.

Fuera de v1 (anotado): verificar en Aurora (pon-prod-db-postgres, public.hechos_z1_v2)
que la tabla tenga filas del dia requeriria credenciales de BD y rompe el patron
solo-lectura-boto3. v1 solo mira el estado del job.

Es de SOLO LECTURA (glue:GetJobRuns, glue:GetTrigger, lambda via
cloudwatch:GetMetricStatistics, events:DescribeRule, dms:DescribeReplicationTasks,
logs:StartQuery/GetQueryResults, sts). NO escribe ni dispara nada.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algo fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_rnmc.py
    python3 monitor_rnmc.py --horas 48
    python3 monitor_rnmc.py --con-avisos
    python3 monitor_rnmc.py --sin-insights
    python3 monitor_rnmc.py --no-export
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
DEFAULT_HORAS = float(os.environ.get("MONITOR_HORAS", "24"))
LAMBDA_ERROR_THRESHOLD = int(os.environ.get("RNMC_LAMBDA_ERROR_THRESHOLD", "3"))
GLUE_ERROR_LOG_GROUP = os.environ.get("GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error")

INSIGHTS_MAX_ESPERAS = int(os.environ.get("INSIGHTS_MAX_ESPERAS", "40"))
INSIGHTS_POLL_SEG = float(os.environ.get("INSIGHTS_POLL_SEG", "1.0"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Naturaleza de los jobs Glue del pipeline RNMC.
# --------------------------------------------------------------------------------------
FRECUENTE = "FRECUENTE"      # corre muchas veces al dia
DIARIO = "DIARIO"            # 1/dia por cron
EVENTO_S3 = "EVENTO_S3"      # disparado por llegada de archivo (no cron)

JOBS_RNMC = {
    "pon-prod-raw-rnmc-incremental_etl": FRECUENTE,
    "pon-prod-homogenize-raw-rnmc-db": EVENTO_S3,
    "RNMC-to-RDS": DIARIO,
}

# Job homogenize (para la correlacion de cadena rota con la Lambda).
JOB_HOMOGENIZE = "pon-prod-homogenize-raw-rnmc-db"

# Lambda disparadora (se mira por metricas CloudWatch).
LAMBDA_LAUNCH = "pon-prod-launch-homogenize-rnmc-job"

# OJO: dos mecanismos de scheduling DISTINTOS, en servicios AWS distintos.
#  - EventBridge rules (events.describe_rule): estado ENABLED/DISABLED.
#  - Glue Triggers (glue.get_trigger): estado ACTIVATED/DEACTIVATED/CREATED.
# El nombre NO dice cual es cual (daily_rds parecia EventBridge por el estilo cron,
# pero es un Glue Trigger -> RNMC-to-RDS). Se confirma con la API correspondiente.

# Reglas EventBridge que deben estar ENABLED (confirmadas EventBridge -> Lambda).
EVENTBRIDGE_RULES = [
    "prod-data-reload-rnmc1",
    "prod-data-reload-rnmc2",
    "prod-data-createview-rnmc",
]

# Glue Triggers que deben estar ACTIVATED. daily_rds dispara RNMC-to-RDS (14:00 UTC).
GLUE_TRIGGERS = [
    "daily_rds",
]

# Tarea DMS puntual (detenida a proposito).
DMS_TASK = "rnmc-prod-source-to-rnmc-prod-target-fullload-2"

# Destino final (solo informativo en v1; Aurora no se consulta).
AURORA_INFO = "pon-prod-db-postgres / public.hechos_z1_v2"

# --------------------------------------------------------------------------------------
# Clasificacion de error (1 patron real de RNMC-to-RDS).
# --------------------------------------------------------------------------------------
PATRONES_ERROR = [
    (re.compile(r"La vista NO contiene la columna requerida: id_hechos"),
     ("Schema drift: la vista Athena origen perdio la columna id_hechos",
      "Revisar/regenerar la vista pon-prod-raw-cnp.boletin_z1_capas_v2.")),
]

ESTADOS_FALLO = {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}
ESTADOS_EN_CURSO = {"RUNNING", "STARTING", "STOPPING", "WAITING"}

OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
INFO = "AVISO"  # se agrupa con AVISO para severidad, se etiqueta aparte
ICON = {OK: "[ OK  ]", WARN: "[AVISO]", FAIL: "[FALLA]"}

RESULTS = []
CON_AVISOS = False
VERBOSE = False
SIN_INSIGHTS = False

_SPIN = "|/-\\"
_spin_i = 0


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


def record(recurso, estado, mensaje="", ubicacion="", accion="", naturaleza="", tipo_error=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "naturaleza": naturaleza, "mensaje": mensaje,
        "accion_sugerida": accion if estado != OK else "", "tipo_error_detectado": tipo_error,
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


def _limpiar(texto):
    return " ".join((texto or "").split())[:200]


# --------------------------------------------------------------------------------------
# Identidad
# --------------------------------------------------------------------------------------
def check_identity():
    paso("Identidad AWS...")
    try:
        ident = aws_client("sts").get_caller_identity()
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
# Logs Insights para clasificar el error del run fallido
# --------------------------------------------------------------------------------------
def _run_insights(logs, query, start_ms, end_ms):
    resp = logs.start_query(
        logGroupName=GLUE_ERROR_LOG_GROUP,
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


def clasificar_error_run(logs, run_id, start_ms, end_ms):
    if SIN_INSIGHTS:
        return None, ""
    query = (
        "fields @message\n"
        f"| filter @logStream like /{run_id}/\n"
        "| filter @message like /Error/ or @message like /Exception/ or @message like /columna/ "
        "or @message like /id_hechos/\n"
        "| sort @timestamp desc\n"
        "| limit 50"
    )
    try:
        filas = _run_insights(logs, query, start_ms, end_ms)
    except Exception:  # noqa: BLE001
        return None, ""
    for f in filas:
        msg = f.get("@message", "")
        for rx, (causa, accion) in PATRONES_ERROR:
            if rx.search(msg):
                return (causa, accion), _limpiar(msg)
    if filas:
        return ("Error no catalogado — revisar manualmente",
                "Revisa el log crudo; agrega el patron si se repite."), _limpiar(filas[0].get("@message", ""))
    return None, ""


# --------------------------------------------------------------------------------------
# Jobs Glue (segun naturaleza)
# --------------------------------------------------------------------------------------
def _ultimo_run(glue, job):
    resp = glue.get_job_runs(JobName=job, MaxResults=1)
    runs = resp.get("JobRuns", [])
    return runs[0] if runs else None


def _run_reciente(run, ventana_ms_ini):
    started = run.get("StartedOn") if run else None
    return bool(started and started.timestamp() * 1000 >= ventana_ms_ini)


def evaluar_job(glue, logs, job, naturaleza, horas, ventana_ms_ini, ventana_ms_fin):
    try:
        run = _ultimo_run(glue, job)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code == "EntityNotFoundException":
            record(job, WARN, naturaleza=naturaleza,
                   mensaje=f"El job '{job}' no existe (nombre por confirmar).",
                   ubicacion=f"Glue job '{job}'",
                   accion="Confirma el nombre del job en la consola de Glue.")
            return
        accion = ("Falta glue:GetJobRuns." if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa el nombre del job y la region.")
        record(job, FAIL, naturaleza=naturaleza,
               mensaje=f"No se pudo consultar el job: {_err_detail(exc)}",
               ubicacion=f"Glue job '{job}'", accion=accion)
        return

    if run is None:
        if naturaleza == EVENTO_S3:
            record(job, INFO, naturaleza=naturaleza,
                   mensaje="Disparado por evento S3: sin corridas registradas (normal si no llego archivo).",
                   ubicacion=f"Glue job '{job}'")
        else:
            record(job, WARN, naturaleza=naturaleza,
                   mensaje="Sin ejecuciones registradas.",
                   ubicacion=f"Glue job '{job}'", accion="Verifica el scheduler/trigger del job.")
        return

    estado = run.get("JobRunState", "?")
    run_id = run.get("Id", "?")
    started = run.get("StartedOn")
    st_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
    reciente = _run_reciente(run, ventana_ms_ini)

    # FALLO: aplica a todas las naturalezas.
    if estado in ESTADOS_FALLO:
        err_msg = _limpiar(run.get("ErrorMessage", "")) or "(sin ErrorMessage)"
        clas, ejemplo = clasificar_error_run(logs, run_id, ventana_ms_ini, ventana_ms_fin)
        cuerpo = f"Ultimo run {estado} ({st_txt}). Motivo: {err_msg}"
        tipo_err = ""
        accion = "Revisa ese run en la consola de Glue / CloudWatch Logs."
        if clas:
            causa, accion_clas = clas
            tipo_err = causa
            cuerpo += f" | Clasificacion: {causa}"
            if ejemplo:
                cuerpo += f" | Log: {ejemplo}"
            accion = accion_clas
        record(job, FAIL, naturaleza=naturaleza, mensaje=cuerpo,
               ubicacion=f"Glue job '{job}' run {run_id}", accion=accion, tipo_error=tipo_err)
        return

    if estado in ESTADOS_EN_CURSO:
        record(job, WARN, naturaleza=naturaleza,
               mensaje=f"Ultimo run en curso ({estado}, {st_txt}).",
               ubicacion=f"Glue job '{job}' run {run_id}",
               accion="Aun ejecutando; vuelve a revisar mas tarde.")
        return

    if estado == "SUCCEEDED":
        if naturaleza == EVENTO_S3:
            # Para evento S3, sin corrida reciente NO es AVISO: se reporta como dato.
            record(job, OK if reciente else INFO, naturaleza=naturaleza,
                   mensaje=(f"Ultimo run OK ({st_txt})." if reciente else
                            f"Ultimo run OK ({st_txt}); sin corrida en la ventana (normal: no llego archivo)."),
                   ubicacion=f"Glue job '{job}' run {run_id}")
        else:  # FRECUENTE / DIARIO
            if reciente:
                record(job, OK, naturaleza=naturaleza,
                       mensaje=f"Ultimo run OK ({st_txt}).",
                       ubicacion=f"Glue job '{job}' run {run_id}")
            else:
                record(job, WARN, naturaleza=naturaleza,
                       mensaje=f"Ultimo run OK pero fuera de la ventana de {horas:g} h ({st_txt}).",
                       ubicacion=f"Glue job '{job}' run {run_id}",
                       accion="No corrio en la ventana esperada; revisa el scheduler/trigger.")
        return

    record(job, WARN, naturaleza=naturaleza,
           mensaje=f"Ultimo run en estado {estado} ({st_txt}).",
           ubicacion=f"Glue job '{job}' run {run_id}",
           accion="Estado no reconocido; revisa el run.")


def check_jobs(glue, logs, horas, ventana_ms_ini, ventana_ms_fin):
    total = len(JOBS_RNMC)
    for i, (job, naturaleza) in enumerate(JOBS_RNMC.items(), start=1):
        paso(f"Jobs RNMC... ({i}/{total}) {naturaleza}")
        evaluar_job(glue, logs, job, naturaleza, horas, ventana_ms_ini, ventana_ms_fin)
    paso(f"Jobs RNMC  ({total} revisados)", fin=True)


# --------------------------------------------------------------------------------------
# Lambda disparadora: metricas CloudWatch + logica de CADENA ROTA
# --------------------------------------------------------------------------------------
def _metrica_lambda(cw, metrica, func_name, start, end):
    """Suma de una metrica (Invocations/Errors) de la Lambda en la ventana."""
    resp = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName=metrica,
        Dimensions=[{"Name": "FunctionName", "Value": func_name}],
        StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
    )
    return sum(dp.get("Sum", 0) for dp in resp.get("Datapoints", []))


def check_lambda_cadena(glue, horas, ventana_ms_ini):
    """
    Logica de cadena rota (correlaciona la Lambda disparadora con el job homogenize):
      - invocada, 0 errores, homogenize con run reciente        -> OK
      - invocada, errores <= umbral, homogenize SI corrio despues -> AVISO (hiccup)
      - invocada, errores > umbral  O (con errores y homogenize sin run nuevo) -> FALLA
      - sin invocaciones en la ventana                          -> dato (no llego archivo)
    """
    cw = aws_client("cloudwatch")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=horas)
    paso("Lambda disparadora (metricas)...")

    try:
        invocaciones = _metrica_lambda(cw, "Invocations", LAMBDA_LAUNCH, start, now)
        errores = _metrica_lambda(cw, "Errors", LAMBDA_LAUNCH, start, now)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        accion = ("Falta cloudwatch:GetMetricStatistics." if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa permisos/region de CloudWatch.")
        record(f"Lambda {LAMBDA_LAUNCH}", WARN, naturaleza="LAMBDA",
               mensaje=f"No se pudieron leer metricas: {_err_detail(exc)}",
               ubicacion=f"CloudWatch AWS/Lambda '{LAMBDA_LAUNCH}'", accion=accion)
        paso("Lambda disparadora  (no evaluada)", fin=True)
        return

    # Sin invocaciones: normal (no llego archivo). Solo dato.
    if invocaciones <= 0:
        record(f"Lambda {LAMBDA_LAUNCH}", INFO, naturaleza="LAMBDA",
               mensaje="Sin invocaciones en la ventana (normal: no llego archivo nuevo a S3).",
               ubicacion=f"CloudWatch AWS/Lambda '{LAMBDA_LAUNCH}'")
        paso("Lambda disparadora  (sin invocaciones)", fin=True)
        return

    # Hubo invocaciones: ver si el job homogenize corrio despues (cadena completa).
    homogenize_reciente = False
    try:
        run = _ultimo_run(glue, JOB_HOMOGENIZE)
        homogenize_reciente = _run_reciente(run, ventana_ms_ini)
    except Exception:  # noqa: BLE001
        homogenize_reciente = False

    if errores <= 0:
        record(f"Lambda {LAMBDA_LAUNCH}", OK, naturaleza="LAMBDA",
               mensaje=f"{invocaciones:.0f} invocacion(es), 0 errores. Cadena S3->homogenize sana.",
               ubicacion=f"CloudWatch AWS/Lambda '{LAMBDA_LAUNCH}'")
    elif errores > LAMBDA_ERROR_THRESHOLD or not homogenize_reciente:
        # Cadena rota: muchos errores, o la Lambda fallo y el job no corrio pese a la invocacion.
        motivo = (f"{errores:.0f} errores (> {LAMBDA_ERROR_THRESHOLD})" if errores > LAMBDA_ERROR_THRESHOLD
                  else f"{errores:.0f} error(es) y el job homogenize NO tiene run nuevo pese a la invocacion")
        record(f"Lambda {LAMBDA_LAUNCH}", FAIL, naturaleza="LAMBDA",
               mensaje=(f"CADENA ROTA: {invocaciones:.0f} invocacion(es), {motivo}. "
                        "La ingesta S3->homogenize no completo."),
               ubicacion=f"CloudWatch AWS/Lambda '{LAMBDA_LAUNCH}' + Glue '{JOB_HOMOGENIZE}'",
               accion="Revisa los logs de la Lambda y el job homogenize; la cadena se corto.")
    else:
        # Errores <= umbral pero el job SI corrio despues: hiccup tolerable.
        record(f"Lambda {LAMBDA_LAUNCH}", WARN, naturaleza="LAMBDA",
               mensaje=(f"{invocaciones:.0f} invocacion(es), {errores:.0f} error(es) (<= {LAMBDA_ERROR_THRESHOLD}) "
                        "pero el job homogenize SI corrio despues (hiccup tolerable)."),
               ubicacion=f"CloudWatch AWS/Lambda '{LAMBDA_LAUNCH}'",
               accion="Vigila si los errores de la Lambda aumentan en frecuencia.")
    paso("Lambda disparadora  (evaluada)", fin=True)


# --------------------------------------------------------------------------------------
# Reglas EventBridge (deben estar ENABLED)
# --------------------------------------------------------------------------------------
def check_eventbridge():
    events = aws_client("events")
    paso("Reglas EventBridge...")
    for regla in EVENTBRIDGE_RULES:
        try:
            resp = events.describe_rule(Name=regla)
            state = resp.get("State", "?")
            if state == "ENABLED":
                record(f"Regla {regla}", OK, naturaleza="EVENTBRIDGE",
                       mensaje="ENABLED.", ubicacion=f"EventBridge rule '{regla}'")
            else:
                record(f"Regla {regla}", WARN, naturaleza="EVENTBRIDGE",
                       mensaje=f"Estado {state} (no ENABLED). Si sigue asi, el pipeline se seca.",
                       ubicacion=f"EventBridge rule '{regla}'",
                       accion="Re-habilita la regla si deberia estar activa.")
        except Exception as exc:  # noqa: BLE001
            code, _ = _err(exc)
            if code == "ResourceNotFoundException":
                record(f"Regla {regla}", WARN, naturaleza="EVENTBRIDGE",
                       mensaje=f"La regla '{regla}' no existe (nombre por confirmar).",
                       ubicacion=f"EventBridge rule '{regla}'",
                       accion="Confirma el nombre de la regla.")
            else:
                record(f"Regla {regla}", WARN, naturaleza="EVENTBRIDGE",
                       mensaje=f"No se pudo consultar: {_err_detail(exc)}",
                       ubicacion=f"EventBridge rule '{regla}'",
                       accion="Revisa permisos events:DescribeRule.")
    paso(f"Reglas EventBridge  ({len(EVENTBRIDGE_RULES)} revisadas)", fin=True)


# --------------------------------------------------------------------------------------
# Glue Triggers (deben estar ACTIVATED). Servicio y API distintos de EventBridge.
# --------------------------------------------------------------------------------------
def check_glue_triggers():
    glue = aws_client("glue")
    paso("Glue Triggers...")
    for nombre in GLUE_TRIGGERS:
        try:
            trig = glue.get_trigger(Name=nombre).get("Trigger", {})
            state = trig.get("State", "?")  # ACTIVATED / DEACTIVATED / CREATED / ...
            tipo = trig.get("Type", "?")     # SCHEDULED / CONDITIONAL / ON_DEMAND
            sched = trig.get("Schedule", "") or ""
            acciones = ", ".join(a.get("JobName", "?") for a in trig.get("Actions", [])) or "sin acciones"
            detalle = f"tipo {tipo}" + (f", {sched}" if sched else "") + f" -> {acciones}"
            if state == "ACTIVATED":
                record(f"Trigger {nombre}", OK, naturaleza="GLUE_TRIGGER",
                       mensaje=f"ACTIVATED ({detalle}).",
                       ubicacion=f"Glue trigger '{nombre}'")
            else:
                record(f"Trigger {nombre}", WARN, naturaleza="GLUE_TRIGGER",
                       mensaje=f"Estado {state} (no ACTIVATED) ({detalle}). Si sigue asi, el job no se dispara.",
                       ubicacion=f"Glue trigger '{nombre}'",
                       accion="Re-activa el trigger si deberia estar programando el job.")
        except Exception as exc:  # noqa: BLE001
            code, _ = _err(exc)
            if code == "EntityNotFoundException":
                record(f"Trigger {nombre}", WARN, naturaleza="GLUE_TRIGGER",
                       mensaje=f"El Glue trigger '{nombre}' no existe (nombre por confirmar).",
                       ubicacion=f"Glue trigger '{nombre}'",
                       accion="Confirma el nombre del trigger en la consola de Glue.")
            else:
                record(f"Trigger {nombre}", WARN, naturaleza="GLUE_TRIGGER",
                       mensaje=f"No se pudo consultar: {_err_detail(exc)}",
                       ubicacion=f"Glue trigger '{nombre}'",
                       accion="Revisa permisos glue:GetTrigger.")
    paso(f"Glue Triggers  ({len(GLUE_TRIGGERS)} revisados)", fin=True)


# --------------------------------------------------------------------------------------
# Tarea DMS puntual (detenida a proposito -> INFO)
# --------------------------------------------------------------------------------------
def check_dms():
    dms = aws_client("dms")
    paso("Tarea DMS (full-load puntual)...")
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
        record(f"DMS {DMS_TASK}", INFO, naturaleza="DMS",
               mensaje=f"No se pudo consultar DMS: {_err_detail(exc)}",
               ubicacion=f"DMS (region {REGION})")
        paso("Tarea DMS  (no evaluada)", fin=True)
        return

    t = next((x for x in tasks if x.get("ReplicationTaskIdentifier") == DMS_TASK), None)
    if t is None:
        record(f"DMS {DMS_TASK}", INFO, naturaleza="DMS",
               mensaje=f"Tarea '{DMS_TASK}' no encontrada.", ubicacion=f"DMS (region {REGION})")
    else:
        status = t.get("Status", "?")
        record(f"DMS {DMS_TASK}", INFO, naturaleza="DMS",
               mensaje=(f"Full-load puntual en estado {status}. "
                        "Parece migracion manual terminada; no es un fallo."),
               ubicacion=f"DMS task '{DMS_TASK}'",
               accion="Si deberia re-migrar, dispararla manualmente.")
    paso("Tarea DMS  (informativo)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — PIPELINE RNMC (lotes, sin API)")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso/info | {fail} falla")
    print(f"  Destino final (no verificado en v1): Aurora {AURORA_INFO}")

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO:" if not CON_AVISOS else "Detalle:"
        print(f"\n  {titulo}")
        for nivel in niveles:
            for r in RESULTS:
                if r["estado"] == nivel:
                    nat = f" [{r['naturaleza']}]" if r.get("naturaleza") else ""
                    print(f"\n  {ICON[nivel]} {r['recurso']}{nat}")
                    print(f"       que pasa : {r['mensaje']}")
                    if r.get("tipo_error_detectado"):
                        print(f"       tipo     : {r['tipo_error_detectado']}")
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Pipeline RNMC OK en toda la cadena.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s)/info ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"rnmc_{ts}.csv")
    cols = ["recurso", "naturaleza", "ubicacion", "estado", "mensaje",
            "accion_sugerida", "tipo_error_detectado"]
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
        description="Estado del pipeline de datos RNMC (lotes, sin API). Cadena S3->homogenize->RDS.")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana en horas (default {DEFAULT_HORAS:g}).")
    parser.add_argument("--sin-insights", action="store_true",
                        help="Omite la clasificacion de errores por Logs Insights.")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien AVISOS e INFO (evento S3, DMS, Lambda sin invocaciones).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE, SIN_INSIGHTS
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado
    SIN_INSIGHTS = args.sin_insights

    now = datetime.now(timezone.utc)
    ventana_ms_ini = int((now - timedelta(hours=args.horas)).timestamp() * 1000)
    ventana_ms_fin = int(now.timestamp() * 1000)

    print("=" * 78)
    print("MONITOR DE PIPELINE RNMC (solo lectura) — lotes, sin API")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana {args.horas:g} h  |  Umbral errores Lambda {LAMBDA_ERROR_THRESHOLD}")
    print("  Cadena: ingesta incremental -> homogenize (evento S3) -> RNMC-to-RDS (Aurora).\n")

    if not check_identity():
        print_summary()
        return 1

    glue = aws_client("glue")
    logs = aws_client("logs")
    check_jobs(glue, logs, args.horas, ventana_ms_ini, ventana_ms_fin)
    check_lambda_cadena(glue, args.horas, ventana_ms_ini)
    check_eventbridge()
    check_glue_triggers()
    check_dms()

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
