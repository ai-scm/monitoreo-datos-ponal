#!/usr/bin/env python3
"""
monitor_auditoria_conversaciones_glue.py — Job Glue de auditoria de conversaciones.
===================================================================================
Corre DIARIO. OJO con el nombre: originalmente este punto se llamo "Sincronizacion
Glue", pero fue un CHOQUE DE NOMBRES. No existe ningun Glue que sincronice la Knowledge
Base del chatbot: la "sincronizacion" de la KB la hace Bedrock mismo (Step Function ->
Lambda -> StartIngestionJob), ver el punto 10. El unico recurso Glue REAL del dominio
chatbot es este job de AUDITORIA, que exporta el historial de conversaciones del chat
(DynamoDB -> Parquet) para auditoria. No tiene relacion con cargar documentos a la KB.

Recurso unico monitoreado:
  pon-prod-expertopol-conversation-audit-glue-full-refresh (Glue job)
  Disparado por un GLUE TRIGGER (conversation-audit-glue-full-refresh), SCHEDULED,
  ACTIVATED, cron diario 02:00 UTC. Verificado con la API correcta (glue.get_trigger),
  NO asumido por el nombre (leccion de daily_rds en RNMC).

Chequeos:
  1. Identidad AWS.
  2. Glue Trigger: glue.get_trigger -> State ACTIVATED = OK; otro = AVISO.
  3. Ultimo run del job: glue.get_job_runs. Mismo patron que monitor_jobs.py:
     SUCCEEDED en la ventana (--horas, 24) = OK; FAILED = FALLA; sin corrida en
     ventana = AVISO. Para los FAILED clasifica el error con Logs Insights usando el
     catalogo GENERICO de Glue (ORA-#####, AccessDenied S3, TASK_WRITE_FAILED, etc.):
     este job no tiene aun un patron de fallo propio (10/10 corridas exitosas), asi
     que se afina si aparece un fallo real.

Es de SOLO LECTURA (glue:GetTrigger, glue:GetJobRuns,
logs:StartQuery/GetQueryResults, sts). NO dispara nada.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_auditoria_conversaciones_glue.py
    python3 monitor_auditoria_conversaciones_glue.py --horas 48
    python3 monitor_auditoria_conversaciones_glue.py --con-avisos
    python3 monitor_auditoria_conversaciones_glue.py --sin-insights
    python3 monitor_auditoria_conversaciones_glue.py --no-export
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
GLUE_ERROR_LOG_GROUP = os.environ.get("GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error")

INSIGHTS_MAX_ESPERAS = int(os.environ.get("INSIGHTS_MAX_ESPERAS", "40"))
INSIGHTS_POLL_SEG = float(os.environ.get("INSIGHTS_POLL_SEG", "1.0"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# Recurso unico: el job de auditoria de conversaciones + su Glue Trigger.
JOB = "pon-prod-expertopol-conversation-audit-glue-full-refresh"
TRIGGER = "conversation-audit-glue-full-refresh"

# --------------------------------------------------------------------------------------
# Catalogo GENERICO de errores de Glue (reutilizado del dominio Base-Datos). Este job no
# tiene aun un patron propio (10/10 corridas OK); se afina si aparece un fallo real.
# --------------------------------------------------------------------------------------
ORA_DESCRIPCIONES = {
    "ORA-00257": "archiver bloqueado / tablespace o FRA lleno",
    "ORA-01017": "usuario o contrasena invalidos (credenciales)",
    "ORA-12170": "timeout de conexion (TNS)",
    "ORA-17002": "error de red / E-S (conexion perdida con la BD)",
}
PATRONES_GLUE = [
    (re.compile(r"java\.lang\.OutOfMemoryError"), ("Memoria agotada (OOM)", "Subir DPU/worker del job.")),
    (re.compile(r"ExecutorLostFailure"), ("Executor perdido", "Un executor de Spark murio; revisar recursos.")),
    (re.compile(r"Container killed"), ("Contenedor terminado (killed)", "YARN/Glue mato el contenedor (memoria).")),
    (re.compile(r"TASK_WRITE_FAILED"), ("Fallo de escritura (Spark)", "Mirar el 'Caused by' anidado (suele ser S3 AccessDenied).")),
    (re.compile(r"s3:(PutObject|ListBucket).*AccessDenied"), ("AccessDenied en S3", "Revisar politica IAM del rol del job.")),
    (re.compile(r"Py4JJavaError"), ("Error Java desde Python (Py4J)", "Excepcion Java propagada al script.")),
    (re.compile(r"ResourceNotFoundException|ValidationException.*DynamoDB|dynamodb"), ("Problema con la tabla DynamoDB origen", "Verificar la tabla DynamoDB de conversaciones.")),
]
ORA_CODE_RE = re.compile(r"ORA-\d{3,5}", re.IGNORECASE)

ESTADOS_FALLO = {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}
ESTADOS_EN_CURSO = {"RUNNING", "STARTING", "STOPPING", "WAITING"}

OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
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


def record(recurso, estado, mensaje="", ubicacion="", accion="", tipo_error=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "mensaje": mensaje, "accion_sugerida": accion if estado != OK else "",
        "tipo_error_detectado": tipo_error,
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
# Glue Trigger (debe estar ACTIVATED)
# --------------------------------------------------------------------------------------
def check_trigger():
    glue = aws_client("glue")
    paso("Glue Trigger...")
    try:
        trig = glue.get_trigger(Name=TRIGGER).get("Trigger", {})
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code == "EntityNotFoundException":
            record(f"Trigger {TRIGGER}", WARN,
                   mensaje=f"El Glue trigger '{TRIGGER}' no existe (nombre por confirmar).",
                   ubicacion=f"Glue trigger '{TRIGGER}'",
                   accion="Confirma el nombre del trigger en la consola de Glue.")
        else:
            accion = ("Falta glue:GetTrigger." if code in ("AccessDeniedException", "AccessDenied")
                      else "Revisa permisos/region de Glue.")
            record(f"Trigger {TRIGGER}", WARN,
                   mensaje=f"No se pudo consultar el trigger: {_err_detail(exc)}",
                   ubicacion=f"Glue trigger '{TRIGGER}'", accion=accion)
        paso("Glue Trigger  (no evaluado)", fin=True)
        return

    state = trig.get("State", "?")   # ACTIVATED / DEACTIVATED / CREATED
    tipo = trig.get("Type", "?")      # SCHEDULED
    sched = trig.get("Schedule", "") or ""
    detalle = f"tipo {tipo}" + (f", {sched}" if sched else "")
    if state == "ACTIVATED":
        record(f"Trigger {TRIGGER}", OK,
               mensaje=f"ACTIVATED ({detalle}).", ubicacion=f"Glue trigger '{TRIGGER}'")
    else:
        record(f"Trigger {TRIGGER}", WARN,
               mensaje=f"Estado {state} (no ACTIVATED) ({detalle}). Si sigue asi, el job no se dispara.",
               ubicacion=f"Glue trigger '{TRIGGER}'",
               accion="Re-activa el trigger si deberia estar programando el job.")
    paso("Glue Trigger  (evaluado)", fin=True)


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
        "| filter @message like /ORA-/ or @message like /Error/ or @message like /Exception/ "
        "or @message like /AccessDenied/ or @message like /FAILED/\n"
        "| sort @timestamp desc\n"
        "| limit 50"
    )
    try:
        filas = _run_insights(logs, query, start_ms, end_ms)
    except Exception:  # noqa: BLE001
        return None, ""
    for f in filas:
        msg = f.get("@message", "")
        m = ORA_CODE_RE.search(msg)
        if m:
            code = m.group(0).upper()
            desc = ORA_DESCRIPCIONES.get(code, "sin descripcion")
            return (f"{code} ({desc})", "Revisar el error Oracle; escalar a DBA si se repite."), _limpiar(msg)
    for f in filas:
        msg = f.get("@message", "")
        for rx, (causa, accion) in PATRONES_GLUE:
            if rx.search(msg):
                return (causa, accion), _limpiar(msg)
    if filas:
        return ("Error no catalogado — revisar manualmente",
                "Revisa el log crudo; agrega el patron si se repite."), _limpiar(filas[0].get("@message", ""))
    return None, ""


# --------------------------------------------------------------------------------------
# Ultimo run del job
# --------------------------------------------------------------------------------------
def check_job(horas):
    glue = aws_client("glue")
    logs = aws_client("logs")
    now = datetime.now(timezone.utc)
    ventana_ms_ini = int((now - timedelta(hours=horas)).timestamp() * 1000)
    ventana_ms_fin = int(now.timestamp() * 1000)

    paso("Job de auditoria...")
    try:
        resp = glue.get_job_runs(JobName=JOB, MaxResults=1)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code == "EntityNotFoundException":
            record(JOB, WARN, mensaje=f"El job '{JOB}' no existe (nombre por confirmar).",
                   ubicacion=f"Glue job '{JOB}'",
                   accion="Confirma el nombre del job en la consola de Glue.")
        else:
            accion = ("Falta glue:GetJobRuns." if code in ("AccessDeniedException", "AccessDenied")
                      else "Revisa el nombre del job y la region.")
            record(JOB, FAIL, mensaje=f"No se pudo consultar el job: {_err_detail(exc)}",
                   ubicacion=f"Glue job '{JOB}'", accion=accion)
        paso("Job de auditoria  (no evaluado)", fin=True)
        return

    runs = resp.get("JobRuns", [])
    if not runs:
        record(JOB, WARN, mensaje="El job no tiene ejecuciones registradas.",
               ubicacion=f"Glue job '{JOB}'", accion="Verifica el trigger del job.")
        paso("Job de auditoria  (sin corridas)", fin=True)
        return

    run = runs[0]
    estado = run.get("JobRunState", "?")
    run_id = run.get("Id", "?")
    started = run.get("StartedOn")
    st_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
    fuera_ventana = bool(started and started.timestamp() * 1000 < ventana_ms_ini)

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
        record(JOB, FAIL, mensaje=cuerpo,
               ubicacion=f"Glue job '{JOB}' run {run_id}", accion=accion, tipo_error=tipo_err)
    elif estado in ESTADOS_EN_CURSO:
        record(JOB, WARN, mensaje=f"Ultimo run en curso ({estado}, {st_txt}).",
               ubicacion=f"Glue job '{JOB}' run {run_id}",
               accion="Aun ejecutando; vuelve a revisar mas tarde.")
    elif estado == "SUCCEEDED":
        if fuera_ventana:
            record(JOB, WARN,
                   mensaje=f"Ultimo run OK pero hace mas de {horas:g} h ({st_txt}).",
                   ubicacion=f"Glue job '{JOB}' run {run_id}",
                   accion="No corrio en la ventana esperada (cron 02:00 UTC); revisa el trigger.")
        else:
            record(JOB, OK, mensaje=f"Ultimo run OK ({st_txt}).",
                   ubicacion=f"Glue job '{JOB}' run {run_id}")
    else:
        record(JOB, WARN, mensaje=f"Ultimo run en estado {estado} ({st_txt}).",
               ubicacion=f"Glue job '{JOB}' run {run_id}",
               accion="Estado no reconocido; revisa el run.")
    paso("Job de auditoria  (evaluado)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — AUDITORIA DE CONVERSACIONES (Glue)")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso | {fail} falla")

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO:" if not CON_AVISOS else "Detalle:"
        print(f"\n  {titulo}")
        for nivel in niveles:
            for r in RESULTS:
                if r["estado"] == nivel:
                    print(f"\n  {ICON[nivel]} {r['recurso']}")
                    print(f"       que pasa : {r['mensaje']}")
                    if r.get("tipo_error_detectado"):
                        print(f"       tipo     : {r['tipo_error_detectado']}")
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Job de auditoria de conversaciones OK.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"auditoria_conversaciones_{ts}.csv")
    cols = ["recurso", "ubicacion", "estado", "mensaje", "accion_sugerida", "tipo_error_detectado"]
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
        description="Estado del job Glue de auditoria de conversaciones del chatbot (NO sincroniza la KB).")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana en horas (default {DEFAULT_HORAS:g}).")
    parser.add_argument("--sin-insights", action="store_true",
                        help="Omite la clasificacion de errores por Logs Insights.")
    parser.add_argument("--con-avisos", action="store_true", help="Muestra tambien los AVISOS.")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE, SIN_INSIGHTS
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado
    SIN_INSIGHTS = args.sin_insights

    print("=" * 78)
    print("MONITOR DE AUDITORIA DE CONVERSACIONES — GLUE (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana {args.horas:g} h")
    print("  OJO: 'Glue' aqui NO sincroniza la KB (eso lo hace Bedrock, punto 10). Es el")
    print("  job de auditoria de conversaciones (DynamoDB -> Parquet), cron 02:00 UTC.\n")

    if not check_identity():
        print_summary()
        return 1

    check_trigger()
    check_job(args.horas)

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
