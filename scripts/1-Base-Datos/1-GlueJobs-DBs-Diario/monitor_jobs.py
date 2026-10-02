#!/usr/bin/env python3
"""
monitor_jobs.py — Estado de los jobs de Glue de las bases Oracle On-Premise.
============================================================================
Corre DIARIO. Para cada job de las 8 bases:
  1. Identidad AWS (con que rol se ejecuta).
  2. Estado del ULTIMO run de cada job en las ultimas --horas (24 por defecto):
     SUCCEEDED / FAILED / RUNNING / sin corridas.
  3. Para los que FALLARON, clasifica el error (codigo Oracle o error Glue/Spark)
     leyendo el log de ESE run con CloudWatch Logs Insights.

Es de SOLO LECTURA (glue:GetJobRuns, logs:StartQuery/GetQueryResults, sts).
NO revisa conexion a las bases (eso es el script A) ni tareas DMS (script C).

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algun job fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_jobs.py                 # ultimas 24 h
    python3 monitor_jobs.py --horas 48
    python3 monitor_jobs.py --con-avisos    # muestra tambien avisos (RUNNING, sin corridas)
    python3 monitor_jobs.py --no-export
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
GLUE_ERROR_LOG_GROUP = os.environ.get("GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error")
DEFAULT_HORAS = float(os.environ.get("MONITOR_HORAS", "24"))

# Parametros de CloudWatch Logs Insights (para clasificar el error del run fallido).
INSIGHTS_MAX_ESPERAS = int(os.environ.get("INSIGHTS_MAX_ESPERAS", "40"))
INSIGHTS_POLL_SEG = float(os.environ.get("INSIGHTS_POLL_SEG", "1.0"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Jobs de las 8 bases. JOB_NAME -> DatabaseName.
# --------------------------------------------------------------------------------------
JOB_NAME_TO_DATABASE = {
    "pon-prod-raw-sivicc_etl": "SIVIC",
    "pon-prod-raw-siedco_etl": "SIEDCO",
    "pon-prod-raw-disec_etl": "DISEC",
    "pon-prod-raw-rnmc-incremental_etl": "RNMC",
    "pon-prod-raw-geopol_etl": "GEOPOL",
    "pon-prod-raw-telepol_etl": "TELEPOL",
    "pon-prod-raw-dboper_etl": "DBOPER",
    "pon-prod-raw-ali_etl": "ALI",
}

# --------------------------------------------------------------------------------------
# Clasificacion de errores (misma logica que el classifier de las lambdas).
# --------------------------------------------------------------------------------------
ORA_DESCRIPCIONES = {
    "ORA-00257": "archiver bloqueado / tablespace o FRA lleno",
    "ORA-01017": "usuario o contrasena invalidos (credenciales)",
    "ORA-01031": "privilegios insuficientes",
    "ORA-01034": "la instancia Oracle no esta disponible",
    "ORA-12154": "no se pudo resolver el identificador de conexion (TNS)",
    "ORA-12170": "timeout de conexion (TNS)",
    "ORA-12505": "el listener no conoce el SID",
    "ORA-12514": "el listener no conoce el servicio",
    "ORA-12541": "no hay listener (TNS)",
    "ORA-17002": "error de red / E-S (conexion perdida con la BD)",
    "ORA-28000": "la cuenta esta bloqueada",
    "ORA-28001": "la contrasena expiro",
}

# Patrones de error Glue/Spark (etiqueta legible + descripcion).
PATRONES_GLUE = {
    r"java\.lang\.OutOfMemoryError": ("Memoria agotada (OOM)", "el job se quedo sin memoria (subir DPU/worker)"),
    r"ExecutorLostFailure":          ("Executor perdido", "un executor de Spark murio"),
    r"Container killed":             ("Contenedor terminado (killed)", "YARN/Glue mato el contenedor (memoria)"),
    r"exit code \d+":                ("Proceso con exit code", "el proceso termino con codigo de error"),
    r"Connection (closed|reset)":    ("Conexion cerrada/reiniciada", "se perdio la conexion (red / BD)"),
    r"Py4JJavaError":                ("Error Java desde Python (Py4J)", "excepcion Java propagada al script"),
}

ORA_CODE_RE = re.compile(r"ORA-\d{3,5}", re.IGNORECASE)

# Estados de un job run de Glue.
ESTADOS_FALLO = {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}
ESTADOS_EN_CURSO = {"RUNNING", "STARTING", "STOPPING", "WAITING"}

# Severidad / estados de salida.
OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
ICON = {OK: "[ OK  ]", WARN: "[AVISO]", FAIL: "[FALLA]"}

RESULTS = []
CON_AVISOS = False
VERBOSE = False

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


def record(recurso, estado, mensaje="", ubicacion="", accion=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "mensaje": mensaje, "accion_sugerida": accion if estado != OK else "",
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
# Logs Insights (para clasificar el error del run fallido)
# --------------------------------------------------------------------------------------
def _run_insights(logs, query, start_ms, end_ms):
    resp = logs.start_query(
        logGroupName=GLUE_ERROR_LOG_GROUP,
        startTime=int(start_ms / 1000), endTime=int(end_ms / 1000),
        queryString=query, limit=100,
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
    """
    Lee las lineas de error del logStream de un JobRunId fallido y devuelve una
    etiqueta de clasificacion + un mensaje de ejemplo. Devuelve (etiqueta, ejemplo).
    """
    query = (
        "fields @message\n"
        f"| filter @logStream like /{run_id}/\n"
        "| filter @message like /ORA-/ or @message like /Exception/ or @message like /ERROR/\n"
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
            return f"{code} ({desc})", _limpiar(msg)
    for f in filas:
        msg = f.get("@message", "")
        for rx, (etiqueta, _desc) in PATRONES_GLUE.items():
            if re.search(rx, msg, re.IGNORECASE):
                return etiqueta, _limpiar(msg)
    # Hay error pero no lo clasificamos: devolvemos el primer mensaje como pista.
    if filas:
        return "Error no clasificado", _limpiar(filas[0].get("@message", ""))
    return None, ""


# --------------------------------------------------------------------------------------
# Estado del ultimo run de cada job
# --------------------------------------------------------------------------------------
def check_jobs(horas):
    glue = aws_client("glue")
    logs = aws_client("logs")
    now = datetime.now(timezone.utc)
    ventana_ms_ini = int((now - timedelta(hours=horas)).timestamp() * 1000)
    ventana_ms_fin = int(now.timestamp() * 1000)

    total = len(JOB_NAME_TO_DATABASE)
    for i, (job, db) in enumerate(JOB_NAME_TO_DATABASE.items(), start=1):
        paso(f"Jobs... ({i}/{total}) {db}")
        try:
            resp = glue.get_job_runs(JobName=job, MaxResults=1)
        except Exception as exc:  # noqa: BLE001
            code, _ = _err(exc)
            if code in ("EntityNotFoundException",):
                accion = f"El job '{job}' no existe. Verifica el nombre en la consola de Glue."
            elif code in ("AccessDeniedException", "AccessDenied"):
                accion = "Falta el permiso glue:GetJobRuns."
            else:
                accion = "Revisa el nombre del job y la region."
            record(f"{db}  ({job})", FAIL,
                   mensaje=f"No se pudo consultar el job: {_err_detail(exc)}",
                   ubicacion=f"Glue job '{job}'", accion=accion)
            continue

        runs = resp.get("JobRuns", [])
        if not runs:
            record(f"{db}  ({job})", WARN,
                   mensaje="El job no tiene ejecuciones registradas.",
                   ubicacion=f"Glue job '{job}'",
                   accion="Verifica si el job deberia haber corrido.")
            continue

        run = runs[0]
        estado = run.get("JobRunState", "?")
        run_id = run.get("Id", "?")
        started = run.get("StartedOn")
        started_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
        # Antiguedad: si el ultimo run es mas viejo que la ventana, se avisa.
        fuera_ventana = bool(started and started.timestamp() * 1000 < ventana_ms_ini)

        if estado == "SUCCEEDED":
            msg = f"Ultimo run OK ({started_txt})."
            if fuera_ventana:
                msg += f" OJO: fue hace mas de {horas:g} h; puede que no haya corrido en la ventana."
                record(f"{db}  ({job})", WARN, mensaje=msg,
                       ubicacion=f"Glue job '{job}' run {run_id}",
                       accion="Verifica el scheduler; el job no corrio en la ventana esperada.")
            else:
                record(f"{db}  ({job})", OK, mensaje=msg,
                       ubicacion=f"Glue job '{job}' run {run_id}")
        elif estado in ESTADOS_EN_CURSO:
            record(f"{db}  ({job})", WARN,
                   mensaje=f"Ultimo run en curso ({estado}, {started_txt}).",
                   ubicacion=f"Glue job '{job}' run {run_id}",
                   accion="Aun ejecutando; vuelve a revisar mas tarde.")
        elif estado in ESTADOS_FALLO:
            err_msg = _limpiar(run.get("ErrorMessage", "")) or "(sin ErrorMessage)"
            paso(f"Jobs... ({i}/{total}) {db} - clasificando error")
            etiqueta, ejemplo = clasificar_error_run(logs, run_id, ventana_ms_ini, ventana_ms_fin)
            cuerpo = f"Ultimo run {estado} ({started_txt}). Motivo: {err_msg}"
            if etiqueta:
                cuerpo += f" | Clasificacion: {etiqueta}"
                if ejemplo:
                    cuerpo += f" | Log: {ejemplo}"
            record(f"{db}  ({job})", FAIL, mensaje=cuerpo,
                   ubicacion=f"Glue job '{job}' run {run_id}",
                   accion="Revisa ese run en la consola de Glue / CloudWatch Logs Insights.")
        else:
            record(f"{db}  ({job})", WARN,
                   mensaje=f"Ultimo run en estado {estado} ({started_txt}).",
                   ubicacion=f"Glue job '{job}' run {run_id}",
                   accion="Estado no terminal reconocido; revisa el run.")

    n_prob = sum(1 for r in RESULTS if r["estado"] != OK and r["recurso"] != "Identidad AWS")
    paso(f"Jobs  ({total} revisados, {n_prob} con aviso/falla)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — ESTADO DE JOBS GLUE")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso | {fail} falla")

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO (jobs fallidos):" if not CON_AVISOS else "Detalle:"
        print(f"\n  {titulo}")
        for nivel in niveles:
            for r in RESULTS:
                if r["estado"] == nivel:
                    print(f"\n  {ICON[nivel]} {r['recurso']}")
                    print(f"       que pasa : {r['mensaje']}")
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Todos los jobs corrieron OK en la ventana.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"jobs_{ts}.csv")
    cols = ["recurso", "ubicacion", "estado", "mensaje", "accion_sugerida"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(RESULTS)
    print(f"\nReporte CSV: {csv_path}")
    home = os.path.expanduser("~")
    if os.path.commonpath([csv_path, home]) == home:
        print(f"Descarga (CloudShell Actions -> Download file): {os.path.relpath(csv_path, home)}")


def main():
    parser = argparse.ArgumentParser(description="Estado de los jobs de Glue de las 8 bases Oracle.")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana en horas (por defecto {DEFAULT_HORAS:g}).")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien los AVISOS (RUNNING, sin corridas, fuera de ventana).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE JOBS GLUE — BASES ORACLE ON-PREMISE (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana {args.horas:g} h\n")

    if not check_identity():
        print_summary()
        return 1
    check_jobs(args.horas)

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
