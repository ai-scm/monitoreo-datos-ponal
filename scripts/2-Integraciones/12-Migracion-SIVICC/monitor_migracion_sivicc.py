#!/usr/bin/env python3
"""
monitor_migracion_sivicc.py — Estado de la migracion/backfills de SIVICC.
=========================================================================
Corre DIARIO. La "migracion SIVICC" NO es una sola cosa: es un conjunto de
backfills historicos ad-hoc + un ETL que ya muto a regimen + una carga estatica
ya terminada + una tarea DMS dormida, cada recurso en una ETAPA (naturaleza)
distinta. Por eso la regla de severidad se aplica SEGUN LA NATURALEZA del recurso,
no una regla unica.

Naturalezas y su criterio:
  - REGIMEN  (ETL diario ya consolidado): como monitor_jobs.py. SUCCEEDED en la
             ventana = OK; sin corrida en ventana = AVISO; FAILED = FALLA.
  - BACKFILL (historico manual, sin trigger): FAILED = FALLA; SUCCEEDED aunque sea
             viejo = OK; sin corrida reciente = DATO (no AVISO), salvo que supere
             SIVICC_BACKFILL_STALE_DAYS (90) sin correr -> AVISO (posible abandono).
  - ESTATICO (ya terminado, debe quedar congelado): OK si no cambio; si hay corrida
             reciente / cambio inesperado -> AVISO ("cambio algo que deberia estar quieto").
  - EXPERIMENTAL (en desarrollo activo): solo INFO. Nunca FALLA ni AVISO (su criterio
             de exito aun no esta definido; promover a categoria real cuando se asiente).
  - DMS_DORMIDA (full-load ready 0%, nunca corrio a proposito): INFO/AVISO, nunca FALLA.

Para los jobs que FALLARON, clasifica el error leyendo el log del run con Logs
Insights (3 patrones validados con evidencia real).

Excluido a proposito: V2_SIVICC-migration-to-RDS NO es SIVICC (mueve datos de CNP a
Postgres con el rol compartido pon-prod-rnmc-migration-role). Se omite para no
monitorear el pipeline equivocado.

Es de SOLO LECTURA (glue:GetJobRuns, dms:DescribeReplicationTasks,
logs:StartQuery/GetQueryResults, sts). NO dispara nada.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algo fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_migracion_sivicc.py
    python3 monitor_migracion_sivicc.py --horas 48
    python3 monitor_migracion_sivicc.py --con-avisos   # muestra avisos e info
    python3 monitor_migracion_sivicc.py --sin-insights # omite clasificacion de errores
    python3 monitor_migracion_sivicc.py --no-export
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
BACKFILL_STALE_DAYS = float(os.environ.get("SIVICC_BACKFILL_STALE_DAYS", "90"))
GLUE_ERROR_LOG_GROUP = os.environ.get("GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error")

INSIGHTS_MAX_ESPERAS = int(os.environ.get("INSIGHTS_MAX_ESPERAS", "40"))
INSIGHTS_POLL_SEG = float(os.environ.get("INSIGHTS_POLL_SEG", "1.0"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Naturaleza de cada recurso. El criterio de severidad depende de esto.
# --------------------------------------------------------------------------------------
REGIMEN = "REGIMEN"            # ETL diario consolidado
BACKFILL = "BACKFILL"          # historico manual sin trigger
ESTATICO = "ESTATICO"          # ya terminado, debe quedar congelado
EXPERIMENTAL = "EXPERIMENTAL"  # en desarrollo activo, solo informativo
DMS_DORMIDA = "DMS_DORMIDA"    # full-load ready 0%, nunca corrio

# Jobs de Glue por naturaleza. JOB_NAME -> naturaleza.
JOBS_SIVICC = {
    "pon-prod-raw-sivicc-armamento-asignaciones-backfill": REGIMEN,
    "pon-prod-raw-sivicc-tamir-actividad-backfill": BACKFILL,
    "pon-prod-raw-sivicc-tamir-cumplimiento-staging-backfill": BACKFILL,
    # Experimental (creado 29-sep, en desarrollo activo). Solo informativo.
    "pon-prod-raw-sivicc-dashboard-incremental-scheduler": EXPERIMENTAL,
    "pon-prod-raw-sivicc-dashboard-incremental-job": EXPERIMENTAL,
}

# Carga estatica ya terminada: su crawler no deberia volver a correr.
CRAWLER_ESTATICO = "pon-prod-sivicc-historico-personas-crawler"
CRAWLER_ESTATICO_INFO = {"recurso": "historico_personas (S3 + crawler)",
                         "detalle": "carga unica estatica desde 2026-01-09"}

# Tarea DMS dormida (full-load ready 0%, nunca corrio a proposito).
DMS_DORMIDA_TASK = "pon-prod-sivicc-historico-persona-to-s3-full-load"

# Falso positivo a EXCLUIR explicitamente (es CNP, no SIVICC).
EXCLUIDO = "V2_SIVICC-migration-to-RDS"

# --------------------------------------------------------------------------------------
# Clasificacion de errores (3 patrones validados con evidencia real de SIVICC).
# --------------------------------------------------------------------------------------
PATRONES_ERROR = [
    (re.compile(r"java\.lang\.String cannot be cast to java\.lang\.Boolean"),
     ("Bug de configuracion/tipo (String casteado a Boolean)",
      "Revisar el parametro/config que espera booleano en el job.")),
    (re.compile(r"RuntimeError: La extracci[oó]n no contiene tamir_actividad_id o fecha"),
     ("Guard de datos faltantes (extraccion vacia o sin columnas clave)",
      "Verificar la fuente: la extraccion no trajo tamir_actividad_id/fecha.")),
    (re.compile(r"upgrading to Spark >= 3\.0|o\d+\.parquet"),
     ("Incompatibilidad de escritura Parquet (Spark < 3.0)",
      "Ajustar la version de Spark / opciones de escritura Parquet del job.")),
]

ESTADOS_FALLO = {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}
ESTADOS_EN_CURSO = {"RUNNING", "STARTING", "STOPPING", "WAITING"}

OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
INFO = "AVISO"  # INFO se agrupa con AVISO para severidad, pero se etiqueta aparte
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
        "| filter @message like /Error/ or @message like /Exception/ or @message like /RuntimeError/ "
        "or @message like /cannot be cast/ or @message like /parquet/\n"
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
# Evaluacion de un job segun su naturaleza
# --------------------------------------------------------------------------------------
def _ultimo_run(glue, job):
    resp = glue.get_job_runs(JobName=job, MaxResults=1)
    runs = resp.get("JobRuns", [])
    return runs[0] if runs else None


def _edad_dias(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0


def evaluar_job(glue, logs, job, naturaleza, horas, ventana_ms_ini, ventana_ms_fin):
    etiqueta = f"{job}"
    try:
        run = _ultimo_run(glue, job)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code == "EntityNotFoundException":
            record(etiqueta, WARN, naturaleza=naturaleza,
                   mensaje=f"El job '{job}' no existe (nombre por confirmar).",
                   ubicacion=f"Glue job '{job}'",
                   accion="Confirma el nombre del job en la consola de Glue.")
            return
        accion = ("Falta glue:GetJobRuns." if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa el nombre del job y la region.")
        record(etiqueta, FAIL, naturaleza=naturaleza,
               mensaje=f"No se pudo consultar el job: {_err_detail(exc)}",
               ubicacion=f"Glue job '{job}'", accion=accion)
        return

    # Experimental: solo informativo, nunca FALLA/AVISO por su estado.
    if naturaleza == EXPERIMENTAL:
        if run is None:
            msg = "EXPERIMENTAL (en desarrollo activo): sin corridas aun."
        else:
            estado = run.get("JobRunState", "?")
            started = run.get("StartedOn")
            st_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
            msg = (f"EXPERIMENTAL (en desarrollo activo, criterio de exito aun no definido). "
                   f"Ultimo run: {estado} ({st_txt}).")
        record(etiqueta, INFO, naturaleza=naturaleza, mensaje=msg,
               ubicacion=f"Glue job '{job}'",
               accion="No se evalua como fallo; promover a categoria real cuando se asiente.")
        return

    if run is None:
        if naturaleza == REGIMEN:
            record(etiqueta, WARN, naturaleza=naturaleza,
                   mensaje="ETL de regimen sin ejecuciones registradas.",
                   ubicacion=f"Glue job '{job}'", accion="Verifica el scheduler del job.")
        else:  # BACKFILL
            record(etiqueta, INFO, naturaleza=naturaleza,
                   mensaje="Backfill manual sin ejecuciones registradas (nunca corrio).",
                   ubicacion=f"Glue job '{job}'",
                   accion="Verifica si deberia haberse corrido manualmente.")
        return

    estado = run.get("JobRunState", "?")
    run_id = run.get("Id", "?")
    started = run.get("StartedOn")
    st_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
    edad = _edad_dias(started)
    fuera_ventana = bool(started and started.timestamp() * 1000 < ventana_ms_ini)

    # FALLO: aplica a TODAS las naturalezas (regimen y backfill).
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
        record(etiqueta, FAIL, naturaleza=naturaleza, mensaje=cuerpo,
               ubicacion=f"Glue job '{job}' run {run_id}", accion=accion, tipo_error=tipo_err)
        return

    if estado in ESTADOS_EN_CURSO:
        record(etiqueta, WARN, naturaleza=naturaleza,
               mensaje=f"Ultimo run en curso ({estado}, {st_txt}).",
               ubicacion=f"Glue job '{job}' run {run_id}",
               accion="Aun ejecutando; vuelve a revisar mas tarde.")
        return

    if estado == "SUCCEEDED":
        if naturaleza == REGIMEN:
            if fuera_ventana:
                record(etiqueta, WARN, naturaleza=naturaleza,
                       mensaje=f"ETL de regimen: ultimo run OK pero hace mas de {horas:g} h ({st_txt}).",
                       ubicacion=f"Glue job '{job}' run {run_id}",
                       accion="No corrio en la ventana esperada; revisa el scheduler.")
            else:
                record(etiqueta, OK, naturaleza=naturaleza,
                       mensaje=f"ETL de regimen: ultimo run OK ({st_txt}).",
                       ubicacion=f"Glue job '{job}' run {run_id}")
        else:  # BACKFILL
            # SUCCEEDED aunque sea viejo = OK; dato de antiguedad; AVISO solo si > stale.
            edad_txt = f"{edad:.0f} d" if edad is not None else "N/A"
            if edad is not None and edad > BACKFILL_STALE_DAYS:
                record(etiqueta, WARN, naturaleza=naturaleza,
                       mensaje=(f"Backfill manual: ultimo run OK pero hace {edad_txt} "
                                f"(supera {BACKFILL_STALE_DAYS:.0f} d). Posible backfill olvidado/abandonado."),
                       ubicacion=f"Glue job '{job}' run {run_id}",
                       accion="Confirma si el backfill historico sigue pendiente o ya se completo.")
            else:
                record(etiqueta, OK, naturaleza=naturaleza,
                       mensaje=f"Backfill manual: ultimo run OK ({st_txt}, hace {edad_txt}).",
                       ubicacion=f"Glue job '{job}' run {run_id}")
        return

    # Estado terminal no reconocido.
    record(etiqueta, WARN, naturaleza=naturaleza,
           mensaje=f"Ultimo run en estado {estado} ({st_txt}).",
           ubicacion=f"Glue job '{job}' run {run_id}",
           accion="Estado no reconocido; revisa el run.")


def check_jobs(horas):
    glue = aws_client("glue")
    logs = aws_client("logs")
    now = datetime.now(timezone.utc)
    ventana_ms_ini = int((now - timedelta(hours=horas)).timestamp() * 1000)
    ventana_ms_fin = int(now.timestamp() * 1000)

    total = len(JOBS_SIVICC)
    for i, (job, naturaleza) in enumerate(JOBS_SIVICC.items(), start=1):
        paso(f"Jobs SIVICC... ({i}/{total}) {naturaleza}")
        evaluar_job(glue, logs, job, naturaleza, horas, ventana_ms_ini, ventana_ms_fin)
    paso(f"Jobs SIVICC  ({total} revisados)", fin=True)


# --------------------------------------------------------------------------------------
# Carga estatica (crawler que no deberia volver a correr)
# --------------------------------------------------------------------------------------
def check_estatico(horas):
    glue = aws_client("glue")
    paso("Carga estatica (historico_personas)...")
    try:
        crawler = glue.get_crawler(Name=CRAWLER_ESTATICO).get("Crawler", {})
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code == "EntityNotFoundException":
            record(CRAWLER_ESTATICO_INFO["recurso"], INFO, naturaleza=ESTATICO,
                   mensaje=f"Crawler estatico '{CRAWLER_ESTATICO}' no encontrado (puede estar eliminado).",
                   ubicacion=f"Glue crawler '{CRAWLER_ESTATICO}'")
        else:
            record(CRAWLER_ESTATICO_INFO["recurso"], INFO, naturaleza=ESTATICO,
                   mensaje=f"No se pudo consultar el crawler estatico: {_err_detail(exc)}",
                   ubicacion=f"Glue crawler '{CRAWLER_ESTATICO}'")
        paso("Carga estatica  (no evaluada)", fin=True)
        return

    last = crawler.get("LastCrawl") or {}
    start = last.get("StartTime")
    edad = _edad_dias(start)
    st_txt = start.strftime("%Y-%m-%d %H:%M UTC") if start else "sin fecha"
    # Un crawl reciente sobre algo que deberia estar congelado es señal rara.
    if edad is not None and edad * 24 < horas:
        record(CRAWLER_ESTATICO_INFO["recurso"], WARN, naturaleza=ESTATICO,
               mensaje=(f"Carga estatica con CRAWL RECIENTE ({st_txt}). Deberia estar congelada "
                        f"desde 2026-01-09; un cambio aqui es señal rara, no buena."),
               ubicacion=f"Glue crawler '{CRAWLER_ESTATICO}'",
               accion="Verifica por que se movio algo que deberia estar terminado.")
    else:
        record(CRAWLER_ESTATICO_INFO["recurso"], OK, naturaleza=ESTATICO,
               mensaje=f"Estatica, sin cambios recientes (ultimo crawl {st_txt}).",
               ubicacion=f"Glue crawler '{CRAWLER_ESTATICO}'")
    paso("Carga estatica  (OK)", fin=True)


# --------------------------------------------------------------------------------------
# Tarea DMS dormida (solo informativo)
# --------------------------------------------------------------------------------------
def check_dms_dormida():
    dms = aws_client("dms")
    paso("Tarea DMS dormida...")
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
        record("DMS historico-persona-to-s3", INFO, naturaleza=DMS_DORMIDA,
               mensaje=f"No se pudo consultar DMS: {_err_detail(exc)}",
               ubicacion=f"DMS (region {REGION})")
        paso("Tarea DMS dormida  (no evaluada)", fin=True)
        return

    t = next((x for x in tasks if x.get("ReplicationTaskIdentifier") == DMS_DORMIDA_TASK), None)
    if t is None:
        record("DMS historico-persona-to-s3", INFO, naturaleza=DMS_DORMIDA,
               mensaje=f"Tarea dormida '{DMS_DORMIDA_TASK}' no encontrada.",
               ubicacion=f"DMS (region {REGION})")
    else:
        status = t.get("Status", "?")
        pct = t.get("ReplicationTaskStats", {}).get("FullLoadProgressPercent", 0)
        record("DMS historico-persona-to-s3", INFO, naturaleza=DMS_DORMIDA,
               mensaje=(f"Full-load DORMIDA: estado {status}, progreso {pct}%. "
                        "Nunca se ha corrido (a proposito). No es un fallo."),
               ubicacion=f"DMS task '{DMS_DORMIDA_TASK}'",
               accion="Si deberia migrar, dispararla manualmente; si no, dejar dormida.")
    paso("Tarea DMS dormida  (informativo)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — MIGRACION / BACKFILLS SIVICC")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso/info | {fail} falla")

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
        print("\n  Todo OK en la migracion/backfills de SIVICC.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s)/info ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"migracion_sivicc_{ts}.csv")
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
        description="Estado de la migracion/backfills de SIVICC (clasificacion por naturaleza del recurso).")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana en horas para los jobs de regimen (default {DEFAULT_HORAS:g}).")
    parser.add_argument("--sin-insights", action="store_true",
                        help="Omite la clasificacion de errores por Logs Insights.")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien AVISOS e INFO (backfills, estatico, experimental, DMS dormida).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE, SIN_INSIGHTS
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado
    SIN_INSIGHTS = args.sin_insights

    print("=" * 78)
    print("MONITOR DE MIGRACION / BACKFILLS SIVICC (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana regimen {args.horas:g} h  |  Backfill stale {BACKFILL_STALE_DAYS:.0f} d")
    print("  Clasificacion por naturaleza: regimen / backfill / estatico / experimental / DMS dormida.")
    print(f"  Excluido a proposito: {EXCLUIDO} (es CNP, no SIVICC).\n")

    if not check_identity():
        print_summary()
        return 1

    check_jobs(args.horas)
    check_estatico(args.horas)
    check_dms_dormida()

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
