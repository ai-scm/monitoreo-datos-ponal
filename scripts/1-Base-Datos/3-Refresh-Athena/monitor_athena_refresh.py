#!/usr/bin/env python3
"""
monitor_athena_refresh.py — Estado del "refresh" de las bases Athena (via crawlers Glue).
=========================================================================================
Corre DIARIO. En esta cuenta NO hay refresh nativo programado de Athena (ni named
queries reales, ni CTAS/MSCK en un cron de Athena). El "refresh" que de verdad
mantiene actualizadas las bases Athena pon-prod-raw-* es el **crawl de Glue** de
los crawlers de la familia raw (pon-prod-raw-*_discoverer y afines).

De los 68 crawlers del inventario, se monitorean 12 (11 fijos + sivicc toggleable);
los otros 56 quedan fuera a proposito (migraciones, BI/reporting, apps de negocio,
PBX, GPS, pruebas, downstream de otros equipos).

Por eso este script, en su v1, verifica el **estado del ultimo crawl** de esos
crawlers (get_crawler -> LastCrawl):
  1. Identidad AWS (con que rol se ejecuta).
  2. Para cada crawler del mapa fijo: Status + StartTime del ultimo crawl.
     - SUCCEEDED dentro de la ventana (--horas, 24 por defecto) -> OK
     - SUCCEEDED fuera de ventana                              -> AVISO
     - FAILED / CANCELLED                                      -> FALLA
     - RUNNING ahora mismo                                     -> AVISO
     - crawler no encontrado (nombre por confirmar)            -> AVISO

Es de SOLO LECTURA (glue:GetCrawler, sts). NO dispara crawls ni queries.

Fuera de v1 (anotado como pendiente):
  - Pipeline Iceberg de sivicc (CTAS+MERGE manual, sin orquestador): no se verifica.
  - Refresh de la particion de "homicidios" disparado por evento S3: no se verifica.
  - prod-data-reload-database NO es Athena (es Postgres/Oracle), queda fuera.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algun crawler fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_athena_refresh.py                 # ultimas 24 h
    python3 monitor_athena_refresh.py --horas 48
    python3 monitor_athena_refresh.py --con-avisos    # muestra tambien avisos
    python3 monitor_athena_refresh.py --sin-sivicc    # omite el crawler extra de sivicc
    python3 monitor_athena_refresh.py --no-export
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_HORAS = float(os.environ.get("ATHENA_REFRESH_WINDOW_HOURS", "24"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Mapa fijo de crawlers monitoreados (dominio del pipeline raw). Son 11 fijos aqui +
# sivicc como extra toggleable (--sin-sivicc) = 12 en total. El inventario completo
# de la cuenta tiene 68 crawlers; los otros 56 quedan fuera a proposito (migraciones,
# BI/reporting, apps de negocio, PBX, GPS, pruebas, downstream de otros equipos).
#
# Notas:
#  - rnmc: el crawler real es el v1 (_discoverer), base pon_prod_raw_comparendo. El
#    _v2 (pon_prod_raw_comparendo_v2) es scaffolding abandonado y NO se monitorea.
#  - simep: suele estar "never run"; se reporta como AVISO (sin crawls registrados).
#  - cetus y secad: CONFIRMADOS en la cuenta (secad con nombre irregular pero real,
#    cron diario). Ambos existen y se monitorean.
#  - ali: en barridos recientes aparecio FAILED mientras antes estaba SUCCEEDED. El
#    script lee el estado REAL en vivo cada corrida; el veredicto lo da la ejecucion.
#  - pon-prod-raw-simep-s3: FUERA (cataloga salida de simep para xcrime/ML, otro dueño).
# --------------------------------------------------------------------------------------
CRAWLERS = {
    "pon-prod-raw-ali_discoverer": {"base": "pon-prod-raw-ali", "job": "pon-prod-raw-ali_etl"},
    "pon-prod-raw-cnp_discoverer": {"base": "pon-prod-raw-cnp", "job": "pon-prod-raw-cnp_etl"},
    "pon-prod-raw-dboper_discoverer": {"base": "pon-prod-raw-dboper", "job": "pon-prod-raw-dboper_etl"},
    "pon-prod-raw-disec_discoverer": {"base": "pon-prod-raw-disec", "job": "pon-prod-raw-disec_etl"},
    "pon-prod-raw-geopol_discoverer": {"base": "pon-prod-raw-geopol", "job": "pon-prod-raw-geopol_etl"},
    "pon-prod-raw-rnmc-incremental_discoverer": {"base": "pon_prod_raw_comparendo", "job": "pon-prod-raw-rnmc-incremental_etl"},
    "pon-prod-raw-siedco_discoverer": {"base": "pon_prod_siedco_raw", "job": "pon-prod-raw-siedco_etl"},
    "pon-prod-raw-simep_discoverer": {"base": "pon-prod-raw-simep", "job": "pon-prod-raw-simep_etl"},
    "pon-prod-raw-telepol_discoverer": {"base": "pon-prod-raw-telepol", "job": "pon-prod-raw-telepol_etl"},
    "pon-prod-raw-cetus_discoverer": {"base": "pon-prod-raw-cetus", "job": "(familia raw)"},
    "pon-prod-secad-crawler": {"base": "pon-prod-raw-secad", "job": "(secad, cron diario)"},
}

# Crawler extra (familia raw, 9no por patron de nombre). Se vigila salvo --sin-sivicc.
CRAWLER_SIVICC = "pon-prod-raw-sivicc_discoverer"
CRAWLER_SIVICC_INFO = {"base": "pon-prod-raw-sivicc", "job": "(pipeline Iceberg manual)"}

# Estados de un crawl de Glue.
ESTADOS_FALLO = {"FAILED", "CANCELLED"}

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
# Estado del ultimo crawl de cada crawler
# --------------------------------------------------------------------------------------
def _evaluar_crawler(glue, crawler_name, info, horas, ventana_ini):
    base = info["base"]
    job = info["job"]
    etiqueta = f"{base}  ({crawler_name})"
    try:
        resp = glue.get_crawler(Name=crawler_name)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code in ("EntityNotFoundException",):
            record(etiqueta, WARN,
                   mensaje=f"El crawler '{crawler_name}' no existe (nombre por confirmar).",
                   ubicacion=f"Glue crawler '{crawler_name}'",
                   accion=f"Confirma el nombre real del crawler de {base} en la consola de Glue.")
            return
        if code in ("AccessDeniedException", "AccessDenied"):
            accion = "Falta el permiso glue:GetCrawler."
        else:
            accion = "Revisa el nombre del crawler y la region."
        record(etiqueta, FAIL,
               mensaje=f"No se pudo consultar el crawler: {_err_detail(exc)}",
               ubicacion=f"Glue crawler '{crawler_name}'", accion=accion)
        return

    crawler = resp.get("Crawler", {})
    estado_crawler = crawler.get("State", "?")  # READY / RUNNING / STOPPING
    last = crawler.get("LastCrawl") or {}
    last_status = last.get("Status", "")          # SUCCEEDED / FAILED / CANCELLED
    start = last.get("StartTime")
    start_txt = start.strftime("%Y-%m-%d %H:%M UTC") if start else "sin fecha"
    fuera_ventana = bool(start and start < ventana_ini)

    # Crawler ejecutandose en este momento.
    if estado_crawler == "RUNNING":
        record(etiqueta, WARN,
               mensaje=f"El crawler esta RUNNING ahora mismo (ultimo crawl: {last_status or 'N/A'}, {start_txt}).",
               ubicacion=f"Glue crawler '{crawler_name}' | job {job}",
               accion="Aun ejecutando; vuelve a revisar mas tarde.")
        return

    if not last_status:
        record(etiqueta, WARN,
               mensaje="El crawler no tiene crawls registrados.",
               ubicacion=f"Glue crawler '{crawler_name}' | job {job}",
               accion="Verifica si el crawler deberia haber corrido.")
        return

    if last_status == "SUCCEEDED":
        msg = f"Ultimo crawl OK ({start_txt})."
        if fuera_ventana:
            msg += f" OJO: fue hace mas de {horas:g} h; puede que no se haya refrescado en la ventana."
            record(etiqueta, WARN, mensaje=msg,
                   ubicacion=f"Glue crawler '{crawler_name}' | base {base}",
                   accion="Verifica el disparador del crawl; no refresco en la ventana esperada.")
        else:
            record(etiqueta, OK, mensaje=msg,
                   ubicacion=f"Glue crawler '{crawler_name}' | base {base}")
    elif last_status in ESTADOS_FALLO:
        err_msg = (last.get("ErrorMessage") or "").strip() or "(sin ErrorMessage)"
        record(etiqueta, FAIL,
               mensaje=f"Ultimo crawl {last_status} ({start_txt}). Motivo: {err_msg}",
               ubicacion=f"Glue crawler '{crawler_name}' | base {base}",
               accion="Revisa ese crawl en la consola de Glue / CloudWatch Logs.")
    else:
        record(etiqueta, WARN,
               mensaje=f"Ultimo crawl en estado {last_status} ({start_txt}).",
               ubicacion=f"Glue crawler '{crawler_name}' | base {base}",
               accion="Estado no terminal reconocido; revisa el crawl.")


def check_crawlers(horas, con_sivicc):
    glue = aws_client("glue")
    now = datetime.now(timezone.utc)
    ventana_ini = now - timedelta(hours=horas)

    objetivos = dict(CRAWLERS)
    if con_sivicc:
        objetivos[CRAWLER_SIVICC] = CRAWLER_SIVICC_INFO

    total = len(objetivos)
    for i, (crawler_name, info) in enumerate(objetivos.items(), start=1):
        paso(f"Crawlers... ({i}/{total}) {info['base']}")
        _evaluar_crawler(glue, crawler_name, info, horas, ventana_ini)

    n_prob = sum(1 for r in RESULTS if r["estado"] != OK and r["recurso"] != "Identidad AWS")
    paso(f"Crawlers  ({total} revisados, {n_prob} con aviso/falla)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — REFRESH DE BASES ATHENA (crawlers Glue)")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso | {fail} falla")

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO (crawlers fallidos):" if not CON_AVISOS else "Detalle:"
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
        print("\n  Todas las bases se refrescaron OK en la ventana.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"athena_refresh_{ts}.csv")
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
    parser = argparse.ArgumentParser(
        description="Estado del refresh de las bases Athena (via el ultimo crawl de los crawlers Glue).")
    parser.add_argument("--horas", type=float, default=DEFAULT_HORAS,
                        help=f"Ventana en horas (por defecto {DEFAULT_HORAS:g}; env ATHENA_REFRESH_WINDOW_HOURS).")
    parser.add_argument("--sin-sivicc", action="store_true",
                        help="No vigila el crawler extra pon-prod-raw-sivicc_discoverer.")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien los AVISOS (RUNNING, sin crawls, fuera de ventana, nombre por confirmar).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE REFRESH ATHENA — CRAWLERS GLUE (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}  |  Ventana {args.horas:g} h")
    print("  v1: verifica el ultimo crawl de los crawlers *_discoverer. "
          "sivicc (Iceberg) y homicidios (S3) quedan fuera.\n")

    if not check_identity():
        print_summary()
        return 1
    check_crawlers(args.horas, con_sivicc=not args.sin_sivicc)

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
