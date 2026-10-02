#!/usr/bin/env python3
"""
monitor_dms.py — Estado de las tareas de replicacion DMS.
=========================================================
Corre DIARIO. Revisa las replication tasks de DMS, detecta las que fallaron o
estan detenidas, clasifica el error y lo resume. **Solo lectura.**

Flujo (mismo esquema que los scripts A y B):
  1. Identidad AWS (con que rol se ejecuta).
  2. Estado de todas las replication tasks (dms:DescribeReplicationTasks):
     las FAILED / STOPPED se analizan; las RUNNING/otras se cuentan como OK.
  3. Clasificacion del error de cada tarea con problema (tipo + severidad + accion),
     y busqueda de los secretos relacionados (por sigla) con su antiguedad.

NO revisa jobs de Glue (script B) ni hace ping a las bases (script A).

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si alguna tarea esta en CRITICAL/HIGH, 0 si todo bien, 2 si args invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_dms.py
    python3 monitor_dms.py --con-avisos   # incluye INFO (paradas manuales, full load fin)
    python3 monitor_dms.py --no-export
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SECRET_AGE_WARN_DAYS = float(os.environ.get("SECRET_AGE_WARN_DAYS", "365"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# Siglas de las bases/unidades para relacionar tareas con secretos (del script original).
SIGLAS = [
    "deara", "deama", "deant", "deata", "debol", "deboy",
    "decal", "decaq", "decas", "decau", "deces", "decho",
    "decor", "decun", "degun", "degua", "deguv", "demet",
    "dequi", "denar", "denor", "demam", "demag", "depuy",
    "deris", "desan", "desap", "desuc", "detol", "deuil",
    "deura", "deval", "devau", "devic",
    "mebuc", "mecar", "mecal", "mebar", "mecuc", "mepas",
    "menev", "memaz", "memot", "mepoy", "mesan", "metib",
    "mevil", "meper", "metun",
    "rnmc", "ali", "siedco", "disec", "geopol", "telepol",
    "sivicc", "cetus", "dboper",
]

# --------------------------------------------------------------------------------------
# Severidad / estados de salida. Mapeamos la severidad del clasificador DMS
# (CRITICAL/HIGH/MEDIUM/INFO) a los 3 estados comunes OK/AVISO/FALLA.
# --------------------------------------------------------------------------------------
OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
ICON = {OK: "[ OK  ]", WARN: "[AVISO]", FAIL: "[FALLA]"}

# CRITICAL/HIGH -> FALLA ; MEDIUM -> AVISO ; INFO -> AVISO (informativo)
SEVERIDAD_A_ESTADO = {
    "CRITICAL": FAIL,
    "HIGH": FAIL,
    "MEDIUM": WARN,
    "INFO": WARN,
}

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


def record(recurso, estado, mensaje="", ubicacion="", accion="", severidad=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "severidad_dms": severidad, "mensaje": mensaje,
        "accion_sugerida": accion if estado != OK else "",
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


def get_segments(name):
    return name.lower().replace("/", "-").replace("_", "-").split("-")


# --------------------------------------------------------------------------------------
# Clasificacion del error de una tarea DMS (logica del script original).
# --------------------------------------------------------------------------------------
def classify_error(task):
    msg = task.get("LastFailureMessage", "").lower()
    status = task.get("Status", "").lower()
    stop_reason = task.get("StopReason", "").lower()

    if not msg:
        if status == "stopped" and "full_load" in stop_reason:
            return ("FULL_LOAD_FINISHED", "INFO", "Carga completa finalizada. No requiere accion.")
        if status == "stopped":
            return ("MANUAL_STOP", "INFO", "Verificar si fue detenida intencionalmente.")
        return ("UNKNOWN", "MEDIUM", "Revisar CloudWatch Logs.")

    if "out of memory" in msg or "memory" in msg:
        return ("RESOURCE_EXHAUSTION", "HIGH", "Aumentar clase de instancia o reducir tablas por tarea.")
    if "oci connection" in msg or "oracle" in msg or "ora-" in msg:
        return ("ORACLE_CONNECTION", "HIGH", "Verificar listener Oracle, credenciales y red.")
    if any(k in msg for k in ("password", "credential", "authentication", "login", "access denied", "logon")):
        return ("AUTHENTICATION", "CRITICAL", "Rotar credenciales en Secrets Manager y actualizar endpoint.")
    if any(k in msg for k in ("connection", "timeout", "network", "unreachable", "refused", "socket")):
        return ("CONNECTIVITY", "HIGH", "Revisar Security Groups, Route Tables y firewall.")
    if any(k in msg for k in ("schema", "table", "column", "not found", "does not exist")):
        return ("SCHEMA_MISMATCH", "MEDIUM", "Validar schemas origen vs destino.")
    if any(k in msg for k in ("data", "validation", "constraint", "duplicate", "truncat")):
        return ("DATA_INTEGRITY", "MEDIUM", "Revisar reglas de transformacion y constraints.")
    if "recovery attempts" in msg:
        if "fatal" in msg or "fatal" in stop_reason:
            return ("FATAL_AFTER_RETRIES", "CRITICAL", "Revisar CloudWatch Logs para causa raiz.")
        return ("RECOVERABLE_EXHAUSTED", "HIGH", "Reiniciar tarea; si persiste, revisar logs.")
    return ("UNKNOWN", "MEDIUM", "Revisar CloudWatch Logs.")


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
# Cache de secretos + relacion tarea -> secretos por sigla
# --------------------------------------------------------------------------------------
_secrets_cache = None


def _load_secrets(sm):
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache
    out = []
    token = None
    while True:
        params = {"NextToken": token} if token else {}
        resp = sm.list_secrets(**params)
        out.extend(resp.get("SecretList", []))
        token = resp.get("NextToken")
        if not token:
            break
    _secrets_cache = out
    return out


def find_related_secrets(sm, task_id):
    try:
        secrets = _load_secrets(sm)
    except Exception:  # noqa: BLE001
        return []
    task_segs = get_segments(task_id)
    task_siglas = [s for s in SIGLAS if s in task_segs]
    if not task_siglas:
        return []
    results = []
    for secret in secrets:
        segs = get_segments(secret.get("Name", ""))
        for sigla in task_siglas:
            if sigla in segs:
                age = None
                lc = secret.get("LastChangedDate")
                if lc:
                    age = (datetime.now(timezone.utc) - lc.replace(tzinfo=timezone.utc)).days
                results.append({"name": secret.get("Name", ""), "age_days": age, "sigla": sigla.upper()})
                break
    return results


# --------------------------------------------------------------------------------------
# Estado de las tareas DMS
# --------------------------------------------------------------------------------------
def check_dms_tasks():
    dms = aws_client("dms")
    sm = aws_client("secretsmanager")

    paso("Listando tareas DMS...")
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
        accion = ("Falta el permiso dms:DescribeReplicationTasks."
                  if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa la region y los permisos de DMS.")
        record("Tareas DMS", FAIL,
               mensaje=f"No se pudieron listar las tareas: {_err_detail(exc)}",
               ubicacion=f"DMS (region {REGION})", accion=accion)
        paso("Tareas DMS  (FALLA al listar)", fin=True)
        return

    total = len(tasks)
    problematicas = [t for t in tasks if t.get("Status", "").lower() in ("failed", "stopped")]
    ok_count = total - len(problematicas)

    # Registro informativo del panorama.
    if ok_count:
        record(f"{ok_count} tarea(s) sin problemas", OK,
               mensaje=f"De {total} tareas DMS, {ok_count} estan corriendo/OK.",
               ubicacion=f"DMS (region {REGION})")

    for idx, task in enumerate(problematicas, start=1):
        task_id = task.get("ReplicationTaskIdentifier", "UNKNOWN")
        paso(f"Analizando tareas con problema... ({idx}/{len(problematicas)}) {task_id}")
        status = task.get("Status", "?")
        last_error = task.get("LastFailureMessage", "") or "(sin mensaje)"
        tipo, sev, accion = classify_error(task)
        estado = SEVERIDAD_A_ESTADO.get(sev, WARN)

        related = find_related_secrets(sm, task_id)
        sec_txt = ""
        for s in related:
            edad = f"{s['age_days']}d" if s["age_days"] is not None else "N/A"
            crit = " [>365d]" if s["age_days"] and s["age_days"] > SECRET_AGE_WARN_DAYS else ""
            sec_txt += f" | Secret {s['name']} [{s['sigla']}] ({edad}{crit})"

        cuerpo = f"Estado {status.upper()} | Tipo {tipo} [{sev}]. {last_error[:150]}{sec_txt}"
        record(task_id, estado, mensaje=cuerpo,
               ubicacion=f"DMS task '{task_id}' (region {REGION})",
               accion=accion, severidad=sev)

    n_prob = sum(1 for r in RESULTS if r["estado"] != OK and r["recurso"] != "Identidad AWS")
    paso(f"Tareas DMS  ({total} totales, {len(problematicas)} con problema, {n_prob} aviso/falla)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — ESTADO DE TAREAS DMS")
    print("=" * 78)
    print(f"  TOTAL: {len(RESULTS)} verificaciones | {ok} OK | {warn} aviso | {fail} falla")

    # Agrupacion por tipo de error (solo tareas con problema).
    por_tipo = {}
    for r in RESULTS:
        if r["severidad_dms"]:
            por_tipo.setdefault(r["severidad_dms"], []).append(r)

    niveles = [FAIL] if not CON_AVISOS else [FAIL, WARN]
    if any(r["estado"] in niveles for r in RESULTS):
        titulo = "Detalle de lo CRITICO (tareas fallidas):" if not CON_AVISOS else "Detalle:"
        print(f"\n  {titulo}")
        for nivel in niveles:
            for r in RESULTS:
                if r["estado"] == nivel and r["recurso"] != "Identidad AWS":
                    print(f"\n  {ICON[nivel]} {r['recurso']}")
                    print(f"       que pasa : {r['mensaje']}")
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Todas las tareas DMS estan OK.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"dms_{ts}.csv")
    cols = ["recurso", "ubicacion", "estado", "severidad_dms", "mensaje", "accion_sugerida"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(RESULTS)
    print(f"\nReporte CSV: {csv_path}")
    home = os.path.expanduser("~")
    if os.path.commonpath([csv_path, home]) == home:
        print(f"Descarga (CloudShell Actions -> Download file): {os.path.relpath(csv_path, home)}")


def main():
    parser = argparse.ArgumentParser(description="Estado de las tareas de replicacion DMS.")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien AVISOS (MEDIUM/INFO: paradas manuales, full load fin, schema, etc.).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE TAREAS DMS (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}\n")

    if not check_identity():
        print_summary()
        return 1
    check_dms_tasks()

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
