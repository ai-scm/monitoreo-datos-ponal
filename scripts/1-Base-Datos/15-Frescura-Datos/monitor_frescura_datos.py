#!/usr/bin/env python3
"""
monitor_frescura_datos.py — Frescura de los datos en las bases Athena (BLOQUEADO v1).
=====================================================================================
Corre DIARIO. Verifica que los DATOS (no solo el crawl) esten frescos: consulta
MAX(columna_de_fecha) por tabla via Athena y compara contra un umbral de dias.

>>> ESTADO: BLOQUEADO. Este script NO puede correr todavia. <<<
Requiere un permiso IAM que el rol de monitoreo NO tiene hoy:
    athena:StartQueryExecution, athena:GetQueryExecution, athena:GetQueryResults
    + escritura S3 en el bucket de resultados de Athena (ATHENA_OUTPUT_S3).
Mientras no se conceda, el script detecta la falta de permiso y sale con un mensaje
claro (no da falsos OK). El codigo esta listo para activarse en cuanto se otorgue.

Diferencia con monitor_athena_refresh.py (punto 3): aquel verifica "¿el crawler
corrio?" (metadata del catalogo). ESTE verifica "¿los datos estan frescos?"
(contenido real, MAX(fecha)). Son preguntas distintas; por eso es un punto aparte.

ALCANCE v1 — SOLO 4 columnas de ALTA confianza (confirmadas con evidencia real).
Las otras 8 bases quedan FUERA hasta validacion con el equipo de datos (ver abajo y
el README): escribir SQL con un nombre de columna inventado daria falsos OK o fallos
silenciosos, que es justo lo que se evita.

Es de SOLO LECTURA sobre los datos (ejecuta SELECT MAX(...); no modifica nada), pero
Athena escribe el resultado de la query en S3 (comportamiento normal de Athena).

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algo esta rancio/fallo, 0 si todo fresco, 2 args, 3 si falta permiso.

Uso en CloudShell (cuando se conceda el permiso):
    export AWS_DEFAULT_REGION=us-east-1
    export ATHENA_OUTPUT_S3=s3://<bucket-resultados-athena>/monitoreo/
    python3 monitor_frescura_datos.py
    python3 monitor_frescura_datos.py --dias 2        # umbral de rancidez
    python3 monitor_frescura_datos.py --con-avisos
    python3 monitor_frescura_datos.py --no-export
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
# Umbral de rancidez por defecto (dias). Si MAX(fecha) es mas viejo -> rancio.
DEFAULT_DIAS = float(os.environ.get("FRESCURA_MAX_DIAS", "2"))
# Bucket de resultados de Athena (OBLIGATORIO para correr). Sin esto no hay query.
ATHENA_OUTPUT_S3 = os.environ.get("ATHENA_OUTPUT_S3", "")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

ATHENA_MAX_ESPERAS = int(os.environ.get("ATHENA_MAX_ESPERAS", "30"))
ATHENA_POLL_SEG = float(os.environ.get("ATHENA_POLL_SEG", "1.0"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# MAPA DE FRESCURA — SOLO las 4 columnas de ALTA confianza (confirmadas).
# Cada entrada: clave legible -> database, tabla, columna de fecha, y como castear.
# "cast": como convertir la columna a fecha comparable en la query.
#   - "timestamp": la columna ya es timestamp/date -> se usa directo.
#   - "parse_fecha": la columna es STRING -> date_parse con formato (ver fmt).
# --------------------------------------------------------------------------------------
FRESCURA = {
    "SIEDCO": {
        "database": "pon_prod_siedco_raw",
        "tabla": "raw_siedco_usr_esta_motivospolicia",
        "columna": "fecha_hecho",
        "cast": "parse_fecha",
        "fmt": "%Y-%m-%d",   # AJUSTAR al formato real del string si difiere.
        "nota": "fecha_hecho es STRING, no timestamp: requiere date_parse/CAST.",
    },
    "SECAD": {
        "database": "pon-prod-raw-secad",
        "tabla": "cadpedidos",
        "columna": "fecha_grabacion",   # PENDIENTE confirmar: fech_caso vs fecha_grabacion.
        "cast": "timestamp",
        "fmt": "",
        "nota": "Confirmar cual es la fecha de negocio: fech_caso o fecha_grabacion.",
    },
    "ALI": {
        "database": "pon-prod-raw-ali",
        "tabla": "data_etiquetada",
        "columna": "fecha_grabacion",
        "cast": "timestamp",
        "fmt": "",
        "nota": "Timestamp real; recordCount confirmado.",
    },
    "SIMEP": {
        "database": "pon-prod-raw-simep",
        "tabla": "v_simep_medidas",       # VISTA (mas fresca que las tablas CDC crudas).
        "columna": "fecha_expedicion",    # Alternativa: fecha_medida.
        "cast": "timestamp",
        "fmt": "",
        "nota": "Se mide la VISTA v_simep_medidas (mas fresca). Decidir vista vs cruda.",
    },
}

# Bases DOCUMENTADAS como PENDIENTES de confirmar (NO se consultan en v1). Solo
# informativo: para que quede claro por que no estan y que falta para incluirlas.
PENDIENTES = {
    "CNP / DBOPER / RNMC(comparendo)": "Las tres apuntan a cnp_hechos; posible verificacion compartida. Decision de arquitectura.",
    "DISEC / SIVICC / GEOPOL": "Tabla primaria inferida por conteo de columnas, no por tamano real. Puede estar equivocada.",
    "TELEPOL": "Volcado estatico de catalogo Oracle Discoverer 2024. NO es candidato de frescura (solo '¿crawleo?').",
    "GEOPOL (detalle)": "158/159 tablas son metadata de replicacion GIS (SDE). Solo geo_cartografia_* tiene fecha de negocio, sin crawl desde ago-2024.",
}

OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
ICON = {OK: "[ OK  ]", WARN: "[AVISO]", FAIL: "[FALLA]"}

# Exit code especial cuando falta el permiso de Athena (bloqueado, no es un fallo de datos).
EXIT_BLOQUEADO = 3

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
# Athena: ejecutar una query y traer el resultado
# --------------------------------------------------------------------------------------
def _athena_query(athena, sql, database):
    """Ejecuta una query Athena y devuelve (filas, error). filas = lista de listas."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(ATHENA_MAX_ESPERAS):
        est = athena.get_query_execution(QueryExecutionId=qid)
        state = est["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                motivo = est["QueryExecution"]["Status"].get("StateChangeReason", state)
                return None, motivo
            res = athena.get_query_results(QueryExecutionId=qid)
            filas = []
            for row in res["ResultSet"]["Rows"]:
                filas.append([c.get("VarCharValue", "") for c in row["Data"]])
            return filas, None
        time.sleep(ATHENA_POLL_SEG)
    return None, f"timeout tras {ATHENA_MAX_ESPERAS * ATHENA_POLL_SEG:g}s"


def _sql_max_fecha(info):
    """Construye el SELECT MAX(...) segun el tipo de columna."""
    col = info["columna"]
    tabla = info["tabla"]
    if info["cast"] == "parse_fecha":
        fmt = info.get("fmt", "%Y-%m-%d")
        # date_parse para columnas string.
        expr = f'date_parse("{col}", \'{fmt}\')'
    else:
        # Columna ya timestamp/date.
        expr = f'CAST("{col}" AS timestamp)'
    return f'SELECT max({expr}) AS max_fecha FROM "{tabla}"'


# --------------------------------------------------------------------------------------
# Verificacion de permiso Athena (bloqueante)
# --------------------------------------------------------------------------------------
def check_permiso_athena(athena):
    """
    Confirma que se puede usar Athena antes de intentar las queries de frescura.
    Devuelve True si hay permiso y bucket; False si esta BLOQUEADO (y lo registra).
    """
    paso("Permiso Athena...")
    if not ATHENA_OUTPUT_S3:
        record("Permiso Athena", FAIL,
               mensaje="Falta ATHENA_OUTPUT_S3 (bucket de resultados). El script no puede correr.",
               ubicacion="config",
               accion="Exporta ATHENA_OUTPUT_S3=s3://<bucket>/ruta/ antes de correr.")
        paso("Permiso Athena  (BLOQUEADO: sin bucket de resultados)", fin=True)
        return False
    # Prueba minima: una query trivial. Si falta el permiso, StartQueryExecution falla.
    try:
        filas, error = _athena_query(athena, "SELECT 1", "default")
        if error:
            record("Permiso Athena", FAIL,
                   mensaje=f"La query de prueba fallo: {error}",
                   ubicacion=f"Athena workgroup {ATHENA_WORKGROUP}",
                   accion="Revisa el permiso athena:* y la escritura en ATHENA_OUTPUT_S3.")
            paso("Permiso Athena  (BLOQUEADO)", fin=True)
            return False
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        if code in ("AccessDeniedException", "AccessDenied"):
            record("Permiso Athena", FAIL,
                   mensaje=("BLOQUEADO: el rol no tiene athena:StartQueryExecution "
                            "(+ GetQueryExecution/GetQueryResults + escritura S3)."),
                   ubicacion=f"Athena workgroup {ATHENA_WORKGROUP}",
                   accion=("Concede athena:StartQueryExecution/GetQueryExecution/GetQueryResults "
                           "y s3:PutObject en el bucket de resultados. Luego reintenta."))
        else:
            record("Permiso Athena", FAIL,
                   mensaje=f"No se pudo usar Athena: {_err_detail(exc)}",
                   ubicacion=f"Athena workgroup {ATHENA_WORKGROUP}",
                   accion="Revisa permisos/region/workgroup de Athena.")
        paso("Permiso Athena  (BLOQUEADO)", fin=True)
        return False
    paso("Permiso Athena  (OK)", fin=True)
    return True


# --------------------------------------------------------------------------------------
# Frescura por tabla
# --------------------------------------------------------------------------------------
def check_frescura(dias):
    athena = aws_client("athena")

    if not check_permiso_athena(athena):
        return False  # bloqueado

    now = datetime.now(timezone.utc)
    total = len(FRESCURA)
    for i, (clave, info) in enumerate(FRESCURA.items(), start=1):
        paso(f"Frescura... ({i}/{total}) {clave}")
        db = info["database"]
        tabla = info["tabla"]
        ubic = f"Athena {db}.{tabla} (col {info['columna']})"
        sql = _sql_max_fecha(info)
        try:
            filas, error = _athena_query(athena, sql, db)
        except Exception as exc:  # noqa: BLE001
            record(clave, FAIL, mensaje=f"Error ejecutando la query: {_err_detail(exc)}",
                   ubicacion=ubic, accion="Revisa el nombre de columna/tabla y el permiso Athena.")
            continue
        if error:
            record(clave, FAIL, mensaje=f"La query fallo: {error}",
                   ubicacion=ubic,
                   accion=f"Verifica columna '{info['columna']}' y formato. {info.get('nota','')}")
            continue
        # filas[0] = encabezado, filas[1] = valor.
        valor = filas[1][0] if len(filas) > 1 and filas[1] else ""
        if not valor:
            record(clave, WARN, mensaje="MAX(fecha) vacio (tabla sin filas o columna nula).",
                   ubicacion=ubic, accion=f"Verifica la tabla. {info.get('nota','')}")
            continue
        # Parsear el valor devuelto (Athena da 'YYYY-MM-DD HH:MM:SS.sss' o 'YYYY-MM-DD').
        max_dt = _parse_athena_ts(valor)
        if max_dt is None:
            record(clave, WARN, mensaje=f"No se pudo interpretar MAX(fecha)='{valor}'.",
                   ubicacion=ubic, accion="Revisa el formato de la columna de fecha.")
            continue
        edad_dias = (now - max_dt).total_seconds() / 86400.0
        if edad_dias > dias:
            record(clave, FAIL,
                   mensaje=f"Datos RANCIOS: ultimo registro {max_dt:%Y-%m-%d} (hace {edad_dias:.1f} d, umbral {dias:g}).",
                   ubicacion=ubic,
                   accion="Verifica el pipeline de ingesta de esta base; no hay datos recientes.")
        else:
            record(clave, OK,
                   mensaje=f"Fresco: ultimo registro {max_dt:%Y-%m-%d} (hace {edad_dias:.1f} d).",
                   ubicacion=ubic)
    paso(f"Frescura  ({total} tablas de alta confianza)", fin=True)

    # Nota informativa de las bases pendientes (no se consultan).
    record("Bases pendientes de confirmar", WARN,
           mensaje=(f"{len(PENDIENTES)} grupos de bases NO se verifican aun (columna de fecha "
                    "sin confirmar con el equipo de datos). Ver README."),
           ubicacion="(fuera de alcance v1)",
           accion="Confirmar la columna de fecha de negocio real para incluirlas.")
    return True


def _parse_athena_ts(valor):
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(valor, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — FRESCURA DE DATOS (Athena)")
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
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Todas las bases de alta confianza con datos frescos.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"frescura_datos_{ts}.csv")
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
        description="Frescura de datos en Athena (MAX(fecha) por tabla). BLOQUEADO sin permiso athena:*.")
    parser.add_argument("--dias", type=float, default=DEFAULT_DIAS,
                        help=f"Umbral de rancidez en dias (default {DEFAULT_DIAS:g}; env FRESCURA_MAX_DIAS).")
    parser.add_argument("--con-avisos", action="store_true", help="Muestra tambien los AVISOS.")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE FRESCURA DE DATOS — ATHENA")
    print("=" * 78)
    print(f"  Region {REGION}  |  Umbral {args.dias:g} d  |  Workgroup {ATHENA_WORKGROUP}")
    print("  v1: solo 4 bases de alta confianza (SIEDCO, SECAD, ALI, SIMEP).")
    print("  REQUIERE permiso athena:* + ATHENA_OUTPUT_S3. Sin eso, sale BLOQUEADO.\n")

    if not check_identity():
        print_summary()
        return 1

    ejecutado = check_frescura(args.dias)

    print_summary()
    if not args.no_export:
        export()

    if not ejecutado:
        print("\n  >>> BLOQUEADO: falta permiso de Athena o ATHENA_OUTPUT_S3. "
              "No se verifico frescura. <<<")
        return EXIT_BLOQUEADO
    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
