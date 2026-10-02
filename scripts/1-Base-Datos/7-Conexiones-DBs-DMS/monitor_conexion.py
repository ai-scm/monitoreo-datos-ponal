#!/usr/bin/env python3
"""
monitor_conexion.py — Cambios de IP/credenciales (+ ping TCP opcional).
=======================================================================
Corre CADA 1-2 SEMANAS. Descubre los endpoints de las bases Oracle on-premise,
revisa la antiguedad de sus credenciales y DETECTA CAMBIOS de host/puerto o de
credenciales comparando contra la corrida anterior (snapshot local en $HOME).

Funciona en CLOUDSHELL NORMAL, sin aprovisionar nada: el descubrimiento, la
antiguedad de credenciales y la deteccion de cambios usan solo APIs de AWS.

Alcance de bases (endpoints descubiertos, sin necesitar contrasena):
  - Las 8 bases del pipeline Glue (host:puerto desde las Glue Connections).
  - Las bases replicadas por DMS (ServerName/Port de los DMS endpoints).
  - Secretos pon-prod-secad-<SIGLA>-on-premise (antiguedad de credenciales).

Deteccion de cambios (lo que realmente importa para "cambios de IP"):
  Guarda un snapshot JSON en $HOME de CloudShell y lo compara con el de la
  corrida anterior. Marca cambios de host, puerto, endpoints nuevos/desaparecidos
  y credenciales cuya edad "rejuvenecio" (rotacion). NO necesita S3 ni permisos
  extra: solo comparar el estado de hoy contra el guardado.

Ping TCP (OPCIONAL, se auto-detecta):
  Comprobar que la base responde por red requiere RUTA on-premise, que solo
  existe en un CloudShell VPC environment. El script detecta si hay ruta:
  - CloudShell normal: OMITE el ping (no da falsos errores).
  - VPC environment (o con --ping forzado): hace el ping TCP real.

Uso en CloudShell (normal):
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_conexion.py                 # descubre + credenciales + cambios
    python3 monitor_conexion.py --con-avisos    # muestra tambien avisos
    python3 monitor_conexion.py --sin-snapshot  # no guarda/compara (solo estado actual)
    python3 monitor_conexion.py --ping          # fuerza el ping TCP (solo util en VPC env)
    python3 monitor_conexion.py --no-export
"""

import argparse
import csv
import json
import os
import re
import socket
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
TCP_TIMEOUT = float(os.environ.get("TCP_TIMEOUT", "6"))
SECRET_AGE_WARN_DAYS = float(os.environ.get("SECRET_AGE_WARN_DAYS", "365"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Las 8 bases del pipeline Glue -> Glue Connection (para leer host:puerto).
# --------------------------------------------------------------------------------------
DATABASE_TO_CONNECTION = {
    "SIVIC": "pon-prod-raw-sivicc-connection",
    "SIEDCO": "pon-prod-raw-siedco-connection",
    "DISEC": "pon-prod-raw-disec-connection",
    "RNMC": "pon-prod-raw-rnmc-incremental_connection",
    "GEOPOL": "pon-prod-raw-geopol-connection",
    "TELEPOL": "pon-prod-raw-telepol-connection",
    "DBOPER": "pon-test-dboper-connection",
    "ALI": "pon-prod-raw-ali-connection",
}

# Secretos de las 8 bases (para antiguedad de credenciales).
DATABASE_TO_SECRET = {
    "SIVIC": "pon-prod-sivicc-connection",
    "SIEDCO": "pon-prod-SIEDCO-on-premise",
    "DISEC": "pon-prod-DISEC-on-premise",
    "RNMC": "pon-prod-rnmc-on-premise",
    "GEOPOL": "pon-prod-geopol-on-premise",
    "TELEPOL": "pon-prod-telepol-on-premise",
    "DBOPER": "pon-prod-DBOPER-on-premise",
    "ALI": "pon-prod-ALI-secret",
}

# Acepta jdbc:oracle:thin:@//HOST:PUERTO/SVC y jdbc:oracle:thin://@HOST:PUERTO/SVC.
JDBC_URL_RE = re.compile(
    r"jdbc:oracle:thin:(?://@|@//|@)(?P<host>[\w.\-]+):(?P<port>\d+)(?:[:/](?P<svc>[\w.\-]+))?",
    re.IGNORECASE,
)

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
# 1) Identidad
# --------------------------------------------------------------------------------------
def check_identity():
    paso("Identidad AWS...")
    try:
        ident = aws_client("sts").get_caller_identity()
        record("Identidad AWS", OK, mensaje=f"Cuenta {ident['Account']}.",
               ubicacion=f"{ident['Arn']} (region {REGION})")
        paso(f"Identidad AWS  (cuenta {ident['Account']})", fin=True)
        return True
    except (ClientError, BotoCoreError) as exc:
        record("Identidad AWS", FAIL,
               mensaje=f"No se pudo obtener la identidad: {_err_detail(exc)}",
               ubicacion=f"STS region {REGION}", accion="Verifica credenciales de CloudShell.")
        paso("Identidad AWS  (FALLA)", fin=True)
        return False


# --------------------------------------------------------------------------------------
# 2) Descubrir endpoints (Glue Connections + DMS endpoints)
# --------------------------------------------------------------------------------------
def descubrir_endpoints(filtro_siglas=None):
    """
    Devuelve dict nombre -> {host, port, service, fuente}.
    Fuentes: las 8 Glue Connections + los DMS source endpoints Oracle.
    Si filtro_siglas (lista en minusculas) tiene valores, solo devuelve los
    endpoints cuyo nombre/host/servicio contenga alguna de esas siglas.
    """
    endpoints = {}

    def _coincide(*textos):
        if not filtro_siglas:
            return True
        blob = " ".join(t.lower() for t in textos if t)
        return any(sig in blob for sig in filtro_siglas)

    # 2a) Glue Connections de las 8 bases.
    glue = aws_client("glue")
    for db, conn in DATABASE_TO_CONNECTION.items():
        paso(f"Descubriendo endpoints... Glue {db}")
        try:
            url = (glue.get_connection(Name=conn).get("Connection", {})
                   .get("ConnectionProperties", {}).get("JDBC_CONNECTION_URL", ""))
            m = JDBC_URL_RE.search(url or "")
            if m and _coincide(db, conn, m.group("host"), m.group("svc") or ""):
                endpoints[db] = {"host": m.group("host"), "port": int(m.group("port")),
                                 "service": m.group("svc") or "", "fuente": "glue"}
        except Exception:  # noqa: BLE001
            pass  # sin permiso o no existe; se reporta como no descubierto mas abajo

    # 2b) DMS endpoints (ServerName/Port visibles sin contrasena).
    try:
        dms = aws_client("dms")
        paso("Descubriendo endpoints... DMS")
        marker = None
        while True:
            params = {"Marker": marker} if marker else {}
            resp = dms.describe_endpoints(**params)
            for ep in resp.get("Endpoints", []):
                if (ep.get("EngineName", "").lower() != "oracle"):
                    continue
                host = ep.get("ServerName")
                port = ep.get("Port")
                if not host or not port:
                    continue
                nombre = ep.get("EndpointIdentifier", host)
                if not _coincide(nombre, host, ep.get("DatabaseName", "")):
                    continue
                endpoints.setdefault(f"dms:{nombre}", {
                    "host": host, "port": int(port),
                    "service": ep.get("DatabaseName", ""), "fuente": "dms",
                })
            marker = resp.get("Marker")
            if not marker:
                break
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        # No es fatal: seguimos con lo que haya de Glue.
        record("Descubrimiento DMS", WARN,
               mensaje=f"No se pudieron listar endpoints DMS: {_err_detail(exc)}",
               ubicacion=f"DMS (region {REGION})",
               accion=("Falta dms:DescribeEndpoints." if code in ("AccessDeniedException", "AccessDenied")
                       else "Revisa permisos/region de DMS."))
    return endpoints


# --------------------------------------------------------------------------------------
# 3) Ping TCP
# --------------------------------------------------------------------------------------
def _detectar_ip_local():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:  # noqa: BLE001
        return "?"


def ping_tcp(host, port, timeout):
    inicio = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, (time.time() - inicio) * 1000, ""
    except socket.timeout:
        return False, (time.time() - inicio) * 1000, f"timeout tras {timeout:g}s"
    except OSError as exc:
        return False, (time.time() - inicio) * 1000, f"{exc.__class__.__name__}: {exc}"


def _hay_ruta_onprem(endpoints, timeout=1.5):
    """
    Detecta rapido si hay ruta on-premise: intenta UN ping corto al primer
    endpoint. Si responde, asumimos que estamos en un entorno con ruta (VPC env).
    Si no, es CloudShell normal y el ping completo se omite.
    """
    for ep in endpoints.values():
        ok, _ms, _d = ping_tcp(ep["host"], ep["port"], timeout)
        return ok  # basta con el primero
    return False


def check_conectividad(endpoints, timeout, forzar_ping):
    # Dedup por (host, port): varias bases comparten endpoint.
    destinos = {}
    for nombre, ep in endpoints.items():
        destinos.setdefault((ep["host"], ep["port"]), []).append(nombre)

    if not destinos:
        record("Conectividad", WARN,
               mensaje="No se descubrio ningun endpoint (revisa permisos glue:GetConnection / dms:DescribeEndpoints).",
               ubicacion=f"region {REGION}")
        return

    # Auto-deteccion de ruta: si no hay ruta y no se forzo, se omite el ping.
    if not forzar_ping:
        paso("Detectando ruta on-premise...")
        if not _hay_ruta_onprem(endpoints):
            record("Ping TCP", WARN,
                   mensaje=(f"Omitido: no hay ruta a las bases on-premise ({len(destinos)} endpoints). "
                            "Es normal en CloudShell normal."),
                   ubicacion="red",
                   accion=("Para el ping real, corre en un CloudShell VPC environment "
                           "(o fuerza con --ping). El descubrimiento y la deteccion de cambios "
                           "SI funcionan aqui."))
            paso("Ping TCP  (omitido: sin ruta on-premise)", fin=True)
            return

    total = len(destinos)
    for i, ((host, port), nombres) in enumerate(sorted(destinos.items()), start=1):
        paso(f"Ping TCP... ({i}/{total}) {host}:{port}")
        ok, ms, detalle = ping_tcp(host, port, timeout)
        bases_txt = ", ".join(nombres)
        if ok:
            record(f"{host}:{port}", OK,
                   mensaje=f"Alcanzable en {ms:.0f} ms. Bases: {bases_txt}.",
                   ubicacion=f"{host}:{port}")
        else:
            record(f"{host}:{port}", FAIL,
                   mensaje=f"NO alcanzable ({detalle}). Bases: {bases_txt}.",
                   ubicacion=f"{host}:{port}",
                   accion="Revisa VPN/ruta/SG/firewall on-premise para esa base.")
    paso(f"Ping TCP  ({total} endpoints probados)", fin=True)


# --------------------------------------------------------------------------------------
# 4) Antiguedad de credenciales (8 bases + siglas secad)
# --------------------------------------------------------------------------------------
def check_credenciales(filtro_siglas=None):
    sm = aws_client("secretsmanager")

    # 4a) los 8 secretos conocidos.
    secretos = dict(DATABASE_TO_SECRET)

    # 4b) secretos pon-prod-secad-<SIGLA>-on-premise (descubrimiento por listado).
    try:
        paso("Buscando secretos secad-<SIGLA>...")
        token = None
        while True:
            params = {"NextToken": token} if token else {}
            resp = sm.list_secrets(**params)
            for s in resp.get("SecretList", []):
                nombre = s.get("Name", "")
                if re.match(r"pon-prod-secad-[\w\-]+-on-premise", nombre, re.IGNORECASE):
                    secretos[nombre] = nombre
            token = resp.get("NextToken")
            if not token:
                break
    except Exception:  # noqa: BLE001
        pass  # si no hay ListSecrets, solo revisamos los 8 conocidos

    # Filtro por sigla: deja solo los secretos cuyo nombre/etiqueta contenga la sigla.
    if filtro_siglas:
        secretos = {et: sn for et, sn in secretos.items()
                    if any(sig in f"{et} {sn}".lower() for sig in filtro_siglas)}

    edades = {}  # nombre_secreto -> edad_dias (para el snapshot)
    total = len(secretos)
    for i, (etiqueta, secret_name) in enumerate(secretos.items(), start=1):
        paso(f"Credenciales... ({i}/{total})")
        try:
            desc = sm.describe_secret(SecretId=secret_name)
            ref = desc.get("LastChangedDate") or desc.get("LastRotatedDate") or desc.get("CreatedDate")
            if ref is None:
                continue
            if ref.tzinfo is None:
                ref = ref.replace(tzinfo=timezone.utc)
            dias = (datetime.now(timezone.utc) - ref).total_seconds() / 86400.0
            edades[secret_name] = round(dias, 1)
            if dias >= SECRET_AGE_WARN_DAYS:
                record(f"Credencial {etiqueta}", WARN,
                       mensaje=f"Secreto de {dias:.0f} dias (supera {SECRET_AGE_WARN_DAYS:.0f}).",
                       ubicacion=f"Secret '{secret_name}'",
                       accion="Considera rotar/actualizar la contrasena.")
        except Exception:  # noqa: BLE001
            continue  # secreto ausente/sin permiso: no crítico para el ping
    return edades


# --------------------------------------------------------------------------------------
# 5) Snapshot local ($HOME) + deteccion de cambios
# --------------------------------------------------------------------------------------
def _snapshot_actual(endpoints, edades):
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "endpoints": {n: {"host": e["host"], "port": e["port"], "service": e.get("service", "")}
                      for n, e in endpoints.items()},
        "credenciales": edades,
    }


def _ruta_snapshot():
    """Snapshot en $HOME de CloudShell (persiste entre corridas sin aprovisionar nada)."""
    base = os.environ.get("SNAPSHOT_DIR") or os.path.expanduser("~")
    return os.path.join(base, ".monitor_conexion_snapshot.json")


def check_cambios(endpoints, edades):
    ruta = _ruta_snapshot()
    actual = _snapshot_actual(endpoints, edades)

    paso("Comparando con snapshot anterior (local)...")
    previo = None
    if os.path.exists(ruta):
        try:
            with open(ruta, encoding="utf-8") as f:
                previo = json.load(f)
        except (ValueError, OSError):
            previo = None

    if previo is None:
        record("Deteccion de cambios", OK,
               mensaje="Primera corrida (no habia snapshot previo). Se guardo la linea base.",
               ubicacion=ruta)
    else:
        prev_ts = previo.get("timestamp", "?")
        cambios = []
        prev_eps = previo.get("endpoints", {})
        for nombre, e in actual["endpoints"].items():
            pe = prev_eps.get(nombre)
            if pe is None:
                cambios.append(f"{nombre}: NUEVO endpoint ({e['host']}:{e['port']})")
            else:
                if pe.get("host") != e["host"]:
                    cambios.append(f"{nombre}: cambio de HOST {pe.get('host')} -> {e['host']}")
                if pe.get("port") != e["port"]:
                    cambios.append(f"{nombre}: cambio de PUERTO {pe.get('port')} -> {e['port']}")
        for nombre in prev_eps:
            if nombre not in actual["endpoints"]:
                cambios.append(f"{nombre}: endpoint DESAPARECIO (estaba {prev_eps[nombre].get('host')})")
        # Credenciales: edad que baja = probable rotacion/cambio de clave.
        prev_cred = previo.get("credenciales", {})
        for secret, edad in actual["credenciales"].items():
            pe = prev_cred.get(secret)
            if pe is not None and edad + 1 < pe:
                cambios.append(f"{secret}: la credencial CAMBIO (edad {pe:.0f}d -> {edad:.0f}d)")

        if cambios:
            for c in cambios:
                record("Cambio detectado", FAIL, mensaje=c,
                       ubicacion=f"vs snapshot del {prev_ts}",
                       accion="Verifica que el cambio sea esperado (mantenimiento) y no un incidente.")
        else:
            record("Deteccion de cambios", OK,
                   mensaje=f"Sin cambios de host/puerto/credencial respecto al {prev_ts}.",
                   ubicacion=ruta)

    # Guardar snapshot actual (sobrescribe la linea base).
    try:
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(actual, f, indent=2)
        paso("Snapshot guardado en $HOME", fin=True)
    except OSError as exc:
        record("Guardar snapshot", FAIL,
               mensaje=f"No se pudo guardar el snapshot local: {exc}",
               ubicacion=ruta,
               accion="Revisa permisos de escritura en $HOME.")


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — CONECTIVIDAD Y CAMBIOS (bases Oracle on-premise)")
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
        print("\n  Todas las bases alcanzables y sin cambios.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s) ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"conexion_{ts}.csv")
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
        description="Ping TCP a las bases Oracle on-premise + deteccion de cambios de IP/credenciales.")
    parser.add_argument("--sin-snapshot", action="store_true",
                        help="No guarda/compara el snapshot; solo reporta el estado actual.")
    parser.add_argument("--ping", action="store_true",
                        help="Fuerza el ping TCP aunque no se detecte ruta (util en VPC environment).")
    parser.add_argument("--sigla",
                        help="Acota a una o varias bases (separadas por coma). Ej: --sigla MEBUC "
                             "o --sigla MEBUC,DEARA. Sin esto, revisa todas.")
    parser.add_argument("--timeout", type=float, default=TCP_TIMEOUT,
                        help=f"Timeout TCP por endpoint en segundos (por defecto {TCP_TIMEOUT:g}).")
    parser.add_argument("--con-avisos", action="store_true", help="Muestra tambien los AVISOS en el resumen.")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE CONEXION / CAMBIOS — BASES ORACLE ON-PREMISE")
    print("=" * 78)
    print(f"  Region {REGION}  |  IP local {_detectar_ip_local()}  |  Timeout {args.timeout:g}s")
    print("  Corre en CloudShell normal. El ping TCP se omite si no hay ruta on-premise.\n")

    if not check_identity():
        print_summary()
        return 1

    filtro = None
    if args.sigla:
        filtro = [s.strip().lower() for s in args.sigla.split(",") if s.strip()]
        print(f"  Filtro por sigla(s): {', '.join(filtro)}\n")
    endpoints = descubrir_endpoints(filtro)
    paso(f"Endpoints descubiertos: {len(endpoints)}", fin=True)
    check_conectividad(endpoints, args.timeout, args.ping)
    edades = check_credenciales(filtro)
    paso(f"Credenciales revisadas: {len(edades)}", fin=True)

    if not args.sin_snapshot:
        check_cambios(endpoints, edades)

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
