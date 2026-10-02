#!/usr/bin/env python3
"""
test_endpoint_dms.py — Prueba de conexion de UN endpoint DMS (dms:TestConnection).
==================================================================================
Prueba REAL de conectividad a la base usando el mecanismo nativo de DMS:
`TestConnection`. DMS ejecuta la prueba desde su replication instance (que SI
esta dentro de la VPC con ruta on-premise), asi que:

  - Funciona desde CLOUDSHELL NORMAL (no necesitas VPC environment ni IPs).
  - No manejas host/puerto: DMS ya conoce el endpoint (ServerName/Port/credencial).

Es un test ACTIVO acotado a UN endpoint (no toca los demas). Por defecto toma el
primer endpoint Oracle y la primera replication instance disponibles; puedes fijar
cuales con --endpoint y --instance.

Flujo:
  1. Descubre (o recibe) el endpoint DMS a probar y una replication instance.
  2. Lanza dms:TestConnection (asincrono).
  3. Sondea describe_connections hasta que la prueba termine.
  4. Muestra: successful / failed, con el mensaje de DMS.

Uso en CloudShell (normal):
    export AWS_DEFAULT_REGION=us-east-1
    python3 test_endpoint_dms.py                       # primer endpoint Oracle + primera instancia
    python3 test_endpoint_dms.py --endpoint mi-endpoint-id
    python3 test_endpoint_dms.py --endpoint mi-endpoint-id --instance arn:aws:dms:...:rep:XXXX
    python3 test_endpoint_dms.py --listar             # solo lista endpoints e instancias y sale

Nota: TestConnection puede tardar ~10-60 s. El script espera con un timeout.
"""

import argparse
import os
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
POLL_MAX = int(os.environ.get("TEST_POLL_MAX", "30"))   # sondeos
POLL_SEG = float(os.environ.get("TEST_POLL_SEG", "3"))  # segundos entre sondeos

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


def _err(exc):
    if isinstance(exc, ClientError):
        e = exc.response.get("Error", {})
        return e.get("Code", "ClientError"), e.get("Message", "")
    return type(exc).__name__, str(exc)


def _err_detail(exc):
    code, msg = _err(exc)
    return f"{code}: {msg}" if msg else code


def listar_endpoints(dms):
    """Devuelve la lista de endpoints (dicts)."""
    out = []
    marker = None
    while True:
        params = {"Marker": marker} if marker else {}
        resp = dms.describe_endpoints(**params)
        out.extend(resp.get("Endpoints", []))
        marker = resp.get("Marker")
        if not marker:
            break
    return out


def listar_instancias(dms):
    """Devuelve la lista de replication instances (dicts)."""
    out = []
    marker = None
    while True:
        params = {"Marker": marker} if marker else {}
        resp = dms.describe_replication_instances(**params)
        out.extend(resp.get("ReplicationInstances", []))
        marker = resp.get("Marker")
        if not marker:
            break
    return out


def elegir_endpoint(endpoints, endpoint_id):
    if endpoint_id:
        for ep in endpoints:
            if ep.get("EndpointIdentifier") == endpoint_id or ep.get("EndpointArn") == endpoint_id:
                return ep
        return None
    # Por defecto: el primer Oracle (source) que haya; si no, el primero.
    for ep in endpoints:
        if ep.get("EngineName", "").lower() == "oracle":
            return ep
    return endpoints[0] if endpoints else None


def elegir_instancia(instancias, instance_arn):
    if instance_arn:
        for ins in instancias:
            if ins.get("ReplicationInstanceArn") == instance_arn or \
               ins.get("ReplicationInstanceIdentifier") == instance_arn:
                return ins
        return None
    # Por defecto: la primera 'available'; si no, la primera.
    for ins in instancias:
        if ins.get("ReplicationInstanceStatus") == "available":
            return ins
    return instancias[0] if instancias else None


def esperar_resultado(dms, endpoint_arn, instance_arn):
    """
    Sondea describe_connections hasta que la prueba de este par termine.
    Devuelve (status, mensaje).
    """
    for _ in range(POLL_MAX):
        resp = dms.describe_connections(Filters=[
            {"Name": "endpoint-arn", "Values": [endpoint_arn]},
        ])
        for c in resp.get("Connections", []):
            if c.get("ReplicationInstanceArn") != instance_arn:
                continue
            status = c.get("Status", "")
            if status in ("successful", "failed"):
                return status, c.get("LastFailureMessage", "")
        time.sleep(POLL_SEG)
    return "timeout", "La prueba no termino a tiempo (aumenta TEST_POLL_MAX/SEG o reintenta)."


def main():
    parser = argparse.ArgumentParser(
        description="Prueba de conexion de UN endpoint DMS via dms:TestConnection (sin IPs, desde CloudShell normal).")
    parser.add_argument("--endpoint", help="EndpointIdentifier o EndpointArn a probar. Por defecto: primer Oracle.")
    parser.add_argument("--instance", help="ReplicationInstance (id o ARN) que ejecuta la prueba. Por defecto: la primera 'available'.")
    parser.add_argument("--listar", action="store_true", help="Solo lista endpoints e instancias disponibles y sale.")
    args = parser.parse_args()

    print("=" * 74)
    print("PRUEBA DE CONEXION DE ENDPOINT DMS (dms:TestConnection)")
    print("=" * 74)
    print(f"  Region {REGION}")
    print("  La prueba la ejecuta DMS desde su red (VPC), no desde CloudShell.\n")

    try:
        ident = aws_client("sts").get_caller_identity()
        print(f"  Identidad: cuenta {ident['Account']}\n")
    except (ClientError, BotoCoreError) as exc:
        print(f"  [FALLA] No se pudo obtener la identidad: {_err_detail(exc)}")
        return 1

    dms = aws_client("dms")

    # Listar endpoints e instancias.
    try:
        endpoints = listar_endpoints(dms)
        instancias = listar_instancias(dms)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        print(f"  [FALLA] No se pudo listar DMS: {_err_detail(exc)}")
        if code in ("AccessDeniedException", "AccessDenied"):
            print("  Falta dms:DescribeEndpoints / dms:DescribeReplicationInstances.")
        return 1

    if args.listar:
        print("  Endpoints DMS:")
        for ep in endpoints:
            print(f"    - {ep.get('EndpointIdentifier')}  [{ep.get('EngineName')}/{ep.get('EndpointType')}]  "
                  f"{ep.get('ServerName','?')}:{ep.get('Port','?')}")
        print("\n  Replication instances:")
        for ins in instancias:
            print(f"    - {ins.get('ReplicationInstanceIdentifier')}  ({ins.get('ReplicationInstanceStatus')})  "
                  f"{ins.get('ReplicationInstanceArn')}")
        return 0

    # Elegir endpoint e instancia.
    ep = elegir_endpoint(endpoints, args.endpoint)
    if not ep:
        print(f"  [FALLA] No se encontro el endpoint {'('+args.endpoint+')' if args.endpoint else '(ningun Oracle)'}."
              " Usa --listar para ver los disponibles.")
        return 1
    ins = elegir_instancia(instancias, args.instance)
    if not ins:
        print("  [FALLA] No hay replication instance disponible para ejecutar la prueba.")
        print("  TestConnection necesita una replication instance. Usa --listar para ver las que haya.")
        return 1

    endpoint_arn = ep["EndpointArn"]
    instance_arn = ins["ReplicationInstanceArn"]
    print(f"  Endpoint : {ep.get('EndpointIdentifier')}  [{ep.get('EngineName')}]  "
          f"{ep.get('ServerName','?')}:{ep.get('Port','?')}")
    print(f"  Instancia: {ins.get('ReplicationInstanceIdentifier')} ({ins.get('ReplicationInstanceStatus')})\n")

    # Lanzar TestConnection.
    print("  Lanzando dms:TestConnection (esto puede tardar)...")
    try:
        dms.test_connection(ReplicationInstanceArn=instance_arn, EndpointArn=endpoint_arn)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        # Si ya existe una conexion de prueba, DMS puede rechazar recrearla: seguimos a sondear.
        if code not in ("InvalidResourceStateFault", "ResourceAlreadyExistsFault"):
            print(f"  [FALLA] No se pudo iniciar la prueba: {_err_detail(exc)}")
            if code in ("AccessDeniedException", "AccessDenied"):
                print("  Falta el permiso dms:TestConnection.")
            return 1
        print("  (Ya habia una prueba para este par; se lee su resultado.)")

    # Esperar y mostrar el resultado.
    print("  Esperando resultado...")
    status, msg = esperar_resultado(dms, endpoint_arn, instance_arn)

    print("\n" + "=" * 74)
    if status == "successful":
        print(f"  [ OK  ] Conexion EXITOSA a {ep.get('EndpointIdentifier')}")
        print(f"          {ep.get('ServerName','?')}:{ep.get('Port','?')} respondio correctamente.")
        rc = 0
    elif status == "failed":
        print(f"  [FALLA] Conexion FALLIDA a {ep.get('EndpointIdentifier')}")
        print(f"          Motivo: {msg or '(sin mensaje de DMS)'}")
        rc = 1
    else:
        print(f"  [AVISO] {status}: {msg}")
        rc = 1
    print("=" * 74)
    return rc


if __name__ == "__main__":
    sys.exit(main())
