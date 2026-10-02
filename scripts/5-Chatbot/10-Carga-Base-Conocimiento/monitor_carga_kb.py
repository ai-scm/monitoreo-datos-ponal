#!/usr/bin/env python3
"""
monitor_carga_kb.py — Estado de la carga/ingesta de las Knowledge Bases (Bedrock).
==================================================================================
Corre DIARIO. La "base de conocimiento" del chatbot es Amazon Bedrock Knowledge Base
(NO Kendra, NO OpenSearch directo). La "carga" real es la INGESTA: Bedrock corre
ingestion jobs que indexan los documentos de cada data source al vector store.

Monitorea un MAPA FIJO de 2 KBs relevantes del chatbot (de las 4 que hay en la cuenta;
las otras 2 son de prueba / otra tecnologia y quedan FUERA a proposito):

  1. Data-no-estructurada-Expertopol (1ECION66RT) — la KB principal, "alimenta al Bot
     Nadia". Data sources descubiertos dinamicamente (Procedimientos, Normativas,
     Generalidades) desde el bucket pon-prod-data-no-estructurada.
     Criterio por data source (ultimo ingestion job):
       - COMPLETE                                   -> OK (sin importar la antiguedad)
       - COMPLETE con numberOfDocumentsFailed > 0   -> AVISO (indexacion parcial)
       - FAILED                                     -> FALLA
       - IN_PROGRESS / STARTING                     -> AVISO
     IMPORTANTE: la carga es MANUAL/humana (no se sube a diario). NO se evalua
     "frescura" por ventana: que la ultima ingesta sea vieja NO es un problema. Lo
     que importa es que, cuando se cargue, NO falle (de ahi los docs fallidos = AVISO).

  2. KBBrChatKbStaENFMEE14EKB2A93F4E2 (NPYXAA8O6H) — bot personalizado, 1 data source
     hacia pon-prod-ml-iapol. DORMIDO: sin actividad de ingesta desde 2025-02-17.
     Tratamiento especial: INFO "inactivo", NUNCA escala a AVISO/FALLA salvo que
     vuelva a tener actividad reciente y esa actividad falle (ahi si seria regresion).

Nota historica (NO es alerta activa): este data source tuvo un error IAM de
propagacion de permisos (s3:ListBucket sobre pon-prod-ml-iapol) resuelto en minutos
hace ~20 meses; desde entonces no corrio ningun ingestion job mas. Un recurso dormido
no es un recurso roto (mismo principio que historico_personas en SIVICC y la DMS
detenida en RNMC).

Es de SOLO LECTURA (bedrock-agent:ListIngestionJobs/GetIngestionJob/ListDataSources/
GetKnowledgeBase, sts). NO dispara ingestas.

Salida: resumen unificado con severidad (OK / AVISO / FALLA) y CSV.
Exit code: 1 si algo fallo, 0 si todo bien, 2 si argumentos invalidos.

Uso en CloudShell:
    export AWS_DEFAULT_REGION=us-east-1
    python3 monitor_carga_kb.py
    python3 monitor_carga_kb.py --con-avisos
    python3 monitor_carga_kb.py --no-export
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
# Dias sin ingesta a partir de los cuales un data source dormido se reporta como inactivo.
INACTIVO_DIAS = float(os.environ.get("KB_INACTIVO_DIAS", "180"))

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})


def aws_client(service):
    return boto3.client(service, region_name=REGION, config=BOTO_CONFIG)


# --------------------------------------------------------------------------------------
# Mapa fijo de las 2 KBs relevantes del chatbot. Las otras 2 de la cuenta
# (ponal-test-delitos-kb, knowledge-base-quick-start-8bznm) quedan FUERA a proposito
# (prueba / otra tecnologia). El crecimiento esperable (nuevos data sources) se cubre
# con descubrimiento dinamico DENTRO de cada KB, no agregando KBs sin control.
# --------------------------------------------------------------------------------------
PRINCIPAL = "PRINCIPAL"    # KB real que alimenta al bot: se evalua normal
DORMIDA = "DORMIDA"        # KB inactiva: INFO, nunca escala salvo regresion

KNOWLEDGE_BASES = {
    "1ECION66RT": {"nombre": "Data-no-estructurada-Expertopol", "naturaleza": PRINCIPAL,
                   "detalle": "alimenta al Bot Nadia (bucket pon-prod-data-no-estructurada)"},
    "NPYXAA8O6H": {"nombre": "KBBrChatKbStaENFMEE14EKB2A93F4E2", "naturaleza": DORMIDA,
                   "detalle": "bot personalizado, inactivo desde 2025-02-17 (bucket pon-prod-ml-iapol)"},
}

# Estados de un ingestion job de Bedrock.
ESTADO_OK = "COMPLETE"
ESTADOS_FALLO = {"FAILED"}
ESTADOS_EN_CURSO = {"STARTING", "IN_PROGRESS"}

OK = "OK"
WARN = "AVISO"
FAIL = "FALLA"
INFO = "AVISO"  # se agrupa con AVISO para severidad, se etiqueta aparte
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


def record(recurso, estado, mensaje="", ubicacion="", accion="", naturaleza=""):
    RESULTS.append({
        "recurso": recurso, "ubicacion": ubicacion, "estado": estado,
        "naturaleza": naturaleza, "mensaje": mensaje,
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


def _edad_dias(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0


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
# Data sources + ultimo ingestion job
# --------------------------------------------------------------------------------------
def _listar_data_sources(bedrock, kb_id):
    data_sources = []
    token = None
    while True:
        params = {"knowledgeBaseId": kb_id, "maxResults": 50}
        if token:
            params["nextToken"] = token
        resp = bedrock.list_data_sources(**params)
        data_sources.extend(resp.get("dataSourceSummaries", []))
        token = resp.get("nextToken")
        if not token:
            break
    return data_sources


def _ultimo_ingestion_job(bedrock, kb_id, ds_id):
    resp = bedrock.list_ingestion_jobs(
        knowledgeBaseId=kb_id, dataSourceId=ds_id,
        sortBy={"attribute": "STARTED_AT", "order": "DESCENDING"},
        maxResults=1,
    )
    jobs = resp.get("ingestionJobSummaries", [])
    if not jobs:
        return None
    job_id = jobs[0].get("ingestionJobId")
    # GetIngestionJob trae las estadisticas (numberOfDocumentsFailed, etc.).
    try:
        full = bedrock.get_ingestion_job(
            knowledgeBaseId=kb_id, dataSourceId=ds_id, ingestionJobId=job_id)
        return full.get("ingestionJob", jobs[0])
    except Exception:  # noqa: BLE001
        return jobs[0]


def _evaluar_data_source_principal(bedrock, kb_id, kb_nombre, ds):
    ds_id = ds.get("dataSourceId", "?")
    ds_nombre = ds.get("name", ds_id)
    etiqueta = f"{kb_nombre} / {ds_nombre}"
    ubic = f"Bedrock KB {kb_id} / data source {ds_id}"

    try:
        job = _ultimo_ingestion_job(bedrock, kb_id, ds_id)
    except Exception as exc:  # noqa: BLE001
        code, _ = _err(exc)
        accion = ("Falta bedrock-agent:ListIngestionJobs/GetIngestionJob."
                  if code in ("AccessDeniedException", "AccessDenied")
                  else "Revisa permisos/region de Bedrock.")
        record(etiqueta, FAIL, naturaleza=PRINCIPAL,
               mensaje=f"No se pudo consultar la ingesta: {_err_detail(exc)}",
               ubicacion=ubic, accion=accion)
        return

    if job is None:
        record(etiqueta, WARN, naturaleza=PRINCIPAL,
               mensaje="Data source sin ingestion jobs registrados.",
               ubicacion=ubic, accion="Verifica si deberia haberse ingestado.")
        return

    estado = job.get("status", "?")
    started = job.get("startedAt")
    st_txt = started.strftime("%Y-%m-%d %H:%M UTC") if started else "sin fecha"
    stats = job.get("statistics", {}) or {}
    docs_fail = int(stats.get("numberOfDocumentsFailed", 0) or 0)
    docs_scan = int(stats.get("numberOfDocumentsScanned", 0) or 0)

    if estado in ESTADOS_FALLO:
        motivo = "; ".join(job.get("failureReasons", []) or []) or "(sin failureReasons)"
        record(etiqueta, FAIL, naturaleza=PRINCIPAL,
               mensaje=f"Ultima ingesta FAILED ({st_txt}). Motivo: {motivo[:200]}",
               ubicacion=ubic,
               accion="Revisa el ingestion job en la consola de Bedrock.")
    elif estado in ESTADOS_EN_CURSO:
        record(etiqueta, WARN, naturaleza=PRINCIPAL,
               mensaje=f"Ingesta en curso ({estado}, {st_txt}).",
               ubicacion=ubic, accion="Aun ejecutando; vuelve a revisar mas tarde.")
    elif estado == ESTADO_OK:
        # La carga de la KB es MANUAL/humana: no se sube a diario. Por eso la antiguedad
        # de la ultima ingesta NO es un problema (quitada la regla de "frescura por
        # ventana" que generaba falsos positivos). Lo que importa es que, cuando se
        # cargue, NO falle: por eso los documentos fallidos SI son AVISO.
        if docs_fail > 0:
            record(etiqueta, WARN, naturaleza=PRINCIPAL,
                   mensaje=(f"Ingesta COMPLETE ({st_txt}) pero con {docs_fail} documento(s) fallido(s) "
                            f"de {docs_scan} escaneados (indexacion parcial). "
                            "Causa tipica: '400 Too many input tokens' (documento supera el max de 8192)."),
                   ubicacion=ubic,
                   accion="Revisa los documentos fallidos (dividir/recortar los que superan el limite de tokens).")
        else:
            record(etiqueta, OK, naturaleza=PRINCIPAL,
                   mensaje=(f"Ultima ingesta OK ({st_txt}, {docs_scan} docs). "
                            "Carga manual: la antiguedad no es relevante mientras no falle."),
                   ubicacion=ubic)
    else:
        record(etiqueta, WARN, naturaleza=PRINCIPAL,
               mensaje=f"Ingesta en estado {estado} ({st_txt}).",
               ubicacion=ubic, accion="Estado no reconocido; revisa el job.")


def _evaluar_kb_dormida(bedrock, kb_id, kb_nombre):
    """KB dormida: solo INFO. Solo escala a FALLA si volvio a tener actividad y fallo."""
    etiqueta = f"{kb_nombre} (dormida)"
    ubic = f"Bedrock KB {kb_id}"
    try:
        data_sources = _listar_data_sources(bedrock, kb_id)
    except Exception as exc:  # noqa: BLE001
        record(etiqueta, INFO, naturaleza=DORMIDA,
               mensaje=f"No se pudieron listar data sources: {_err_detail(exc)}",
               ubicacion=ubic)
        return

    hubo_actividad_reciente_fallida = False
    detalle_regresion = ""
    ultima_fecha = None
    for ds in data_sources:
        ds_id = ds.get("dataSourceId", "?")
        try:
            job = _ultimo_ingestion_job(bedrock, kb_id, ds_id)
        except Exception:  # noqa: BLE001
            continue
        if not job:
            continue
        started = job.get("startedAt")
        edad = _edad_dias(started)
        if started and (ultima_fecha is None or started > ultima_fecha):
            ultima_fecha = started
        # Regresion: actividad reciente (dentro de INACTIVO_DIAS) Y fallida.
        if edad is not None and edad <= INACTIVO_DIAS and job.get("status") in ESTADOS_FALLO:
            hubo_actividad_reciente_fallida = True
            motivo = "; ".join(job.get("failureReasons", []) or []) or "(sin failureReasons)"
            detalle_regresion = f"data source {ds_id}: {motivo[:150]}"

    if hubo_actividad_reciente_fallida:
        record(etiqueta, FAIL, naturaleza=DORMIDA,
               mensaje=(f"REGRESION: la KB dormida volvio a tener actividad de ingesta y FALLO. "
                        f"{detalle_regresion}"),
               ubicacion=ubic,
               accion="Revisa el ingestion job; la KB inactiva volvio a correr y fallo.")
    else:
        ult_txt = ultima_fecha.strftime("%Y-%m-%d") if ultima_fecha else "sin registro"
        record(etiqueta, INFO, naturaleza=DORMIDA,
               mensaje=(f"Inactiva (sin ingesta reciente; ultima actividad {ult_txt}). "
                        "Dormida a proposito; no es un fallo."),
               ubicacion=ubic)


def check_knowledge_bases():
    bedrock = aws_client("bedrock-agent")
    total = len(KNOWLEDGE_BASES)
    for i, (kb_id, info) in enumerate(KNOWLEDGE_BASES.items(), start=1):
        kb_nombre = info["nombre"]
        paso(f"Knowledge Bases... ({i}/{total}) {kb_nombre}")
        if info["naturaleza"] == DORMIDA:
            _evaluar_kb_dormida(bedrock, kb_id, kb_nombre)
            continue
        # PRINCIPAL: descubrir data sources dinamicamente y evaluar cada uno.
        try:
            data_sources = _listar_data_sources(bedrock, kb_id)
        except Exception as exc:  # noqa: BLE001
            code, _ = _err(exc)
            accion = ("Falta bedrock-agent:ListDataSources."
                      if code in ("AccessDeniedException", "AccessDenied")
                      else "Revisa permisos/region de Bedrock.")
            record(f"{kb_nombre}", FAIL, naturaleza=PRINCIPAL,
                   mensaje=f"No se pudieron listar data sources: {_err_detail(exc)}",
                   ubicacion=f"Bedrock KB {kb_id}", accion=accion)
            continue
        if not data_sources:
            record(f"{kb_nombre}", WARN, naturaleza=PRINCIPAL,
                   mensaje="La KB no tiene data sources.", ubicacion=f"Bedrock KB {kb_id}",
                   accion="Verifica la configuracion de la KB.")
            continue
        for ds in data_sources:
            _evaluar_data_source_principal(bedrock, kb_id, kb_nombre, ds)

    paso(f"Knowledge Bases  ({total} KBs revisadas)", fin=True)


# --------------------------------------------------------------------------------------
# Resumen + exportacion
# --------------------------------------------------------------------------------------
def print_summary():
    ok = sum(1 for r in RESULTS if r["estado"] == OK)
    warn = sum(1 for r in RESULTS if r["estado"] == WARN)
    fail = sum(1 for r in RESULTS if r["estado"] == FAIL)

    print("\n" + "=" * 78)
    print("RESUMEN — CARGA/INGESTA DE KNOWLEDGE BASES (Bedrock)")
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
                    if r["ubicacion"]:
                        print(f"       donde    : {r['ubicacion']}")
                    if r["accion_sugerida"]:
                        print(f"       accion   : {r['accion_sugerida']}")
    elif fail == 0 and warn == 0:
        print("\n  Todas las KBs del chatbot con ingesta OK.")

    if not CON_AVISOS and warn > 0:
        print(f"\n  ({warn} aviso(s)/info ocultos. Usa --con-avisos para verlos; el CSV los incluye.)")


def export():
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"carga_kb_{ts}.csv")
    cols = ["recurso", "naturaleza", "ubicacion", "estado", "mensaje", "accion_sugerida"]
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
        description="Estado de la carga/ingesta de las Knowledge Bases (Bedrock) del chatbot.")
    parser.add_argument("--con-avisos", action="store_true",
                        help="Muestra tambien AVISOS e INFO (docs fallidos, KB dormida).")
    parser.add_argument("--detallado", action="store_true", help="Salida larga (sin progreso dinamico).")
    parser.add_argument("--no-export", action="store_true", help="No genera CSV.")
    args = parser.parse_args()

    global CON_AVISOS, VERBOSE
    CON_AVISOS = args.con_avisos
    VERBOSE = args.detallado

    print("=" * 78)
    print("MONITOR DE CARGA / INGESTA KNOWLEDGE BASES — Bedrock (solo lectura)")
    print("=" * 78)
    print(f"  Region {REGION}")
    print("  2 KBs del chatbot (las 2 de prueba quedan fuera). Data sources: descubrimiento dinamico.")
    print("  Carga manual: no se evalua frescura; solo que la ingesta no falle y sin docs fallidos.\n")

    if not check_identity():
        print_summary()
        return 1

    check_knowledge_bases()

    print_summary()
    if not args.no_export:
        export()

    return 1 if any(r["estado"] == FAIL for r in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
