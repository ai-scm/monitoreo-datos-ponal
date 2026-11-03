#!/usr/bin/env python3
"""Valida nombres, cobertura y contenido PKL de matrices; solo lectura de S3.

Reconstruye representaciones inertes de GridPredictionArray y arrays numericos.
No importa numpy/open_cp ni ejecuta sus constructores al leer los archivos.
"""

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import os
import pickle
import pickletools
import re
import struct
import sys
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


BUCKET = "pon-prod-ml-iapol"
OWNER = "926162397524"
REGION = "us-east-1"
ROOT_PREFIX = "PrediccionMdP"
VERSION = "2026-11-03.1"
POLICY = "matrices-pkl-2026-11-03-v1"
HORAS_ESPERADAS = {f"{hora:02d}00" for hora in range(24)}
UTC_MENOS_5 = timezone(timedelta(hours=-5))
MAX_CELDAS = 2_000_000
MAX_OPCODES = 100_000
MAX_CACHE_BYTES = 8 * 1024**2
# maskPrediction del generador marca con -1 las celdas fuera de la zona.
VALOR_SIN_DATO = -1
ESTADOS_CONTENIDO = {"OK", "AVISO", "FALLA", "NO EVALUADO"}
COLUMNAS_DETALLE = (
    "semana", "sigla", "unidad", "archivo", "ruta_s3", "fecha_archivo", "hora_archivo",
    "nombre_estado", "contenido_estado", "estado", "origen_revision", "tamano_bytes",
    "fecha_modificacion", "filas", "columnas", "tipo_dato", "celdas_total", "celdas_validas",
    "celdas_enmascaradas", "celdas_sin_dato", "no_finitos", "negativos", "ceros", "minimo", "maximo", "suma",
    "sin_contraste", "xsize", "ysize", "xoffset", "yoffset", "razon",
)
COLUMNAS_FALTANTES = (
    "semana", "sigla", "unidad", "fecha", "tipo_archivo", "horas_presentes",
    "cantidad_faltantes", "horas_faltantes", "archivos_faltantes", "ruta_s3",
)


class FormatoNoSoportado(ValueError):
    """No hay evidencia suficiente para interpretar este formato de PKL."""


class LecturaIncompleta(RuntimeError):
    pass


class LimiteRevision(LecturaIncompleta):
    pass


class _DType:
    """Descriptor numerico, sin recurrir a numpy.dtype."""

    def __init__(self, descriptor, align=False, copy=True):
        if not isinstance(descriptor, str):
            raise FormatoNoSoportado("Tipo de dato compuesto o no numerico")
        match = re.fullmatch(r"([<>=|]?)([fiub?])(\d*)", descriptor)
        if not match:
            raise FormatoNoSoportado("Tipo de dato no numerico o no soportado")
        self.endian, self.kind, length = match.groups()
        self.size = int(length or "1")
        codes = {
            ("f", 2): "e", ("f", 4): "f", ("f", 8): "d",
            ("i", 1): "b", ("i", 2): "h", ("i", 4): "i", ("i", 8): "q",
            ("u", 1): "B", ("u", 2): "H", ("u", 4): "I", ("u", 8): "Q",
            ("b", 1): "?", ("?", 1): "?",
        }
        if (self.kind, self.size) not in codes:
            raise FormatoNoSoportado("Precision o tipo de dato no soportado")
        self.code = codes[self.kind, self.size]

    def __setstate__(self, state):
        if not isinstance(state, tuple) or len(state) != 8 or state[0] not in (3, 4):
            raise FormatoNoSoportado("Estado de dtype no soportado")
        if state[1] not in ("<", ">", "=", "|") or any(v is not None for v in state[2:5]):
            raise FormatoNoSoportado("Dtype estructurado o byte order no soportado")
        self.endian = state[1]

    def format(self):
        endian = self.endian or "="
        if endian == "|":
            if self.size != 1:
                raise FormatoNoSoportado("Byte order indeterminado para tipo multibyte")
            endian = "="
        return endian + self.code


class _Array:
    """Guarda forma, dtype y bytes; no instancia arrays nativos ni objetos externos."""

    def __setstate__(self, state):
        if not isinstance(state, tuple) or len(state) not in (4, 5):
            raise ValueError("Estado de array incompleto")
        if len(state) == 5:
            if state[0] not in (0, 1):
                raise FormatoNoSoportado("Version de estado de array no soportada")
            state = state[1:]
        shape, dtype, order, raw = state
        if not isinstance(shape, tuple) or len(shape) > 8 or any(
            type(n) is not int or n < 0 for n in shape
        ):
            raise ValueError("Dimensiones de array invalidas")
        if type(dtype) is not _DType or type(order) is not bool:
            raise FormatoNoSoportado("Descriptor de array no soportado")
        if not isinstance(raw, (bytes, bytearray)):
            raise FormatoNoSoportado("Array sin bloque numerico binario")
        if len(raw) != math.prod(shape) * dtype.size:
            raise ValueError("Bytes de la matriz no corresponden a sus dimensiones y tipo")
        self.shape, self.dtype, self.order, self.raw = shape, dtype, order, raw
        self.mask = None


class _MaskedArray(_Array):
    def __setstate__(self, state):
        if not isinstance(state, tuple) or len(state) != 7:
            raise FormatoNoSoportado("Estado de array enmascarado no soportado")
        super().__setstate__(state[:5])
        mask = state[5]
        if not isinstance(mask, (bytes, bytearray)) or len(mask) != math.prod(self.shape):
            raise ValueError("Mascara incompleta o de dimensiones distintas")
        self.mask = mask


class _Grid:
    def __setstate__(self, state):
        if not isinstance(state, dict) or any(not isinstance(k, str) for k in state):
            raise ValueError("Estado de grilla invalido")
        self.state = state


def _reconstruct(subtype, shape, dtype):
    if subtype is not _Array or shape != (0,) or dtype not in (b"b", "b"):
        raise FormatoNoSoportado("Constructor de array no soportado")
    return _Array()


def _mareconstruct(subtype, baseclass, shape, dtype):
    if subtype is not _MaskedArray or baseclass is not _Array or shape != (0,) or dtype not in (b"b", "b"):
        raise FormatoNoSoportado("Constructor de mascara no soportado")
    return _MaskedArray()


def _frombuffer(raw, dtype, shape, order):
    if order not in ("C", "F"):
        raise FormatoNoSoportado("Orden de array no soportado")
    result = _Array()
    result.__setstate__((1, shape, dtype, order == "F", raw))
    return result


def _scalar(dtype, raw):
    if type(dtype) is not _DType or not isinstance(raw, (bytes, bytearray)) or len(raw) != dtype.size:
        raise ValueError("Escalar numerico invalido")
    return struct.unpack(dtype.format(), raw)[0]


def _encode(text, encoding="latin1", errors="strict"):
    if not isinstance(text, str) or encoding not in ("latin1", "latin-1") or errors != "strict":
        raise FormatoNoSoportado("Codificacion de PKL antiguo no soportada")
    return text.encode("latin1")


class _LectorRestringido(pickle.Unpickler):
    def find_class(self, module, name):
        allowed = {
            ("open_cp.predictors", "GridPredictionArray"): _Grid,
            ("numpy", "ndarray"): _Array,
            ("numpy", "dtype"): _DType,
            ("numpy.ma.core", "MaskedArray"): _MaskedArray,
            ("numpy.ma.core", "_mareconstruct"): _mareconstruct,
            ("_codecs", "encode"): _encode,
        }
        for prefix in ("numpy.core", "numpy._core"):
            allowed[prefix + ".multiarray", "_reconstruct"] = _reconstruct
            allowed[prefix + ".multiarray", "scalar"] = _scalar
            allowed[prefix + ".numeric", "_frombuffer"] = _frombuffer
        if (module, name) not in allowed:
            raise FormatoNoSoportado("Clase o constructor fuera del formato permitido: " + module + "." + name)
        return allowed[module, name]

    def persistent_load(self, pid):
        raise FormatoNoSoportado("Referencias externas de pickle no permitidas")


def leer_pkl(raw):
    """Valida el flujo antes de reconstruir solo clases inertes locales."""
    stop = None
    prohibited = {"EXT1", "EXT2", "EXT4", "PERSID", "BINPERSID", "NEXT_BUFFER", "READONLY_BUFFER"}
    for count, (opcode, arg, position) in enumerate(pickletools.genops(raw), 1):
        if count > MAX_OPCODES:
            raise LimiteRevision("PKL supera el presupuesto de instrucciones")
        if opcode.name in prohibited:
            raise FormatoNoSoportado("PKL usa extensiones o buffers externos no admitidos")
        if opcode.name == "PROTO" and arg > 5:
            raise FormatoNoSoportado("Protocolo pickle no soportado")
        if opcode.name == "FRAME" and arg > len(raw) - position - 9:
            raise ValueError("Frame de pickle truncado")
        if opcode.name == "STOP":
            stop = position
    if stop is None or stop + 1 != len(raw):
        raise ValueError("PKL truncado o con bytes adicionales despues de STOP")
    return _LectorRestringido(io.BytesIO(raw), fix_imports=False, encoding="latin1").load()


def numero(value):
    return type(value) in (int, float) and (not isinstance(value, float) or math.isfinite(value))


def inspeccionar_matriz(raw, max_celdas=MAX_CELDAS):
    result = dict(contenido_estado="FALLA", estado="FALLA")
    if not raw:
        return dict(result, razon="Archivo de cero bytes")
    try:
        grid = leer_pkl(raw)
        if type(grid) is not _Grid or not hasattr(grid, "state"):
            raise FormatoNoSoportado("Se esperaba una grilla open_cp.predictors.GridPredictionArray")
        state = grid.state
        for field in ("_xsize", "_ysize", "_xoffset", "_yoffset", "_matrix"):
            if field not in state:
                raise ValueError("Grilla sin atributo requerido: " + field)
        for field in ("_xsize", "_ysize", "_xoffset", "_yoffset"):
            if not numero(state[field]):
                raise ValueError("Metadato espacial no numerico o no finito: " + field)
            result[field[1:]] = state[field]
        if state["_xsize"] <= 0 or state["_ysize"] <= 0:
            raise ValueError("El tamano de celda debe ser positivo")
        matrix = state["_matrix"]
        if type(matrix) not in (_Array, _MaskedArray) or not hasattr(matrix, "shape"):
            raise FormatoNoSoportado("La grilla no contiene una matriz numerica compatible")
        if len(matrix.shape) != 2 or any(n == 0 for n in matrix.shape):
            raise ValueError("La matriz debe tener dos dimensiones no vacias")
        count = math.prod(matrix.shape)
        if count > max_celdas:
            raise LimiteRevision("Matriz supera el presupuesto de celdas")
        result.update(filas=matrix.shape[0], columnas=matrix.shape[1],
                      tipo_dato=matrix.dtype.kind + str(matrix.dtype.size), celdas_total=count)
        stats = Counter()
        minimum = maximum = None
        total = 0.0
        for i, (value,) in enumerate(struct.iter_unpack(matrix.dtype.format(), matrix.raw)):
            if matrix.mask is not None and matrix.mask[i]:
                stats["celdas_enmascaradas"] += 1
                continue
            if value == VALOR_SIN_DATO:
                stats["celdas_sin_dato"] += 1
                continue
            stats["celdas_validas"] += 1
            if not math.isfinite(value):
                stats["no_finitos"] += 1
                continue
            minimum = value if minimum is None else min(minimum, value)
            maximum = value if maximum is None else max(maximum, value)
            stats["negativos"] += value < 0
            stats["ceros"] += value == 0
            total += value
        result.update({key: stats[key] for key in (
            "celdas_validas", "celdas_enmascaradas", "celdas_sin_dato", "no_finitos", "negativos", "ceros")})
        result.update(minimo=minimum if minimum is not None else "",
                      maximo=maximum if maximum is not None else "",
                      suma=total if math.isfinite(total) else "",
                      sin_contraste=minimum is not None and minimum == maximum)
        errors, notices = [], []
        if stats["no_finitos"]:
            errors.append(f"{stats['no_finitos']} celdas sin mascara con NaN o infinito")
        if stats["negativos"]:
            errors.append(f"{stats['negativos']} intensidades negativas sin mascara")
        if not stats["celdas_validas"]:
            errors.append("Matriz sin celdas utilizables: todas enmascaradas o sin dato (-1)")
        if not math.isfinite(total):
            notices.append("Suma fuera de rango numerico; valores individuales revisados")
        if minimum is not None and minimum == maximum:
            notices.append("Todas las intensidades utilizables son cero" if maximum == 0 else
                           "Intensidades constantes: resultado sin contraste; no demuestra modelo Uniform")
        status = "FALLA" if errors else "AVISO" if notices else "OK"
        result.update(contenido_estado=status, estado=status,
                      razon="; ".join(errors + notices) or "PKL, grilla 2D, mascara y valores numericos revisados")
    except (FormatoNoSoportado, LimiteRevision) as exc:
        result.update(contenido_estado="NO EVALUADO", estado="INCOMPLETO", razon=str(exc))
    except (ValueError, EOFError, pickle.UnpicklingError, TypeError, AttributeError,
            IndexError, KeyError, struct.error, OverflowError, RecursionError) as exc:
        result.update(contenido_estado="FALLA", estado="FALLA",
                      razon="PKL o estructura de matriz invalida: " + str(exc)[:240])
    return result


def semana_actual():
    hoy = datetime.now(UTC_MENOS_5).date()
    return (hoy - timedelta(days=hoy.weekday())).strftime("%Y%m%d")


def semana_siguiente():
    return (validar_fecha(semana_actual()) + timedelta(days=7)).strftime("%Y%m%d")


def positivo(value):
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError("Debe ser un entero positivo")
    return n


def argumentos(argv=None):
    parser = argparse.ArgumentParser(description="Valida nombres, cobertura horaria y contenido de matrices PKL en S3.")
    parser.add_argument("--desde", default=semana_siguiente(), metavar="YYYYMMDD")
    parser.add_argument("--hasta", default=None, metavar="YYYYMMDD", help="Por defecto igual a --desde")
    parser.add_argument("--sigla", help="Limita la revision a un CAD")
    parser.add_argument("--unidad", help="Limita la revision a una unidad; requiere --sigla")
    parser.add_argument("--mostrar-todas", action="store_true", help="Muestra detalle por unidad")
    parser.add_argument("--solo-metadata", action="store_true", help="Revisa presencia; contenido queda NO EVALUADO")
    parser.add_argument("--max-lecturas", type=positivo, default=1000, help="GET nuevos por ejecucion; default 1000")
    parser.add_argument("--max-segundos", type=positivo, default=180, help="Presupuesto de lecturas entre llamadas; default 180")
    parser.add_argument("--max-mib", type=positivo, default=16, help="Maximo por PKL; default 16 MiB")
    parser.add_argument("--sin-cache", action="store_true", help="Relee contenido aunque no haya cambiado")
    parser.add_argument("--salida", type=Path, default=Path.cwd(), help="Directorio del ZIP y de la cache local")
    args = parser.parse_args(argv)
    args.hasta = args.hasta or args.desde
    if args.unidad and not args.sigla:
        parser.error("--unidad requiere --sigla")
    return args


def validar_fecha(fecha_str):
    try:
        return datetime.strptime(fecha_str, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("Fecha invalida; use YYYYMMDD: " + fecha_str) from exc


def conectar_s3():
    import boto3
    from botocore.config import Config

    client = boto3.client("s3", region_name=REGION, config=Config(
        connect_timeout=5, read_timeout=20, retries={"total_max_attempts": 2, "mode": "standard"}))
    client.head_bucket(Bucket=BUCKET, ExpectedBucketOwner=OWNER)
    return client


def listar_carpetas(s3, prefix):
    folders = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix, Delimiter="/", ExpectedBucketOwner=OWNER
    ):
        folders.update(p["Prefix"].rstrip("/").split("/")[-1] for p in page.get("CommonPrefixes", []))
    return sorted(folders)


def listar_semanas(s3, fecha_desde, fecha_hasta):
    start, end = validar_fecha(fecha_desde), validar_fecha(fecha_hasta)
    weeks = []
    for year in range(start.year, end.year + 1):
        for name in listar_carpetas(s3, f"{ROOT_PREFIX}/{year}/"):
            if not re.fullmatch(r"\d{8}", name):
                continue
            try:
                if start <= validar_fecha(name) <= end:
                    weeks.append(name)
            except ValueError:
                continue
    return sorted(set(weeks))


def listar_siglas(s3, semana, sigla_filtro=None):
    folders = listar_carpetas(s3, f"{ROOT_PREFIX}/{semana[:4]}/{semana}/")
    return [sigla_filtro.upper()] if sigla_filtro and sigla_filtro.upper() in folders else [] if sigla_filtro else folders


def listar_unidades(s3, semana, sigla):
    return listar_carpetas(s3, f"{ROOT_PREFIX}/{semana[:4]}/{semana}/{sigla}/")


def patron_prediccion_unidad(unidad):
    return re.compile(rf"^Prediccion_{re.escape(unidad)}_(?P<fecha>\d{{8}})_(?P<hora>\d{{4}})\.pkl$")


def stamp(value):
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


def huella(obj):
    return [obj.get("ETag", ""), obj["Size"], stamp(obj.get("LastModified"))]


def peor(a, b):
    order = {"OK": 0, "AVISO": 1, "NO EVALUADO": 2, "INCOMPLETO": 2, "FALLA": 3}
    return a if order[a] >= order[b] else b


class Revision:
    def __init__(self, args):
        self.args = args
        self.fin = time.monotonic() + args.max_segundos
        self.lecturas = 0
        self.cache = args.salida / ".estado_matrices"
        self.cache.mkdir(parents=True, exist_ok=True)

    def ruta_cache(self, semana, sigla, unidad):
        scope = f"{BUCKET}/{semana}/{sigla}/{unidad}"
        return self.cache / (hashlib.sha256(scope.encode()).hexdigest() + ".json.gz")

    def cargar(self, path):
        if self.args.sin_cache:
            return {}
        try:
            with gzip.open(path, "rb") as stream:
                data = stream.read(MAX_CACHE_BYTES + 1)
            if len(data) > MAX_CACHE_BYTES:
                return {}
            state = json.loads(data)
            if state.get("policy") != POLICY or not isinstance(state.get("archivos"), dict):
                return {}
            def valid(entry):
                if not isinstance(entry, dict) or not isinstance(entry.get("resultado"), dict):
                    return False
                result = entry["resultado"]
                if result.get("contenido_estado") not in ESTADOS_CONTENIDO or result.get("estado") not in ("OK", "AVISO", "FALLA", "INCOMPLETO"):
                    return False
                if result["contenido_estado"] in ("OK", "AVISO"):
                    return all(k in result for k in ("filas", "columnas", "tipo_dato", "celdas_total", "celdas_validas", "xsize", "ysize", "xoffset", "yoffset", "razon"))
                return isinstance(result.get("razon"), str)
            return {k: v for k, v in state["archivos"].items() if valid(v)}
        except (OSError, ValueError, TypeError, EOFError, AttributeError):
            return {}

    def guardar(self, path, entries):
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            raw = json.dumps(dict(policy=POLICY, archivos=entries), allow_nan=False).encode()
            with gzip.open(tmp, "wb") as stream:
                stream.write(raw)
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()

    def leer(self, s3, obj):
        if self.lecturas >= self.args.max_lecturas or time.monotonic() >= self.fin:
            raise LimiteRevision("Presupuesto agotado; repetir el comando conservando .estado_matrices/")
        if not obj.get("ETag"):
            raise LecturaIncompleta("S3 no devolvio ETag para vincular listado y contenido")
        self.lecturas += 1
        response = s3.get_object(Bucket=BUCKET, Key=obj["Key"], IfMatch=obj["ETag"], ExpectedBucketOwner=OWNER)
        body = response["Body"]
        try:
            if (response.get("ETag") != obj["ETag"] or response.get("ContentLength") != obj["Size"]
                    or stamp(response.get("LastModified")) != stamp(obj.get("LastModified"))):
                raise LecturaIncompleta("Objeto modificado entre listado y lectura; repetir revision")
            raw = body.read(self.args.max_mib * 1024**2 + 1)
        finally:
            body.close()
        if len(raw) != obj["Size"]:
            raise LecturaIncompleta("Cuerpo incompleto o superior al presupuesto de bytes")
        return raw


def fila_base(semana, sigla, unidad, obj):
    return dict(semana=semana, sigla=sigla, unidad=unidad, archivo=obj["Key"].rsplit("/", 1)[-1],
                ruta_s3="s3://" + BUCKET + "/" + obj["Key"], tamano_bytes=obj["Size"],
                fecha_modificacion=stamp(obj.get("LastModified")), nombre_estado="OK",
                contenido_estado="NO EVALUADO", estado="OK", origen_revision="PENDIENTE", razon="")


def validar_unidad(s3, semana, sigla, unidad, revision):
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/{sigla}/{unidad}/MatricesPrediccion/"
    pattern = patron_prediccion_unidad(unidad)
    rows, cobertura = [], defaultdict(set)
    cache_path = revision.ruta_cache(semana, sigla, unidad)
    cache = revision.cargar(cache_path)
    updates = dict(cache)
    listed, received, interrupted = True, set(), False
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/", ExpectedBucketOwner=OWNER
        ):
            for obj in page.get("Contents", []):
                name = obj["Key"][len(prefix):]
                if not name or "/" in name or not name.startswith("Prediccion_"):
                    continue
                row = fila_base(semana, sigla, unidad, obj)
                rows.append(row)
                match = pattern.fullmatch(name)
                valid = False
                if match:
                    row.update(fecha_archivo=match["fecha"], hora_archivo=match["hora"])
                    try:
                        validar_fecha(match["fecha"])
                        valid = match["hora"] in HORAS_ESPERADAS
                    except ValueError:
                        pass
                if not valid:
                    row.update(nombre_estado="FALLA", estado="FALLA", origen_revision="NOMBRE_INVALIDO",
                               razon="Nombre, unidad, fecha o hora invalidos; se espera Prediccion_UNIDAD_YYYYMMDD_HHMM.pkl")
                    continue
                cobertura[match["fecha"]].add(match["hora"])
                received.add(name)
                if obj["Size"] == 0:
                    row.update(contenido_estado="FALLA", estado="FALLA", origen_revision="METADATA",
                               razon="Archivo de cero bytes")
                    continue
                if revision.args.solo_metadata:
                    row.update(estado="INCOMPLETO", origen_revision="SOLO_METADATA", razon="Contenido no solicitado: --solo-metadata")
                    continue
                old = cache.get(name, {})
                if obj.get("ETag") and old.get("huella") == huella(obj):
                    row.update(old["resultado"])
                    row["origen_revision"] = "CACHE"
                    continue
                if obj["Size"] > revision.args.max_mib * 1024**2:
                    row.update(estado="INCOMPLETO", origen_revision="LIMITE_TAMANO", razon="Archivo supera --max-mib")
                    continue
                try:
                    result = inspeccionar_matriz(revision.leer(s3, obj))
                    row.update(result, origen_revision="GET")
                    updates[name] = dict(huella=huella(obj), resultado=result)
                    if len(received) % 25 == 0:
                        revision.guardar(cache_path, updates)
                except LimiteRevision as exc:
                    row.update(estado="INCOMPLETO", origen_revision="PRESUPUESTO", razon=str(exc))
                except Exception as exc:
                    row.update(contenido_estado="NO EVALUADO", estado="INCOMPLETO", origen_revision="LECTURA_INCOMPLETA",
                               razon="No se pudo completar lectura o guardar cache: " + str(exc)[:240])
    except KeyboardInterrupt:
        listed, interrupted = False, True
        for row in rows:
            if row["contenido_estado"] == "NO EVALUADO" and row["estado"] == "OK":
                row.update(estado="INCOMPLETO", origen_revision="INTERRUMPIDO", razon="Lectura interrumpida")
    except Exception as exc:
        listed = False
        rows.append(dict(semana=semana, sigla=sigla, unidad=unidad, archivo="", ruta_s3="s3://" + BUCKET + "/" + prefix,
                         nombre_estado="NO EVALUADO", contenido_estado="NO EVALUADO", estado="INCOMPLETO",
                         origen_revision="LISTADO_INCOMPLETO", razon="No se completo el listado: " + str(exc)[:240]))
    # Reusar solo datos del archivo revisado; las comparaciones se hacen de nuevo por unidad/semana.
    baseline = None
    for row in rows:
        if row["contenido_estado"] not in ("OK", "AVISO") or not row.get("filas"):
            continue
        current = tuple(row[k] for k in ("filas", "columnas", "xsize", "ysize", "xoffset", "yoffset"))
        if baseline is None:
            baseline = current
        elif current != baseline:
            row["estado"] = peor(row["estado"], "AVISO")
            row["razon"] += "; Forma o ubicacion de grilla diferente de otra matriz de esta unidad/semana"
    try:
        revision.guardar(cache_path, {k: v for k, v in updates.items() if not listed or k in received})
    except OSError as exc:
        rows.append(dict(semana=semana, sigla=sigla, unidad=unidad, archivo="", ruta_s3=str(cache_path),
                         nombre_estado="NO EVALUADO", contenido_estado="NO EVALUADO", estado="INCOMPLETO",
                         origen_revision="CACHE_NO_GUARDADA", razon="No se pudo persistir cache: " + str(exc)[:200]))
    faltantes = []
    if listed:
        for day in range(7):
            date = (validar_fecha(semana) + timedelta(days=day)).strftime("%Y%m%d")
            missing = sorted(HORAS_ESPERADAS - cobertura[date])
            if missing:
                faltantes.append(dict(semana=semana, sigla=sigla, unidad=unidad, fecha=date,
                    tipo_archivo="Prediccion", horas_presentes=len(cobertura[date]), cantidad_faltantes=len(missing),
                    horas_faltantes=";".join(missing), archivos_faltantes=";".join(f"Prediccion_{unidad}_{date}_{h}.pkl" for h in missing),
                    ruta_s3="s3://" + BUCKET + "/" + prefix))
    if interrupted:
        rows.append(dict(semana=semana, sigla=sigla, unidad=unidad, archivo="", nombre_estado="NO EVALUADO",
                         contenido_estado="NO EVALUADO", estado="INCOMPLETO", origen_revision="INTERRUMPIDO", razon="Revision interrumpida"))
    return len(received), rows, faltantes, interrupted


def tabla(resumen):
    headers = ("CAD", "Unid-semana", "Presentes", "PKL OK", "AVISO", "FALLA", "Sin revisar", "Faltantes")
    lines = [[cad] + [str(s[k]) for k in ("unidades", "presentes", "ok", "aviso", "falla", "pendientes", "faltantes")]
             for cad, s in sorted(resumen.items())]
    widths = [max(len(headers[i]), max((len(row[i]) for row in lines), default=0)) for i in range(len(headers))]
    print(" | ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in lines:
        print(" | ".join(v.ljust(w) for v, w in zip(row, widths)))


def reporte(s3, args):
    args.salida = args.salida.expanduser().resolve()
    args.salida.mkdir(parents=True, exist_ok=True)
    revision = Revision(args)
    now = datetime.now(UTC_MENOS_5)
    suffix = "_" + re.sub(r"[^A-Za-z0-9_-]", "_", args.sigla.upper()) if args.sigla else ""
    zip_path = args.salida / f"ResultadoMatrices{suffix}_{now:%Y%m%d_%H%M%S_%f}.zip"
    start, end = validar_fecha(args.desde), validar_fecha(args.hasta)
    label = f"{start:%Y_%m%d}-{end:%Y_%m%d}"
    resumen = defaultdict(Counter)
    general, interrupted, units = [], False, 0
    print(f"MATRICES | v{VERSION} | {BUCKET} | Semanas {args.desde} a {args.hasta}")
    print(f"Contenido: {'NO EVALUADO (--solo-metadata)' if args.solo_metadata else 'PKL, grilla y valores numericos'} | GET nuevos: {args.max_lecturas}; presupuesto: {args.max_segundos} s")
    with tempfile.TemporaryDirectory(prefix=".matrices_", dir=args.salida) as temp:
        detail = Path(temp) / f"detalle_matrices_rango_{label}.csv"
        missing = Path(temp) / f"horas_faltantes_matrices_rango_{label}.csv"
        with detail.open("w", encoding="utf-8-sig", newline="") as ds, missing.open("w", encoding="utf-8-sig", newline="") as ms:
            dw = csv.DictWriter(ds, fieldnames=COLUMNAS_DETALLE, extrasaction="ignore")
            mw = csv.DictWriter(ms, fieldnames=COLUMNAS_FALTANTES, extrasaction="ignore")
            dw.writeheader(); mw.writeheader()
            try:
                weeks = listar_semanas(s3, args.desde, args.hasta)
                if not weeks:
                    general.append("No se encontraron semanas para procesar")
                for week in weeks:
                    cads = listar_siglas(s3, week, args.sigla)
                    if args.sigla and not cads:
                        general.append(f"{week}: sin carpeta del CAD solicitado {args.sigla}")
                    for cad in cads:
                        unit_list = listar_unidades(s3, week, cad)
                        if args.unidad:
                            unit_list = [u for u in unit_list if u == args.unidad]
                        if not unit_list:
                            general.append(f"{week}/{cad}: sin unidades para procesar")
                        for unit in unit_list:
                            count, rows, absent, interrupted = validar_unidad(s3, week, cad, unit, revision)
                            dw.writerows(rows); mw.writerows(absent)
                            stats = resumen[cad]
                            stats["unidades"] += 1; stats["presentes"] += count
                            stats["faltantes"] += sum(r["cantidad_faltantes"] for r in absent)
                            for row in rows:
                                key = {"OK": "ok", "AVISO": "aviso", "FALLA": "falla", "INCOMPLETO": "pendientes"}[row["estado"]]
                                stats[key] += 1
                            units += 1
                            if args.mostrar_todas or units % 25 == 0:
                                print(f"[INFO] {units} unidades-semana revisadas; ultima {week}/{cad}/{unit}; GET nuevos {revision.lecturas}", flush=True)
                            if interrupted:
                                raise KeyboardInterrupt
            except KeyboardInterrupt:
                interrupted = True
                general.append("Captura interrumpida; informe parcial")
            except Exception as exc:
                general.append("Descubrimiento incompleto: " + str(exc)[:240])
            if not units:
                general.append("Sin unidades analizadas; no se declara resultado satisfactorio")
            for note in general:
                dw.writerow(dict(archivo="", nombre_estado="NO EVALUADO", contenido_estado="NO EVALUADO",
                    estado="INCOMPLETO", origen_revision="CONTROL_GENERAL", razon=note))
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(detail, detail.name); archive.write(missing, missing.name)
    tabla(resumen)
    for note in general:
        print("[INCOMPLETO] " + note)
    print(f"Informe: {zip_path} | 2 CSV: detalle por archivo y horas faltantes")
    print(f"GET nuevos: {revision.lecturas} | Cache: {revision.cache}")
    print("Presentes = nombres horarios encontrados; el contenido tiene su propio estado. Faltantes solo se calculan con listado completo.")
    print("Valores constantes/cero son AVISO; no identifican el modelo ni certifican calidad predictiva.")
    pending = any(s["pendientes"] for s in resumen.values()) or bool(general)
    if pending and not args.solo_metadata:
        print("[INFO] Si hubo limites de lectura, repetir el comando para continuar desde la cache.")
    if interrupted:
        return 130
    if any(s["falla"] or s["faltantes"] for s in resumen.values()):
        return 1
    return 3 if pending else 0


def main(argv=None):
    try:
        args = argumentos(argv)
        if validar_fecha(args.hasta) < validar_fecha(args.desde):
            raise ValueError("--hasta no puede ser anterior a --desde")
        return reporte(conectar_s3(), args)
    except KeyboardInterrupt:
        print("[INCOMPLETO] Interrumpido antes de completar el informe")
        return 130
    except Exception as exc:
        print("[BLOQUEADO] " + type(exc).__name__ + ": " + str(exc)[:400])
        return 3


if __name__ == "__main__":
    sys.exit(main())
