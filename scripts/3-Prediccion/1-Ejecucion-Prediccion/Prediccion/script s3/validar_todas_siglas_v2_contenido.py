#!/usr/bin/env python3
"""Copia independiente de V2: nombres, cobertura horaria y contenido GeoJSON.

El V2 original se conserva intacto. Solo lee resultados en S3; los informes y
la cache se guardan localmente. No requiere geopandas ni shapely.
"""

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


BUCKET = "pon-prod-ml-iapol"
OWNER = "926162397524"
REGION = "us-east-1"
ROOT_PREFIX = "PrediccionMdP"
VERSION = "v2-contenido-2026-11-03.1"
POLICY = "prediccion-geojson-2026-11-03-v1"
HORAS_ESPERADAS = {f"{hora:02d}00" for hora in range(24)}
UTC_MENOS_5 = timezone(timedelta(hours=-5))
MAX_ELEMENTOS = 1_000_000
MAX_FEATURES = 100_000
MAX_CACHE_BYTES = 8 * 1024**2
ESTADOS_CONTENIDO = {"OK", "AVISO", "FALLA", "NO EVALUADO"}
COLUMNAS_DETALLE = (
    "semana", "sigla", "unidad", "archivo", "ruta_s3", "fecha_archivo", "hora_archivo",
    "nombre_estado", "contenido_estado", "estado", "origen_revision", "tamano_bytes",
    "fecha_modificacion", "features", "tipos_geometria", "geometrias_nulas",
    "anillos_invalidos", "coordenadas_invalidas", "unidad_distinta", "fecha_distinta",
    "cad_distinto", "propiedades_ausentes", "intensidades_invalidas", "intensidades_cero",
    "intensidades_distintas", "intensidad_min", "intensidad_max", "sin_contraste", "razon",
)
COLUMNAS_FALTANTES = (
    "semana", "sigla", "unidad", "fecha", "tipo_archivo", "horas_presentes",
    "cantidad_faltantes", "horas_faltantes", "archivos_faltantes", "ruta_s3",
)


class LecturaIncompleta(RuntimeError):
    pass


class LimiteRevision(LecturaIncompleta):
    pass


def numero(value):
    return type(value) in (int, float) and (type(value) is int or math.isfinite(value))


def identidad_unidad(value):
    if type(value) is int:
        return str(value)
    if type(value) is float and math.isfinite(value) and value.is_integer():
        return str(int(value))
    return value.strip() if isinstance(value, str) else None


def json_sin_duplicados(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Clave JSON repetida")
        result[key] = value
    return result


def constante_invalida(value):
    raise ValueError("Constante JSON no finita")


def crs_geografico(data):
    """Sin CRS se aplica GeoJSON WGS84. Otros CRS quedan sin interpretar."""
    if "crs" not in data or data["crs"] is None:
        return True
    crs = data["crs"]
    if not isinstance(crs, dict) or crs.get("type") != "name" or not isinstance(crs.get("properties"), dict):
        return False
    name = crs["properties"].get("name")
    return isinstance(name, str) and name.upper() in {
        "EPSG:4326", "URN:OGC:DEF:CRS:EPSG::4326", "OGC:CRS84",
        "URN:OGC:DEF:CRS:OGC:1.3:CRS84", "URN:OGC:DEF:CRS:OGC::CRS84",
    }


def inspeccionar_geojson(raw, cad, unidad, fecha, hora, max_elementos=MAX_ELEMENTOS):
    """Controles estructurales e identidad; no certifica topologia ni precision."""
    result = dict(contenido_estado="FALLA", estado="FALLA")
    if not raw:
        return dict(result, razon="Archivo de cero bytes")
    try:
        data = json.loads(raw.decode("utf-8-sig"), parse_constant=constante_invalida,
                          object_pairs_hook=json_sin_duplicados)
        if not isinstance(data, dict) or data.get("type") != "FeatureCollection" or not isinstance(data.get("features"), list):
            return dict(result, razon="Se requiere FeatureCollection con features en lista")
        if len(data["features"]) > MAX_FEATURES:
            raise LimiteRevision("GeoJSON supera el presupuesto de features")
        errors, notices, partial = Counter(), Counter(), Counter()
        stack = [data]
        elements = 0
        while stack:
            value = stack.pop()
            elements += 1
            if elements > max_elementos:
                raise LimiteRevision("GeoJSON supera el presupuesto de elementos")
            if isinstance(value, float) and not math.isfinite(value):
                errors["Numeros JSON no finitos"] += 1
            elif isinstance(value, dict):
                stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)
        geographic = crs_geografico(data)
        if not geographic:
            partial["CRS no soportado; coordenadas geograficas sin evaluar"] += 1
        if not data["features"]:
            errors["FeatureCollection vacia; sin resultado utilizable"] += 1

        stats, kinds = Counter(), Counter()
        intensities = set()

        def position(point):
            if not isinstance(point, list) or len(point) not in (2, 3) or not all(numero(v) for v in point):
                return False
            return not geographic or (-180 <= point[0] <= 180 and -90 <= point[1] <= 90)

        def polygon(coords):
            if not isinstance(coords, list) or not coords:
                stats["anillos_invalidos"] += 1
                return
            for ring in coords:
                if not isinstance(ring, list):
                    stats["anillos_invalidos"] += 1
                    continue
                valid_points = all(position(point) for point in ring)
                stats["coordenadas_invalidas"] += sum(not position(point) for point in ring)
                if len(ring) < 4 or ring[0] != ring[-1]:
                    stats["anillos_invalidos"] += 1
                elif valid_points and len({tuple(point[:2]) for point in ring}) < 3:
                    stats["anillos_invalidos"] += 1

        for feature in data["features"]:
            if (not isinstance(feature, dict) or feature.get("type") != "Feature"
                    or "properties" not in feature or "geometry" not in feature):
                errors["Feature sin estructura requerida"] += 1
                continue
            props = feature["properties"]
            if not isinstance(props, dict):
                errors["properties debe contener los datos del resultado"] += 1
            else:
                for name in ("id_unidadbase", "prediction_date", "intensity_val"):
                    if name not in props:
                        stats["propiedades_ausentes"] += 1
                if "id_unidadbase" in props:
                    if identidad_unidad(props["id_unidadbase"]) != unidad:
                        stats["unidad_distinta"] += 1
                if "prediction_date" in props and props["prediction_date"] != fecha + "_" + hora:
                    stats["fecha_distinta"] += 1
                if "CAD" in props and props["CAD"] is not None and props["CAD"] != cad:
                    stats["cad_distinto"] += 1
                if "intensity_val" in props:
                    intensity = props["intensity_val"]
                    if not numero(intensity) or intensity < 0:
                        stats["intensidades_invalidas"] += 1
                    else:
                        intensities.add(intensity)
                        stats["intensidades_cero"] += intensity == 0
                        if intensity > 255:
                            notices["Intensidad mayor a la escala observada 0-255; revisar productor"] += 1
                for name, low, high in (("centroid_x", -180, 180), ("centroid_y", -90, 90)):
                    if name in props and (not numero(props[name]) or geographic and not low <= props[name] <= high):
                        errors["Centroide invalido o fuera de rango"] += 1

            geom = feature["geometry"]
            if geom is None:
                stats["geometrias_nulas"] += 1
                continue
            if not isinstance(geom, dict) or not isinstance(geom.get("type"), str):
                errors["Geometria sin tipo valido"] += 1
                continue
            kind = geom["type"]
            kinds[kind] += 1
            if "crs" in geom and not crs_geografico(geom):
                partial["CRS en geometria no soportado"] += 1
                continue
            if kind == "Polygon":
                polygon(geom.get("coordinates"))
            elif kind == "MultiPolygon":
                coords = geom.get("coordinates")
                if not isinstance(coords, list) or not coords:
                    stats["anillos_invalidos"] += 1
                else:
                    for poly in coords:
                        polygon(poly)
            elif kind in ("Point", "MultiPoint", "LineString", "MultiLineString", "GeometryCollection"):
                partial["Geometria fuera del perfil poligonal; requiere revision"] += 1
            else:
                errors["Tipo de geometria no reconocido"] += 1

        for name, label in (
            ("propiedades_ausentes", "Propiedades requeridas ausentes"),
            ("unidad_distinta", "id_unidadbase distinto de la ruta o nulo"),
            ("fecha_distinta", "prediction_date distinto del nombre o nulo"),
            ("cad_distinto", "CAD distinto de la ruta"),
            ("intensidades_invalidas", "intensity_val nulo, no numerico, no finito o negativo"),
            ("geometrias_nulas", "Geometria nula; sin resultado espacial"),
            ("anillos_invalidos", "Poligonos vacios o anillos abiertos, cortos o degenerados"),
            ("coordenadas_invalidas", "Coordenadas invalidas, no finitas o fuera de rango"),
        ):
            if stats[name]:
                errors[label] += stats[name]
        constant = bool(intensities) and len(intensities) == 1
        if constant:
            notices["Todas las intensidades son cero" if next(iter(intensities)) == 0 else
                    "Intensidades constantes; sin contraste, no demuestra modelo Uniform"] += 1
        status = "FALLA" if errors else "INCOMPLETO" if partial else "AVISO" if notices else "OK"
        result.update({key: stats[key] for key in (
            "geometrias_nulas", "anillos_invalidos", "coordenadas_invalidas", "unidad_distinta",
            "fecha_distinta", "cad_distinto", "propiedades_ausentes", "intensidades_invalidas", "intensidades_cero")})
        result.update(contenido_estado="NO EVALUADO" if partial else status, estado=status,
            features=len(data["features"]), tipos_geometria=";".join(f"{k}:{n}" for k, n in sorted(kinds.items())),
            intensidades_distintas=len(intensities), intensidad_min=min(intensities) if intensities else "",
            intensidad_max=max(intensities) if intensities else "", sin_contraste=constant,
            razon="; ".join(f"{label} ({n})" for group in (errors, partial, notices) for label, n in group.items())
                or "GeoJSON, poligonos, coordenadas, unidad, fecha e intensidades revisados")
    except LimiteRevision as exc:
        result.update(contenido_estado="NO EVALUADO", estado="INCOMPLETO", razon=str(exc))
    except (ValueError, UnicodeError, RecursionError, OverflowError, TypeError, IndexError) as exc:
        result.update(razon="GeoJSON ilegible, ambiguo o invalido: " + str(exc)[:200])
    return result


def semana_actual():
    hoy = datetime.now(UTC_MENOS_5).date()
    return (hoy - timedelta(days=hoy.weekday())).strftime("%Y%m%d")


def positivo(value):
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError("Debe ser un entero positivo")
    return n


def argumentos(argv=None):
    parser = argparse.ArgumentParser(description="Copia V2: nombres, cobertura horaria y contenido de predicciones GeoJSON.")
    parser.add_argument("--desde", default=semana_actual(), metavar="YYYYMMDD")
    parser.add_argument("--hasta", default=None, metavar="YYYYMMDD",
                        help="Sin valor: todas las semanas desde --desde dentro de ese mismo anio, como V2")
    parser.add_argument("--sigla", help="Limita la revision a un CAD")
    parser.add_argument("--unidad", help="Limita la revision a una unidad; requiere --sigla")
    parser.add_argument("--mostrar-todas", action="store_true", help="Muestra progreso por unidad")
    parser.add_argument("--solo-metadata", action="store_true", help="Contenido queda NO EVALUADO")
    parser.add_argument("--max-lecturas", type=positivo, default=1000, help="GET nuevos; default 1000")
    parser.add_argument("--max-segundos", type=positivo, default=180, help="Presupuesto entre lecturas; default 180")
    parser.add_argument("--max-mib", type=positivo, default=16, help="Maximo por GeoJSON; default 16 MiB")
    parser.add_argument("--sin-cache", action="store_true", help="Relee contenido sin reutilizar resultados")
    parser.add_argument("--salida", type=Path, default=Path.cwd(), help="Directorio de informes y cache")
    args = parser.parse_args(argv)
    if args.unidad and not args.sigla:
        parser.error("--unidad requiere --sigla")
    return args


def validar_fecha(fecha_str):
    try:
        if not re.fullmatch(r"\d{8}", fecha_str):
            raise ValueError("Se requieren ocho digitos")
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
    start = validar_fecha(fecha_desde)
    end = validar_fecha(fecha_hasta) if fecha_hasta else datetime(start.year, 12, 31)
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
    return re.compile(rf"^Prediccion_{re.escape(unidad)}_(?P<fecha>\d{{8}})_(?P<hora>\d{{4}})\.geojson$")


def stamp(value):
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


def huella(obj):
    return [obj.get("ETag", ""), obj["Size"], stamp(obj.get("LastModified"))]


class Revision:
    def __init__(self, args):
        self.args = args
        self.fin = time.monotonic() + args.max_segundos
        self.lecturas = 0
        self.cache = args.salida / ".estado_predicciones_v2_contenido"
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
                    return all(k in result for k in ("features", "tipos_geometria", "intensidades_distintas", "sin_contraste", "razon"))
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
            raise LimiteRevision("Presupuesto agotado; repetir el comando conservando .estado_predicciones_v2_contenido/")
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
    prefix = f"{ROOT_PREFIX}/{semana[:4]}/{semana}/{sigla}/{unidad}/Prediccion/"
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
                               razon="Nombre, unidad, fecha o hora invalidos; se espera Prediccion_UNIDAD_YYYYMMDD_HHMM.geojson")
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
                    result = inspeccionar_geojson(revision.leer(s3, obj), sigla, unidad, match['fecha'], match['hora'])
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
                    horas_faltantes=";".join(missing), archivos_faltantes=";".join(f"Prediccion_{unidad}_{date}_{h}.geojson" for h in missing),
                    ruta_s3="s3://" + BUCKET + "/" + prefix))
    if interrupted:
        rows.append(dict(semana=semana, sigla=sigla, unidad=unidad, archivo="", nombre_estado="NO EVALUADO",
                         contenido_estado="NO EVALUADO", estado="INCOMPLETO", origen_revision="INTERRUMPIDO", razon="Revision interrumpida"))
    return len(received), rows, faltantes, interrupted


def tabla(resumen):
    headers = ("CAD", "Unid-semana", "Presentes", "GeoJSON OK", "AVISO", "FALLA", "Sin revisar", "Faltantes")
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
    folder = args.salida / f"ResultadoAnalisisContenido{suffix}_{now:%Y%m%d_%H%M%S_%f}"
    folder.mkdir()
    start = validar_fecha(args.desde)
    end = validar_fecha(args.hasta) if args.hasta else datetime(start.year, 12, 31)
    label = f"{start:%Y_%m%d}-{end:%Y_%m%d}"
    detail = folder / f"detalle_prediccion_rango_{label}.csv"
    missing = folder / f"horas_faltantes_rango_{label}.csv"
    resumen = defaultdict(Counter)
    general, interrupted, units = [], False, 0
    print(f"PREDICCIONES | {VERSION} | {BUCKET} | Desde {args.desde} hasta {args.hasta or 'ultima disponible del anio'}")
    print(f"Contenido: {'NO EVALUADO (--solo-metadata)' if args.solo_metadata else 'GeoJSON e identidad'} | GET nuevos: {args.max_lecturas}; presupuesto: {args.max_segundos} s")
    with detail.open("w", encoding="utf-8-sig", newline="") as ds, missing.open("w", encoding="utf-8-sig", newline="") as ms:
        dw = csv.DictWriter(ds, fieldnames=COLUMNAS_DETALLE, extrasaction="ignore")
        mw = csv.DictWriter(ms, fieldnames=COLUMNAS_FALTANTES)
        dw.writeheader()
        mw.writeheader()
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
                        dw.writerows(rows)
                        mw.writerows(absent)
                        ds.flush()
                        ms.flush()
                        stats = resumen[cad]
                        stats["unidades"] += 1
                        stats["presentes"] += count
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
    tabla(resumen)
    for note in general:
        print("[INCOMPLETO] " + note)
    print(f"Informes: {detail} | {missing}")
    print(f"GET nuevos: {revision.lecturas} | Cache: {revision.cache}")
    print("Presentes = nombres horarios encontrados; el contenido tiene su propio estado. Faltantes solo con listado completo.")
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
        validar_fecha(args.desde)
        if args.hasta and validar_fecha(args.hasta) < validar_fecha(args.desde):
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
