"""Pruebas de la copia V2 con contenido. Sin AWS ni librerias geoespaciales."""

import copy
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[1] / "validar_todas_siglas_v2_contenido.py"
SPEC = importlib.util.spec_from_file_location("prediccion_v2_contenido", PATH)
v = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v)


def documento(valores=(0, 12)):
    ring = [[-75.1, 4.7], [-75.0, 4.7], [-75.0, 4.8], [-75.1, 4.7]]
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [copy.deepcopy(ring)]},
         "properties": {"id_unidadbase": "51716", "prediction_date": "20261102_0000", "intensity_val": val}}
        for val in valores]}


def raw(data=None):
    return json.dumps(documento() if data is None else data).encode()


def revisar(data=None, contenido=None, **kwargs):
    return v.inspeccionar_geojson(raw(data) if contenido is None else contenido, "MEPER", "51716", "20261102", "0000", **kwargs)


class Cuerpo(io.BytesIO):
    cerrado = False

    def close(self):
        self.cerrado = True
        super().close()


class S3:
    def __init__(self, records):
        self.records = records
        self.gets = []
        self.bodies = []
        self.mutate = False
        self.fail_get = False
        self.fail_list = False

    def get_paginator(self, operation):
        if operation != "list_objects_v2":
            raise AssertionError(operation)
        return self

    def paginate(self, **kwargs):
        if self.fail_list:
            raise OSError("AccessDenied al listar")
        prefix = kwargs["Prefix"]
        contents, folders = [], set()
        for key, (raw, stamp) in self.records.items():
            if not key.startswith(prefix):
                continue
            relative = key[len(prefix):]
            if "/" in relative:
                folders.add(prefix + relative.split("/", 1)[0] + "/")
            elif relative:
                contents.append(dict(Key=key, Size=len(raw), LastModified=stamp, ETag=hashlib.md5(raw).hexdigest()))
        yield dict(Contents=contents, CommonPrefixes=[dict(Prefix=k) for k in sorted(folders)])

    def get_object(self, **kwargs):
        self.gets.append(kwargs)
        if self.fail_get:
            raise OSError("AccessDenied al leer")
        raw, stamp = self.records[kwargs["Key"]]
        body = Cuerpo(raw)
        self.bodies.append(body)
        return dict(Body=body, ETag="cambio" if self.mutate else hashlib.md5(raw).hexdigest(),
                    ContentLength=len(raw), LastModified=stamp)


def revision(folder, **kwargs):
    args = v.argumentos(["--desde", "20261102", "--sigla", "MEPER", "--unidad", "51716", "--salida", str(folder)])
    for key, value in kwargs.items():
        setattr(args, key, value)
    return v.Revision(args)


def archivo(raw, date="20261102", hour="0000", name=None):
    filename = name or f"Prediccion_51716_{date}_{hour}.geojson"
    key = "PrediccionMdP/2026/20261102/MEPER/51716/Prediccion/" + filename
    return {key: (raw, datetime(2026, 11, 1, tzinfo=timezone.utc))}



class Contenido(unittest.TestCase):
    def test_perfil_valido(self):
        r = revisar()
        self.assertEqual(r["estado"], "OK")
        self.assertEqual((r["features"], r["intensidad_min"], r["intensidad_max"]), (2, 0, 12))
        self.assertFalse(r["sin_contraste"])

    def test_utf8_bom(self):
        self.assertEqual(revisar(contenido=b"\xef\xbb\xbf" + raw())["estado"], "OK")

    def test_vacio_corrupto_truncado_doble_codificacion_y_trailing(self):
        for contenido in (b"", b"<html>error</html>", raw()[:-1], json.dumps(raw().decode()).encode(), raw() + b"sobrante", b"\xff"):
            with self.subTest(contenido=contenido[:20]):
                self.assertEqual(revisar(contenido=contenido)["estado"], "FALLA")

    def test_claves_repetidas_y_constantes_no_finitas(self):
        for contenido in (b'{"type":"FeatureCollection","type":"FeatureCollection","features":[]}',
                          raw().replace(b'"intensity_val": 12', b'"intensity_val": NaN'),
                          raw().replace(b'"intensity_val": 12', b'"intensity_val": Infinity')):
            self.assertEqual(revisar(contenido=contenido)["estado"], "FALLA")

    def test_no_finito_por_exponente_incluso_en_campos_adicionales(self):
        d = documento()
        d["extra"] = float("inf")
        self.assertEqual(revisar(contenido=raw(d).replace(b"Infinity", b"1e999"))["estado"], "FALLA")

    def test_collection_y_features_requeridos(self):
        for d in ({}, [], {"type": "FeatureCollection", "features": {}},
                  {"type": "FeatureCollection", "features": []},
                  {"type": "FeatureCollection", "features": [None]}):
            self.assertEqual(revisar(d)["estado"], "FALLA")

    def test_propiedades_nulas_y_requeridas(self):
        for field in ("id_unidadbase", "prediction_date", "intensity_val"):
            d = documento()
            del d["features"][0]["properties"][field]
            self.assertEqual(revisar(d)["estado"], "FALLA")
        d["features"][0]["properties"] = None
        self.assertEqual(revisar(d)["estado"], "FALLA")

    def test_unidad_del_contenido_contra_ruta(self):
        d = documento()
        d["features"][0]["properties"]["id_unidadbase"] = "50330"
        r = revisar(d)
        self.assertEqual((r["estado"], r["unidad_distinta"]), ("FALLA", 1))

    def test_ids_numericos_integrales_y_nulos(self):
        for value in (51716, 51716.0):
            d = documento()
            d["features"][0]["properties"]["id_unidadbase"] = value
            self.assertEqual(revisar(d)["estado"], "OK")
        for value in (None, True, {}, 51716.5):
            d["features"][0]["properties"]["id_unidadbase"] = value
            self.assertEqual(revisar(d)["estado"], "FALLA")

    def test_fecha_hora_contenido_contra_nombre(self):
        for value in ("20261102_0100", "20261103_0000", None, 20261102):
            d = documento()
            d["features"][0]["properties"]["prediction_date"] = value
            r = revisar(d)
            self.assertEqual((r["estado"], r["fecha_distinta"]), ("FALLA", 1))

    def test_cad_opcional_y_si_presente_se_contrasta(self):
        for value, status in (("MEPER", "OK"), ("METIB", "FALLA"), (None, "OK")):
            d = documento()
            d["features"][0]["properties"]["CAD"] = value
            self.assertEqual(revisar(d)["estado"], status)

    def test_intensidades_invalidas(self):
        for value in (None, "2", True, -1, -0.5, {}, []):
            d = documento()
            d["features"][0]["properties"]["intensity_val"] = value
            r = revisar(d)
            self.assertEqual((r["estado"], r["intensidades_invalidas"]), ("FALLA", 1))

    def test_constantes_y_ceros_son_avisos_sin_afirmar_uniform(self):
        for values in ((0,), (0, 0), (0.0001, 0.0001)):
            r = revisar(documento(values))
            self.assertEqual(r["estado"], "AVISO")
            self.assertTrue(r["sin_contraste"])

    def test_fuera_escala_observada_es_aviso(self):
        self.assertEqual(revisar(documento((0, 256)))["estado"], "AVISO")

    def test_geometria_nula_o_mal_formada(self):
        for geometry in (None, {}, {"type": "Desconocido"}, {"type": 42}, {"type": "Polygon", "coordinates": []}):
            d = documento()
            d["features"][0]["geometry"] = geometry
            self.assertEqual(revisar(d)["estado"], "FALLA")

    def test_multipoligono_y_huecos(self):
        d = documento()
        coords = d["features"][0]["geometry"]["coordinates"]
        d["features"][0]["geometry"] = {"type": "MultiPolygon", "coordinates": [copy.deepcopy(coords), copy.deepcopy(coords)]}
        r = revisar(d)
        self.assertEqual(r["estado"], "OK")
        self.assertIn("MultiPolygon:1", r["tipos_geometria"])

    def test_anillos_abiertos_cortos_repetidos_y_vacios(self):
        for ring in ([], [[0, 0]], [[0, 0], [1, 1], [2, 2], [3, 3]],
                     [[0, 0], [0, 0], [0, 0], [0, 0]]):
            d = documento()
            d["features"][0]["geometry"]["coordinates"] = [ring]
            r = revisar(d)
            self.assertEqual(r["estado"], "FALLA")
            self.assertGreater(r["anillos_invalidos"], 0)

    def test_posiciones_invalidas(self):
        for point in ([181, 0], [0, 91], ["-75", 4], [True, 4], [0], None, [0, 0, 0, 0]):
            d = documento()
            d["features"][0]["geometry"]["coordinates"][0][1] = point
            r = revisar(d)
            self.assertEqual(r["estado"], "FALLA")
            self.assertGreater(r["coordenadas_invalidas"], 0)

    def test_altitud_y_crs_4326(self):
        d = documento()
        for f in d["features"]:
            f["geometry"]["coordinates"][0] = [p + [1.5] for p in f["geometry"]["coordinates"][0]]
        d["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}
        self.assertEqual(revisar(d)["estado"], "OK")

    def test_crs_no_soportado_no_da_falso_ok_ni_fallo_por_rango(self):
        d = documento()
        d["crs"] = {"type": "name", "properties": {"name": "EPSG:3857"}}
        d["features"][0]["geometry"]["coordinates"] = [[[800000, 400000], [800001, 400000], [800001, 400001], [800000, 400000]]]
        r = revisar(d)
        self.assertEqual((r["estado"], r["contenido_estado"]), ("INCOMPLETO", "NO EVALUADO"))
        self.assertEqual(r["coordenadas_invalidas"], 0)

    def test_geometria_fuera_perfil_queda_sin_evaluar(self):
        d = documento()
        d["features"][0]["geometry"] = {"type": "Point", "coordinates": [-75, 4]}
        self.assertEqual(revisar(d)["estado"], "INCOMPLETO")

    def test_limite_elementos_y_features(self):
        self.assertEqual(revisar(max_elementos=5)["estado"], "INCOMPLETO")
        with patch.object(v, "MAX_FEATURES", 1):
            self.assertEqual(revisar()["estado"], "INCOMPLETO")

    def test_centroides_si_presentes(self):
        for value in (200, None, "a", True):
            d = documento()
            d["features"][0]["properties"]["centroid_x"] = value
            self.assertEqual(revisar(d)["estado"], "FALLA")


class Lecturas(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.s3 = S3(archivo(raw()))

    def tearDown(self):
        self.tmp.cleanup()

    def revisar_unidad(self, **kwargs):
        r = revision(self.folder, **kwargs)
        return v.validar_unidad(self.s3, "20261102", "MEPER", "51716", r)

    def test_get_condicional_y_cierre_del_cuerpo(self):
        count, rows, missing, interrupted = self.revisar_unidad()
        self.assertEqual((count, rows[0]["estado"], interrupted), (1, "OK", False))
        self.assertEqual(len(self.s3.gets), 1)
        self.assertEqual(self.s3.gets[0]["IfMatch"], hashlib.md5(raw()).hexdigest())
        self.assertEqual(self.s3.gets[0]["ExpectedBucketOwner"], v.OWNER)
        self.assertTrue(self.s3.bodies[0].cerrado)

    def test_cache_y_cambios_etag_o_fecha(self):
        self.revisar_unidad()
        self.assertEqual(self.revisar_unidad()[1][0]["origen_revision"], "CACHE")
        self.assertEqual(len(self.s3.gets), 1)
        key = next(iter(self.s3.records))
        self.s3.records[key] = (raw(documento((0, 13))), datetime(2026, 11, 1, tzinfo=timezone.utc))
        self.revisar_unidad()
        self.assertEqual(len(self.s3.gets), 2)
        content, stamp = self.s3.records[key]
        self.s3.records[key] = (content, stamp + timedelta(seconds=1))
        self.revisar_unidad()
        self.assertEqual(len(self.s3.gets), 3)

    def test_cache_corrupta_o_politica_distinta(self):
        self.revisar_unidad()
        r = revision(self.folder)
        path = r.ruta_cache("20261102", "MEPER", "51716")
        path.write_bytes(b"corrupto")
        self.revisar_unidad()
        self.assertEqual(len(self.s3.gets), 2)
        with gzip.open(path, "rt") as f:
            d = json.load(f)
        d["policy"] = "anterior"
        with gzip.open(path, "wt") as f:
            json.dump(d, f)
        self.revisar_unidad()
        self.assertEqual(len(self.s3.gets), 3)

    def test_presupuesto_y_reanudacion(self):
        d = documento()
        for f in d["features"]:
            f["properties"]["prediction_date"] = "20261102_0100"
        self.s3.records.update(archivo(raw(d), hour="0100"))
        first = self.revisar_unidad(max_lecturas=1)[1]
        self.assertEqual([r["estado"] for r in first], ["OK", "INCOMPLETO"])
        second = self.revisar_unidad(max_lecturas=1)[1]
        self.assertEqual([r["estado"] for r in second], ["OK", "OK"])
        self.assertEqual([r["origen_revision"] for r in second], ["CACHE", "GET"])

    def test_solo_metadata_no_consume_get_y_preserva_cache(self):
        self.revisar_unidad()
        r = self.revisar_unidad(solo_metadata=True)[1][0]
        self.assertEqual((r["estado"], r["contenido_estado"]), ("INCOMPLETO", "NO EVALUADO"))
        self.assertEqual(len(self.s3.gets), 1)
        self.assertEqual(self.revisar_unidad()[1][0]["origen_revision"], "CACHE")

    def test_cambio_durante_get_y_acceso_denegado(self):
        self.s3.mutate = True
        self.assertEqual(self.revisar_unidad()[1][0]["estado"], "INCOMPLETO")
        self.assertTrue(self.s3.bodies[-1].cerrado)
        self.s3.fail_get = True
        self.assertEqual(self.revisar_unidad()[1][0]["estado"], "INCOMPLETO")

    def test_archivo_vacio_grande_o_nombre_invalido_sin_get(self):
        for contenido, name, status in (
            (b"", "Prediccion_51716_20261102_0000.geojson", "FALLA"),
            (b"x" * (1024**2 + 1), "Prediccion_51716_20261102_0000.geojson", "INCOMPLETO"),
            (raw(), "Prediccion_51716_20261102_0.geojson", "FALLA")):
            self.s3.records = archivo(contenido, name=name)
            self.assertEqual(self.revisar_unidad(max_mib=1)[1][0]["estado"], status)
        self.assertEqual(len(self.s3.gets), 0)

    def test_puntos_apoyo_y_turno_ignorados(self):
        self.s3.records.update(archivo(b"ignorado", name="PuntosApoyo_51716_20261102_0000.geojson"))
        self.s3.records.update(archivo(b"ignorado", name="Turno/Prediccion_51716_20261102_1.geojson"))
        count, rows, missing, interrupted = self.revisar_unidad()
        self.assertEqual((count, len(rows), len(self.s3.gets)), (1, 1, 1))

    def test_listado_incompleto_no_inventa_faltantes(self):
        self.s3.fail_list = True
        count, rows, missing, interrupted = self.revisar_unidad()
        self.assertEqual((rows[0]["estado"], missing), ("INCOMPLETO", []))

    def test_interrupcion_no_deja_falso_ok(self):
        with patch.object(self.s3, "get_object", side_effect=KeyboardInterrupt):
            count, rows, missing, interrupted = self.revisar_unidad()
        self.assertTrue(interrupted)
        self.assertFalse(missing)
        self.assertTrue(all(r["estado"] != "OK" for r in rows))

    def test_rango_por_defecto_igual_v2_y_cruce_de_anio_explicito(self):
        self.s3.records.update({"PrediccionMdP/2026/20261109/A/x": (b"", None), "PrediccionMdP/2027/20270104/A/x": (b"", None)})
        self.assertEqual(v.listar_semanas(self.s3, "20261102", None), ["20261102", "20261109"])
        self.assertEqual(v.listar_semanas(self.s3, "20261102", "20270104"), ["20261102", "20261109", "20270104"])

    def test_reporte_dos_csv_incluye_fallas_y_pendientes(self):
        args = v.argumentos(["--desde", "20261102", "--hasta", "20261102", "--sigla", "MEPER", "--salida", str(self.folder)])
        with redirect_stdout(io.StringIO()):
            code = v.reporte(self.s3, args)
        self.assertEqual(code, 1)
        files = list(self.folder.glob("ResultadoAnalisisContenido*/*.csv"))
        self.assertEqual(len(files), 2)
        detail = next(p for p in files if p.name.startswith("detalle_"))
        with detail.open(encoding="utf-8-sig") as f:
            row = next(csv.DictReader(f))
        self.assertEqual((row["estado"], row["features"]), ("OK", "2"))

    def test_semana_completa_y_contenido_valido(self):
        self.s3.records = {}
        for i in range(7):
            date = (datetime(2026, 11, 2) + timedelta(days=i)).strftime("%Y%m%d")
            for hour in sorted(v.HORAS_ESPERADAS):
                d = documento()
                for f in d["features"]:
                    f["properties"]["prediction_date"] = date + "_" + hour
                self.s3.records.update(archivo(raw(d), date=date, hour=hour))
        args = v.argumentos(["--desde", "20261102", "--hasta", "20261102", "--salida", str(self.folder)])
        with redirect_stdout(io.StringIO()):
            code = v.reporte(self.s3, args)
        self.assertEqual(code, 0)
        self.assertEqual(len(self.s3.gets), 168)

    def test_sin_unidades_no_da_exito(self):
        self.s3.records = {}
        args = v.argumentos(["--desde", "20261102", "--hasta", "20261102", "--salida", str(self.folder)])
        with redirect_stdout(io.StringIO()):
            self.assertEqual(v.reporte(self.s3, args), 3)

    def test_cli_valida_antes_de_conectar_a_aws(self):
        with patch.object(v, "conectar_s3") as connect, redirect_stdout(io.StringIO()):
            self.assertEqual(v.main(["--desde", "2026112"]), 3)
        connect.assert_not_called()

    def test_nombres_y_cobertura_conservan_resultado_del_original(self):
        spec = importlib.util.spec_from_file_location("v2_original", PATH.with_name("validar_todas_siglas v2.py"))
        original = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"boto3": types.ModuleType("boto3")}):
            spec.loader.exec_module(original)
        self.s3.records.update(archivo(raw(), name="Prediccion_51716_20261102_2359.geojson"))
        a = original.validar_unidad(self.s3, "20261102", "MEPER", "51716")
        b = self.revisar_unidad()
        self.assertEqual(a[0], b[0])
        self.assertEqual([r["archivo"] for r in a[1]], [r["archivo"] for r in b[1] if r["nombre_estado"] == "FALLA"])
        self.assertEqual([(r["fecha"], r["horas_faltantes"]) for r in a[2]], [(r["fecha"], r["horas_faltantes"]) for r in b[2]])


if __name__ == "__main__":
    unittest.main()
