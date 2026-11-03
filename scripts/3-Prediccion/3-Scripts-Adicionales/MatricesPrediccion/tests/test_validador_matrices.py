"""Pruebas sin AWS, numpy ni open_cp. Los PKL sinteticos usan el formato observado."""

import copyreg
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import math
import pickle
import struct
import sys
import tempfile
import types
import unittest
import zipfile
from contextlib import contextmanager, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1] / "validador_matrices_cloudshell.py"
SPEC = importlib.util.spec_from_file_location("validador_matrices", PATH)
v = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v)


@contextmanager
def formato_observado():
    names = ["open_cp", "open_cp.predictors", "numpy", "numpy.core", "numpy.core.multiarray",
             "numpy._core", "numpy._core.multiarray", "numpy._core.numeric", "numpy.ma", "numpy.ma.core"]
    modules = {name: types.ModuleType(name) for name in names}
    previous = {name: sys.modules.get(name) for name in names}
    sys.modules.update(modules)
    grid = type("GridPredictionArray", (), {"__module__": "open_cp.predictors"})

    def dtype_init(self, name, align=False, copy=True):
        self.name = name

    def dtype_reduce(self):
        endian = self.name[0] if self.name.startswith(("<", ">")) else "<"
        return dtype, (self.name, False, True), (3, endian, None, None, None, -1, -1, 0)

    dtype = type("dtype", (), {"__module__": "numpy", "__init__": dtype_init, "__reduce__": dtype_reduce})
    array = type("ndarray", (), {"__module__": "numpy"})
    masked = type("MaskedArray", (), {"__module__": "numpy.ma.core"})

    def reconstruct(subtype, shape, code):
        return subtype()

    reconstruct.__module__, reconstruct.__qualname__ = "numpy.core.multiarray", "_reconstruct"

    def mareconstruct(subtype, baseclass, shape, code):
        return subtype()

    mareconstruct.__module__, mareconstruct.__qualname__ = "numpy.ma.core", "_mareconstruct"

    def frombuffer(raw, dtype, shape, order):
        return array()

    frombuffer.__module__, frombuffer.__qualname__ = "numpy._core.numeric", "_frombuffer"

    def array_reduce(self):
        if getattr(self, "modern", False):
            return frombuffer, (bytearray(self.raw), self.dtype, self.shape, "C")
        return reconstruct, (array, (0,), b"b"), (1, self.shape, self.dtype, False, self.raw)

    def mask_reduce(self):
        return mareconstruct, (masked, array, (0,), "b"), (1, self.shape, self.dtype, False, self.raw, self.mask, None)

    array.__reduce__, masked.__reduce__ = array_reduce, mask_reduce
    modules["open_cp.predictors"].GridPredictionArray = grid
    modules["numpy"].ndarray, modules["numpy"].dtype = array, dtype
    modules["numpy.core.multiarray"]._reconstruct = reconstruct
    modules["numpy._core.multiarray"]._reconstruct = reconstruct
    modules["numpy._core.numeric"]._frombuffer = frombuffer
    modules["numpy.ma.core"].MaskedArray, modules["numpy.ma.core"]._mareconstruct = masked, mareconstruct

    def build(values=(0.0, 1.0, 2.0, 3.0), shape=(2, 2), mask=None, descriptor="f8", protocol=3,
              modern=False, raw=None, grid_fields=None):
        obj = grid()
        obj._xsize, obj._ysize, obj._xoffset, obj._yoffset = 200, 200, -8428800, 532600
        data = masked() if mask is not None else array()
        data.shape, data.dtype, data.modern = shape, dtype(descriptor), modern
        code = {"f8": "d", "f4": "f", "i4": "i", "u2": "H"}.get(descriptor.lstrip("<>"), "d")
        endian = ">" if descriptor.startswith(">") else "<"
        data.raw = raw if raw is not None else struct.pack(endian + code * len(values), *values)
        if mask is not None:
            data.mask = bytes(mask)
        obj._matrix = data
        for key, value in (grid_fields or {}).items():
            if value is None:
                delattr(obj, key)
            else:
                setattr(obj, key, value)
        return pickle.dumps(obj, protocol=protocol)

    try:
        yield build
    finally:
        for name, old in previous.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


def muestra(**kwargs):
    with formato_observado() as build:
        return build(**kwargs)


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
    filename = name or f"Prediccion_51716_{date}_{hour}.pkl"
    key = "PrediccionMdP/2026/20261102/MEPER/51716/MatricesPrediccion/" + filename
    return {key: (raw, datetime(2026, 11, 1, tzinfo=timezone.utc))}


class Contenido(unittest.TestCase):
    def test_grilla_numerica_valida(self):
        result = v.inspeccionar_matriz(muestra())
        self.assertEqual(result["contenido_estado"], "OK")
        self.assertEqual((result["filas"], result["columnas"]), (2, 2))
        self.assertEqual((result["minimo"], result["maximo"], result["suma"]), (0, 3, 6))

    def test_protocolos_observado_y_moderno(self):
        for protocol in (2, 3, 4, 5):
            with self.subTest(protocol=protocol):
                self.assertEqual(v.inspeccionar_matriz(muestra(protocol=protocol))["estado"], "OK")
        self.assertEqual(v.inspeccionar_matriz(muestra(protocol=5, modern=True))["estado"], "OK")

    def test_precisiones_y_enteros(self):
        for descriptor in ("f4", "f8", "i4", "u2"):
            with self.subTest(dtype=descriptor):
                self.assertEqual(v.inspeccionar_matriz(muestra(values=(0, 1, 2, 3), descriptor=descriptor))["estado"], "OK")

    def test_archivo_vacio_y_truncado(self):
        for raw in (b"", muestra()[:-1], b"no es un pickle"):
            with self.subTest(length=len(raw)):
                self.assertEqual(v.inspeccionar_matriz(raw)["estado"], "FALLA")

    def test_bytes_adicionales(self):
        self.assertEqual(v.inspeccionar_matriz(muestra() + b"extra")["estado"], "FALLA")

    def test_endian_big(self):
        result = v.inspeccionar_matriz(muestra(descriptor=">f8"))
        self.assertEqual((result["estado"], result["minimo"], result["maximo"]), ("OK", 0, 3))

    def test_frame_truncado(self):
        raw = muestra(protocol=4)
        raw = raw[:3] + struct.pack("<Q", 1_000_000) + raw[11:]
        self.assertEqual(v.inspeccionar_matriz(raw)["estado"], "FALLA")

    def test_no_finitos_sin_mascara(self):
        for number in (math.nan, math.inf, -math.inf):
            result = v.inspeccionar_matriz(muestra(values=(0.0, number, 2.0, 3.0)))
            self.assertEqual(result["estado"], "FALLA")
            self.assertEqual(result["no_finitos"], 1)

    def test_negativos_no_sentinela(self):
        result = v.inspeccionar_matriz(muestra(values=(-2.0, 0.0, 1.0, 2.0)))
        self.assertEqual(result["estado"], "FALLA")
        self.assertEqual(result["negativos"], 1)

    def test_sentinela_menos_uno_excluido_de_las_intensidades(self):
        result = v.inspeccionar_matriz(muestra(values=(-1.0, 0.0, 1.0, 3.0)))
        self.assertEqual(result["estado"], "OK")
        self.assertEqual(result["celdas_sin_dato"], 1)
        self.assertEqual(result["celdas_validas"], 3)
        self.assertEqual((result["negativos"], result["minimo"], result["suma"]), (0, 0, 4))

    def test_todas_sin_dato_es_falla(self):
        result = v.inspeccionar_matriz(muestra(values=(-1.0,) * 4))
        self.assertEqual(result["estado"], "FALLA")
        self.assertEqual(result["celdas_sin_dato"], 4)

    def test_mascara_no_evalua_relleno(self):
        result = v.inspeccionar_matriz(muestra(values=(math.nan, 0.0, 2.0, 3.0), mask=(1, 0, 0, 0)))
        self.assertEqual(result["estado"], "OK")
        self.assertEqual(result["celdas_enmascaradas"], 1)
        self.assertEqual(result["no_finitos"], 0)

    def test_todas_enmascaradas(self):
        result = v.inspeccionar_matriz(muestra(mask=(1, 1, 1, 1)))
        self.assertEqual(result["estado"], "FALLA")

    def test_mascara_truncada(self):
        self.assertEqual(v.inspeccionar_matriz(muestra(mask=(1, 0)))["estado"], "FALLA")

    def test_2d_y_contenido_no_vacio(self):
        for shape, values in [((4,), (0, 1, 2, 3)), ((1, 2, 2), (0, 1, 2, 3)), ((0, 0), ())]:
            with self.subTest(shape=shape):
                self.assertEqual(v.inspeccionar_matriz(muestra(shape=shape, values=values))["estado"], "FALLA")

    def test_dimension_no_coincide_con_bytes(self):
        self.assertEqual(v.inspeccionar_matriz(muestra(shape=(2, 3)))["estado"], "FALLA")

    def test_limite_de_celdas(self):
        result = v.inspeccionar_matriz(muestra(), max_celdas=3)
        self.assertEqual((result["estado"], result["contenido_estado"]), ("INCOMPLETO", "NO EVALUADO"))

    def test_grilla_espacial_invalida(self):
        for fields in ({"_xsize": 0}, {"_ysize": -1}, {"_xoffset": math.inf}, {"_yoffset": "texto"}, {"_matrix": None}):
            with self.subTest(fields=fields):
                self.assertEqual(v.inspeccionar_matriz(muestra(grid_fields=fields))["estado"], "FALLA")

    def test_cero_y_constante_son_avisos(self):
        for number in (0.0, 0.5):
            result = v.inspeccionar_matriz(muestra(values=(number,) * 4))
            self.assertEqual(result["estado"], "AVISO")
            self.assertTrue(result["sin_contraste"])

    def test_tipo_no_soportado_no_es_ok(self):
        result = v.inspeccionar_matriz(pickle.dumps({"datos": [1, 2]}))
        self.assertEqual((result["estado"], result["contenido_estado"]), ("INCOMPLETO", "NO EVALUADO"))
        self.assertEqual(v.inspeccionar_matriz(muestra(descriptor="O8"))["estado"], "INCOMPLETO")

    def test_no_ejecuta_globales(self):
        class Otro:
            def __reduce__(self):
                return eval, ("1 + 1",)
        raw = pickle.dumps(Otro(), protocol=3)
        with patch("builtins.eval", side_effect=AssertionError("No debe ejecutarse")):
            result = v.inspeccionar_matriz(raw)
        self.assertEqual(result["estado"], "INCOMPLETO")

    def test_rechaza_extensiones_que_podrian_omitir_find_class(self):
        copyreg.add_extension("builtins", "eval", 241)
        try:
            class Otro:
                def __reduce__(self):
                    return eval, ("1 + 1",)
            raw = pickle.dumps(Otro(), protocol=3)
            result = v.inspeccionar_matriz(raw)
            self.assertEqual(result["estado"], "INCOMPLETO")
        finally:
            copyreg.remove_extension("builtins", "eval", 241)


class Lecturas(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.folder = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def run_unit(self, s3, **kwargs):
        return v.validar_unidad(s3, "20261102", "MEPER", "51716", revision(self.folder, **kwargs))

    def test_lee_con_condicion_y_cierra_body(self):
        s3 = S3(archivo(muestra()))
        count, rows, missing, _ = self.run_unit(s3)
        self.assertEqual(count, 1)
        self.assertEqual(rows[0]["contenido_estado"], "OK")
        self.assertEqual(sum(r["cantidad_faltantes"] for r in missing), 167)
        self.assertEqual(s3.gets[0]["IfMatch"], hashlib.md5(next(iter(s3.records.values()))[0]).hexdigest())
        self.assertTrue(s3.bodies[0].cerrado)

    def test_cache_reutilizable_y_cambio_invalida(self):
        s3 = S3(archivo(muestra()))
        self.run_unit(s3)
        _, rows, _, _ = self.run_unit(s3)
        self.assertEqual(len(s3.gets), 1)
        self.assertEqual(rows[0]["origen_revision"], "CACHE")
        key = next(iter(s3.records))
        s3.records[key] = (muestra(values=(0.0, 1.0, 4.0, 9.0)), s3.records[key][1] + timedelta(seconds=1))
        _, rows, _, _ = self.run_unit(s3)
        self.assertEqual(len(s3.gets), 2)
        self.assertEqual(rows[0]["maximo"], 9)

    def test_cache_incompleta_y_politica_anterior_se_releen(self):
        s3 = S3(archivo(muestra()))
        for mode in ("resultado_incompleto", "politica_anterior"):
            self.run_unit(s3)
            file = next((self.folder / ".estado_matrices").glob("*.gz"))
            with gzip.open(file, "rb") as f:
                state = json.load(f)
            if mode == "resultado_incompleto":
                entry = next(iter(state["archivos"].values()))
                entry["resultado"] = dict(estado="OK", contenido_estado="OK")
            else:
                state["policy"] = "anterior"
            with gzip.open(file, "wb") as f:
                f.write(json.dumps(state).encode())
            before = len(s3.gets)
            _, rows, _, _ = self.run_unit(s3)
            self.assertEqual(len(s3.gets), before + 1)
            self.assertEqual(rows[0]["origen_revision"], "GET")

    def test_cache_corrupta_no_declara_exito_sin_releer(self):
        s3 = S3(archivo(muestra()))
        self.run_unit(s3)
        next((self.folder / ".estado_matrices").glob("*.gz")).write_bytes(b"corrupta")
        self.run_unit(s3)
        self.assertEqual(len(s3.gets), 2)

    def test_presupuesto_y_reanudacion(self):
        records = archivo(muestra())
        records.update(archivo(muestra(), hour="0100"))
        s3 = S3(records)
        _, rows, _, _ = self.run_unit(s3, max_lecturas=1)
        self.assertEqual([r["contenido_estado"] for r in rows], ["OK", "NO EVALUADO"])
        _, rows, _, _ = self.run_unit(s3, max_lecturas=1)
        self.assertEqual([r["contenido_estado"] for r in rows], ["OK", "OK"])
        self.assertEqual(len(s3.gets), 2)

    def test_no_lee_archivo_grande_vacio_ni_nombre_invalido(self):
        records = archivo(b"", hour="0000")
        records.update(archivo(b"x" * (1024**2 + 1), hour="0100"))
        records.update(archivo(muestra(), name="Prediccion_51716_20261102_0100_var.pkl"))
        s3 = S3(records)
        _, rows, _, _ = self.run_unit(s3, max_mib=1)
        self.assertEqual(s3.gets, [])
        self.assertEqual([r["estado"] for r in rows], ["FALLA", "INCOMPLETO", "FALLA"])

    def test_modo_metadata_no_consume_get_y_conserva_cache(self):
        s3 = S3(archivo(muestra()))
        self.run_unit(s3)
        _, rows, _, _ = self.run_unit(s3, solo_metadata=True)
        self.assertEqual(rows[0]["contenido_estado"], "NO EVALUADO")
        self.run_unit(s3)
        self.assertEqual(len(s3.gets), 1)

    def test_cambio_durante_lectura_no_es_resultado_sano(self):
        s3 = S3(archivo(muestra()))
        s3.mutate = True
        _, rows, _, _ = self.run_unit(s3)
        self.assertEqual(rows[0]["estado"], "INCOMPLETO")
        self.assertTrue(s3.bodies[0].cerrado)

    def test_acceso_denegado_al_leer_y_al_listar(self):
        s3 = S3(archivo(muestra()))
        s3.fail_get = True
        _, rows, _, _ = self.run_unit(s3)
        self.assertEqual(rows[0]["contenido_estado"], "NO EVALUADO")
        s3.fail_list = True
        _, rows, missing, _ = self.run_unit(s3)
        self.assertEqual(rows[0]["estado"], "INCOMPLETO")
        self.assertEqual(missing, [])

    def test_interrupcion_no_deja_archivo_con_estado_ok(self):
        s3 = S3(archivo(muestra()))
        with patch.object(s3, "get_object", side_effect=KeyboardInterrupt):
            _, rows, missing, interrupted = self.run_unit(s3)
        self.assertTrue(interrupted)
        self.assertEqual(rows[0]["estado"], "INCOMPLETO")
        self.assertEqual(missing, [])

    def test_listado_parcial_no_inventa_faltantes(self):
        s3 = S3(archivo(muestra()))
        original = s3.paginate
        def partial(**kwargs):
            yield from original(**kwargs)
            raise OSError("Segunda pagina denegada")
        s3.paginate = partial
        _, rows, missing, _ = self.run_unit(s3)
        self.assertEqual(rows[-1]["origen_revision"], "LISTADO_INCOMPLETO")
        self.assertEqual(missing, [])

    def test_rango_entre_anios(self):
        records = {
            f"PrediccionMdP/{week[:4]}/{week}/MEPER/51716/MatricesPrediccion/x.pkl":
                (b"x", datetime(2026, 12, 28, tzinfo=timezone.utc))
            for week in ("20261228", "20270104")
        }
        self.assertEqual(v.listar_semanas(S3(records), "20261228", "20270104"), ["20261228", "20270104"])

    def test_forma_diferente_dentro_de_unidad_es_aviso(self):
        records = archivo(muestra())
        records.update(archivo(muestra(shape=(1, 4)), hour="0100"))
        _, rows, _, _ = self.run_unit(S3(records))
        self.assertEqual(rows[1]["estado"], "AVISO")

    def test_reporte_completo_dos_csv_y_salida_falla_por_faltantes(self):
        s3 = S3(archivo(muestra()))
        args = v.argumentos(["--desde", "20261102", "--sigla", "MEPER", "--unidad", "51716", "--salida", str(self.folder)])
        with redirect_stdout(io.StringIO()):
            code = v.reporte(s3, args)
        self.assertEqual(code, 1)
        archive = next(self.folder.glob("*.zip"))
        with zipfile.ZipFile(archive) as z:
            self.assertEqual(len(z.namelist()), 2)
            self.assertIsNone(z.testzip())
            name = next(n for n in z.namelist() if n.startswith("detalle_"))
            rows = list(csv.DictReader(io.StringIO(z.read(name).decode("utf-8-sig"))))
            self.assertEqual(rows[0]["contenido_estado"], "OK")

    def test_semana_completa_y_todos_los_contenidos(self):
        records = {}
        payload = muestra()
        for day in range(7):
            date = (datetime(2026, 11, 2) + timedelta(days=day)).strftime("%Y%m%d")
            for hour in range(24):
                records.update(archivo(payload, date=date, hour=f"{hour:02}00"))
        args = v.argumentos(["--desde", "20261102", "--sigla", "MEPER", "--unidad", "51716", "--salida", str(self.folder)])
        s3 = S3(records)
        with redirect_stdout(io.StringIO()):
            code = v.reporte(s3, args)
        self.assertEqual(code, 0)
        self.assertEqual(len(s3.gets), 168)

    def test_sin_unidades_no_es_exito(self):
        args = v.argumentos(["--desde", "20261102", "--salida", str(self.folder)])
        with redirect_stdout(io.StringIO()):
            code = v.reporte(S3({}), args)
        self.assertEqual(code, 3)


if __name__ == "__main__":
    unittest.main()
