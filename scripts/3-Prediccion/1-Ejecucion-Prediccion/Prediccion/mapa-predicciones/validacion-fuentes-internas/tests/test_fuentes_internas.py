"""Pruebas sin AWS de la validacion de fuentes internas. Ejecutar desde validacion-fuentes-internas/:
    python -m unittest discover -s tests -v"""

import contextlib
import io
import json
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import validar_fuentes_internas as vf  # noqa: E402  (agrega validacion-secad al path)
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402

AHORA = datetime(2026, 10, 29, 9, 0)
V = vs.calcular_ventana(AHORA, 15, date(2026, 10, 28))  # ventana 14-oct a 28-oct, previa 29-sep a 13-oct, hoy parcial 29-oct
CTX = vf.Contexto(V.todos, date(2026, 10, 29), {'MEVALMNVCCD07E03000000001': '555'})


def linea(props, lon=-75.5, lat=6.2, coma=True):
    texto = '{ "type": "Feature", "properties": ' + json.dumps(props, ensure_ascii=False) + \
            ', "geometry": { "type": "Point", "coordinates": [ ' + f'{lon}, {lat}' + ' ] } }' + (',' if coma else '')
    return texto.encode('utf-8')


CABECERA = [b'{', b'"type": "FeatureCollection",', b'"name": "x",', b'"features": [']


class Capas(unittest.TestCase):
    def test_orden_pedido_y_cobertura(self):
        self.assertEqual(list(vf.CAPAS), ['gescov', 'mtrs', 'siedco-i', 'siedco-ii', 'siedco-iii', 'simep', 'sirap', 'iris-p1'])
        self.assertEqual(vf.SOLO_SIGLA, {'sirap'})

    def test_claves_de_los_archivos(self):
        self.assertEqual(vf.TABLAS['siedco-iii'], 'DisecDelitosTurno3.geojson')
        self.assertEqual(vf.TABLAS['sirap'], 'raw_geopol_usr_pmu_servicios_extra.geojson')  # nombre real aunque la fuente sea pmu_actividades
        self.assertTrue(all(d['key'].startswith('Fuentes_externas_geojson/') for d in vf.CAPAS.values()))


class SqlFuente(unittest.TestCase):
    def test_gescov_solo_cumple(self):
        sql = vf.construir_sql_fuente('gescov', date(2026, 9, 29), date(2026, 10, 28))
        self.assertIn('"usr_disec.v_evos_planes"', sql)
        self.assertIn("estado_cumplimiento = 'Cumple'", sql)
        self.assertIn('TRY_CAST(latitud AS double) IS NOT NULL', sql)

    def test_siedco_turnos_por_hora(self):
        self.assertIn('(hora_24 >= 22 OR hora_24 < 7)', vf.construir_sql_fuente('siedco-i', date(2026, 10, 1), date(2026, 10, 2)))
        self.assertIn('hora_24 BETWEEN 7 AND 13', vf.construir_sql_fuente('siedco-ii', date(2026, 10, 1), date(2026, 10, 2)))
        s3 = vf.construir_sql_fuente('siedco-iii', date(2026, 10, 1), date(2026, 10, 2))
        self.assertIn('hora_24 BETWEEN 14 AND 21', s3)
        self.assertIn('WHERE subcategoria IS NOT NULL', s3)

    def test_filtros_de_las_demas_fuentes(self):
        self.assertIn("estado_existencia != 'No existe'", vf.construir_sql_fuente('iris-p1', date(2026, 10, 1), date(2026, 10, 2)))
        self.assertIn('fecha_creacion <= current_date', vf.construir_sql_fuente('simep', date(2026, 10, 1), date(2026, 10, 2)))
        self.assertIn('raw_geopol_usr_pmu_actividades', vf.construir_sql_fuente('sirap', date(2026, 10, 1), date(2026, 10, 2)))
        self.assertNotIn('AND estado', vf.construir_sql_fuente('mtrs', date(2026, 10, 1), date(2026, 10, 2)))

    def test_vm_solo_donde_la_vista_es_materializada(self):
        self.assertEqual({c for c, d in vf.CAPAS.items() if d.get('vm')}, {'gescov', 'iris-p1'})
        self.assertIn('MAX(fecha_actualiza_vm)', vf.construir_sql_vm('gescov'))


class RecorridoDelArchivo(unittest.TestCase):
    def escanear(self, capa, lineas, ctx=CTX):
        return vf.escanear(CABECERA + lineas + [b']', b'}'], vf.CAPAS[capa], ctx)

    def test_agrega_por_sigla_unidad_y_dia_solo_dentro_de_la_ventana(self):
        l = [linea({'fecha_inicio_plan': '2026-10-20T00:00:00', 'sigla_unidad': 'dexx', 'id_unidad': '30771.0'}),
             linea({'fecha_inicio_plan': '2026-10-20T00:00:00', 'sigla_unidad': 'DEXX', 'id_unidad': '30771.0'}),
             linea({'fecha_inicio_plan': '2026-08-01T00:00:00', 'sigla_unidad': 'DEXX', 'id_unidad': '1.0'})]  # fuera de la ventana
        r = self.escanear('gescov', l)
        self.assertEqual(r.total, 3)
        self.assertEqual(r.obs['DEXX']['30771']['2026-10-20'].filas, 2)
        self.assertEqual(r.por_dia, {'2026-10-20': 2})
        self.assertEqual((r.min_fecha, r.max_fecha), ('2026-08-01', '2026-10-20'))  # el resumen cubre todo el archivo

    def test_fechas_sin_valor_absurdas_y_futuras(self):
        l = [linea({'fecha_inicio_mesa_tecnica': None, 'sigla': 'DEXX'}),
             linea({'fecha_inicio_mesa_tecnica': '0004-08-21 12:00:00', 'sigla': 'DEXX'}),
             linea({'fecha_inicio_mesa_tecnica': '2026-11-29 00:00:00', 'sigla': 'DEXX'}),
             linea({'fecha_inicio_mesa_tecnica': '2026-10-07 13:01:00', 'sigla': 'DEXX'})]
        r = self.escanear('mtrs', l)
        self.assertEqual((r.total, r.sin_fecha, r.absurdas, r.futuras), (4, 1, 1, 1))
        self.assertEqual(r.max_fecha, '2026-10-07')  # la fecha futura no cuenta como "ultimo registro"

    def test_unidad_por_cuadrante_del_catalogo(self):
        l = [linea({'fecha_creacion_irisp1': '2026-10-10T08:00:00', 'sigla_unidad': 'MEVAL', 'nro_cuadrante': 'MEVALMNVCCD07E03000000001'}),
             linea({'fecha_creacion_irisp1': '2026-10-10T09:00:00', 'sigla_unidad': 'MEVAL', 'nro_cuadrante': 'DESCONOCIDO'})]
        r = self.escanear('iris-p1', l)
        self.assertEqual(set(r.obs['MEVAL']), {'555', ''})
        self.assertEqual(r.sin_unidad, 1)

    def test_simep_deduce_la_sigla_de_la_unidad(self):
        arbol = sc.Arbol([sc.Unidad('51080', 'ESTACION X', 'MEMED', '7.0', '')])
        ctx = vf.Contexto(V.todos, date(2026, 10, 29), {}, arbol)
        r = self.escanear('simep', [linea({'fecha_creacion': '2026-10-12 08:22:00.000', 'id_unidad': '51080.0'})], ctx)
        self.assertEqual(set(r.obs), {'MEMED'})

    def test_sirap_es_solo_sigla(self):
        r = self.escanear('sirap', [linea({'fecha_inicio': '2026-10-12 06:34:34.000', 'unidad': 'MEBUC', 'cuadrante_id': '4532.0'})])
        self.assertEqual(r.obs, {'MEBUC': {'': {'2026-10-12': r.obs['MEBUC']['']['2026-10-12']}}})

    def test_siedco_usa_la_hora_y_marca_coordenadas_fuera_de_colombia(self):
        base = {'fecha_hecho': '2026-10-27', 'hora_hecho': '22:44:00', 'sigla_undepertenece': 'MEBOG', 'juris.cai': 'CAI X'}
        r = self.escanear('siedco-i', [linea(base), linea(base, lon=-0.12, lat=51.5)])
        a = r.obs['MEBOG']['']['2026-10-27']
        self.assertEqual((a.filas, a.puntos, a.coord_invalida, a.ultima), (2, 1, 1, '2026-10-27 22:44:00'))

    def test_filtro_de_sigla_y_lineas_dañadas(self):
        ctx = vf.Contexto(V.todos, date(2026, 10, 29), siglas={'DEXX'})
        l = [linea({'fecha_inicio_plan': '2026-10-20T00:00:00', 'sigla_unidad': 'DEYY', 'id_unidad': '1.0'}),
             linea({'fecha_inicio_plan': '2026-10-20T00:00:00', 'sigla_unidad': 'DEXX', 'id_unidad': '2.0'}),
             b'{ "type": "Feature", "properties": { "fecha_inicio_plan": "2026-10-20T00:00:00" , roto']
        r = self.escanear('gescov', l, ctx)
        self.assertEqual(set(r.obs), {'DEXX'})
        self.assertEqual(r.malas, 1)

    def test_registro_sin_sigla_ni_unidad(self):
        r = self.escanear('mtrs', [linea({'fecha_inicio_mesa_tecnica': '2026-10-05 10:00:00', 'cuadrante': 'X'})])
        self.assertEqual(set(r.obs), {vf.SIN_SIGLA})


def meta(horas_atras):
    ahora_utc = AHORA.replace(tzinfo=sc.UTC_MENOS_5).astimezone(timezone.utc)
    return dict(bytes=1024 ** 2, modificado=ahora_utc - timedelta(hours=horas_atras))


def esc_con(capa, por_dia, **kw):
    e = vf.Escaneo()
    e.por_dia = dict(por_dia)
    e.total = kw.pop('total', sum(por_dia.values()))
    e.max_fecha = kw.pop('max_fecha', max(por_dia) if por_dia else '')
    for k, x in kw.items():
        setattr(e, k, x)
    return e


DIAS_VENTANA = V.dias


class ReglasDeCapa(unittest.TestCase):
    def codigos(self, capa, esc, m=None, fuente=None, vm=None):
        return {n['codigo']: n for n in vf.evaluar_capa(capa, esc, m or meta(5), fuente, vm, V, AHORA)}

    def test_capa_sana(self):
        por_dia = {d: 100 for d in DIAS_VENTANA}
        self.assertEqual(self.codigos('simep', esc_con('simep', por_dia), fuente=por_dia), {})

    def test_archivo_desactualizado(self):
        por_dia = {d: 100 for d in DIAS_VENTANA}
        n = self.codigos('simep', esc_con('simep', por_dia), m=meta(60))['ARCHIVO_DESACTUALIZADO']
        self.assertEqual((n['estado'], n['nivel']), (vs.FALLA, 'CARGA'))
        self.assertIn('hace 60 h', n['mensaje'])

    def test_ventana_vacia_con_vista_materializada_sin_refrescar(self):
        e = esc_con('gescov', {}, total=1162, max_fecha='2026-09-30', min_fecha='2026-09-30')
        vm = dict(max_vm='2026-10-01 02:02:19', dias=28)
        n = self.codigos('gescov', e, vm=vm)['CAPA_SIN_REGISTROS_EN_VENTANA']
        self.assertEqual(n['estado'], vs.FALLA)
        self.assertIn('2026-09-30 (hace 29 dias)', n['mensaje'])
        self.assertIn('no se refresca desde 2026-10-01 (hace 28 dias)', n['mensaje'])

    def test_ultimo_registro_viejo_segun_el_umbral_de_cada_capa(self):
        e = esc_con('siedco-i', {d: 50 for d in DIAS_VENTANA[:8]}, max_fecha='2026-10-21')
        self.assertIn('ULTIMO_REGISTRO_VIEJO', self.codigos('siedco-i', e))
        e = esc_con('mtrs', {d: 2 for d in DIAS_VENTANA[:3]}, max_fecha='2026-10-07')  # 22 dias, el umbral de MTRS es 30
        self.assertNotIn('ULTIMO_REGISTRO_VIEJO', self.codigos('mtrs', e))

    def test_archivo_vacio(self):
        self.assertEqual(self.codigos('sirap', esc_con('sirap', {}, total=0, max_fecha='')).keys(), {'ARCHIVO_VACIO'})

    def test_fechas_invalidas_y_futuras(self):
        por_dia = {d: 100 for d in DIAS_VENTANA}
        c = self.codigos('mtrs', esc_con('mtrs', por_dia, sin_fecha=3, absurdas=7, futuras=9))
        self.assertEqual({k for k in c}, {'FECHAS_INVALIDAS', 'FECHAS_FUTURAS'})
        self.assertIn('10 de 1500', c['FECHAS_INVALIDAS']['mensaje'])

    def test_contraste_con_la_fuente(self):
        por_dia = {d: 100 for d in DIAS_VENTANA}
        fuente = dict(por_dia)
        fuente['2026-10-20'] = 103    # dentro de la tolerancia
        self.assertEqual(self.codigos('simep', esc_con('simep', por_dia), fuente=fuente), {})
        fuente['2026-10-21'] = 130    # el archivo tiene el 77 %: faltan registros
        n = self.codigos('simep', esc_con('simep', por_dia), fuente=fuente)['ARCHIVO_DIFIERE_DE_LA_FUENTE']
        self.assertEqual(n['estado'], vs.FALLA)
        self.assertIn('2026-10-21: archivo 100 vs fuente 130', n['mensaje'])
        fuente['2026-10-21'] = 105    # el archivo tiene el 95 %: diferencia menor
        self.assertEqual(self.codigos('simep', esc_con('simep', por_dia), fuente=fuente)['ARCHIVO_DIFIERE_DE_LA_FUENTE']['estado'], vs.AVISO)

    def test_comparacion_ignora_dias_de_hoy(self):
        por_dia = {d: 100 for d in DIAS_VENTANA}
        fuente = {**por_dia, '2026-10-29': 50}
        self.assertEqual(vf.comparar_con_fuente(por_dia, fuente, DIAS_VENTANA), [])


def corrida(estado, dia, hora='04:30', error=None):
    r = dict(JobRunState=estado, StartedOn=datetime.fromisoformat(f'{dia}T{hora}:00').replace(tzinfo=sc.UTC_MENOS_5))
    if error:
        r['ErrorMessage'] = error
    return r


DIAS_JOB = [(date(2026, 10, 14) + timedelta(days=i)).isoformat() for i in range(15)]


class Jobs(unittest.TestCase):
    def test_job_diario_al_dia(self):
        runs = [corrida('SUCCEEDED', d) for d in DIAS_JOB + ['2026-10-29']]
        nov, res = vf.evaluar_jobs(runs, None, DIAS_JOB, AHORA)
        self.assertEqual(nov, [])
        self.assertEqual((res['diario']['exitos'], res['diario']['sin_corrida']), (16, 0))

    def test_job_diario_vencido(self):
        runs = [corrida('SUCCEEDED', d) for d in DIAS_JOB[:-3]] + [corrida('FAILED', '2026-10-28', error='OOM')]
        nov, _ = vf.evaluar_jobs(runs, None, DIAS_JOB, AHORA)
        self.assertEqual([(n['codigo'], n['estado']) for n in nov], [('JOB_FUENTES_INTERNAS_VENCIDO', vs.FALLA)])
        self.assertIn('OOM', nov[0]['mensaje'])

    def test_un_dia_sin_corrida_exitosa_es_aviso(self):
        runs = [corrida('SUCCEEDED', d) for d in DIAS_JOB + ['2026-10-29'] if d != '2026-10-20']
        nov, res = vf.evaluar_jobs(runs, None, DIAS_JOB, AHORA)
        self.assertEqual([(n['codigo'], n['estado']) for n in nov], [('JOB_FUENTES_INTERNAS_DIAS_SIN_CORRIDA', vs.AVISO)])
        self.assertIn('2026-10-20', nov[0]['mensaje'])

    def test_la_corrida_de_hoy_espera_hasta_las_6(self):
        runs = [corrida('SUCCEEDED', d) for d in DIAS_JOB]
        nov, _ = vf.evaluar_jobs(runs, None, DIAS_JOB, datetime(2026, 10, 29, 4, 0))
        self.assertEqual(nov, [])

    def test_job_semanal_de_iris(self):
        diario = [corrida('SUCCEEDED', d) for d in DIAS_JOB + ['2026-10-29']]
        nov, _ = vf.evaluar_jobs(diario, [corrida('SUCCEEDED', '2026-10-25', '19:00')], DIAS_JOB, AHORA)
        self.assertEqual(nov, [])
        nov, _ = vf.evaluar_jobs(diario, [corrida('SUCCEEDED', '2026-10-04', '19:00')], DIAS_JOB, AHORA)
        self.assertEqual([n['codigo'] for n in nov], ['JOB_SEMANAL_IRIS_VENCIDO'])


class Integracion(unittest.TestCase):
    def test_sirap_sin_filas_de_unidad_y_registros_sin_sigla(self):
        obs = {'sirap': {'MEBUC': {'': {d: vs.Acum(filas=30, puntos=30, ultima=d) for d in V.base + V.dias}},
                         vf.SIN_SIGLA: {'': {d: vs.Acum(filas=5, puntos=5, ultima=d) for d in V.base + V.dias}}}}
        arbol = sc.Arbol([sc.Unidad('1', 'METROPOLITANA', 'MEBUC', '5.0', ''), sc.Unidad('2', 'ESTACION', 'MEBUC', '7.0', '1')])
        siglas, unidades, nov = vs.analizar(obs, arbol, V, vs.Umbrales(), ['sirap'], None, vf.TABLAS, solo_sigla=vf.SOLO_SIGLA, avisar_sin_unidad=False)
        nov = vf.marcar_sin_sigla(siglas, nov)
        self.assertEqual(unidades, [])
        sin = [n for n in nov if n['codigo'] == 'REGISTROS_SIN_SIGLA']
        self.assertEqual(len(sin), 1)
        self.assertIn('el mapa los ubica por coordenada', sin[0]['mensaje'])
        self.assertIn('75 registros sin sigla', sin[0]['mensaje'])

    def test_solo_sql_sin_aws(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            codigo = vf.main(['--solo-sql', '--no-export', '--fecha-fin', '2026-10-28', '--capas', 'simep', 'iris-p1'])
        salida = out.getvalue()
        self.assertEqual(codigo, 0)
        self.assertIn('simep_datac', salida)
        self.assertIn('v_general_irisp', salida)
        self.assertIn('MAX(fecha_actualiza_vm)', salida)
        self.assertNotIn('v_evos_planes', salida)

    def test_argumentos_invalidos(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            vf.main(['--hilos', '0'])
        self.assertEqual(cm.exception.code, 2)

    def test_rezago_de_siedco_fuera_de_rango(self):
        for valor in ('-1', '15'):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
                vf.main(['--rezago-siedco', valor])
            self.assertEqual(cm.exception.code, 2)

    def test_solo_siedco_tiene_rezago_por_defecto(self):
        a = vf.parser().parse_args([])
        self.assertEqual(a.rezago_siedco, 2)
        self.assertEqual({c for c, d in vf.CAPAS.items() if 'turno' in d}, {'siedco-i', 'siedco-ii', 'siedco-iii'})


if __name__ == '__main__':
    unittest.main()
