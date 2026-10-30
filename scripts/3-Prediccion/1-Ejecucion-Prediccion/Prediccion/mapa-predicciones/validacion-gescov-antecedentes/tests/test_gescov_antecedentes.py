"""Pruebas sin AWS de la validacion GESCOV + Antecedentes. Ejecutar desde validacion-gescov-antecedentes/:
    python -m unittest discover -s tests -v"""

import contextlib
import io
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import validar_gescov_antecedentes as vg  # noqa: E402  (agrega validacion-secad al path)
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402

LOG_OK = """============================================================
GESCOP Daily Migration - Inicio
Inicio: 2026-10-22 01:00:50
============================================================
Obteniendo credenciales...
✓ 2185 registros de enriquecimiento cargados
📤 Extrayendo registros de Oracle ( fecha_inicio >= 2026-10-21 )
✓ 11125 registros extraídos de Oracle
✓ 10606 registros enriquecidos - 519 descartados
✓ 506 registros insertados
🗑️  Eliminando registros con fecha_inicio < 2024-10-20...
✓ 5701 registros eliminados
✅ MIGRACIÓN COMPLETADA
Registros insertados: 506
Total en tabla: 6,212,784
Rango fechas: 2024-10-20 00:00:00 - 2026-10-21 23:00:00
"""


def ejecucion(run, extraidos=10000, descartados=480, elim=5500, total=None, max_inicio=None, completada=True, error=None, log=True):
    """Ejecucion de la tarea de las 01:00 del dia `run` (YYYY-MM-DD): carga los puntos del dia anterior."""
    r = date.fromisoformat(run)
    x = r - timedelta(days=1)
    detalle = dict(desde=x.isoformat(), extraidos=extraidos, enriquecidos=extraidos - descartados, descartados=descartados,
                   insertados=500, eliminados=elim, total=total, min_inicio='2024-10-20 00:00:00',
                   max_inicio=max_inicio or f'{x} 22:00:00', completada=completada, error=error) if log else None
    return dict(inicio=datetime.combine(r, time(1, 0, 50)), stream=f'ecs/gescov/{run}', detalle=detalle)


def semana_gescov(totales=True, **kw):
    """Ejecuciones del 21 al 28-oct con totales coherentes: total = previo + enriquecidos - eliminados (+4020 por dia)."""
    return [ejecucion(f'2026-10-{d}', total=6_200_000 + 4020 * i if totales else None, **kw) for i, d in enumerate(range(21, 29))]


DIAS = [(date(2026, 10, 20) + timedelta(days=i)).isoformat() for i in range(8)]  # fecha_inicio 20 al 27
AHORA = datetime(2026, 10, 28, 12, 0)


class Capas(unittest.TestCase):
    def test_orden_y_nombres_del_menu(self):
        self.assertEqual(list(vg.CAPAS), ['planes-programados-historico', 'antecedentes-personas', 'antecedentes-vehiculos', 'antecedentes-imei',
                                          'antecedentes-personas-positivos', 'antecedentes-vehiculos-positivos', 'antecedentes-imei-positivos'])
        self.assertEqual(vg.CAPAS['antecedentes-vehiculos-positivos']['nombre'], 'Antecedentes: Vehículos (Solo positivos)')

    def test_los_positivos_comparten_tabla_con_filtro(self):
        for base in ('personas', 'vehiculos', 'imei'):
            todas, pos = vg.CAPAS[f'antecedentes-{base}'], vg.CAPAS[f'antecedentes-{base}-positivos']
            self.assertEqual(todas['fuente'], pos['fuente'])
            self.assertIsNone(todas['filtro'])
            self.assertEqual(pos['filtro'], 'tiene_antecedentes = 1')

    def test_jobs_y_tablas_rds_de_cada_fuente(self):
        self.assertEqual({k: (f['job'], f['rds']) for k, f in vg.FUENTES.items()},
                         {'persona': ('7', 'auditoria_cons_persona_v2'), 'vehiculo': ('8', 'auditoria_cons_vehiculo_v2'),
                          'imei': ('9', 'auditoria_cons_imei_v2')})


class SqlSivicc(unittest.TestCase):
    def test_persona_todas(self):
        sql = vg.construir_sql_sivicc('persona', None, date(2026, 9, 28), date(2026, 10, 28))
        self.assertIn('FROM "pon-prod-raw-sivicc"."raw_sivicc_usr_mnvcc_auditoria_cons_persona"', sql)
        self.assertIn('COUNT(DISTINCT "id_auditoria_persona") AS llamadas', sql)
        self.assertIn("\"fecha\" >= TIMESTAMP '2026-09-28 00:00:00' AND \"fecha\" < TIMESTAMP '2026-10-29 00:00:00'", sql)
        self.assertIn('CAST("undeminuta" AS varchar)', sql)  # unidad de la minuta cuando undelaborando es nulo, como el job
        self.assertNotIn('tiene_antecedentes', sql)
        self.assertNotIn('IN (', sql)

    def test_vehiculo_positivos_y_sigla(self):
        sql = vg.construir_sql_sivicc('vehiculo', vg.POSITIVOS, date(2026, 10, 1), date(2026, 10, 2), {'MEPAS'})
        self.assertIn('"fecha_creacion"', sql)
        self.assertIn('AND tiene_antecedentes = 1', sql)
        self.assertIn("upper(trim(fisicalaborando)) IN ('MEPAS')", sql)
        self.assertIn('BETWEEN -82 AND -66', sql)

    def test_sin_sigla_se_normaliza(self):
        fila = dict(sigla='', codigo_unidad='500', dia='2026-10-20', filas='4', llamadas='4', puntos='4', coord_invalida='0', hora_invalida='0',
                    hora_futura='0', desfase_particion='0', ultima_hora_caso='2026-10-20 10:00:00')
        self.assertEqual(set(vg.parsear_sivicc([fila])), {vg.SIN_SIGLA})

    def test_copia_gescov(self):
        sql = vg.construir_sql_copia_gescov()
        self.assertIn('"pon-prod-raw-disec"."raw_disec_usr_disec_gescov_punto"', sql)
        self.assertIn('MAX(fecha_creacion)', sql)


def corrida_job(capa, tabla, dia, hora='22:14', estado='SUCCEEDED', error=None):
    """Corrida de V2_SIVICC-migration-to-RDS a las `hora` locales (UTC-5) del `dia`."""
    inicio = datetime.fromisoformat(f'{dia}T{hora}:00').replace(tzinfo=timezone(timedelta(hours=-5)))
    r = dict(JobRunState=estado, StartedOn=inicio, Arguments={'--CAPA': capa, '--DB_TABLE': tabla, '--S3_PATH': 's3://x'})
    if error:
        r['ErrorMessage'] = error
    return r


def corridas_job(dias, saltar=()):
    runs = []
    for d in dias:
        for capa, tabla in (('7', 'auditoria_cons_persona_V2'), ('8', 'auditoria_cons_vehiculo_V2'), ('9', 'auditoria_cons_imei_V2')):
            if (capa, d) not in saltar:
                runs.append(corrida_job(capa, tabla, d))
    return runs


CAPAS_POR_FUENTE = {'persona': ['antecedentes-personas', 'antecedentes-personas-positivos'],
                    'vehiculo': ['antecedentes-vehiculos', 'antecedentes-vehiculos-positivos'],
                    'imei': ['antecedentes-imei', 'antecedentes-imei-positivos']}
DIAS_JOB = [(date(2026, 10, 13) + timedelta(days=i)).isoformat() for i in range(15)]  # 13 al 27


class CargaSivicc(unittest.TestCase):
    def test_todo_al_dia(self):
        runs = corridas_job(DIAS_JOB + ['2026-10-28'][:0])
        nov, res = vg.evaluar_carga_sivicc(runs, DIAS_JOB, AHORA, CAPAS_POR_FUENTE)
        self.assertEqual(nov, [])
        self.assertEqual((res['persona']['exitos'], res['persona']['sin_corrida']), (15, 0))
        self.assertAlmostEqual(res['persona']['ultima_hace_h'], 13.8, delta=0.5)  # 27-oct 22:14 -> 28-oct 12:00

    def test_ultima_carga_vieja_es_falla_en_las_dos_capas_de_la_tabla(self):
        runs = corridas_job(DIAS_JOB[:-3], saltar=())  # ultimas cargas del 24-oct
        runs.append(corrida_job('7', 'auditoria_cons_persona_V2', '2026-10-27', estado='FAILED', error='Exception: boom'))
        nov, _ = vg.evaluar_carga_sivicc(runs, DIAS_JOB, AHORA, CAPAS_POR_FUENTE)
        persona = [n for n in nov if n['codigo'] == 'CARGA_RDS_VENCIDA' and 'personas' in n['capa']]
        self.assertEqual({n['capa'] for n in persona}, set(CAPAS_POR_FUENTE['persona']))
        self.assertTrue(all(n['estado'] == vs.FALLA for n in persona))
        self.assertIn('boom', persona[0]['mensaje'])

    def test_un_dia_sin_corrida_con_cargas_posteriores_es_aviso(self):
        runs = corridas_job(DIAS_JOB, saltar={('8', '2026-10-20')})
        nov, res = vg.evaluar_carga_sivicc(runs, DIAS_JOB, AHORA, CAPAS_POR_FUENTE)
        self.assertEqual({(n['codigo'], n['estado']) for n in nov}, {('CARGA_RDS_DIAS_SIN_CORRIDA', vs.AVISO)})
        self.assertEqual(len(nov), 2)  # las dos capas de vehiculos
        self.assertIn('2026-10-20', nov[0]['mensaje'])
        self.assertEqual(res['vehiculo']['sin_corrida'], 1)

    def test_corridas_de_otras_tablas_no_cuentan(self):
        runs = corridas_job(DIAS_JOB)
        runs += [corrida_job('7', 'auditoria_cons_persona_prueba', '2026-10-28', hora='23:00')]
        _, res = vg.evaluar_carga_sivicc(runs, DIAS_JOB, AHORA, CAPAS_POR_FUENTE)
        self.assertEqual(res['persona']['exitos'], 15)

    def test_sin_historial(self):
        nov, _ = vg.evaluar_carga_sivicc([], DIAS_JOB, AHORA, {'imei': ['antecedentes-imei']})
        self.assertEqual([n['codigo'] for n in nov], ['CARGA_RDS_VENCIDA'])


class LogGescov(unittest.TestCase):
    def test_parsea_una_ejecucion_completa(self):
        d = vg.parsear_log_gescov(LOG_OK)
        self.assertEqual((d['desde'], d['extraidos'], d['enriquecidos'], d['descartados']), ('2026-10-21', 11125, 10606, 519))
        self.assertEqual((d['insertados'], d['eliminados'], d['total']), (506, 5701, 6_212_784))
        self.assertEqual((d['min_inicio'], d['max_inicio'], d['completada'], d['error']), ('2024-10-20 00:00:00', '2026-10-21 23:00:00', True, None))

    def test_log_vacio_es_none_y_error_se_reconoce(self):
        self.assertIsNone(vg.parsear_log_gescov(''))
        d = vg.parsear_log_gescov('Inicio: 2026-10-23 01:00:50\n❌ ERROR EN MIGRACIÓN: connection refused\n')
        self.assertFalse(d['completada'])
        self.assertEqual(d['error'], 'connection refused')


class EjecucionesGescov(unittest.TestCase):
    def test_semana_normal_sin_novedades(self):
        nov, res = vg.evaluar_gescov(semana_gescov(), DIAS, AHORA)
        self.assertEqual(nov, [])
        self.assertEqual((res['con_ejecucion'], res['sin_ejecucion'], res['fallidas']), (8, 0, 0))

    def test_dia_sin_ejecucion_es_falla(self):
        ejs = [e for e in semana_gescov() if e['inicio'].date() != date(2026, 10, 24)]
        nov, res = vg.evaluar_gescov(ejs, DIAS, AHORA)
        self.assertEqual([(n['codigo'], n['estado']) for n in nov if n['codigo'] == 'GESCOV_SIN_EJECUCION'], [('GESCOV_SIN_EJECUCION', vs.FALLA)])
        sin = next(n for n in nov if n['codigo'] == 'GESCOV_SIN_EJECUCION')
        self.assertIn('2026-10-24', sin['mensaje'])
        self.assertIn('fecha_inicio 2026-10-23', sin['mensaje'])  # la corrida del 24 carga los puntos del 23
        self.assertEqual(res['sin_ejecucion'], 1)

    def test_la_corrida_de_hoy_espera_hasta_las_3(self):
        ejs = [e for e in semana_gescov() if e['inicio'].date() != date(2026, 10, 28)]
        nov, res = vg.evaluar_gescov(ejs, DIAS, datetime(2026, 10, 28, 2, 0))
        self.assertEqual((nov, res['pendientes']), ([], 1))
        nov, _ = vg.evaluar_gescov(ejs, DIAS, datetime(2026, 10, 28, 9, 0))
        self.assertEqual([n['codigo'] for n in nov], ['GESCOV_SIN_EJECUCION'])

    def test_ejecucion_fallida(self):
        ejs = semana_gescov()
        ejs[3] = ejecucion('2026-10-24', completada=False, error='ORA-12541: no listener', total=None)
        nov, res = vg.evaluar_gescov(ejs, DIAS, AHORA)
        falla = [n for n in nov if n['codigo'] == 'GESCOV_EJECUCION_FALLIDA']
        self.assertEqual(len(falla), 1)
        self.assertIn('ORA-12541', falla[0]['mensaje'])
        self.assertEqual(res['fallidas'], 1)

    def test_volumen_bajo_descartes_altos_y_dia_no_cargado(self):
        ejs = semana_gescov()
        ejs[1] = ejecucion('2026-10-22', extraidos=2000, descartados=100, total=ejs[1]['detalle']['total'])
        ejs[4] = ejecucion('2026-10-25', extraidos=10000, descartados=1500, total=ejs[4]['detalle']['total'])
        ejs[5] = ejecucion('2026-10-26', max_inicio='2026-10-24 23:00:00', total=ejs[5]['detalle']['total'])
        nov, _ = vg.evaluar_gescov(ejs, DIAS, AHORA)
        por_codigo = {n['codigo']: n for n in nov}
        self.assertEqual(por_codigo['GESCOV_VOLUMEN_BAJO']['estado'], vs.AVISO)
        self.assertEqual(por_codigo['GESCOV_DESCARTES_ALTOS']['estado'], vs.AVISO)
        self.assertIn('15.0%', por_codigo['GESCOV_DESCARTES_ALTOS']['mensaje'])
        self.assertEqual(por_codigo['GESCOV_DIA_NO_CARGADO']['estado'], vs.FALLA)

    def test_total_inconsistente(self):
        ejs = semana_gescov()
        ejs[4]['detalle']['total'] += 3000  # alguien inserto filas por fuera de la tarea
        nov, _ = vg.evaluar_gescov(ejs, DIAS, AHORA)
        inc = [n for n in nov if n['codigo'] == 'GESCOV_TOTAL_INCONSISTENTE']
        self.assertTrue(inc)
        self.assertIn('+3,000', inc[0]['mensaje'])

    def test_logs_expirados_se_avisan_una_vez(self):
        ejs = semana_gescov()
        for i in range(3):
            ejs[i] = ejecucion(ejs[i]['inicio'].date().isoformat(), log=False)
        nov, res = vg.evaluar_gescov(ejs, DIAS, AHORA)
        exp = [n for n in nov if n['codigo'] == 'GESCOV_LOGS_EXPIRADOS']
        self.assertEqual(len(exp), 1)
        self.assertEqual((res['sin_detalle'], res['con_detalle']), (3, 5))
        self.assertIn('3 de 8', exp[0]['mensaje'])


class MapeoEstaciones(unittest.TestCase):
    def arbol(self):
        texto = ('Unnamed: 0,NOMBRE,TIUN_CODIGO,CONSECUTIVO,SIGLA_FISICA,UNDE_CONSECUTIVO\n'
                 '1,DEPARTAMENTO X,4.0,10,DEXX,\n'
                 '2,ESTACION UNO,7.0,11,DEXX,10.0\n'
                 '3,ESTACION DOS,7.0,12,DEXX,10.0\n'
                 '4,ESTACION TRES,7.0,13,DEXX,10.0\n'
                 '5,DISTRITO,164.0,14,DEXX,10.0\n'
                 '6,ESTACION OTRA SIGLA,7.0,15,DEYY,10.0\n')
        unidades, _ = sc.parsear_catalogo(texto)
        return sc.Arbol(unidades)

    def estaciones(self):
        return [dict(nombre='ESTACION UNO', sigla='DEXX', id='11'),            # bien
                dict(nombre='ESTACION VIEJA', sigla='DEXX', id='999'),          # no existe
                dict(nombre='DISTRITO', sigla='DEXX', id='14'),                 # tipo filtrado: el mapa no lo muestra
                dict(nombre='ESTACION OTRA', sigla='DEXX', id='15')]            # el catalogo la tiene en otra sigla

    def test_clasifica_los_problemas_de_mapeo(self):
        nov, sin_entrada, res = vg.evaluar_mapeo_gescov(self.estaciones(), self.arbol())
        unidades = {n['unidad_id']: n['codigo'] for n in nov if n['nivel'] == 'UNIDAD'}
        self.assertEqual(unidades, {'999': 'GESCOV_MAPEO_A_UNIDAD_INEXISTENTE', '14': 'GESCOV_MAPEO_A_UNIDAD_OCULTA', '15': 'GESCOV_MAPEO_SIGLA_DISTINTA'})
        self.assertTrue(all(n['estado'] == vs.AVISO and n['capa'] == vg.GESCOV for n in nov))
        self.assertEqual(res['con_problema'], 3)

    def test_estaciones_del_catalogo_sin_entrada(self):
        nov, sin_entrada, res = vg.evaluar_mapeo_gescov(self.estaciones(), self.arbol())
        self.assertEqual({u.id for u in sin_entrada}, {'12', '13'})
        resumen = next(n for n in nov if n['codigo'] == 'GESCOV_ESTACIONES_SIN_ENTRADA')
        self.assertIn('2 de 4 estaciones del catalogo', resumen['mensaje'])
        self.assertEqual((res['estaciones_catalogo'], res['sin_entrada']), (4, 2))

    def test_cargar_estaciones(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = Path(d) / 'estaciones.csv'
            ruta.write_text('"nombre_clean","sigla_fisica","unidad_id"\n"ESTACION UNO","dexx","11"\n"SIN ID","DEXX",""\n', encoding='utf-8')
            self.assertEqual(vg.cargar_estaciones(ruta), [dict(nombre='ESTACION UNO', sigla='DEXX', id='11')])
            ruta.write_text('a,b\n1,2\n', encoding='utf-8')
            with self.assertRaises(sc.Blocked):
                vg.cargar_estaciones(ruta)

    def test_copia_de_athena_desactualizada(self):
        nov = vg.evaluar_copia_athena_gescov('2026-08-21 20:27:07', AHORA)
        self.assertEqual([(n['codigo'], n['estado']) for n in nov], [('COPIA_ATHENA_GESCOV_DESACTUALIZADA', vs.AVISO)])
        self.assertIn('67 dias', nov[0]['mensaje'])
        self.assertEqual(vg.evaluar_copia_athena_gescov('2026-10-28 03:00:00', AHORA), [])
        self.assertEqual(vg.evaluar_copia_athena_gescov(None, AHORA), [])


class Integracion(unittest.TestCase):
    def test_consultas_sin_sigla_con_mensaje_propio(self):
        v = vs.calcular_ventana(datetime(2026, 10, 28, 9), 3, date(2026, 10, 27))
        obs = {'antecedentes-imei': {vg.SIN_SIGLA: {'': {d: vs.Acum(filas=40, puntos=40, ultima=f'{d} 10:00:00') for d in v.base + v.dias}}}}
        siglas, _, nov = vs.analizar(obs, None, v, vs.Umbrales(), ['antecedentes-imei'], None, vg.TABLAS)
        nov = vs.marcar_sin_sigla(siglas, nov, {vg.SIN_SIGLA}, 'consultas', 'CONSULTAS_SIN_SIGLA', 'sigla')
        sin = [n for n in nov if n['codigo'] == 'CONSULTAS_SIN_SIGLA']
        self.assertEqual(len(sin), 1)
        self.assertIn('120 consultas sin sigla en la ventana', sin[0]['mensaje'])
        self.assertEqual(siglas[0]['tabla'], 'raw_sivicc_usr_mnvcc_auditoria_cons_imei')

    def test_solo_sql_sin_aws(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            codigo = vg.main(['--solo-sql', '--sin-esquema', '--sin-catalogo', '--no-export', '--fecha-fin', '2026-10-27',
                              '--capas', 'antecedentes-personas', 'antecedentes-imei-positivos', 'planes-programados-historico'])
        salida = out.getvalue()
        self.assertEqual(codigo, 0)
        self.assertIn('Ventana 2026-10-13 a 2026-10-27', salida)
        self.assertIn('auditoria_cons_persona', salida)
        self.assertIn('auditoria_cons_imei', salida)
        self.assertNotIn('auditoria_cons_vehiculo', salida)
        self.assertIn('raw_disec_usr_disec_gescov_punto', salida)

    def test_argumentos_invalidos(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            vg.main(['--max-descartados-pct', '120'])
        self.assertEqual(cm.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
