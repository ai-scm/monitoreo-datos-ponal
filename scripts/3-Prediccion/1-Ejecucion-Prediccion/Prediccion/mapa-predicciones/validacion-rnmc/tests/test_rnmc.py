"""Pruebas sin AWS de la validacion RNMC. Ejecutar desde validacion-rnmc/:  python -m unittest discover -s tests -v"""

import contextlib
import io
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import validar_rnmc as vr  # noqa: E402  (agrega validacion-secad al path)
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402

VISTAS = ['rnmc-rinas', 'rnmc-incautaciones', 'comportamientos-contrarios-convivencia-materializados']


def corrida(estado, utc, args=None, error=None, id_='jr_1'):
    r = dict(JobRunState=estado, StartedOn=datetime.fromisoformat(utc).replace(tzinfo=timezone.utc), Id=id_)
    if args:
        r['Arguments'] = args
    if error:
        r['ErrorMessage'] = error
    return r


def diaria(dia, estado='SUCCEEDED', error=None):
    """Corrida programada de las 09:00 de Colombia (14:00 UTC) de `dia`."""
    return corrida(estado, f'{dia}T14:00:00', error=error, id_=f'jr_{dia}')


class SqlVista(unittest.TestCase):
    def test_sql_de_una_capa(self):
        sql = vr.construir_sql_vista(vr.CAPAS['rnmc-rinas']['filtro'], date(2026, 10, 1), date(2026, 10, 15), {'MEPAS'})
        self.assertIn('FROM "pon-prod-raw-cnp"."boletin_z1_capas_v2"', sql)
        self.assertIn("fecha >= TIMESTAMP '2026-10-01 00:00:00' AND fecha < TIMESTAMP '2026-10-16 00:00:00'", sql)
        self.assertIn("(articulo = '27' AND numeral = '1')", sql)
        self.assertIn("sigla IN ('MEPAS')", sql)
        self.assertIn('BETWEEN -82 AND -66', sql)
        self.assertIn('GROUP BY 1, 2, 3', sql)

    def test_filtros_reproducen_el_bff(self):
        self.assertEqual(vr.CAPAS['rnmc-incautaciones']['filtro'], 'incautacion = 1')
        filtro = vr.CAPAS['comportamientos-contrarios-convivencia-materializados']['filtro']
        self.assertIn("NOT (COALESCE(articulo, '') = '27' AND COALESCE(numeral, '') = '1')", filtro)
        self.assertIn("COALESCE(CAST(incautacion AS varchar), '0') <> '1'", filtro)

    def test_sql_de_creaciones_una_columna_por_capa(self):
        sql = vr.construir_sql_creaciones(date(2026, 10, 1), VISTAS)
        for i in range(3):
            self.assertIn(f'AS c{i}', sql)
        self.assertIn("fecha_creacion >= TIMESTAMP '2026-10-01 00:00:00'", sql)

    def test_orden_y_nombres_de_las_cinco_capas(self):
        self.assertEqual(list(vr.CAPAS), ['rnmc-rinas', 'rnmc-incautaciones', 'rnmc-mediaciones-situ', 'rnmc-mediaciones-sala',
                                          'comportamientos-contrarios-convivencia-materializados'])
        self.assertEqual({d['tabla'] for d in vr.CAPAS.values() if d['origen'] == 'capa'}, {'capa_16_1', 'capa_16_2'})


class Atribucion(unittest.TestCase):
    def test_cuadrante_a_unidad_sigla_vacia_y_sin_cuadrante(self):
        def fila(**k):
            base = dict(sigla='MEPAS', cuadrante='Q1', dia='2026-10-20', filas='3', llamadas='3', puntos='3', coord_invalida='0',
                        hora_invalida='0', hora_futura='0', desfase_particion='0', ultima_hora_caso='2026-10-20 10:00:00')
            return {**base, **k}

        filas = [fila(), fila(cuadrante='Q9'), fila(cuadrante='NO HAY CUADRANTE', sigla=''), fila(cuadrante='', sigla='')]
        obs = vr.parsear_vista(filas, {'Q1': '500'})
        self.assertEqual(set(obs['MEPAS']), {'500', 'Q9'})  # Q9 no esta en el catalogo: queda como cuadrante
        self.assertEqual(set(obs[vr.SIN_SIGLA]), {''})  # sin sigla ni cuadrante: sin unidad
        self.assertEqual(obs[vr.SIN_SIGLA]['']['2026-10-20'].filas, 6)

    def test_catalogo_con_cuadrantes(self):
        texto = ('Unnamed: 0,NOMBRE,TIUN_CODIGO,CONSECUTIVO,SIGLA_SIVICC,DEPENDENCIA,SIGLA_FISICA,SIGLA_PAPA,UNDE_CONSECUTIVO,ORDENAMIENTO,UNIDAD_ID,CUADRANTES,CUADRANTES_ID\n'
                 '1,DEPARTAMENTO X,4.0,29,,DEPARTAMENTO X,DEAMA,DEAMA,1.0,1,,,\n'
                 '2,   CAI FRONTERA,14.0,24723,C08,CAI FRONTERA,DEAMA,DEAMA,29.0,111,24723.0,2.0,"DEAMAMNVCCC01E01C08000003,DEAMAMNVCCC01E01C08000004"\n')
        unidades, _ = sc.parsear_catalogo(texto)
        mapa = sc.Arbol(unidades).cuadrante_a_unidad()
        self.assertEqual(mapa, {'DEAMAMNVCCC01E01C08000003': '24723', 'DEAMAMNVCCC01E01C08000004': '24723'})


class InterpretarCorridas(unittest.TestCase):
    def test_corrida_por_defecto_carga_el_dia_anterior(self):
        self.assertEqual(vr.fechas_cubiertas(diaria('2026-10-28')), {date(2026, 10, 27)})

    def test_fallo_y_otra_tabla_no_cuentan(self):
        self.assertEqual(vr.fechas_cubiertas(diaria('2026-10-27', 'FAILED')), set())
        prueba = corrida('SUCCEEDED', '2026-10-23T20:37:00', {'--DB_TABLE': 'public.hechos_z1_v2_prueba'})
        self.assertEqual(vr.fechas_cubiertas(prueba), set())

    def test_rango_manual_explicito(self):
        q = "SELECT v.* FROM \"pon-prod-raw-cnp\".\"boletin_z1_capas_v2\" v WHERE v.fecha_creacion >= timestamp '2026-10-15' AND v.fecha_creacion < timestamp '2026-10-17'"
        self.assertEqual(vr.fechas_cubiertas(corrida('SUCCEEDED', '2026-10-28T20:00:00', {'--QUERY': q})), {date(2026, 10, 15), date(2026, 10, 16)})

    def test_query_manual_con_otros_filtros_no_se_interpreta(self):
        q = "SELECT * FROM v WHERE fecha_creacion >= timestamp '2026-10-15' AND fecha_creacion < timestamp '2026-10-17' AND cuadrante2 LIKE 'MEBOG%'"
        self.assertIsNone(vr.fechas_cubiertas(corrida('SUCCEEDED', '2026-10-28T20:00:00', {'--QUERY': q})))

    def test_consulta_por_defecto_escrita_en_el_argumento(self):
        q = "SELECT * FROM v WHERE fecha_creacion >= date_add('day', -1, current_date) AND fecha_creacion < current_date"
        self.assertEqual(vr.fechas_cubiertas(corrida('SUCCEEDED', '2026-10-28T14:00:00', {'--QUERY': q})), {date(2026, 10, 27)})


class CargaRds(unittest.TestCase):
    ahora = datetime(2026, 10, 28, 12, 0)
    dias = [(date(2026, 10, 14) + timedelta(days=i)).isoformat() for i in range(14)]  # 14 al 27
    creaciones = {'2026-10-26': dict(total=5562, c0=592, c1=2307, c2=2663)}

    def corridas(self):
        runs = [diaria(f'2026-10-{d}') for d in range(15, 29) if d != 27]
        runs.append(diaria('2026-10-27', 'FAILED', 'La vista NO contiene la columna requerida: id_hechos'))
        return runs

    def test_dia_sin_carga_es_falla_con_impacto(self):
        nov, res = vr.evaluar_carga(self.corridas(), self.creaciones, self.dias, self.ahora, VISTAS)
        self.assertEqual([(n['codigo'], n['estado']) for n in nov], [('CARGA_RDS_FALLIDA', vs.FALLA)])
        m = nov[0]['mensaje']
        self.assertIn('hechos creados el 2026-10-26', m)
        self.assertIn('id_hechos', m)
        self.assertIn('5562 hechos (592 rinas, 2307 incautaciones, 2663 comportam.)', m)
        self.assertEqual((res['sin_carga'], res['duplicados']), (1, 0))

    def test_reintento_exitoso_resuelve_el_fallo(self):
        runs = self.corridas() + [corrida('SUCCEEDED', '2026-10-27T20:00:00', {'--QUERY': "x WHERE fecha_creacion >= timestamp '2026-10-26' AND fecha_creacion < timestamp '2026-10-27'"})]
        nov, _ = vr.evaluar_carga(runs, self.creaciones, self.dias, self.ahora, VISTAS)
        self.assertEqual(nov, [])

    def test_carga_duplicada_es_aviso(self):
        runs = self.corridas() + [diaria('2026-10-27', 'SUCCEEDED'), corrida('SUCCEEDED', '2026-10-24T20:00:00', id_='manual')]
        nov, res = vr.evaluar_carga(runs, self.creaciones, self.dias, self.ahora, VISTAS)
        duplicadas = [n for n in nov if n['codigo'] == 'CARGA_RDS_DUPLICADA']
        self.assertEqual(len(duplicadas), 1)
        self.assertIn('hechos creados el 2026-10-23', duplicadas[0]['mensaje'])
        self.assertEqual(duplicadas[0]['estado'], vs.AVISO)
        self.assertEqual(res['duplicados'], 1)

    def test_el_dia_de_ayer_queda_pendiente_antes_de_las_11(self):
        runs = [diaria(f'2026-10-{d}') for d in range(15, 28)]  # falta la corrida de hoy (28)
        nov, res = vr.evaluar_carga(runs, {}, self.dias, datetime(2026, 10, 28, 8, 0), VISTAS)
        self.assertEqual((nov, res['pendientes']), ([], 1))
        nov, _ = vr.evaluar_carga(runs, {}, self.dias, datetime(2026, 10, 28, 12, 0), VISTAS)
        self.assertEqual([n['codigo'] for n in nov], ['CARGA_RDS_AUSENTE'])

    def test_dias_anteriores_al_historial_no_se_evaluan(self):
        runs = [diaria(f'2026-10-{d}') for d in range(20, 29)]
        nov, res = vr.evaluar_carga(runs, {}, self.dias, self.ahora, VISTAS)
        self.assertEqual(nov, [])
        self.assertLess(res['revisados'], len(self.dias))

    def test_sin_corridas(self):
        self.assertEqual(vr.evaluar_carga([], {}, self.dias, self.ahora, VISTAS)[0], [])


def etl(estado, utc, tabla, query, error=None):
    return corrida(estado, utc, {'--nombre_capa': tabla, '--query': query}, error, id_=f'etl_{tabla}_{utc}')


Q_DIA = "SELECT s.id FROM t s WHERE s.fecha_solicitud >= TIMESTAMP '{d0} 00:00:00' AND s.fecha_solicitud < TIMESTAMP '{d1} 00:00:00'"


class EtlDeMediaciones(unittest.TestCase):
    ahora = datetime(2026, 10, 28, 12, 0)
    dias = [(date(2026, 10, 14) + timedelta(days=i)).isoformat() for i in range(14)]  # 14 al 27

    def corridas(self, saltar=()):
        runs = []
        for d in range(15, 29):
            carga = date(2026, 10, d) - timedelta(days=1)
            if carga.isoformat() in saltar:
                continue
            runs.append(etl('SUCCEEDED', f'2026-10-{d:02d}T09:00:00', 'capa_16_1', Q_DIA.format(d0=carga, d1=carga + timedelta(days=1))))
        return runs

    def test_interpreta_el_dia_que_carga_cada_query(self):
        self.assertEqual(vr.dias_de_query(Q_DIA.format(d0='2026-10-27', d1='2026-10-28')), {date(2026, 10, 27)})
        self.assertEqual(vr.dias_de_query('... "YEAR" = \'2026\' AND "MONTH" = \'10\' AND "DAY" = \'7\' ...'), {date(2026, 10, 7)})
        self.assertEqual(vr.dias_de_query("x BETWEEN DATE '2026-10-01' AND DATE '2026-10-03'"),
                         {date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)})
        self.assertEqual(len(vr.dias_de_query(Q_DIA.format(d0='2026-10-01', d1='2026-10-04'))), 3)
        self.assertIsNone(vr.dias_de_query('SELECT 1'))

    def test_dia_sin_carga_por_systemexit(self):
        runs = self.corridas(saltar=('2026-10-26',))
        runs.append(etl('FAILED', '2026-10-27T09:00:00', 'capa_16_1', Q_DIA.format(d0='2026-10-26', d1='2026-10-27'), 'SystemExit: '))
        obs = {'MEPAS': {'1': {'2026-10-26': vs.Acum(filas=0)}}}
        nov, res = vr.evaluar_etl(runs, 'rnmc-mediaciones-situ', obs, self.dias, self.ahora)
        self.assertEqual([(n['codigo'], n['estado'], n['capa']) for n in nov], [('CARGA_CAPA_FALLIDA', vs.FALLA, 'rnmc-mediaciones-situ')])
        self.assertIn('No hay datos para procesar', nov[0]['mensaje'])
        self.assertIn('capa_16_1', nov[0]['mensaje'])
        self.assertEqual((res['sin_carga'], res['revisados']), (1, 14))

    def test_reintento_exitoso_resuelve(self):
        runs = self.corridas(saltar=('2026-10-26',))
        runs.append(etl('FAILED', '2026-10-27T09:00:00', 'capa_16_1', Q_DIA.format(d0='2026-10-26', d1='2026-10-27'), 'SystemExit: '))
        runs.append(etl('SUCCEEDED', '2026-10-28T15:00:00', 'capa_16_1', Q_DIA.format(d0='2026-10-26', d1='2026-10-27')))
        self.assertEqual(vr.evaluar_etl(runs, 'rnmc-mediaciones-situ', {}, self.dias, self.ahora)[0], [])

    def test_solo_cuentan_las_corridas_de_su_tabla(self):
        runs = self.corridas(saltar=('2026-10-26',))
        runs.append(etl('SUCCEEDED', '2026-10-27T09:00:00', 'capa_16_2', Q_DIA.format(d0='2026-10-26', d1='2026-10-27')))
        nov, _ = vr.evaluar_etl(runs, 'rnmc-mediaciones-situ', {}, self.dias, self.ahora)
        self.assertEqual([n['codigo'] for n in nov], ['CARGA_CAPA_AUSENTE'])

    def test_el_dia_de_ayer_espera_hasta_las_7(self):
        runs = self.corridas(saltar=('2026-10-27',))
        self.assertEqual(vr.evaluar_etl(runs, 'rnmc-mediaciones-situ', {}, self.dias, datetime(2026, 10, 28, 6, 0))[0], [])
        self.assertEqual(len(vr.evaluar_etl(runs, 'rnmc-mediaciones-situ', {}, self.dias, datetime(2026, 10, 28, 12, 0))[0]), 1)


class SinSigla(unittest.TestCase):
    def test_reemplaza_los_hallazgos_genericos(self):
        filas = [dict(sigla='(SIN_SIGLA)', capa='rnmc-rinas', filas=29, estado=vs.AVISO, codigos='SIGLA_FUERA_CATALOGO', detalle='x'),
                 dict(sigla='NO HA', capa='rnmc-mediaciones-situ', filas=8, estado=vs.AVISO, codigos='UNIDAD_NO_ASIGNADA', detalle='x'),
                 dict(sigla='MEPAS', capa='rnmc-rinas', filas=10, estado=vs.OK, codigos='', detalle='')]
        nov = [dict(nivel='SIGLA', sigla='(SIN_SIGLA)', capa='rnmc-rinas', codigo='SIGLA_FUERA_CATALOGO', estado=vs.AVISO, mensaje='x'),
               dict(nivel='SIGLA', sigla='MEPAS', capa='rnmc-rinas', codigo='CAIDA_VOLUMEN', estado=vs.AVISO, mensaje='y')]
        res = vr.marcar_sin_sigla(filas, nov)
        self.assertEqual(sorted((n['sigla'], n['codigo']) for n in res),
                         [('(SIN_SIGLA)', 'HECHOS_SIN_SIGLA'), ('MEPAS', 'CAIDA_VOLUMEN'), ('NO HA', 'HECHOS_SIN_SIGLA')])
        self.assertEqual(filas[0]['codigos'], 'HECHOS_SIN_SIGLA')
        self.assertEqual(filas[2]['codigos'], '')
        self.assertIn('29 hechos sin sigla ni cuadrante', next(n for n in res if n['sigla'] == '(SIN_SIGLA)')['mensaje'])


class VolumenNacional(unittest.TestCase):
    def obs(self, por_dia):
        return {'rnmc-rinas': {'MEPAS': {'1': {d: vs.Acum(filas=n, puntos=n) for d, n in por_dia.items()}}}}

    def test_dia_con_poco_volumen(self):
        v = vs.calcular_ventana(datetime(2026, 10, 28, 9), 5, date(2026, 10, 27))
        por_dia = {d: 100 for d in v.base + v.dias}
        por_dia[v.dias[2]] = 20
        nov = vr.dias_atipicos(self.obs(por_dia), v, ['rnmc-rinas'])
        self.assertEqual([(n['codigo'], n['nivel']) for n in nov], [('DIA_ATIPICO', 'NACIONAL')])
        self.assertIn(v.dias[2], nov[0]['mensaje'])

    def test_volumen_normal_o_muy_bajo_no_avisa(self):
        v = vs.calcular_ventana(datetime(2026, 10, 28, 9), 5, date(2026, 10, 27))
        self.assertEqual(vr.dias_atipicos(self.obs({d: 100 for d in v.base + v.dias}), v, ['rnmc-rinas']), [])
        self.assertEqual(vr.dias_atipicos(self.obs({d: 3 for d in v.base + v.dias}), v, ['rnmc-rinas']), [])


class Integracion(unittest.TestCase):
    def test_analizar_usa_las_tablas_de_rnmc(self):
        v = vs.calcular_ventana(datetime(2026, 10, 28, 9), 3, date(2026, 10, 27))
        obs = {'rnmc-rinas': {'MEPAS': {'500': {d: vs.Acum(filas=30, puntos=30, ultima=f'{d} 10:00:00') for d in v.base + v.dias}}}}
        siglas, unidades, _ = vs.analizar(obs, None, v, vs.Umbrales(), ['rnmc-rinas'], None, vr.TABLAS)
        self.assertEqual(siglas[0]['tabla'], 'boletin_z1_capas_v2 (riñas)')
        self.assertEqual((unidades[0]['unidad_id'], unidades[0]['estado']), ('500', vs.OK))

    def test_solo_sql_sin_aws(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            codigo = vr.main(['--solo-sql', '--sin-esquema', '--sin-catalogo', '--no-export', '--fecha-fin', '2026-10-27',
                              '--capas', 'rnmc-rinas', 'rnmc-mediaciones-sala'])
        salida = out.getvalue()
        self.assertEqual(codigo, 0)
        self.assertIn('boletin_z1_capas_v2', salida)
        self.assertIn('"capa_16_2"', salida)
        self.assertNotIn('capa_16_1', salida)
        self.assertIn('Ventana 2026-10-13 a 2026-10-27', salida)  # 15 dias por defecto

    def test_argumentos_invalidos(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            vr.main(['--ventana-dias', '40'])
        self.assertEqual(cm.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
