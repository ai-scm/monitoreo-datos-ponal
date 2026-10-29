"""Pruebas sin AWS de la validacion SECAD. Ejecutar desde validacion-secad/:  python -m unittest discover -s tests -v"""

import contextlib
import io
import sys
import unittest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402

CATALOGO = """UNIDAD_ID;DEPENDENCIA;SIGLA_FISICA;TIPO;ID_UNIDAD_PADRE
1;DIRECCION;DISEC;1.0;
100.0;METROPOLITANA X;mexx;5;1
101;ESTACION 1;MEXX;7;100
102;DISTRITO;MEXX;164;101
103;CAI A;MEXX;9;102
104;CAI B;MEXX;9;101
64780;SUBESTACION SIN POLIGONO;MEXX;9;101
105;HIJA DE SIN POLIGONO;MEXX;9;64780
200;DEPARTAMENTO Y;DEYY;4;1
201;ESTACION Z;DEYY;7;200
"""


def arbol():
    unidades, _ = sc.parsear_catalogo(CATALOGO)
    return sc.Arbol(unidades)


def acum(filas, dia, **kw):
    return vs.Acum(filas=filas, puntos=kw.pop('puntos', filas), ultima=f'{dia} 12:00:00', **kw)


def serie(valores, **kw):
    """{dia: filas} -> {dia: Acum}"""
    return {d: acum(n, d, **kw) for d, n in valores.items()}


def semana(ini, fin, por_dia):
    return {date.fromisoformat(f'2026-10-{d:02d}').isoformat(): por_dia for d in range(ini, fin + 1)}


sano = semana(14, 27, 10)
V = vs.calcular_ventana(datetime(2026, 10, 28, 9, 0), 7, date(2026, 10, 27))  # ventana 21-27, base 14-20
U = vs.Umbrales()


class Catalogo(unittest.TestCase):
    def test_encabezados_normaliza_ids_tipos_y_siglas(self):
        unidades, formato = sc.parsear_catalogo(CATALOGO)
        u = {x.id: x for x in unidades}
        self.assertIn('encabezados', formato)
        self.assertEqual((u['100'].sigla, u['100'].tipo, u['100'].padre), ('MEXX', '5.0', '1'))
        self.assertEqual(u['1'].padre, '')

    def test_csv_real_del_backend_usa_consecutivo_como_id(self):
        texto = ('﻿Unnamed: 0,NOMBRE,TIUN_CODIGO,CONSECUTIVO,SIGLA_SIVICC,DEPENDENCIA,SIGLA_FISICA,SIGLA_PAPA,UNDE_CONSECUTIVO,ORDENAMIENTO,UNIDAD_ID\n'
                 '2,DEPARTAMENTO DE POLICIA AMAZONAS,4.0,29,,DEPARTAMENTO DE POLICIA AMAZONAS,DEAMA,DEAMA,17405.0,1,\n'
                 '11,   ESTACION DE POLICIA LETICIA,7.0,24721,E01,ESTACION DE POLICIA LETICIA,DEAMA,DEAMA,29.0,71,24721.0\n'
                 '13,      CAI FRONTERA,14.0,24723,C08,CAI FRONTERA,DEAMA,DEAMA,24721.0,111,24723.0\n')
        unidades, formato = sc.parsear_catalogo(texto)
        self.assertNotIn('plana', formato)
        u = {x.id: x for x in unidades}
        self.assertEqual((u['29'].nombre, u['29'].tipo, u['29'].sigla, u['29'].padre), ('DEPARTAMENTO DE POLICIA AMAZONAS', '4.0', 'DEAMA', '17405'))
        self.assertEqual((u['24723'].nombre, u['24723'].padre), ('CAI FRONTERA', '24721'))  # sangria del NOMBRE recortada
        a = sc.Arbol(unidades)
        self.assertEqual(set(a.ids_con_descendientes('29')), {'29', '24721', '24723'})

    def test_formato_posicional_del_backend(self):
        texto = 'x,CAI UNO,9.0,500.0,,,mepas,,100.0\n,sin marca,9.0,501.0,,,mepas,,100.0\nx,corta,9.0\n'
        unidades, formato = sc.parsear_catalogo(texto)
        self.assertIn('posicional', formato)
        self.assertEqual([(u.id, u.nombre, u.sigla, u.tipo, u.padre) for u in unidades], [('500', 'CAI UNO', 'MEPAS', '9.0', '100')])

    def test_catalogo_sin_unidades_bloquea(self):
        with self.assertRaises(sc.Blocked):
            sc.parsear_catalogo('a,b,c\n1,2,3\n')


class ArbolMapa(unittest.TestCase):
    def test_descendientes_como_el_mapa(self):
        a = arbol()
        # 102 es tipo filtrado: 103 sube a 101; 64780 y su hija no se ven
        self.assertEqual(set(a.ids_con_descendientes('101')), {'101', '103', '104'})
        self.assertNotIn('102', a.visibles)
        self.assertNotIn('64780', a.visibles)
        self.assertNotIn('105', a.visibles)
        self.assertEqual(set(a.ids_con_descendientes('100')), {'100', '101', '103', '104'})

    def test_siglas_y_tipo_sigla(self):
        a = arbol()
        self.assertEqual(a.siglas_con_territorio(), {'MEXX', 'DEYY'})
        self.assertTrue(a.es_sigla_completa('100'))
        self.assertFalse(a.es_sigla_completa('101'))

    def test_ciclo_no_cuelga(self):
        a = sc.Arbol([sc.Unidad('1', 'A', 'S', '4.0', '2'), sc.Unidad('2', 'B', 'S', '4.0', '1')])
        self.assertTrue(a.ids_con_descendientes('1'))

    def test_recorrido_en_orden_de_arbol(self):
        orden = [(u.id, n) for u, n in arbol().recorrido()]
        self.assertEqual(orden, [('200', 0), ('201', 1), ('100', 0), ('101', 1), ('103', 2), ('104', 2)])  # por sigla, luego arbol


class Ventana(unittest.TestCase):
    def test_ventana_y_base(self):
        self.assertEqual((V.dias[0], V.dias[-1]), ('2026-10-21', '2026-10-27'))
        self.assertEqual((V.base[0], V.base[-1]), ('2026-10-14', '2026-10-20'))
        w = vs.calcular_ventana(datetime(2026, 10, 28, 9, 0), 7)
        self.assertEqual((w.dias[-1], w.parcial), ('2026-10-27', '2026-10-28'))

    def test_particiones_cruzan_de_mes_sin_cast(self):
        sql = vs.predicado_particiones([date(2026, 9, 30), date(2026, 10, 1), date(2026, 10, 2)], sc.COLUMNAS)
        self.assertNotIn('CAST', sql)  # comparaciones directas: Athena las traslada a Glue
        self.assertIn('"anio" = \'2026\' AND "mes" IN (\'09\', \'9\') AND "dia" = \'30\'', sql)
        self.assertIn('"mes" = \'10\' AND "dia" IN (\'1\', \'2\', \'01\', \'02\')', sql)

    def test_particiones_enteras_se_comparan_como_numero(self):
        tipos = {'anio': 'int', 'mes': 'bigint', 'dia': 'int'}
        sql = vs.predicado_particiones([date(2026, 10, 7), date(2026, 10, 8)], sc.COLUMNAS, tipos)
        self.assertIn('"anio" = 2026 AND "mes" = 10 AND "dia" IN (7, 8)', sql)


class Sql(unittest.TestCase):
    def test_sql_basico(self):
        sql = vs.construir_sql('pon-prod-capas-iapol', 'capa_13', sc.COLUMNAS, [date(2026, 10, 27)], datetime(2026, 10, 28, 9),
                               {'MEPAS'}, 'nume_llamada')
        self.assertIn('FROM "pon-prod-capas-iapol"."capa_13"', sql)
        self.assertIn('"sigla" IN (\'MEPAS\')', sql)
        self.assertIn('COUNT(DISTINCT "nume_llamada")', sql)
        self.assertIn('BETWEEN -82 AND -66', sql)
        self.assertIn("TIMESTAMP '2026-10-29 09:00:00'", sql)
        self.assertIn('GROUP BY 1, 2, 3', sql)

    def test_punto_por_columna_en_vez_de_hora_y_coordenadas(self):
        sql = vs.construir_sql('db', 't', sc.COLUMNAS, [date(2026, 10, 27)], datetime(2026, 10, 28), col_punto='id_hechos')
        self.assertIn('THEN CAST("id_hechos" AS varchar) END) AS puntos', sql)
        self.assertNotIn("'|'", sql)

    def test_sql_sin_llamada_ni_sigla(self):
        sql = vs.construir_sql('db', 't', sc.COLUMNAS, [date(2026, 10, 27)], datetime(2026, 10, 28))
        self.assertIn('CAST(NULL AS bigint) AS llamadas', sql)
        self.assertNotIn(' IN (\'', sql)

    def test_nombres_se_escapan(self):
        sql = vs.construir_sql('db', 't"x', sc.COLUMNAS, [date(2026, 10, 27)], datetime(2026, 10, 28), {"O'NEIL"})
        self.assertIn('"t""x"', sql)
        self.assertIn("'O''NEIL'", sql)


class ReglasEvaluar(unittest.TestCase):
    def codigos(self, filas_por_dia, nivel='SIGLA', **kw):
        return [c for c, _, _ in vs.evaluar(vs.metricas(serie(filas_por_dia, **kw), V), nivel, U)]

    def test_sano(self):
        datos = {**semana(14, 20, 10), **semana(21, 27, 10)}
        self.assertEqual(self.codigos(datos), [])

    def test_cae_a_cero_es_falla_en_sigla_y_aviso_en_unidad(self):
        datos = semana(14, 20, 10)
        h = vs.evaluar(vs.metricas(serie(datos), V), 'SIGLA', U)
        self.assertEqual((h[0][0], h[0][1]), ('CAIDA_A_CERO', vs.FALLA))
        h = vs.evaluar(vs.metricas(serie(datos), V), 'UNIDAD', U)
        self.assertEqual((h[0][0], h[0][1]), ('CAIDA_A_CERO', vs.AVISO))

    def test_sin_base_es_solo_aviso(self):
        h = vs.evaluar(vs.metricas({}, V), 'SIGLA', U)
        self.assertEqual([(c, e) for c, e, _ in h], [('SIN_DATOS', vs.AVISO)])

    def test_todas_las_coordenadas_invalidas(self):
        datos = semana(21, 27, 5)
        h = vs.evaluar(vs.metricas(serie(datos, puntos=0, coord_invalida=5), V), 'UNIDAD', U)
        self.assertEqual((h[0][0], h[0][1]), ('SIN_PUNTOS_VALIDOS', vs.FALLA))

    def test_racha_solo_si_se_esperaban_datos(self):
        alto = {**semana(14, 25, 10)}  # 26 y 27 sin datos, ~10 por dia esperado
        self.assertIn('RACHA_SIN_DATOS', self.codigos(alto))
        bajo = {**semana(14, 25, 1)}  # ~1 fila por dia: 2 dias sin datos (se esperaban ~2) no son anomalia
        self.assertNotIn('RACHA_SIN_DATOS', self.codigos(bajo, nivel='UNIDAD'))
        largo = {**semana(14, 21, 1)}  # 6 dias seguidos sin datos con ese ritmo si lo son
        self.assertIn('RACHA_SIN_DATOS', self.codigos(largo, nivel='UNIDAD'))

    def test_racha_de_poco_volumen_es_aviso_si_no_alcanza_el_umbral_de_falla(self):
        datos = {**semana(14, 25, 4)}  # ~4 filas/dia, 2 dias sin datos: se esperaban ~8
        h = vs.evaluar(vs.metricas(serie(datos), V), 'SIGLA', vs.Umbrales(min_esperado_falla=10))
        self.assertEqual([(c, e) for c, e, _ in h if c == 'RACHA_SIN_DATOS'], [('RACHA_SIN_DATOS', vs.AVISO)])
        h = vs.evaluar(vs.metricas(serie(datos), V), 'SIGLA', vs.Umbrales())
        self.assertEqual([(c, e) for c, e, _ in h if c == 'RACHA_SIN_DATOS'], [('RACHA_SIN_DATOS', vs.FALLA)])

    def test_caida_de_volumen(self):
        datos = {**semana(14, 20, 20), **semana(21, 27, 5)}
        self.assertIn('CAIDA_VOLUMEN', self.codigos(datos))

    def test_dias_sin_datos_solo_a_nivel_sigla(self):
        datos = {**semana(14, 27, 10)}
        del datos['2026-10-23']
        self.assertIn('DIAS_SIN_DATOS', self.codigos(datos, 'SIGLA'))
        self.assertNotIn('DIAS_SIN_DATOS', self.codigos(datos, 'UNIDAD'))

    def test_calidad(self):
        datos = {**semana(14, 27, 100)}
        codigos = self.codigos(datos, coord_invalida=5, hora_invalida=5, hora_futura=1, desfase=5)
        for esperado in ('COORD_INVALIDA', 'HORA_INVALIDA', 'HORA_FUTURA', 'DESFASE_PARTICION'):
            self.assertIn(esperado, codigos)


class Analisis(unittest.TestCase):
    def datos(self):
        sano = semana(14, 27, 10)
        caido = {**semana(14, 20, 2)}  # CAI B: 14 filas la semana previa y 0 ahora
        return {
            'secad-rinas': {
                'MEXX': {
                    '103': serie(sano),
                    '104': serie(caido),
                    '101': serie(semana(14, 27, 3)),
                    '0': serie(semana(14, 27, 4)),  # sin unidad asignada: entra en la sigla pero no en una unidad
                },
                'ZZZ': {'900': serie(semana(14, 27, 10))},  # sigla ajena al catalogo
            },
            'secad-hurtos-general': {
                'MEXX': {'103': serie(sano), '104': serie(semana(14, 27, 2))},
            },
        }

    def correr(self, siglas=None):
        capas = ['secad-rinas', 'secad-hurtos-general']
        return vs.analizar(self.datos(), arbol(), V, U, capas, siglas)

    def test_unidades_se_suman_con_descendientes_y_cai_caido_se_detecta(self):
        _, unidades, _ = self.correr()
        u = {(f['unidad_id'], f['capa']): f for f in unidades}
        est = u[('101', 'secad-rinas')]
        self.assertEqual(est['filas'], 7 * (3 + 10 + 0))  # propia + CAI A + CAI B (0 en la ventana)
        self.assertEqual(est['filas_propias'], 21)
        self.assertEqual(est['consulta_mapa'], 'unidad + 2 descendientes')
        self.assertEqual(est['estado'], vs.OK)
        cai_b = u[('104', 'secad-rinas')]
        self.assertEqual(cai_b['estado'], vs.AVISO)
        self.assertIn('CAIDA_A_CERO', cai_b['codigos'])
        self.assertEqual(u[('104', 'secad-hurtos-general')]['estado'], vs.OK)  # en la otra capa si hay datos

    def test_sigla_completa_incluye_filas_sin_unidad(self):
        siglas, unidades, _ = self.correr()
        s = {(f['sigla'], f['capa']): f for f in siglas}
        mexx = s[('MEXX', 'secad-rinas')]
        self.assertEqual(mexx['filas'], 7 * (10 + 0 + 3 + 4))
        self.assertEqual(mexx['filas_sin_unidad'], 28)
        self.assertIn('UNIDAD_NO_ASIGNADA', mexx['codigos'])
        raiz = next(f for f in unidades if f['unidad_id'] == '100' and f['capa'] == 'secad-rinas')
        self.assertEqual(raiz['consulta_mapa'], 'sigla completa')
        self.assertEqual(raiz['filas'], mexx['filas'])

    def test_sigla_sin_datos_en_ninguna_capa(self):
        siglas, unidades, nov = self.correr()
        deyy = [f for f in siglas if f['sigla'] == 'DEYY']
        self.assertTrue(all(f['estado'] == vs.FALLA for f in deyy))
        grave = [n for n in nov if n['sigla'] == 'DEYY' and n['nivel'] == 'SIGLA' and n['capa'] == 'TODAS']
        self.assertEqual(grave[0]['estado'], vs.FALLA)
        u = [n for n in nov if n['nivel'] == 'UNIDAD' and n['sigla'] == 'DEYY' and n['capa'] == 'TODAS']
        self.assertEqual({n['unidad_id'] for n in u}, {'200', '201'})
        self.assertTrue(all(n['sigla_sin_datos'] == 'SI' and n['estado'] == vs.AVISO for n in u))

    def test_csv_de_novedades_sin_ruido_y_con_contexto(self):
        _, unidades, nov = self.correr()
        self.assertNotIn('SIN_DATOS', {n['codigo'] for n in nov})  # sin actividad no es novedad...
        self.assertTrue(any('SIN_DATOS' in f['codigos'].split(' | ') for f in unidades))  # ...pero queda en el resumen
        self.assertEqual(nov[0]['estado'], vs.FALLA)  # FALLA primero
        self.assertEqual([n['nivel'] for n in nov if n['estado'] == vs.FALLA][0], 'SIGLA')
        columnas = {'nivel', 'estado', 'sigla', 'unidad_id', 'unidad', 'tipo', 'capa', 'codigo', 'mensaje', 'contexto', 'sigla_sin_datos'}
        self.assertTrue(all(columnas <= set(n) for n in nov))
        deyy = next(n for n in nov if n['nivel'] == 'UNIDAD' and n['unidad_id'] == '200' and n['capa'] == 'TODAS')
        self.assertEqual(deyy['contexto'], 'la sigla no tiene filas en la ventana')
        self.assertEqual(deyy['tipo'], '4.0')

    def test_contexto_menciona_filas_sin_unidad_asignada(self):
        obs = {'secad-rinas': {'MEXX': {'0': serie(semana(14, 27, 5))}}}  # todo el volumen sin unidad
        _, _, nov = vs.analizar(obs, arbol(), V, U, ['secad-rinas'], {'MEXX'})
        r = next(n for n in nov if n['nivel'] == 'UNIDAD' and n['unidad_id'] == '103' and n['capa'] == 'TODAS')
        self.assertIn('35 (100.0%) sin unidad asignada', r['contexto'])

    def test_causa_de_las_filas_sin_unidad(self):
        a = arbol()
        self.assertEqual(vs.causa_sin_unidad('0', 'MEXX', a), 'sin codigo')
        self.assertEqual(vs.causa_sin_unidad('777', 'MEXX', a), 'no existe en el catalogo')
        self.assertEqual(vs.causa_sin_unidad('102', 'MEXX', a), 'unidad oculta en el arbol (tipo 164.0)')
        self.assertEqual(vs.causa_sin_unidad('201', 'MEXX', a), 'unidad de otra sigla (DEYY)')
        self.assertEqual(vs.causa_sin_unidad('500', 'MEXX', None), 'codigo ajeno')

    def test_el_mensaje_explica_por_que_no_hay_unidad(self):
        obs = {'secad-rinas': {'MEXX': {'103': serie(semana(14, 27, 10)), '777': serie(semana(14, 27, 2)), '102': serie(semana(14, 27, 1))}}}
        siglas, _, _ = vs.analizar(obs, arbol(), V, U, ['secad-rinas'])
        detalle = next(f for f in siglas if f['sigla'] == 'MEXX')['detalle']
        self.assertIn('no existe en el catalogo: 14', detalle)
        self.assertIn('unidad oculta en el arbol (tipo 164.0): 7', detalle)
        self.assertIn('777(14)', detalle)

    def test_sigla_sin_territorio_nunca_es_falla(self):
        obs = {'secad-rinas': {'ZZZ': {'900': serie(semana(14, 20, 10))}, 'MEXX': {'103': serie(sano)}}}  # ZZZ no tiene departamento/metropolitana
        siglas, _, nov = vs.analizar(obs, arbol(), V, U, ['secad-rinas'])
        zzz = next(f for f in siglas if f['sigla'] == 'ZZZ')
        self.assertEqual(zzz['estado'], vs.AVISO)
        self.assertIn('CAIDA_A_CERO', zzz['codigos'])
        self.assertFalse([n for n in nov if n['sigla'] == 'ZZZ' and n['estado'] == vs.FALLA])

    def test_sigla_fuera_del_catalogo(self):
        siglas, _, nov = self.correr()
        zzz = next(f for f in siglas if f['sigla'] == 'ZZZ' and f['capa'] == 'secad-rinas')
        self.assertIn('SIGLA_FUERA_CATALOGO', zzz['codigos'])
        fuera = [n for n in nov if n['codigo'] == 'SIGLA_FUERA_CATALOGO' and n['sigla'] == 'ZZZ']
        self.assertEqual([n['capa'] for n in fuera], ['TODAS'])  # una sola vez, no por capa

    def test_filtro_de_sigla(self):
        siglas, unidades, _ = self.correr({'MEXX'})
        self.assertEqual({f['sigla'] for f in siglas}, {'MEXX'})
        self.assertEqual({f['sigla'] for f in unidades}, {'MEXX'})

    def test_sin_catalogo_solo_observadas(self):
        siglas, unidades, _ = vs.analizar(self.datos(), None, V, U, ['secad-rinas'])
        self.assertEqual({f['sigla'] for f in siglas}, {'MEXX', 'ZZZ'})
        self.assertEqual({f['unidad_id'] for f in unidades}, {'101', '103', '104', '900'})  # el '0' no es unidad

    def test_diario_marca_catalogo(self):
        filas = vs.filas_diario(self.datos(), arbol(), V)
        cat = {(f['sigla'], f['codigo_unidad']): f['en_catalogo'] for f in filas}
        self.assertEqual((cat[('MEXX', '103')], cat[('MEXX', '0')], cat[('ZZZ', '900')]), ('SI', 'SIN_CODIGO', 'NO'))


class Parseo(unittest.TestCase):
    def test_filas_athena(self):
        filas = [dict(sigla='mexx', codigo_unidad='103', dia='2026-10-27', filas='5', llamadas=None, puntos='4', coord_invalida='1',
                      hora_invalida='0', hora_futura='0', desfase_particion='0', ultima_hora_caso='2026-10-27 23:10:00.000'),
                 dict(sigla='MEXX', codigo_unidad='103.0', dia='2026-10-27', filas='1', llamadas=None, puntos='1', coord_invalida='0',
                      hora_invalida='0', hora_futura='0', desfase_particion='0', ultima_hora_caso=None)]
        obs = vs.parsear_filas(filas)
        a = obs['MEXX']['103']['2026-10-27']
        self.assertEqual((a.filas, a.puntos, a.coord_invalida, a.llamadas, a.ultima), (6, 5, 1, None, '2026-10-27 23:10:00'))


class FakeCliente:
    def __init__(self):
        self.estados = ['RUNNING', 'SUCCEEDED']
        self.iniciadas = []

    def start_query_execution(self, **kw):
        self.iniciadas.append(kw)
        return {'QueryExecutionId': f'q{len(self.iniciadas)}'}

    def get_query_execution(self, QueryExecutionId):
        estado = self.estados[0] if len(self.estados) == 1 else self.estados.pop(0)
        return {'QueryExecution': {'Status': {'State': estado}, 'QueryExecutionId': QueryExecutionId}}

    def get_paginator(self, nombre):
        assert nombre == 'get_query_results'
        return self

    def paginate(self, QueryExecutionId):
        info = {'ResultSetMetadata': {'ColumnInfo': [{'Name': 'sigla'}, {'Name': 'llamadas'}]}}
        return [{'ResultSet': {**info, 'Rows': [{'Data': [{'VarCharValue': 'sigla'}, {'VarCharValue': 'llamadas'}]},
                                                  {'Data': [{'VarCharValue': 'MEXX'}, {}]}]}},
                {'ResultSet': {**info, 'Rows': [{'Data': [{'VarCharValue': 'DEYY'}, {'VarCharValue': '7'}]}]}}]


class FakeSesion:
    def __init__(self):
        self.cliente = FakeCliente()

    def client(self, nombre, config=None):
        return self.cliente


class RunnerAthena(unittest.TestCase):
    def test_lee_filas_sin_encabezado_y_con_nulos(self):
        sesion = FakeSesion()
        a = sc.Athena(sesion, 'db', 'primary', 's3://b/x/', timeout=30, log=lambda *_: None)
        orig = sc.time.sleep
        sc.time.sleep = lambda _: None
        try:
            r = a.ejecutar({'capa': 'SELECT 1'})
        finally:
            sc.time.sleep = orig
        filas, _ = r['capa']
        self.assertEqual(filas, [{'sigla': 'MEXX', 'llamadas': None}, {'sigla': 'DEYY', 'llamadas': '7'}])
        kw = sesion.cliente.iniciadas[0]
        self.assertEqual(kw['ResultConfiguration']['OutputLocation'], 's3://b/x/')
        self.assertEqual(kw['QueryExecutionContext']['Database'], 'db')

    def test_falla_de_athena_bloquea(self):
        sesion = FakeSesion()
        sesion.cliente.estados = ['FAILED']
        a = sc.Athena(sesion, 'db', log=lambda *_: None)
        with self.assertRaises(sc.Blocked):
            a.ejecutar({'capa': 'SELECT 1'})


class Cli(unittest.TestCase):
    def test_solo_sql_sin_aws(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            codigo = vs.main(['--solo-sql', '--sin-esquema', '--sin-catalogo', '--no-export', '--fecha-fin', '2026-10-27',
                              '--sigla', 'mepas', '--capas', 'secad-rinas'])
        self.assertEqual(codigo, 0)
        salida = out.getvalue()
        self.assertIn('"capa_13"', salida)
        self.assertIn("'MEPAS'", salida)
        self.assertNotIn('capa_11', salida)

    def test_argumentos_invalidos(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            vs.main(['--ventana-dias', '0'])
        self.assertEqual(cm.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
