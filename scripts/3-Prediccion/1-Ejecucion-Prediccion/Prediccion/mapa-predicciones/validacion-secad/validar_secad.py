#!/usr/bin/env python3
"""Validacion de las capas SECAD del mapa por sigla y unidad. Solo lectura, fuente Athena.

Revisa la ultima semana de las tres capas SECAD del mapa y la compara con la semana previa:
  secad-rinas                         -> pon-prod-capas-iapol.capa_13
  secad-hurtos-violento-y-homicidio   -> pon-prod-capas-iapol.capa_11
  secad-hurtos-general                -> pon-prod-capas-iapol.capa_12
Son las tablas que consulta el BFF del mapa, filtrando por sigla, codigo_unidad y particion
anio/mes/dia. Una consulta por capa agrupa por sigla, codigo_unidad y dia; el resto se calcula
aqui. Para cada unidad se suma ella y sus descendientes, igual que el payload del mapa
(MapPage.buildLayerPayload); departamentos y metropolitanas se piden solo por sigla.

Uso (credenciales AWS ya iniciadas en la maquina):
  python validar_secad.py --catalogo EstructuraNacionaOperativalPONAL.csv
  python validar_secad.py --sigla MEPAS --catalogo ...              # una sigla
  python validar_secad.py --solo-sql --sin-esquema --sin-catalogo   # SQL sin tocar AWS
  python descubrir_secad.py                                         # primero: revisar esquema

Salida en salida/: resumen_siglas, resumen_unidades, novedades, diario (.csv) y meta (.json/.sql).
Exit: 0 sin FALLA (puede haber AVISO), 1 FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupcion.
"""

import argparse
import os
import csv
import json
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import secad_comun as sc
from botocore.exceptions import BotoCoreError, ClientError

OK, AVISO, FALLA = 'OK', 'AVISO', 'FALLA'
ORDEN = {OK: 0, AVISO: 1, FALLA: 2}
SIN_CODIGO = {'', '0'}  # codigo_unidad que deja el etiquetado cuando el punto no cae en ninguna unidad
CORTO = {'secad-rinas': 'Rinas', 'secad-hurtos-violento-y-homicidio': 'Hurto viol+hom',
         'secad-hurtos-general': 'Hurto general'}


# --------------------------------------------------------------------------------------
# Ventana de fechas
# --------------------------------------------------------------------------------------
@dataclass
class Ventana:
    dias: list           # ISO, dias completos evaluados
    base: list           # ISO, ventana previa de igual largo
    parcial: str = ''    # dia en curso: solo sirve para la ultima fecha, no cuenta en los totales

    @property
    def todos(self):
        return self.base + self.dias + ([self.parcial] if self.parcial else [])


def calcular_ventana(ahora, n, fecha_fin=None):
    """Por defecto la ventana termina ayer (hoy esta incompleto), como monitor_datos_entrada."""
    if fecha_fin is None:
        ultimo, parcial = ahora.date() - timedelta(days=1), ahora.date().isoformat()
    else:
        ultimo, parcial = fecha_fin, ''
    dias = [(ultimo - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]
    base = [(ultimo - timedelta(days=n + i)).isoformat() for i in range(n - 1, -1, -1)]
    return Ventana(dias, base, parcial)


# --------------------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------------------
TIPOS_ENTEROS = ('tinyint', 'smallint', 'int', 'integer', 'bigint')


def _condicion(columna, valores, numerico):
    lits = [v if numerico else sc.literal(v) for v in valores]
    return f'{sc.identifier(columna)} = {lits[0]}' if len(lits) == 1 else f'{sc.identifier(columna)} IN ({", ".join(lits)})'


def predicado_particiones(dias, c, tipos=None):
    """Una condicion por mes con comparaciones directas sobre anio/mes/dia, que Athena puede
    trasladar a Glue (un CAST sobre la particion obliga a traer todas las particiones). Las
    particiones son texto sin cero a la izquierda ('7', '10'); se aceptan ambas formas por si
    alguna se escribio con relleno. Si Glue dice que la columna es entera, se compara como numero."""
    tipos = tipos or {}
    num = {k: tipos.get(c[k], 'string').lower() in TIPOS_ENTEROS for k in ('anio', 'mes', 'dia')}

    def formas(n, ancho, numerico):
        return [str(n)] if numerico else sorted({str(n), f'{n:0{ancho}d}'})

    grupos = {}
    for d in sorted(dias):
        grupos.setdefault((d.year, d.month), []).append(d.day)
    partes = [f'({_condicion(c["anio"], formas(y, 4, num["anio"]), num["anio"])} '
              f'AND {_condicion(c["mes"], formas(m, 2, num["mes"]), num["mes"])} '
              f'AND {_condicion(c["dia"], sorted({v for x in dd for v in formas(x, 2, num["dia"])}, key=lambda s: (len(s), s)), num["dia"])})'
              for (y, m), dd in sorted(grupos.items())]
    return '(' + '\n       OR '.join(partes) + ')'


def construir_sql(database, tabla, c, dias, ahora, siglas=None, col_llamada=None, tipos=None, col_punto=None):
    q = {k: sc.identifier(v) for k, v in c.items()}
    ts = f'TRY_CAST({q["fecha"]} AS timestamp)'
    dia = ("concat(lpad(CAST(CAST({anio} AS INTEGER) AS varchar), 4, '0'), '-', "
           "lpad(CAST(CAST({mes} AS INTEGER) AS varchar), 2, '0'), '-', "
           "lpad(CAST(CAST({dia} AS INTEGER) AS varchar), 2, '0'))").format(**q)
    coord_ok = (f'(TRY_CAST({q["lat"]} AS double) BETWEEN {sc.LAT_MIN} AND {sc.LAT_MAX} '
                f'AND TRY_CAST({q["lon"]} AS double) BETWEEN {sc.LON_MIN} AND {sc.LON_MAX})')
    llamadas = f'COUNT(DISTINCT {sc.identifier(col_llamada)})' if col_llamada else 'CAST(NULL AS bigint)'
    futuro = (ahora + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    # SECAD: el front deja un punto por llamada (misma hora y coordenadas). Otras capas: un punto por col_punto.
    clave_punto = (f'CAST({sc.identifier(col_punto)} AS varchar)' if col_punto else
                   "concat(coalesce(CAST({fecha} AS varchar), ''), '|', CAST({lat} AS varchar), '|', CAST({lon} AS varchar))".format(**q))
    filtro_sigla = f'\n  AND {q["sigla"]} IN ({", ".join(sc.literal(s) for s in sorted(siglas))})' if siglas else ''
    return f"""SELECT
  CAST({q["sigla"]} AS varchar) AS sigla,
  CAST({q["unidad"]} AS varchar) AS codigo_unidad,
  {dia} AS dia,
  COUNT(*) AS filas,
  {llamadas} AS llamadas,
  COUNT(DISTINCT CASE WHEN {coord_ok} THEN {clave_punto} END) AS puntos,
  SUM(CASE WHEN {coord_ok} THEN 0 ELSE 1 END) AS coord_invalida,
  SUM(CASE WHEN {ts} IS NULL THEN 1 ELSE 0 END) AS hora_invalida,
  SUM(CASE WHEN {ts} > TIMESTAMP '{futuro}' THEN 1 ELSE 0 END) AS hora_futura,
  SUM(CASE WHEN {ts} IS NOT NULL AND date_format({ts}, '%Y-%m-%d') <> {dia} THEN 1 ELSE 0 END) AS desfase_particion,
  CAST(MAX({ts}) AS varchar) AS ultima_hora_caso
FROM {sc.identifier(database)}.{sc.identifier(tabla)}
WHERE {predicado_particiones(dias, c, tipos)}{filtro_sigla}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""


# --------------------------------------------------------------------------------------
# Resultados de Athena -> estructuras
# --------------------------------------------------------------------------------------
class Acum:
    __slots__ = ('filas', 'llamadas', 'puntos', 'coord_invalida', 'hora_invalida', 'hora_futura', 'desfase', 'ultima')

    def __init__(self, filas=0, llamadas=None, puntos=0, coord_invalida=0, hora_invalida=0, hora_futura=0, desfase=0, ultima=''):
        self.filas, self.llamadas, self.puntos = filas, llamadas, puntos
        self.coord_invalida, self.hora_invalida, self.hora_futura = coord_invalida, hora_invalida, hora_futura
        self.desfase, self.ultima = desfase, ultima

    def sumar(self, o):
        self.filas += o.filas
        self.puntos += o.puntos
        self.coord_invalida += o.coord_invalida
        self.hora_invalida += o.hora_invalida
        self.hora_futura += o.hora_futura
        self.desfase += o.desfase
        if o.llamadas is not None:
            self.llamadas = (self.llamadas or 0) + o.llamadas
        self.ultima = max(self.ultima, o.ultima)


def _entero(valor):
    return int(float(valor)) if valor not in (None, '') else 0


def parsear_filas(filas):
    """Filas de Athena -> {sigla: {codigo_unidad: {dia: Acum}}}."""
    obs = {}
    for r in filas:
        sigla = (r.get('sigla') or '').strip().upper()
        codigo = (r.get('codigo_unidad') or '').strip()
        codigo = sc.normalizar_id(codigo) or codigo
        dia = (r.get('dia') or '').strip()
        if not dia:
            continue
        a = Acum(_entero(r.get('filas')), None if r.get('llamadas') is None else _entero(r['llamadas']),
                 _entero(r.get('puntos')), _entero(r.get('coord_invalida')), _entero(r.get('hora_invalida')),
                 _entero(r.get('hora_futura')), _entero(r.get('desfase_particion')),
                 (r.get('ultima_hora_caso') or '').split('.')[0])
        obs.setdefault(sigla, {}).setdefault(codigo, {}).setdefault(dia, Acum()).sumar(a)
    return obs


def combinar(series):
    res = {}
    for s in series:
        for dia, a in s.items():
            res.setdefault(dia, Acum()).sumar(a)
    return res


def total(serie, dias):
    t = Acum()
    for d in dias:
        if d in serie:
            t.sumar(serie[d])
    return t


# --------------------------------------------------------------------------------------
# Metricas y reglas
# --------------------------------------------------------------------------------------
@dataclass
class Umbrales:
    min_base: int = 10             # filas de la semana previa para considerar que "debia haber datos"
    caida_pct: float = 50          # caida de puntos vs semana previa
    racha_dias: int = 2            # dias seguidos sin filas al cierre de la ventana
    min_esperado: float = 3        # filas esperadas en esa racha para no confundirla con baja actividad
    max_invalidos_pct: float = 1   # calidad: porcentaje tolerado
    min_esperado_falla: float = 3  # filas esperadas en la racha para que a nivel sigla sea FALLA y no AVISO


def pct(parte, todo):
    return round(100 * parte / todo, 2) if todo else 0.0


def metricas(serie, v, rezago=0):
    """`rezago`: ultimos dias de la ventana que la fuente registra con atraso; cuentan en los totales pero no en la racha
    final ni en los dias sin datos (aun no tienen por que estar completos)."""
    w, b = total(serie, v.dias), total(serie, v.base)
    evaluados = v.dias[:max(len(v.dias) - rezago, 0)] if rezago > 0 else v.dias
    con_datos = [d for d in v.dias if d in serie and serie[d].filas > 0]
    racha = 0
    for d in reversed(evaluados):
        if d in con_datos:
            break
        racha += 1
    ultima = max((serie[d].ultima for d in v.todos if d in serie), default='')
    return dict(
        filas=w.filas, llamadas=w.llamadas, puntos=w.puntos, filas_base=b.filas, puntos_base=b.puntos,
        var_pct=round(100 * (w.puntos - b.puntos) / b.puntos, 1) if b.puntos else None,
        dias_ventana=len(v.dias), dias_base=len(v.base), dias_con_datos=len(con_datos),
        dias_sin_datos=[d for d in evaluados if d not in con_datos], racha_final=racha, ultima_hora_caso=ultima,
        coord_invalida=w.coord_invalida, coord_invalida_pct=pct(w.coord_invalida, w.filas),
        hora_invalida=w.hora_invalida, hora_invalida_pct=pct(w.hora_invalida, w.filas),
        hora_futura=w.hora_futura, desfase=w.desfase, desfase_pct=pct(w.desfase, w.filas))


def evaluar(m, nivel, u):
    """Hallazgos [(codigo, estado, mensaje)] de una serie. A nivel sigla lo grave es FALLA; a
    nivel unidad es AVISO, porque una unidad pequena puede no tener casos."""
    alta = FALLA if nivel == 'SIGLA' else AVISO
    h = []
    if m['filas'] == 0:
        if m['filas_base'] >= u.min_base:
            h.append(('CAIDA_A_CERO', alta, f'{m["filas_base"]} filas en la semana previa y 0 en la ventana'))
        else:
            h.append(('SIN_DATOS', AVISO, 'sin filas en la ventana ni base suficiente en la semana previa'))
        return h
    if m['puntos'] == 0:
        h.append(('SIN_PUNTOS_VALIDOS', FALLA, f'{m["filas"]} filas pero ninguna con coordenadas validas: el mapa no dibuja nada '
                                               'y el BFF responde 500'))
        return h
    if m['racha_final'] >= u.racha_dias:
        esperado = (m['filas'] + m['filas_base']) / max(m['dias_ventana'] + m['dias_base'], 1) * m['racha_final']
        if esperado >= u.min_esperado:
            h.append(('RACHA_SIN_DATOS', alta if esperado >= u.min_esperado_falla else AVISO, f'{m["racha_final"]} dia(s) sin filas al cierre de la ventana '
                                               f'(se esperaban ~{esperado:.0f}); ultimo registro {m["ultima_hora_caso"] or "n/d"}'))
    if m['filas_base'] >= u.min_base and m['puntos_base'] > 0 and m['puntos'] < m['puntos_base'] * (1 - u.caida_pct / 100):
        h.append(('CAIDA_VOLUMEN', AVISO, f'{m["puntos_base"]} -> {m["puntos"]} puntos ({m["var_pct"]}%) vs semana previa'))
    if nivel == 'SIGLA' and m['dias_sin_datos'] and m['filas'] >= u.min_base and not any(x[0] == 'RACHA_SIN_DATOS' for x in h):
        h.append(('DIAS_SIN_DATOS', AVISO, 'dias sin filas: ' + ', '.join(m['dias_sin_datos'])))
    if m['coord_invalida_pct'] > u.max_invalidos_pct:
        h.append(('COORD_INVALIDA', AVISO, f'{m["coord_invalida"]} filas ({m["coord_invalida_pct"]}%) con coordenadas nulas o fuera de '
                                           'Colombia; el BFF las descarta'))
    if m['hora_invalida_pct'] > u.max_invalidos_pct:
        h.append(('HORA_INVALIDA', AVISO, f'{m["hora_invalida"]} filas ({m["hora_invalida_pct"]}%) con hora_caso nula o ilegible'))
    if m['hora_futura']:
        h.append(('HORA_FUTURA', AVISO, f'{m["hora_futura"]} filas con hora_caso a mas de 24 h en el futuro'))
    if m['desfase_pct'] > u.max_invalidos_pct:
        h.append(('DESFASE_PARTICION', AVISO, f'{m["desfase"]} filas ({m["desfase_pct"]}%) donde hora_caso cae en otro dia que su '
                                              'particion; el mapa filtra por particion'))
    return h


def peor(hallazgos):
    return max((e for _, e, _ in hallazgos), key=ORDEN.get, default=OK)


# --------------------------------------------------------------------------------------
# Analisis por sigla y unidad
# --------------------------------------------------------------------------------------
COLS_METRICAS = ['filas', 'llamadas', 'puntos', 'filas_base', 'puntos_base', 'var_pct', 'dias_con_datos', 'dias_ventana',
                 'racha_final', 'ultima_hora_caso', 'coord_invalida_pct', 'hora_invalida_pct', 'hora_futura', 'desfase_pct']


def _fila(m, hallazgos):
    fila = {k: m[k] for k in COLS_METRICAS}
    fila['estado'] = peor(hallazgos)
    fila['codigos'] = ' | '.join(c for c, _, _ in hallazgos)
    fila['detalle'] = ' | '.join(f'{c}: {msg}' for c, _, msg in hallazgos)
    return fila


def causa_sin_unidad(codigo, sigla, arbol):
    """Por que un codigo_unidad no corresponde a una unidad que el mapa muestre para esa sigla."""
    if codigo in SIN_CODIGO:
        return 'sin codigo'
    if arbol is None:
        return 'codigo ajeno'
    unidad = arbol.por_id.get(codigo)
    if unidad is None:
        return 'no existe en el catalogo'
    if codigo not in arbol.visibles:
        return f'unidad oculta en el arbol (tipo {unidad.tipo})'
    if unidad.sigla != sigla:
        return f'unidad de otra sigla ({unidad.sigla})'
    return 'codigo ajeno'


def analizar(obs, arbol, v, u, capas, siglas=None, tablas=None, solo_sigla=(), avisar_sin_unidad=True, rezago=None):
    """obs[capa] = {sigla: {codigo_unidad: {dia: Acum}}}. Devuelve (filas_siglas, filas_unidades, novedades).
    solo_sigla: capas que no traen unidad (se evaluan solo por sigla). avisar_sin_unidad=False omite UNIDAD_NO_ASIGNADA
    (para capas donde el mapa ubica por coordenadas y la unidad de los datos es solo una aproximacion).
    rezago: {capa: dias} ultimos dias de la ventana que la fuente registra con atraso (ver `metricas`)."""
    ids = list(capas)
    rezago = rezago or {}
    tablas = tablas or {c: t for c, (t, _) in sc.CAPAS.items()}  # capa -> nombre de la tabla/vista que se muestra
    observadas = {s for c in ids for s in obs.get(c, {})}
    esperadas = arbol.siglas_con_territorio() if arbol else set()
    lista = sorted((esperadas | observadas) & set(siglas) if siglas else esperadas | observadas)
    ids_sigla = {}
    if arbol:
        for i in arbol.visibles:
            ids_sigla.setdefault(arbol.por_id[i].sigla, set()).add(i)

    filas_siglas, filas_unidades, novedades = [], [], []
    filas_sigla_capa = {}

    ctx_sigla = {}  # sigla -> (filas en la ventana, filas sin unidad), sumando las capas

    def registrar(nivel, base, capa, hallazgos, extra):
        """Una fila por hallazgo en novedades. SIN_DATOS (sin filas y sin base) no es una novedad: es una
        unidad sin actividad y queda solo en los resumenes; la ausencia total se reporta como SIN_DATOS_EN_TODAS_LAS_CAPAS."""
        for codigo, estado, mensaje in hallazgos:
            if codigo == 'SIN_DATOS':
                continue
            fila = dict(nivel=nivel, estado=estado, sigla='', unidad_id='', unidad='', tipo='', capa=capa, codigo=codigo,
                        mensaje=mensaje, contexto='', sigla_sin_datos='')
            fila.update(base)
            fila.update(extra)
            novedades.append(fila)

    def contexto_unidad(sigla):
        filas, sin = ctx_sigla.get(sigla, (0, 0))
        if not filas:
            return 'la sigla no tiene filas en la ventana'
        return (f'la sigla tiene {filas} filas en la ventana, {sin} ({pct(sin, filas)}%) sin unidad asignada '
                f'(codigo_unidad 0 o ajeno al catalogo)')

    # ---- nivel sigla: lo que muestra el mapa al elegir toda la sigla (sin id_unidad)
    for sigla in lista:
        por_capa = {}
        for capa in ids:
            codigos = obs.get(capa, {}).get(sigla, {})
            m = metricas(combinar(codigos.values()), v, rezago.get(capa, 0))
            h = evaluar(m, 'SIGLA', u)
            if arbol and sigla not in esperadas:  # sigla sin territorio (direcciones, escuelas...): nunca es FALLA
                h = [(c, AVISO if e == FALLA else e, msg) for c, e, msg in h]
            visibles = ids_sigla.get(sigla, set())
            sin_unidad, ejemplos, causas = 0, [], {}
            for cod, serie in codigos.items():
                f = total(serie, v.dias).filas
                asignada = cod in visibles if arbol else cod not in SIN_CODIGO
                if f and not asignada:
                    sin_unidad += f
                    ejemplos.append((f, cod))
                    causa = causa_sin_unidad(cod, sigla, arbol)
                    causas[causa] = causas.get(causa, 0) + f
            pct_sin = pct(sin_unidad, m['filas'])
            if avisar_sin_unidad and capa not in solo_sigla and pct_sin > u.max_invalidos_pct:
                top = ', '.join(f'{c or "<vacio>"}({f})' for f, c in sorted(ejemplos, reverse=True)[:3])
                por_causa = '; '.join(f'{k}: {n}' for k, n in sorted(causas.items(), key=lambda kv: -kv[1]))
                h.append(('UNIDAD_NO_ASIGNADA', AVISO, f'{sin_unidad} filas ({pct_sin}%) sin unidad visible en esta sigla ({por_causa}); '
                                                      f'codigos principales: {top}; salen al elegir la sigla pero no una unidad'))
            if arbol and sigla not in esperadas:
                h.append(('SIGLA_FUERA_CATALOGO', AVISO, 'hay datos de esta sigla pero el catalogo no la tiene como departamento/metropolitana'))
            por_capa[capa] = (m, h, sin_unidad, pct_sin)
        todas_vacias = all(por_capa[c][0]['filas'] == 0 and por_capa[c][0]['filas_base'] == 0 for c in ids)
        ctx_sigla[sigla] = (sum(por_capa[c][0]['filas'] for c in ids), sum(por_capa[c][2] for c in ids))
        for capa in ids:
            m, h, sin_unidad, pct_sin = por_capa[capa]
            fila = dict(sigla=sigla, capa=capa, tabla=tablas[capa], **_fila(m, h))
            fila.update(filas_sin_unidad=sin_unidad, pct_sin_unidad=pct_sin)
            filas_siglas.append(fila)
            filas_sigla_capa[(sigla, capa)] = m['filas']
            registrar('SIGLA', dict(sigla=sigla, unidad_id='', unidad=''), capa, [x for x in h if x[0] != 'SIGLA_FUERA_CATALOGO'],
                      dict(sigla_sin_datos=''))
        if arbol and sigla not in esperadas:  # es un hecho de la sigla, no de cada capa: se informa una sola vez
            registrar('SIGLA', dict(sigla=sigla, unidad_id='', unidad=''), 'TODAS',
                      [('SIGLA_FUERA_CATALOGO', AVISO, 'hay datos de esta sigla pero el catalogo no la tiene como departamento/metropolitana')],
                      dict(sigla_sin_datos=''))
        if todas_vacias and (not arbol or sigla in esperadas):
            registrar('SIGLA', dict(sigla=sigla, unidad_id='', unidad=''), 'TODAS',
                      [('SIN_DATOS_EN_TODAS_LAS_CAPAS', FALLA, 'ninguna capa SECAD tiene filas en 2 semanas para esta sigla')],
                      dict(sigla_sin_datos=''))
            for f in filas_siglas[-len(ids):]:
                f['estado'] = FALLA
                f['codigos'] = (f['codigos'] + ' | ' if f['codigos'] else '') + 'SIN_DATOS_EN_TODAS_LAS_CAPAS'

    # ---- nivel unidad: lo que muestra el mapa al elegir cada unidad
    if arbol:
        evaluar_unidades = [(un, nivel) for un, nivel in arbol.recorrido() if un.sigla in lista and un.sigla in esperadas]
    else:
        pares = sorted({(s, c) for capa in ids for s, cods in obs.get(capa, {}).items() for c in cods
                        if c not in SIN_CODIGO and (not siglas or s in siglas)})
        evaluar_unidades = [(sc.Unidad(c, '(sin catalogo)', s), 0) for s, c in pares]

    capas_unidad = [c for c in ids if c not in solo_sigla]
    for un, nivel_arbol in (evaluar_unidades if capas_unidad else []):
        if not arbol:
            ids_pedidos = [un.id]
        elif arbol.es_sigla_completa(un.id):
            ids_pedidos = None  # el mapa pide solo la sigla, sin id_unidad
        else:
            ids_pedidos = arbol.ids_con_descendientes(un.id)
        resumen = {}
        for capa in capas_unidad:
            codigos = obs.get(capa, {}).get(un.sigla, {})
            if ids_pedidos is None:  # el mapa pide solo por sigla: entran todas las filas, incluidas las sin unidad
                serie = combinar(codigos.values())
            else:
                serie = combinar(codigos[i] for i in ids_pedidos if i in codigos)
            m = metricas(serie, v, rezago.get(capa, 0))
            h = evaluar(m, 'UNIDAD', u)
            propia = total(codigos.get(un.id, {}), v.dias).filas
            fila = dict(sigla=un.sigla, unidad_id=un.id, unidad=un.nombre, tipo=un.tipo, nivel_arbol=nivel_arbol, padre_id=un.padre,
                        consulta_mapa='sigla completa' if ids_pedidos is None else f'unidad + {len(ids_pedidos) - 1} descendientes',
                        capa=capa, tabla=tablas[capa], filas_propias=propia, **_fila(m, h))
            sigla_vacia = filas_sigla_capa.get((un.sigla, capa), 0) == 0
            fila['sigla_sin_datos'] = 'SI' if sigla_vacia else ''
            filas_unidades.append(fila)
            resumen[capa] = m['filas']
            registrar('UNIDAD', dict(sigla=un.sigla, unidad_id=un.id, unidad=un.nombre, tipo=un.tipo), capa, h,
                      dict(sigla_sin_datos=fila['sigla_sin_datos']))
        if all(f == 0 for f in resumen.values()):
            sigla_vacia = all(filas_sigla_capa.get((un.sigla, c), 0) == 0 for c in capas_unidad)
            registrar('UNIDAD', dict(sigla=un.sigla, unidad_id=un.id, unidad=un.nombre, tipo=un.tipo), 'TODAS',
                      [('SIN_DATOS_EN_TODAS_LAS_CAPAS', AVISO, 'la unidad (con sus descendientes) no tiene filas en ninguna capa SECAD en la ventana')],
                      dict(sigla_sin_datos='SI' if sigla_vacia else '', contexto=contexto_unidad(un.sigla)))
    # FALLA primero, luego siglas y unidades; el orden estable conserva sigla y arbol dentro de cada grupo
    novedades.sort(key=lambda n: (-ORDEN[n['estado']], n['nivel'] != 'SIGLA'))
    return filas_siglas, filas_unidades, novedades


def filas_diario(obs, arbol, v, tablas=None):
    tablas = tablas or {c: t for c, (t, _) in sc.CAPAS.items()}
    en_dias = {d: ('parcial' if d == v.parcial else 'ventana' if d in v.dias else 'base') for d in v.todos}
    filas = []
    for capa, por_sigla in obs.items():
        for sigla, por_codigo in sorted(por_sigla.items()):
            for codigo, serie in sorted(por_codigo.items()):
                if not arbol:
                    cat = ''
                elif codigo in SIN_CODIGO:
                    cat = 'SIN_CODIGO'
                else:
                    un = arbol.por_id.get(codigo)
                    cat = 'SI' if un and un.sigla == sigla and codigo in arbol.visibles else 'NO'
                for dia, a in sorted(serie.items()):
                    filas.append(dict(capa=capa, tabla=tablas[capa], sigla=sigla, codigo_unidad=codigo, en_catalogo=cat,
                                      dia=dia, periodo=en_dias.get(dia, ''), filas=a.filas, llamadas=a.llamadas, puntos=a.puntos,
                                      coord_invalida=a.coord_invalida, hora_invalida=a.hora_invalida, hora_futura=a.hora_futura,
                                      desfase_particion=a.desfase, ultima_hora_caso=a.ultima))
    return filas


DIAS_SEMANA = ('lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo')
MIN_MISMO_DIA_SEMANA = 3


def dias_atipicos(obs, v, capas, umbral=0.4, min_mediana=30, por_dia_semana=False, rezago=0):
    """Dias de la ventana con menos de `umbral` veces la mediana diaria nacional (ventana y ventana previa).

    `por_dia_semana`: compara cada dia con la mediana de los mismos dias de la semana (si hay al menos 3), para capas con ciclo
    semanal (SIMEP baja en fines de semana). `rezago`: ultimos dias de la ventana que no se evaluan ni entran en la mediana
    porque la fuente los registra con atraso y siempre estan incompletos (SIEDCO); un entero o {capa: dias}."""
    novedades = []
    for capa in capas:
        n_rez = rezago.get(capa, 0) if isinstance(rezago, dict) else rezago
        excluidos = set(v.dias[max(len(v.dias) - n_rez, 0):]) if n_rez > 0 else set()
        por_dia = {}
        for por_codigo in obs.get(capa, {}).values():
            for serie in por_codigo.values():
                for dia, a in serie.items():
                    if dia in v.base or dia in v.dias:
                        por_dia[dia] = por_dia.get(dia, 0) + a.filas
        valores = {d: por_dia.get(d, 0) for d in v.base + v.dias if d not in excluidos}
        mediana = statistics.median(valores.values()) if valores else 0
        for d in v.dias:
            if d in excluidos:
                continue
            n = por_dia.get(d, 0)
            referencia = f'de la mediana diaria ({round(mediana)}) de los {len(valores)} dias revisados'
            esperado = mediana
            if por_dia_semana:
                dow = date.fromisoformat(d).weekday()
                mismos = [x for k, x in valores.items() if date.fromisoformat(k).weekday() == dow]
                if len(mismos) >= MIN_MISMO_DIA_SEMANA:
                    esperado = statistics.median(mismos)
                    referencia = f'de la mediana de los {DIAS_SEMANA[dow]} ({round(esperado)}) de los {len(mismos)} revisados'
            if esperado < min_mediana or n >= umbral * esperado:
                continue
            novedades.append(dict(nivel='NACIONAL', estado=AVISO, sigla='', unidad_id='', unidad='', tipo='', capa=capa,
                                  codigo='DIA_ATIPICO',
                                  mensaje=f'el {d} tiene {n} filas en todo el pais, {round(100 * n / esperado)}% {referencia}',
                                  contexto='volumen nacional; revisar si la fuente dejo de reportar ese dia', sigla_sin_datos=''))
    return novedades


def marcar_sin_sigla(filas_siglas, novedades, sin_siglas, que='hechos', codigo='HECHOS_SIN_SIGLA', sin='sigla ni cuadrante',
                     consecuencia='no se les puede asignar unidad y el mapa, que filtra por sigla, no los muestra'):
    """Filas sin sigla (o con una sigla artefacto como 'NO HA') no son una 'sigla fuera de catalogo': el mapa, que
    filtra por sigla, no puede mostrarlas. Se reemplazan los hallazgos genericos por uno propio."""
    def mensaje(n):
        return f'{n} {que} sin {sin} en la ventana: {consecuencia}'

    for f in filas_siglas:
        if f['sigla'] in sin_siglas and f['filas']:
            f['estado'], f['codigos'], f['detalle'] = AVISO, codigo, f'{codigo}: ' + mensaje(f['filas'])
    resultado = [n for n in novedades
                 if not (n['nivel'] == 'SIGLA' and n['sigla'] in sin_siglas and n['codigo'] in ('SIGLA_FUERA_CATALOGO', 'UNIDAD_NO_ASIGNADA'))]
    for f in filas_siglas:
        if f['sigla'] in sin_siglas and f['filas']:
            resultado.append(dict(nivel='SIGLA', estado=AVISO, sigla=f['sigla'], unidad_id='', unidad='', tipo='', capa=f['capa'],
                                  codigo=codigo, mensaje=mensaje(f['filas']), contexto='', sigla_sin_datos=''))
    return resultado


# --------------------------------------------------------------------------------------
# Salida
# --------------------------------------------------------------------------------------
def escribir_csv(ruta, filas, columnas=None):
    columnas = columnas or (list(filas[0]) if filas else [])
    with open(ruta, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=columnas, extrasaction='ignore')
        w.writeheader()
        w.writerows(filas)


def imprimir(filas_siglas, filas_unidades, novedades, capas, v, max_lineas, cortos=None):
    ids = list(capas)
    cortos = cortos or CORTO
    ancho = max(15, max(len(cortos[c]) for c in ids) + 2)
    print('\n=== SIGLAS: puntos en la ventana y estado por capa (el mapa al elegir toda la sigla) ===')
    w = max([8] + [len(f['sigla']) + 2 for f in filas_siglas])
    print('SIGLA'.ljust(w) + ''.join(cortos[c].ljust(ancho) for c in ids) + 'UNIDADES CON NOVEDAD')
    por = {(f['sigla'], f['capa']): f for f in filas_siglas}
    con_novedad = {}
    for f in filas_unidades:
        if f['estado'] != OK:
            con_novedad.setdefault(f['sigla'], set()).add(f['unidad_id'])
    n_unidades = {}
    for f in filas_unidades:
        n_unidades.setdefault(f['sigla'], set()).add(f['unidad_id'])
    for sigla in sorted({f['sigla'] for f in filas_siglas}):
        celdas = ''.join(f'{por[(sigla, c)]["puntos"]} {por[(sigla, c)]["estado"]}'.ljust(ancho) for c in ids)
        print(f'{sigla:<{w}}{celdas}{len(con_novedad.get(sigla, ()))}/{len(n_unidades.get(sigla, ()))}')

    graves = [n for n in novedades if n['nivel'] == 'SIGLA' and n['estado'] == FALLA]
    if graves:
        print('\n=== FALLAS A NIVEL SIGLA ===')
        for n in graves:
            nombre = 'todas las capas' if n['capa'] == 'TODAS' else cortos[n['capa']]
            print(f'  [FALLA] {n["sigla"]} | {nombre} | {n["codigo"]}: {n["mensaje"]}')

    print('\n=== NOVEDADES POR UNIDAD (sigla > unidad; detalle completo en novedades.csv) ===')
    por_unidad, orden = {}, []
    for n in novedades:
        if n['nivel'] != 'UNIDAD' or n['sigla_sin_datos'] == 'SI':
            continue
        clave = (n['sigla'], n['unidad_id'])
        if clave not in por_unidad:
            por_unidad[clave] = dict(nombre=n['unidad'], estado=OK, partes=[])
            orden.append(clave)
        entrada = por_unidad[clave]
        entrada['estado'] = max(entrada['estado'], n['estado'], key=ORDEN.get)
        entrada['partes'].append(f'{"TODAS" if n["capa"] == "TODAS" else cortos[n["capa"]]}: {n["codigo"]}')
    omitidas = len({(n['sigla'], n['unidad_id']) for n in novedades if n['nivel'] == 'UNIDAD' and n['sigla_sin_datos'] == 'SI'})
    for k, clave in enumerate(orden):
        if k >= max_lineas:
            print(f'  ... {len(orden) - max_lineas} unidades mas en novedades.csv (--max-lineas para ver mas)')
            break
        e = por_unidad[clave]
        print(f'  [{e["estado"]}] {clave[0]} > {e["nombre"]} ({clave[1]}) | ' + '; '.join(e['partes']))
    if not orden:
        print('  (sin novedades por unidad fuera de las siglas sin datos)')
    if omitidas:
        print(f'  ({omitidas} unidades omitidas porque toda su sigla esta sin datos en esa capa; ver novedades.csv)')

    print('\n=== RESUMEN ===')
    for capa in ids:
        est = [f['estado'] for f in filas_siglas if f['capa'] == capa]
        print(f'  {cortos[capa]:<16} siglas: {est.count(FALLA)} FALLA, {est.count(AVISO)} AVISO, {est.count(OK)} OK')
    unidades = {(f['sigla'], f['unidad_id']): 0 for f in filas_unidades}
    for f in filas_unidades:
        k = (f['sigla'], f['unidad_id'])
        unidades[k] = max(unidades[k], ORDEN[f['estado']])
    sin_datos = [(n['sigla'], n['unidad_id']) for n in novedades
                 if n['nivel'] == 'UNIDAD' and n['capa'] == 'TODAS']
    print(f'  Unidades evaluadas: {len(unidades)} | con novedad: {sum(1 for x in unidades.values() if x)} '
          f'| sin datos en ninguna capa: {len(sin_datos)}')
    print(f'  Ventana {v.dias[0]} a {v.dias[-1]} vs semana previa {v.base[0]} a {v.base[-1]}'
          f'{" | hoy parcial " + v.parcial if v.parcial else ""}')


# --------------------------------------------------------------------------------------
# Programa
# --------------------------------------------------------------------------------------
def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile', help='Perfil AWS (opcional)')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA, help='Cuenta esperada; aborta si no coincide')
    p.add_argument('--database', default=sc.DATABASE)
    p.add_argument('--capas', nargs='+', choices=list(sc.CAPAS), default=list(sc.CAPAS), help='Capas a validar (todas por defecto)')
    p.add_argument('--sigla', nargs='+', help='Limita a estas siglas (ej. MEPAS MEBOG)')
    p.add_argument('--catalogo', default=sc.CATALOGO_S3, help='CSV de estructura de unidades: ruta local o s3:// (por defecto el del backend)')
    p.add_argument('--sin-catalogo', action='store_true', help='Sin catalogo: solo siglas y unidades observadas en los datos')
    p.add_argument('--ventana-dias', type=int, default=7, help='Dias completos a validar y de la semana previa (7)')
    p.add_argument('--fecha-fin', help='Ultimo dia de la ventana (YYYY-MM-DD); por defecto ayer')
    p.add_argument('--min-base', type=int, default=10, help='Filas de la semana previa para exigir datos (10)')
    p.add_argument('--caida-pct', type=float, default=50, help='Aviso de caida de puntos vs semana previa (50)')
    p.add_argument('--racha-dias', type=int, default=2, help='Dias seguidos sin filas al cierre para avisar (2)')
    p.add_argument('--max-invalidos-pct', type=float, default=1, help='Calidad: porcentaje tolerado (1)')
    p.add_argument('--col', action='append', metavar='CLAVE=COLUMNA',
                   help=f'Cambia una columna ({", ".join(sc.COLUMNAS)}, llamada); ej. --col fecha=fecha_caso')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'), help='Destino de resultados de Athena, s3://bucket/prefijo/ (o variable ATHENA_OUTPUT_S3); obligatorio si el workgroup no define uno')
    p.add_argument('--timeout', type=int, default=300, help='Segundos de espera de Athena (300)')
    p.add_argument('--solo-sql', action='store_true', help='Genera el SQL sin ejecutarlo')
    p.add_argument('--sin-esquema', action='store_true', help='No consulta Glue para validar columnas (con --solo-sql no usa AWS)')
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true', help='No escribe archivos')
    p.add_argument('--max-lineas', type=int, default=80, help='Unidades a listar en consola (80)')
    p.add_argument('--detallado', action='store_true', help='Imprime tambien el SQL')
    return p


def validar_args(p, a):
    if a.ventana_dias < 1 or a.ventana_dias > 31:
        p.error('--ventana-dias debe estar entre 1 y 31')
    if min(a.min_base, a.racha_dias) < 1:
        p.error('--min-base y --racha-dias deben ser mayores que cero')
    if not (0 <= a.caida_pct <= 100 and 0 <= a.max_invalidos_pct <= 100):
        p.error('Los porcentajes deben estar entre 0 y 100')
    a.fecha_fin_d = None
    if a.fecha_fin:
        try:
            a.fecha_fin_d = datetime.strptime(a.fecha_fin, '%Y-%m-%d').date()
        except ValueError:
            p.error('--fecha-fin debe tener formato YYYY-MM-DD')
        if a.fecha_fin_d >= sc.ahora_colombia().date():
            p.error('--fecha-fin debe ser un dia anterior a hoy (dia completo)')
    a.columnas = dict(sc.COLUMNAS)
    a.col_llamada = None
    for item in a.col or []:
        clave, _, valor = item.partition('=')
        if clave not in list(sc.COLUMNAS) + ['llamada'] or not valor.strip():
            p.error(f'--col invalido: {item}')
        if clave == 'llamada':
            a.col_llamada = valor.strip().lower()
        else:
            a.columnas[clave] = valor.strip().lower()
    if a.output_s3 and not a.output_s3.startswith('s3://'):
        p.error('--output-s3 debe comenzar por s3://')
    a.siglas = {s.upper() for s in a.sigla} if a.sigla else None


def ejecutar(a):
    ahora = sc.ahora_colombia()
    v = calcular_ventana(ahora, a.ventana_dias, a.fecha_fin_d)
    dias_sql = [date.fromisoformat(d) for d in v.todos]
    marca = ahora.strftime('%Y-%m-%d_%H-%M-%S')
    umbrales = Umbrales(a.min_base, a.caida_pct, a.racha_dias, 3, a.max_invalidos_pct)
    usa_aws = not (a.solo_sql and a.sin_esquema and (a.sin_catalogo or not a.catalogo.startswith('s3://')))
    sesion, ident = None, None
    if usa_aws:
        sesion = sc.crear_sesion(a.profile, a.region)
        ident = sc.verificar_identidad(sesion, a.cuenta)
        print(f'[INFO] Cuenta {ident["Account"]} | {ident["Arn"]} | region {a.region}')
    print(f'[INFO] Ventana {v.dias[0]} a {v.dias[-1]} | base {v.base[0]} a {v.base[-1]}'
          f'{" | hoy parcial " + v.parcial if v.parcial else ""} | dias en la consulta: {len(dias_sql)}')

    arbol = None
    if not a.sin_catalogo:
        unidades, formato = sc.cargar_catalogo(a.catalogo, sesion)
        arbol = sc.Arbol(unidades)
        print(f'[INFO] Catalogo {a.catalogo}: {len(unidades)} filas ({formato}); {len(arbol.por_id)} unidades, '
              f'{len(arbol.visibles)} visibles en el mapa, {len(arbol.siglas_con_territorio())} siglas con territorio')
    else:
        print('[AVISO] Sin catalogo: no se detectan unidades ni siglas sin ningun dato; solo las que aparecen en Athena.')

    glue = sesion.client('glue', config=sc.BOTO_CONFIG) if (sesion and not a.sin_esquema) else None
    llamada = {}
    sqls = {}
    for capa in a.capas:
        tabla = sc.CAPAS[capa][0]
        col_llamada, cols = a.col_llamada, None
        if glue:
            cols, _, _ = sc.columnas_tabla(glue, a.database, tabla)
            faltan = [f'{k}={n}' for k, n in a.columnas.items() if n not in cols]
            if faltan:
                raise sc.Blocked(f'{a.database}.{tabla} no tiene las columnas {", ".join(faltan)}. Disponibles: {", ".join(sorted(cols))}. '
                                 'Corrige con --col CLAVE=COLUMNA (python descubrir_secad.py muestra el esquema).')
            if col_llamada is None and sc.COLUMNA_LLAMADA in cols:
                col_llamada = sc.COLUMNA_LLAMADA
        llamada[capa] = col_llamada
        sqls[capa] = construir_sql(a.database, tabla, a.columnas, dias_sql, ahora, a.siglas, col_llamada, cols)
    if glue:
        print('[INFO] Esquema Glue verificado: ' + ', '.join(f'{sc.CAPAS[c][0]} (llamada: {llamada[c] or "no"})' for c in a.capas))

    salida = Path(a.salida)
    if not a.no_export:
        salida.mkdir(parents=True, exist_ok=True)
        (salida / f'consultas_secad_{marca}.sql').write_text('\n;\n'.join(f'-- {c}\n{s}' for c, s in sqls.items()), encoding='utf-8')
    if a.solo_sql or a.detallado:
        for capa, sql in sqls.items():
            print(f'\n-- {capa}\n{sql}')
    if a.solo_sql:
        print('[INFO] --solo-sql: no se ejecuto nada en Athena.')
        return 0

    athena = sc.Athena(sesion, a.database, a.workgroup, a.output_s3, a.timeout)
    print(f'[INFO] Ejecutando {len(sqls)} consulta(s) en Athena (workgroup {a.workgroup}); Ctrl+C las cancela.')
    resultados = athena.ejecutar(sqls)
    obs, stats = {}, {}
    for capa, (filas, info) in resultados.items():
        obs[capa] = parsear_filas(filas)
        escaneado = info.get('Statistics', {}).get('DataScannedInBytes', 0)
        stats[capa] = dict(grupos=len(filas), bytes_escaneados=escaneado, query_id=info.get('QueryExecutionId'))
        print(f'[INFO] {sc.CAPAS[capa][0]}: {len(filas)} grupos sigla/unidad/dia, {escaneado / 1024 ** 2:.1f} MiB escaneados')
        if not filas:
            print(f'[AVISO] {sc.CAPAS[capa][0]} no devolvio filas para la ventana; revisa particiones, siglas y columnas.')

    filas_siglas, filas_unidades, novedades = analizar(obs, arbol, v, umbrales, a.capas, a.siglas)
    imprimir(filas_siglas, filas_unidades, novedades, a.capas, v, a.max_lineas)

    if not a.no_export:
        escribir_csv(salida / f'resumen_siglas_{marca}.csv', filas_siglas)
        escribir_csv(salida / f'resumen_unidades_{marca}.csv', filas_unidades)
        escribir_csv(salida / f'novedades_{marca}.csv', novedades,
                     ['nivel', 'estado', 'sigla', 'unidad_id', 'unidad', 'tipo', 'capa', 'codigo', 'mensaje', 'contexto', 'sigla_sin_datos'])
        escribir_csv(salida / f'diario_{marca}.csv', filas_diario(obs, arbol, v))
        meta = dict(generado=ahora.isoformat(timespec='seconds'), cuenta=ident and ident['Account'], ventana=vars(v),
                    umbrales=vars(umbrales), capas={c: sc.CAPAS[c][0] for c in a.capas}, columnas=a.columnas,
                    col_llamada=llamada, catalogo=None if a.sin_catalogo else a.catalogo, consultas=stats)
        (salida / f'meta_secad_{marca}.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
        print(f'[INFO] Archivos en {salida} (sufijo {marca})')
    hay_falla = any(f['estado'] == FALLA for f in filas_siglas + filas_unidades) or any(n['estado'] == FALLA for n in novedades)
    return 1 if hay_falla else 0


def main(argv=None):
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding='utf-8', errors='replace')
        except (AttributeError, ValueError):
            pass
    p = parser()
    a = p.parse_args(argv)
    validar_args(p, a)
    try:
        return ejecutar(a)
    except sc.Blocked as exc:
        print(f'[BLOQUEADO] {exc}', file=sys.stderr)
        return 3
    except TimeoutError as exc:
        print(f'[BLOQUEADO] {exc}', file=sys.stderr)
        return 3
    except (ClientError, BotoCoreError) as exc:
        print(f'[BLOQUEADO] {sc.detalle_error(exc)}', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('\n[AVISO] Interrumpido por el usuario.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
