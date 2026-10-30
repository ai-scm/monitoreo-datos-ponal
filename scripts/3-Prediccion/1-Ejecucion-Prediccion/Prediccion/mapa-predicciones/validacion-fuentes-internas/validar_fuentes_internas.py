#!/usr/bin/env python3
"""Validacion de las fuentes internas del mapa (Gescov, MTRS, SIEDCO I/II/III, SIMEP, SIRAP, IRIS P1). Solo lectura.

Como se sirven (pon-xcrime-appServices ExternalLayersService + job pon-prod-xcrime-update-fuentes-internas):
  vistas de Athena --(job de Glue, a diario 04:30)--> un GeoJSON nacional por capa en S3 (bucket xcrime-ml-ponal-dev-mdp,
  carpeta Fuentes_externas_geojson/) --> el backend recorta por la caja del poligono de la unidad --> el front filtra por
  fecha y por poligono. A diferencia de las capas de SECAD/RNMC/antecedentes, la unidad se asigna ESPACIALMENTE.

Que valida (15 dias por defecto, contra los 15 anteriores):
  1. El archivo que lee el mapa, recorrido en streaming (un feature por linea): registros por sigla, unidad y dia, fechas
     invalidas o futuras, coordenadas fuera de Colombia, ultimo registro. Sigla y unidad salen de las propiedades de cada
     registro (id_unidad, sigla_unidad, cuadrante...): es una APROXIMACION de la asignacion espacial del mapa.
  2. Frescura: hora de ultima modificacion del archivo y corridas del job diario (y del semanal de IRIS P1).
  3. Contraste con la vista de origen en Athena, dia por dia, con los mismos filtros del job.
  4. Para GESCOV e IRIS P1, que la vista materializada de origen (fecha_actualiza_vm) se haya refrescado.
SIRAP no trae unidad: solo se evalua por sigla.

Uso:
  python validar_fuentes_internas.py --output-s3 s3://bucket/prefijo/ --profile <perfil>
  python validar_fuentes_internas.py --solo-sql --sin-catalogo
Reutiliza el motor de ../validacion-secad. Exit: 0 sin FALLA, 1 FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupcion.
"""

import argparse
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'validacion-secad'))  # motor compartido
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402
from botocore.exceptions import BotoCoreError, ClientError  # noqa: E402

BUCKET = 'xcrime-ml-ponal-dev-mdp'
CARPETA = 'Fuentes_externas_geojson'
JOB_DIARIO = 'pon-prod-xcrime-update-fuentes-internas'
JOB_SEMANAL = 'internal_layer_irisp1'
SIN_SIGLA = '(SIN_SIGLA)'
INICIO_FEATURE = b'{ "type": "Feature"'

# Copiado de pon_prod_update_fuentes_internas.py (job pon-prod-xcrime-update-fuentes-internas, 2026-08-11): clasifica cada
# registro de SIEDCO; los que no caen en ninguna subcategoria no salen en la capa. Si el job cambia, actualizar aqui.
SIEDCO_SUBCATEGORIA = """CASE
      WHEN tematica = 'CAPTURAS' AND circunstancia_captura LIKE 'ORDEN JUDICIAL%' THEN 'ORDEN JUDICIAL'
      WHEN tematica = 'CAPTURAS' AND circunstancia_captura LIKE 'FLAGRANCIA%' THEN 'FLAGRANCIA'
      WHEN tematica = 'AUTOMOTORES RECUPERADOS' THEN 'AUTOMOTORES'
      WHEN tematica = 'MOTOCICLETAS RECUPERADAS' THEN 'MOTOCICLETAS'
      WHEN tematica = 'INCAUTACION DROGA' AND clase_bien = 'COCAINA' THEN 'COCAINA'
      WHEN tematica = 'INCAUTACION DROGA' AND clase_bien = 'HEROINA' THEN 'HEROINA'
      WHEN tematica = 'INCAUTACION DROGA' AND clase_bien = 'BASE DE COCA' THEN 'BASE DE COCA'
      WHEN tematica = 'INCAUTACION DROGA' AND clase_bien = 'BASUCO' THEN 'BAZUCO'
      WHEN tematica = 'INCAUTACION DROGA' AND clase_bien = 'MARIHUANA' THEN 'MARIHUANA'
      WHEN tematica = 'INCAUTACION ARMAS DE FUEGO' AND permiso_arma = 'CON_PERMISO' THEN 'CON PERMISO'
      WHEN tematica = 'INCAUTACION ARMAS DE FUEGO' AND permiso_arma = 'SIN_PERMISO' THEN 'SIN PERMISO'
      WHEN tematica = 'MERCANCIA RECUPERADA' AND COALESCE(tipo_bien, '-') <> 'VEHICULOS' THEN 'RECUPERADA'
      WHEN tematica = 'MERCANCIA INCAUTADA' AND COALESCE(tipo_bien, '-') <> 'ARMAMENTO'
           AND COALESCE(clase_bien, '-') NOT IN ('COCAINA', 'HEROINA', 'BASE DE COCA', 'BASUCO', 'MARIHUANA') THEN 'INCAUTADA'
      WHEN tematica = 'HOMICIDIO DOLOSO O INTENCIONAL' THEN 'HOMICIDIO'
      WHEN tematica = 'LESIONES PERSONALES' THEN 'LESIONES PERSONALES'
      WHEN tematica IN ('HURTO PERSONAS', 'HURTO RESIDENCIAS', 'HURTO COMERCIO', 'HURTO AUTOMOTORES',
                        'HURTO MOTOCICLETAS', 'HURTO BANCOS', 'HURTO A SEMOVIENTES', 'HURTO CELULAR',
                        'HURTO A BICICLETAS', 'HURTO A CABLE') THEN 'HURTO'
      WHEN tematica IN ('SECUESTRO SIMPLE', 'SECUESTRO EXTORSIVO') THEN 'SECUESTRO'
      WHEN tematica = 'DELITOS SEXUALES' THEN 'DELITOS SEXUALES'
      WHEN tematica = 'EXTORSION' THEN 'EXTORSION'
      WHEN tematica = 'HURTO PIRATERIA' THEN 'PIRATERIA'
      WHEN tematica = 'TERRORISMO' THEN 'TERRORISMO'
      WHEN tematica = 'VIOLENCIA INTRAFAMILIAR' THEN 'VIOLENCIA INTRAFAMILIAR'
      WHEN tematica = 'ACCIONES SUBVERSIVAS' THEN 'ACCIONES SUBVERSIVAS'
    END"""
# Turnos operativos por hora del hecho (los mismos del job): I 22:00-06:59, II 07:00-13:59, III 14:00-21:59.
SIEDCO_TURNOS = ('(hora_24 >= 22 OR hora_24 < 7)', 'hora_24 BETWEEN 7 AND 13', 'hora_24 BETWEEN 14 AND 21')
VISTA_SIEDCO = '"pon_prod_raw_siedco_delitos_temporal"."vw_delitos_corregido_schema_temporal"'

# Orden pedido: Gescov, MTRS, SIEDCO (los tres turnos), SIMEP, SIRAP, IRIS P1. `unidad` = (modo, propiedad): 'id' si la
# propiedad es el id_unidad del catalogo; 'cuadrante' si es un cuadrante que se traduce con CUADRANTES_ID; None si no hay.
# `atraso`: dias maximos entre hoy y el registro mas reciente. `vm`: la vista de origen es una vista materializada con
# fecha_actualiza_vm. `serie`: capa con flujo diario (se revisan los dias atipicos a nivel nacional).
CAPAS = {
    'gescov': dict(nombre='Gescov', corto='Gescov', key=f'{CARPETA}/Gescov/Gescov.geojson', fecha='fecha_inicio_plan', hora=None,
                   sigla='sigla_unidad', unidad=('id', 'id_unidad'), atraso=3, serie=True,
                   vista='"pon-prod-raw-disec"."usr_disec.v_evos_planes"', col_fecha='fecha_inicio_plan',
                   filtro="estado_cumplimiento = 'Cumple'", vm='"pon-prod-raw-disec"."usr_disec.v_evos_planes"'),
    'mtrs': dict(nombre='MTRS', corto='MTRS', key=f'{CARPETA}/Mtrs_riesgo_ubicacion/v_mtrs_riesgo_ubicacion.geojson',
                 fecha='fecha_inicio_mesa_tecnica', hora=None, sigla='sigla', unidad=('cuadrante', 'cuadrante'), atraso=30, serie=False,
                 vista='"pon-prod-raw-disec"."usr_disec.v_mtrs_riesgo_ubicacion"', col_fecha='fecha_inicio_mesa_tecnica', filtro=None, vm=None),
    'siedco-i': dict(nombre='SIEDCO I', corto='SIEDCO I', key=f'{CARPETA}/Disec/DisecDelitosTurno1.geojson', fecha='fecha_hecho', hora='hora_hecho',
                     sigla='sigla_undepertenece', unidad=('cuadrante', 'juris.cai'), atraso=3, serie=True, turno=0),
    'siedco-ii': dict(nombre='SIEDCO II', corto='SIEDCO II', key=f'{CARPETA}/Disec/DisecDelitosTurno2.geojson', fecha='fecha_hecho', hora='hora_hecho',
                      sigla='sigla_undepertenece', unidad=('cuadrante', 'juris.cai'), atraso=3, serie=True, turno=1),
    'siedco-iii': dict(nombre='SIEDCO III', corto='SIEDCO III', key=f'{CARPETA}/Disec/DisecDelitosTurno3.geojson', fecha='fecha_hecho', hora='hora_hecho',
                       sigla='sigla_undepertenece', unidad=('cuadrante', 'juris.cai'), atraso=3, serie=True, turno=2),
    'simep': dict(nombre='SIMEP', corto='SIMEP', key=f'{CARPETA}/Simep/Simep.geojson', fecha='fecha_creacion', hora=None, sigla=None,
                  unidad=('id', 'id_unidad'), atraso=3, serie=True, vista='"pon-prod-raw-disec"."simep_datac"', col_fecha='fecha_creacion',
                  filtro='fecha_creacion <= current_date', vm=None),
    'sirap': dict(nombre='SIRAP', corto='SIRAP', key=f'{CARPETA}/Sirap/raw_geopol_usr_pmu_servicios_extra.geojson', fecha='fecha_inicio', hora=None,
                  sigla='unidad', unidad=(None, None), atraso=3, serie=True, vista='"pon-prod-raw-geopol"."raw_geopol_usr_pmu_actividades"',
                  col_fecha='fecha_inicio', filtro=None, vm=None),
    'iris-p1': dict(nombre='IRIS P1', corto='IRIS P1', key=f'{CARPETA}/General_iris/IrisP1.geojson', fecha='fecha_creacion_irisp1', hora=None,
                    sigla='sigla_unidad', unidad=('cuadrante', 'nro_cuadrante'), atraso=30, serie=False,
                    vista='"pon-prod-raw-disec"."usr_disec.v_general_irisp"', col_fecha='fecha_creacion_irisp1',
                    filtro="estado_existencia != 'No existe'", vm='"pon-prod-raw-disec"."usr_disec.v_general_irisp"'),
}
SOLO_SIGLA = {c for c, d in CAPAS.items() if d['unidad'][0] is None}
TABLAS = {c: d['key'].rsplit('/', 1)[1] for c, d in CAPAS.items()}
CORTOS = {c: d['corto'] for c, d in CAPAS.items()}
RANGO_NIVEL = {'CARGA': 0, 'NACIONAL': 1, 'SIGLA': 2, 'UNIDAD': 3}


def novedad(nivel, estado, capa, codigo, mensaje, contexto=''):
    return dict(nivel=nivel, estado=estado, sigla='', unidad_id='', unidad='', tipo='', capa=capa, codigo=codigo,
                mensaje=mensaje, contexto=contexto, sigla_sin_datos='')


# --------------------------------------------------------------------------------------
# SQL de la fuente (lo que el job lee de Athena)
# --------------------------------------------------------------------------------------
COORDS_OK = 'TRY_CAST(latitud AS double) IS NOT NULL AND TRY_CAST(longitud AS double) IS NOT NULL'


def construir_sql_fuente(capa, primer_dia, ultimo_dia):
    """Registros por dia de la fecha de la capa en la vista de origen, con los filtros del job y solo con coordenadas
    numericas (el job descarta las demas). Es lo que deberia tener el archivo."""
    c = CAPAS[capa]
    if 'turno' in c:
        return f"""SELECT CAST(fecha_hecho AS varchar) AS dia, COUNT(*) AS filas
FROM (
  SELECT fecha_hecho, latitud, longitud,
    {SIEDCO_SUBCATEGORIA} AS subcategoria
  FROM {VISTA_SIEDCO}
  WHERE {SIEDCO_TURNOS[c['turno']]} AND fecha_hecho >= DATE '{primer_dia}' AND fecha_hecho <= DATE '{ultimo_dia}'
)
WHERE subcategoria IS NOT NULL AND latitud IS NOT NULL AND longitud IS NOT NULL
GROUP BY 1
ORDER BY 1
"""
    dia = f'CAST({c["col_fecha"]} AS date)'
    filtro = f'AND {c["filtro"]}\n  ' if c['filtro'] else ''
    return f"""SELECT CAST({dia} AS varchar) AS dia, COUNT(*) AS filas
FROM {c['vista']}
WHERE {dia} >= DATE '{primer_dia}' AND {dia} <= DATE '{ultimo_dia}'
  {filtro}AND {COORDS_OK}
GROUP BY 1
ORDER BY 1
"""


def construir_sql_vm(capa):
    return f"""SELECT date_format(MAX(fecha_actualiza_vm), '%Y-%m-%d %H:%i:%s') AS max_vm
FROM {CAPAS[capa]['vm']}
"""


# --------------------------------------------------------------------------------------
# Recorrido del archivo GeoJSON (un feature por linea)
# --------------------------------------------------------------------------------------
class Contexto:
    def __init__(self, dias_interes, hoy, cuadrantes=None, arbol=None, siglas=None):
        self.dias_interes = set(dias_interes)
        self.hoy = hoy
        self.limite_futuro = (hoy + timedelta(days=1)).isoformat()
        self.cuadrantes = cuadrantes or {}
        self.arbol = arbol
        self.siglas = siglas


class Escaneo:
    def __init__(self):
        self.total = self.sin_fecha = self.absurdas = self.futuras = self.malas = 0
        self.max_fecha = self.min_fecha = ''
        self.por_dia = {}
        self.obs = {}
        self.sin_unidad = 0


def _regex_fecha(campo):
    return re.compile(rb'"' + re.escape(campo.encode()) + rb'"\s*:\s*"(\d{4})-(\d{2})-(\d{2})')


def _codigo_unidad(cfg, props, ctx):
    modo, prop = cfg['unidad']
    valor = props.get(prop) if prop else None
    if modo is None or valor is None:
        return ''
    texto = str(valor).strip()
    if modo == 'id':
        return sc.normalizar_id(texto) or texto
    return ctx.cuadrantes.get(texto, '')


def escanear(lineas, cfg, ctx):
    """lineas: iterable de bytes (una por linea del GeoJSON). Solo se parsea el JSON de los registros cuyo dia interesa."""
    r = Escaneo()
    rx = _regex_fecha(cfg['fecha'])
    for linea in lineas:
        if not linea.startswith(INICIO_FEATURE):
            continue
        r.total += 1
        m = rx.search(linea)
        if not m:
            r.sin_fecha += 1
            continue
        dia = f'{m.group(1).decode()}-{m.group(2).decode()}-{m.group(3).decode()}'
        if dia < '2000-01-01':
            r.absurdas += 1
            continue
        if dia > ctx.limite_futuro:
            r.futuras += 1
            continue
        if not r.max_fecha or dia > r.max_fecha:
            r.max_fecha = dia
        if not r.min_fecha or dia < r.min_fecha:
            r.min_fecha = dia
        if dia not in ctx.dias_interes:
            continue
        try:
            f = json.loads(linea.rstrip().rstrip(b','))
        except ValueError:
            r.malas += 1
            continue
        props = f.get('properties') or {}
        sigla = str(props.get(cfg['sigla']) or '').strip().upper() if cfg['sigla'] else ''
        codigo = _codigo_unidad(cfg, props, ctx)
        if not sigla and codigo and ctx.arbol and codigo in ctx.arbol.por_id:
            sigla = ctx.arbol.por_id[codigo].sigla
        sigla = sigla or SIN_SIGLA
        if ctx.siglas and sigla not in ctx.siglas:
            continue
        xy = (f.get('geometry') or {}).get('coordinates')
        valido = (isinstance(xy, list) and len(xy) >= 2 and isinstance(xy[0], (int, float)) and isinstance(xy[1], (int, float))
                  and sc.LON_MIN <= xy[0] <= sc.LON_MAX and sc.LAT_MIN <= xy[1] <= sc.LAT_MAX)
        hora = str(props.get(cfg['hora']) or '') if cfg['hora'] else ''
        acum = vs.Acum(filas=1, puntos=int(valido), coord_invalida=int(not valido), ultima=f'{dia} {hora}'.strip())
        r.obs.setdefault(sigla, {}).setdefault(codigo, {}).setdefault(dia, vs.Acum()).sumar(acum)
        r.por_dia[dia] = r.por_dia.get(dia, 0) + 1
        if not codigo:
            r.sin_unidad += 1
    return r


# --------------------------------------------------------------------------------------
# Evaluaciones de la capa, del archivo y de los jobs
# --------------------------------------------------------------------------------------
def comparar_con_fuente(por_dia_archivo, por_dia_fuente, dias, tol_pct=3.0, tol_abs=3):
    """[(dia, archivo, fuente)] de los dias donde el archivo difiere de la vista de origen mas que la tolerancia."""
    difieren = []
    for d in dias:
        a, f = por_dia_archivo.get(d, 0), por_dia_fuente.get(d, 0)
        if abs(f - a) > max(tol_abs, f * tol_pct / 100):
            difieren.append((d, a, f))
    return difieren


def evaluar_capa(capa, esc, meta, fuente_por_dia, vm, v, ahora, max_horas_archivo=30, max_dias_vm=3):
    """Hallazgos de una capa a partir del recorrido del archivo, su fecha de modificacion y la vista de origen."""
    cfg = CAPAS[capa]
    nov = []
    ahora_utc = ahora.replace(tzinfo=sc.UTC_MENOS_5).astimezone(timezone.utc)
    nombre = TABLAS[capa]
    if meta:
        horas = (ahora_utc - meta['modificado']).total_seconds() / 3600
        if horas > max_horas_archivo:
            nov.append(novedad('CARGA', vs.FALLA, capa, 'ARCHIVO_DESACTUALIZADO',
                               f'{nombre} se modifico por ultima vez hace {horas:.0f} h (limite {max_horas_archivo} h): el job diario no lo esta regenerando',
                               f'{meta["modificado"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M}'))
    if esc is None:
        return nov
    en_ventana = sum(esc.por_dia.get(d, 0) for d in v.dias)
    hoy = ahora.date()
    edad = (hoy - date.fromisoformat(esc.max_fecha)).days if esc.max_fecha else None
    causa = ''
    if vm and vm['dias'] is not None and vm['dias'] > max_dias_vm:
        causa = f' La vista de origen es una vista materializada que no se refresca desde {vm["max_vm"][:10]} (hace {vm["dias"]} dias).'
    if esc.total == 0:
        nov.append(novedad('NACIONAL', vs.FALLA, capa, 'ARCHIVO_VACIO', f'{nombre} no tiene ningun registro.' + causa))
    elif en_ventana == 0:
        ultimo = f'; el registro mas reciente es del {esc.max_fecha} (hace {edad} dias)' if esc.max_fecha else ''
        nov.append(novedad('NACIONAL', vs.FALLA, capa, 'CAPA_SIN_REGISTROS_EN_VENTANA',
                           f'{nombre} no tiene registros en {v.dias[0]} a {v.dias[-1]}{ultimo}. El mapa no muestra nada de esta fuente en la ventana.' + causa,
                           f'{esc.total} registros en total ({esc.min_fecha} a {esc.max_fecha})'))
    elif edad is not None and edad > cfg['atraso']:
        nov.append(novedad('NACIONAL', vs.FALLA, capa, 'ULTIMO_REGISTRO_VIEJO',
                           f'El registro mas reciente de {nombre} es del {esc.max_fecha}, hace {edad} dias (limite {cfg["atraso"]}).' + causa))
    elif causa:
        nov.append(novedad('CARGA', vs.FALLA, capa, 'FUENTE_VM_SIN_REFRESCAR', causa.strip()))
    if esc.sin_fecha or esc.absurdas:
        n = esc.sin_fecha + esc.absurdas
        nov.append(novedad('NACIONAL', vs.AVISO, capa, 'FECHAS_INVALIDAS',
                           f'{n} de {esc.total} registros ({round(100 * n / esc.total, 2)}%) no traen fecha ({esc.sin_fecha}) o la traen antes del 2000 '
                           f'({esc.absurdas}) en {cfg["fecha"]}: con un filtro de fechas el mapa no los muestra'))
    if esc.futuras:
        nov.append(novedad('NACIONAL', vs.AVISO, capa, 'FECHAS_FUTURAS',
                           f'{esc.futuras} registros con {cfg["fecha"]} posterior a manana: no entran en ninguna ventana hasta que llegue la fecha'))
    if fuente_por_dia is not None and esc.total:
        dias = [d for d in v.dias if d <= (hoy - timedelta(days=1)).isoformat()]
        dif = comparar_con_fuente(esc.por_dia, fuente_por_dia, dias)
        if dif:
            grave = any(a < f * 0.9 for _, a, f in dif)
            detalle = '; '.join(f'{d}: archivo {a} vs fuente {f}' for d, a, f in dif[:5]) + ('; ...' if len(dif) > 5 else '')
            nov.append(novedad('CARGA', vs.FALLA if grave else vs.AVISO, capa, 'ARCHIVO_DIFIERE_DE_LA_FUENTE',
                               f'{len(dif)} dia(s) de la ventana donde {nombre} no coincide con su vista de origen en Athena ({detalle})'
                               + ('; faltan registros en el archivo' if grave else ''), 'tolerancia: 3 % o 3 registros'))
    return nov


def evaluar_jobs(corridas_diario, corridas_semanal, dias, ahora, max_horas=30, max_dias_semanal=9):
    """Salud del job diario que regenera los archivos y del semanal de IRIS P1."""
    nov, res = [], {}
    ahora_utc = ahora.replace(tzinfo=sc.UTC_MENOS_5).astimezone(timezone.utc)
    if corridas_diario is not None:
        term = [r for r in corridas_diario if r.get('JobRunState') in sc.TERMINALES]
        exitos = sorted((r for r in term if r['JobRunState'] == 'SUCCEEDED'), key=lambda r: r['StartedOn'])
        fallos = sorted((r for r in term if r['JobRunState'] != 'SUCCEEDED'), key=lambda r: r['StartedOn'])
        con_exito = {r['StartedOn'].astimezone(sc.UTC_MENOS_5).date().isoformat() for r in exitos}
        hoy_c = ahora.date()
        revisar = [d for d in dias + [hoy_c.isoformat()] if d <= hoy_c.isoformat()]
        sin_corrida = [d for d in revisar if d not in con_exito and not (d == hoy_c.isoformat() and ahora.hour < 6)]
        horas = (ahora_utc - exitos[-1]['StartedOn']).total_seconds() / 3600 if exitos else None
        res['diario'] = dict(exitos=len(exitos), fallidas=len(fallos), sin_corrida=len(sin_corrida), ultima_hace_h=None if horas is None else round(horas, 1))
        err = f' Ultimo error: {(fallos[-1].get("ErrorMessage") or "sin detalle")[:140]}' if fallos else ''
        if horas is None or horas > max_horas:
            nov.append(novedad('CARGA', vs.FALLA, 'TODAS', 'JOB_FUENTES_INTERNAS_VENCIDO',
                               f'{JOB_DIARIO} ' + ('no tiene corridas exitosas en el historial leido' if horas is None else f'tuvo su ultima corrida exitosa hace {horas:.0f} h (limite {max_horas} h)')
                               + f': los archivos de las fuentes internas no se estan regenerando.{err}', f'job {JOB_DIARIO}'))
        elif sin_corrida:
            nov.append(novedad('CARGA', vs.AVISO, 'TODAS', 'JOB_FUENTES_INTERNAS_DIAS_SIN_CORRIDA',
                               f'{JOB_DIARIO} no tuvo corrida exitosa los dias {", ".join(sin_corrida)}; cada corrida regenera todo desde las vistas, '
                               f'asi que la siguiente exitosa recupera esos dias.{err}', f'job {JOB_DIARIO}'))
    if corridas_semanal is not None:
        exitos = sorted((r for r in corridas_semanal if r.get('JobRunState') == 'SUCCEEDED'), key=lambda r: r['StartedOn'])
        dias_s = (ahora_utc - exitos[-1]['StartedOn']).total_seconds() / 86400 if exitos else None
        res['semanal'] = dict(exitos=len(exitos), ultima_hace_dias=None if dias_s is None else round(dias_s, 1))
        if dias_s is None or dias_s > max_dias_semanal:
            nov.append(novedad('CARGA', vs.AVISO, 'iris-p1', 'JOB_SEMANAL_IRIS_VENCIDO',
                               f'{JOB_SEMANAL} (reescribe IrisP1.geojson y Simep.geojson cada domingo) '
                               + ('no tiene corridas exitosas' if dias_s is None else f'corrio por ultima vez hace {dias_s:.0f} dias'), f'job {JOB_SEMANAL}'))
    return nov, res


def marcar_sin_sigla(filas_siglas, novedades):
    return vs.marcar_sin_sigla(filas_siglas, novedades, {SIN_SIGLA}, 'registros', 'REGISTROS_SIN_SIGLA', 'sigla',
                               'el mapa los ubica por coordenada, pero no entran en la validacion por sigla ni unidad')


# --------------------------------------------------------------------------------------
# Salida y programa
# --------------------------------------------------------------------------------------
def ordenar(novedades):
    novedades.sort(key=lambda n: (-vs.ORDEN[n['estado']], RANGO_NIVEL.get(n['nivel'], 9)))
    return novedades


def imprimir_archivos(filas):
    print('\n=== ARCHIVOS PUBLICADOS (lo que lee el mapa) ===')
    print(f'{"CAPA":<12}{"MiB":>7}  {"MODIFICADO":<17}{"REGISTROS":>10}{"EN VENTANA":>11}  {"ULTIMO REGISTRO":<26}{"VS FUENTE":<12}')
    for f in filas:
        print(f'{f["capa_nombre"]:<12}{f["mib"]:>7}  {f["modificado"]:<17}{f["registros"]:>10}{f["en_ventana"]:>11}  {f["ultimo_registro"]:<26}{f["vs_fuente"]:<12}')


def imprimir_extra(extra, res_jobs):
    print('\n=== JOBS QUE GENERAN LOS ARCHIVOS ===')
    if not res_jobs:
        print('  (no evaluados)')
    if 'diario' in res_jobs:
        d = res_jobs['diario']
        print(f'  {JOB_DIARIO}: corridas exitosas {d["exitos"]} | fallidas {d["fallidas"]} | dias sin corrida exitosa {d["sin_corrida"]} | '
              f'ultima exitosa hace {d["ultima_hace_h"]} h')
    if 'semanal' in res_jobs:
        s = res_jobs['semanal']
        print(f'  {JOB_SEMANAL}: corridas exitosas {s["exitos"]} | ultima exitosa hace {s["ultima_hace_dias"]} dias')
    print('\n=== HALLAZGOS POR CAPA Y NACIONALES ===')
    graves = [x for x in extra if x['nivel'] in ('CARGA', 'NACIONAL')]
    for n in graves:
        capa = 'todas' if n['capa'] == 'TODAS' else CORTOS.get(n['capa'], n['capa'])
        print(f'  [{n["estado"]}] {capa} | {n["codigo"]}: {n["mensaje"]}')
    if not graves:
        print('  (ninguno)')


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile', help='Perfil AWS (opcional)')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA, help='Cuenta esperada; aborta si no coincide')
    p.add_argument('--capas', nargs='+', choices=list(CAPAS), default=list(CAPAS), help='Capas a validar (todas por defecto)')
    p.add_argument('--sigla', nargs='+', help='Limita el analisis por sigla/unidad a estas siglas (ej. MEPAS MEBOG)')
    p.add_argument('--catalogo', default=sc.CATALOGO_S3, help='CSV de estructura de unidades: ruta local o s3:// (por defecto el del backend)')
    p.add_argument('--sin-catalogo', action='store_true', help='Sin catalogo: solo siglas y unidades observadas')
    p.add_argument('--bucket', default=BUCKET, help=f'Bucket de los GeoJSON ({BUCKET})')
    p.add_argument('--ventana-dias', type=int, default=15, help='Dias completos a validar y de la ventana previa (15)')
    p.add_argument('--fecha-fin', help='Ultimo dia de la ventana (YYYY-MM-DD); por defecto ayer')
    p.add_argument('--min-base', type=int, default=20, help='Registros de la ventana previa para exigir datos (20)')
    p.add_argument('--caida-pct', type=float, default=50, help='Aviso de caida de puntos vs ventana previa (50)')
    p.add_argument('--racha-dias', type=int, default=2, help='Dias seguidos sin registros al cierre para avisar (2)')
    p.add_argument('--min-esperado-falla', type=float, default=5,
                   help='Registros esperados en una racha sin datos para que una sigla sea FALLA y no AVISO (5)')
    p.add_argument('--max-invalidos-pct', type=float, default=1, help='Calidad: porcentaje tolerado (1)')
    p.add_argument('--dia-atipico-pct', type=float, default=40, help='Dia nacional atipico si es menor a este %% de la mediana del mismo dia de la semana (40)')
    p.add_argument('--rezago-siedco', type=int, default=2,
                   help='Ultimos dias de la ventana que SIEDCO registra con atraso y no se evaluan como racha ni dia atipico (2; 0 los evalua)')
    p.add_argument('--max-horas-archivo', type=float, default=30, help='Horas maximas desde la ultima modificacion de un archivo (30)')
    p.add_argument('--max-dias-vm', type=float, default=3, help='Dias maximos sin refrescar la vista materializada de origen (3)')
    p.add_argument('--hilos', type=int, default=4, help='Archivos que se recorren a la vez (4)')
    p.add_argument('--sin-archivos', action='store_true', help='No descargar los GeoJSON: solo fuentes de Athena, fechas de archivo y jobs')
    p.add_argument('--sin-fuente', action='store_true', help='No consultar las vistas de origen en Athena')
    p.add_argument('--sin-jobs', action='store_true', help='No leer el historial de los jobs de Glue')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'),
                   help='Destino de resultados de Athena, s3://bucket/prefijo/ (o variable ATHENA_OUTPUT_S3); obligatorio si el workgroup no define uno')
    p.add_argument('--timeout', type=int, default=600, help='Segundos de espera de Athena (600)')
    p.add_argument('--solo-sql', action='store_true', help='Genera el SQL sin ejecutarlo')
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true', help='No escribe archivos')
    p.add_argument('--max-lineas', type=int, default=60, help='Unidades a listar en consola (60)')
    p.add_argument('--detallado', action='store_true', help='Imprime tambien el SQL')
    return p


def validar_args(p, a):
    if not 1 <= a.ventana_dias <= 31:
        p.error('--ventana-dias debe estar entre 1 y 31')
    if not 0 <= a.rezago_siedco < a.ventana_dias:
        p.error('--rezago-siedco debe ser 0 o menor que --ventana-dias')
    if min(a.min_base, a.racha_dias, a.min_esperado_falla, a.max_horas_archivo, a.max_dias_vm, a.hilos) <= 0:
        p.error('--min-base, --racha-dias, --min-esperado-falla, --max-horas-archivo, --max-dias-vm e --hilos deben ser mayores que cero')
    if not all(0 <= x <= 100 for x in (a.caida_pct, a.max_invalidos_pct, a.dia_atipico_pct)):
        p.error('Los porcentajes deben estar entre 0 y 100')
    a.fecha_fin_d = None
    if a.fecha_fin:
        try:
            a.fecha_fin_d = datetime.strptime(a.fecha_fin, '%Y-%m-%d').date()
        except ValueError:
            p.error('--fecha-fin debe tener formato YYYY-MM-DD')
        if a.fecha_fin_d >= sc.ahora_colombia().date():
            p.error('--fecha-fin debe ser un dia anterior a hoy (dia completo)')
    if a.output_s3 and not a.output_s3.startswith('s3://'):
        p.error('--output-s3 debe comenzar por s3://')
    a.siglas = {s.upper() for s in a.sigla} if a.sigla else None


def _fila_archivo(capa, esc, meta, fuente, v, hoy):
    ult = f'{esc.max_fecha} (hace {(hoy - date.fromisoformat(esc.max_fecha)).days} d)' if esc and esc.max_fecha else 'n/d'
    if fuente is None:
        vs_fuente = 'sin consultar'
    elif esc is None:
        vs_fuente = 'n/d'
    else:
        dif = comparar_con_fuente(esc.por_dia, fuente, [d for d in v.dias if d <= (hoy - timedelta(days=1)).isoformat()])
        vs_fuente = f'{len(dif)} dia(s) difieren' if dif else 'coincide'
    return dict(capa=capa, capa_nombre=CAPAS[capa]['nombre'], archivo=TABLAS[capa], mib=round(meta['bytes'] / 1024 ** 2, 1) if meta else '',
                modificado=f'{meta["modificado"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M}' if meta else 'n/d',
                registros=esc.total if esc else '', en_ventana=sum(esc.por_dia.get(d, 0) for d in v.dias) if esc else '',
                ultimo_registro=ult, sin_fecha=esc.sin_fecha if esc else '', fechas_antes_2000=esc.absurdas if esc else '',
                fechas_futuras=esc.futuras if esc else '', vs_fuente=vs_fuente)


def ejecutar(a):
    ahora = sc.ahora_colombia()
    v = vs.calcular_ventana(ahora, a.ventana_dias, a.fecha_fin_d)
    primer_dia, ultimo_dia = date.fromisoformat(v.todos[0]), date.fromisoformat(v.todos[-1])
    marca = ahora.strftime('%Y-%m-%d_%H-%M-%S')
    umbrales = vs.Umbrales(a.min_base, a.caida_pct, a.racha_dias, 3, a.max_invalidos_pct, a.min_esperado_falla)
    capas_con_vista = [c for c in a.capas if 'vista' in CAPAS[c] or 'turno' in CAPAS[c]]
    sqls = {} if a.sin_fuente else {c: construir_sql_fuente(c, primer_dia, ultimo_dia) for c in capas_con_vista}
    if not a.sin_fuente:
        sqls.update({f'_vm_{c}': construir_sql_vm(c) for c in a.capas if CAPAS[c].get('vm')})
    salida = Path(a.salida)
    if not a.no_export:
        salida.mkdir(parents=True, exist_ok=True)
        (salida / f'consultas_fuentes_internas_{marca}.sql').write_text('\n;\n'.join(f'-- {c}\n{s}' for c, s in sqls.items()), encoding='utf-8')
    print(f'[INFO] Ventana {v.dias[0]} a {v.dias[-1]} | previa {v.base[0]} a {v.base[-1]}{" | hoy parcial " + v.parcial if v.parcial else ""}')
    if a.solo_sql:
        for capa, sql in sqls.items():
            print(f'\n-- {capa}\n{sql}')
        print('[INFO] --solo-sql: no se ejecuto nada.')
        return 0

    sesion = sc.crear_sesion(a.profile, a.region)
    ident = sc.verificar_identidad(sesion, a.cuenta)
    print(f'[INFO] Cuenta {ident["Account"]} | {ident["Arn"]} | region {a.region}')
    arbol, cuadrantes = None, {}
    if not a.sin_catalogo:
        unidades, formato = sc.cargar_catalogo(a.catalogo, sesion)
        arbol = sc.Arbol(unidades)
        cuadrantes = arbol.cuadrante_a_unidad()
        print(f'[INFO] Catalogo {a.catalogo}: {len(unidades)} filas ({formato}); {len(arbol.visibles)} unidades visibles, '
              f'{len(arbol.siglas_con_territorio())} siglas con territorio, {len(cuadrantes)} cuadrantes')
    else:
        print('[AVISO] Sin catalogo: no se detectan unidades ni siglas sin ningun dato; solo las observadas.')
    if a.detallado:
        for capa, sql in sqls.items():
            print(f'\n-- {capa}\n{sql}')

    # 1) vistas de origen (rapido)
    fuente, vm = {}, {}
    if sqls:
        athena = sc.Athena(sesion, 'pon-prod-raw-disec', a.workgroup, a.output_s3, a.timeout)
        print(f'[INFO] Consultando {len(sqls)} vista(s) de origen en Athena (workgroup {a.workgroup}); Ctrl+C las cancela.')
        for nombre, (filas, info) in athena.ejecutar(sqls).items():
            if nombre.startswith('_vm_'):
                capa = nombre[4:]
                max_vm = (filas[0].get('max_vm') if filas else None) or ''
                vm[capa] = dict(max_vm=max_vm, dias=(ahora - datetime.fromisoformat(max_vm)).days if max_vm else None)
            else:
                fuente[nombre] = {f['dia']: int(f['filas']) for f in filas}

    # 2) jobs
    c_diario = c_semanal = None
    if not a.sin_jobs:
        try:
            glue = sesion.client('glue', config=sc.BOTO_CONFIG)
            c_diario = sc.leer_corridas(glue, JOB_DIARIO, date.fromisoformat(v.dias[0]))
            c_semanal = sc.leer_corridas(glue, JOB_SEMANAL, date.fromisoformat(v.dias[0]) - timedelta(days=14)) if 'iris-p1' in a.capas else None
            print(f'[INFO] Historial de {JOB_DIARIO}: {len(c_diario)} corridas')
        except (ClientError, BotoCoreError) as exc:
            print(f'[AVISO] No se pudo leer el historial de los jobs ({sc.detalle_error(exc)}); queda sin evaluar.')

    # 3) archivos publicados
    s3 = sesion.client('s3', config=sc.BOTO_CONFIG)
    metas, escaneos = {}, {}
    for capa in a.capas:
        try:
            h = s3.head_object(Bucket=a.bucket, Key=CAPAS[capa]['key'])
            metas[capa] = dict(bytes=h['ContentLength'], modificado=h['LastModified'])
        except (ClientError, BotoCoreError) as exc:
            print(f'[AVISO] No se pudo leer {CAPAS[capa]["key"]} ({sc.detalle_error(exc)}).')
    if not a.sin_archivos:
        ctx = Contexto(v.todos, ahora.date(), cuadrantes, arbol, a.siglas)
        total_mib = sum(m['bytes'] for m in metas.values()) / 1024 ** 2
        print(f'[INFO] Recorriendo {len(metas)} archivo(s), {total_mib:.0f} MiB, {a.hilos} a la vez (unos minutos)...', flush=True)

        def tarea(capa):
            cuerpo = s3.get_object(Bucket=a.bucket, Key=CAPAS[capa]['key'])['Body']
            return capa, escanear(cuerpo.iter_lines(chunk_size=1 << 20), CAPAS[capa], ctx)

        with ThreadPoolExecutor(max_workers=a.hilos) as pool:
            for capa, esc in pool.map(tarea, [c for c in a.capas if c in metas]):
                escaneos[capa] = esc
                print(f'[INFO] {CAPAS[capa]["nombre"]}: {esc.total} registros, {sum(esc.por_dia.get(d, 0) for d in v.dias)} en la ventana, '
                      f'ultimo {esc.max_fecha or "n/d"}', flush=True)

    # 4) analisis
    obs = {c: e.obs for c, e in escaneos.items()}
    capas_datos = [c for c in a.capas if c in escaneos]
    rezago = {c: a.rezago_siedco for c in CAPAS if 'turno' in CAPAS[c]}
    filas_siglas, filas_unidades, novedades = [], [], []
    if capas_datos:
        filas_siglas, filas_unidades, novedades = vs.analizar(obs, arbol, v, umbrales, capas_datos, a.siglas, TABLAS,
                                                              solo_sigla=SOLO_SIGLA & set(capas_datos), avisar_sin_unidad=False,
                                                              rezago=rezago)
        novedades = marcar_sin_sigla(filas_siglas, novedades)
    extra, filas_archivos = [], []
    hoy = ahora.date()
    for capa in a.capas:
        esc = escaneos.get(capa)
        extra += evaluar_capa(capa, esc, metas.get(capa), fuente.get(capa), vm.get(capa), v, ahora, a.max_horas_archivo, a.max_dias_vm)
        filas_archivos.append(_fila_archivo(capa, esc, metas.get(capa), fuente.get(capa), v, hoy))
    extra += vs.dias_atipicos(obs, v, [c for c in capas_datos if CAPAS[c]['serie']], a.dia_atipico_pct / 100,
                              por_dia_semana=True, rezago=rezago)
    carga, res_jobs = evaluar_jobs(c_diario, c_semanal, v.dias, ahora)
    extra += carga
    todas = ordenar(extra + novedades)

    if capas_datos:
        vs.imprimir(filas_siglas, filas_unidades, novedades, capas_datos, v, a.max_lineas, CORTOS)
    imprimir_archivos(filas_archivos)
    imprimir_extra(extra, res_jobs)

    if not a.no_export:
        if capas_datos:
            vs.escribir_csv(salida / f'resumen_siglas_{marca}.csv', filas_siglas)
            vs.escribir_csv(salida / f'resumen_unidades_{marca}.csv', filas_unidades)
            vs.escribir_csv(salida / f'diario_{marca}.csv', vs.filas_diario(obs, arbol, v, TABLAS))
        vs.escribir_csv(salida / f'archivos_{marca}.csv', filas_archivos)
        vs.escribir_csv(salida / f'novedades_{marca}.csv', todas,
                        ['nivel', 'estado', 'sigla', 'unidad_id', 'unidad', 'tipo', 'capa', 'codigo', 'mensaje', 'contexto', 'sigla_sin_datos'])
        meta = dict(generado=ahora.isoformat(timespec='seconds'), cuenta=ident['Account'], ventana=vars(v), umbrales=vars(umbrales),
                    archivos={c: dict(clave=CAPAS[c]['key'], **{k: str(x) for k, x in (metas.get(c) or {}).items()}) for c in a.capas},
                    vistas_vm=vm, jobs=res_jobs, catalogo=None if a.sin_catalogo else a.catalogo)
        (salida / f'meta_fuentes_internas_{marca}.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
        print(f'[INFO] Archivos en {salida} (sufijo {marca}); el CSV de novedades es novedades_{marca}.csv')
    hay_falla = any(n['estado'] == vs.FALLA for n in todas) or any(f['estado'] == vs.FALLA for f in filas_siglas + filas_unidades)
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
    except (sc.Blocked, TimeoutError) as exc:
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
