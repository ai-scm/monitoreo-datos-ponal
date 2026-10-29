#!/usr/bin/env python3
"""Validacion de las capas Planes GESCOV y Antecedentes del mapa. Solo lectura; Athena + historiales de AWS.

Capas del mapa y su origen real (pon-iapol-capas/capas_adicionales/app/capas_config.json):
  antecedentes-personas / -vehiculos / -imei            RDS public.auditoria_cons_{persona,vehiculo,imei}_v2
  antecedentes-personas-positivos / -vehiculos- / -imei-  lo mismo, solo tiene_antecedentes = true
  planes-programados-historico                          RDS public.v_planes_programados_historico
Antecedentes: el job de Glue V2_SIVICC-migration-to-RDS copia cada noche (22:xx) las tablas crudas de Athena
pon-prod-raw-sivicc.raw_sivicc_usr_mnvcc_auditoria_cons_* a RDS. `sigla` (fisicalaborando) y `codigo_unidad`
(undelaborando, o la unidad de la minuta si es nula) vienen de la fuente, sin etiquetado por poligono; descarta
filas sin coordenadas y es idempotente (compara claves). Aqui se validan esas tablas crudas por sigla y unidad,
igual que SECAD/RNMC, mas la frescura de las corridas del job.
Planes GESCOV: una tarea ECS diaria (01:00) copia de Oracle DISEC (V_PLANESPROGRAMADOS) el dia anterior a RDS y
asigna la unidad con estaciones.csv, descartando lo que no mapea. La copia de Athena (pon-prod-raw-disec) no sirve:
esta desactualizada. Se valida el proceso diario con sus logs de CloudWatch (7 dias de retencion) y el mapeo de
estaciones.csv contra el catalogo; NO hay datos por sigla/unidad porque solo existen en RDS.
Este script no consulta RDS.

Uso:
  python validar_gescov_antecedentes.py --output-s3 s3://bucket/prefijo/ --profile <perfil>
  python validar_gescov_antecedentes.py --solo-sql --sin-esquema --sin-catalogo
Reutiliza el motor de ../validacion-secad. Exit: 0 sin FALLA, 1 FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupcion.
"""

import argparse
import csv
import json
import os
import re
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'validacion-secad'))  # motor compartido
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402
from botocore.exceptions import BotoCoreError, ClientError  # noqa: E402

DB_SIVICC = 'pon-prod-raw-sivicc'
DB_DISEC = 'pon-prod-raw-disec'
JOB_SIVICC = 'V2_SIVICC-migration-to-RDS'
LOG_GESCOV = '/ecs/pon-prod-addcapas-migrations-gescov-migration'
TABLA_COPIA_GESCOV = 'raw_disec_usr_disec_gescov_punto'
SIN_SIGLA = '(SIN_SIGLA)'
GESCOV = 'planes-programados-historico'


def _ruta_estaciones():
    """Copia de estaciones.csv en el repo hermano pon-iapol-capas (carpeta PONAL junto a monitoreo-datos-ponal)."""
    padres = Path(__file__).resolve().parents
    raiz = padres[7] if len(padres) > 7 else Path('.')
    return raiz / 'pon-iapol-capas' / 'migrations' / 'gescop_migration' / 'daily_update' / 'estaciones.csv'


ESTACIONES_CSV = _ruta_estaciones()

# Tablas crudas de SIVICC. `job` es el --CAPA con que V2_SIVICC-migration-to-RDS carga cada una; col_unde es la
# unidad de la minuta que usa el job cuando undelaborando es nulo.
FUENTES = {
    'persona': dict(tabla='raw_sivicc_usr_mnvcc_auditoria_cons_persona', col_id='id_auditoria_persona', col_fecha='fecha',
                    col_unde='undeminuta', job='7', rds='auditoria_cons_persona_v2'),
    'vehiculo': dict(tabla='raw_sivicc_usr_mnvcc_auditoria_cons_vehiculo', col_id='id_vehiculo', col_fecha='fecha_creacion',
                     col_unde='unde_minuta', job='8', rds='auditoria_cons_vehiculo_v2'),
    'imei': dict(tabla='raw_sivicc_usr_mnvcc_auditoria_cons_imei', col_id='id_imei', col_fecha='fecha_creacion',
                 col_unde='unde_minuta', job='9', rds='auditoria_cons_imei_v2'),
}
CAMPOS_COMUNES = ['fisicalaborando', 'undelaborando', 'latitud', 'longitud', 'tiene_antecedentes']
POSITIVOS = 'tiene_antecedentes = 1'

# Orden del menu del mapa (ids del front).
CAPAS = {
    GESCOV: dict(nombre='Planes Gescov Programados', corto='GESCOV', origen='gescov', tabla='v_planes_programados_historico (RDS)'),
    'antecedentes-personas': dict(nombre='Antecedentes: Personas', corto='Personas', origen='sivicc', fuente='persona', filtro=None),
    'antecedentes-vehiculos': dict(nombre='Antecedentes: Vehículos', corto='Vehiculos', origen='sivicc', fuente='vehiculo', filtro=None),
    'antecedentes-imei': dict(nombre='Antecedentes: IMEI', corto='IMEI', origen='sivicc', fuente='imei', filtro=None),
    'antecedentes-personas-positivos': dict(nombre='Antecedentes: Personas (Solo positivos)', corto='Personas (+)', origen='sivicc',
                                            fuente='persona', filtro=POSITIVOS),
    'antecedentes-vehiculos-positivos': dict(nombre='Antecedentes: Vehículos (Solo positivos)', corto='Vehiculos (+)', origen='sivicc',
                                             fuente='vehiculo', filtro=POSITIVOS),
    'antecedentes-imei-positivos': dict(nombre='Antecedentes: IMEI (Solo positivos)', corto='IMEI (+)', origen='sivicc',
                                        fuente='imei', filtro=POSITIVOS),
}
for _c, _d in CAPAS.items():
    if _d['origen'] == 'sivicc':
        _d['tabla'] = FUENTES[_d['fuente']]['tabla'] + (' (positivos)' if _d['filtro'] else '')
TABLAS = {c: d['tabla'] for c, d in CAPAS.items()}
CORTOS = {c: d['corto'] for c, d in CAPAS.items()}
RANGO_NIVEL = {'CARGA': 0, 'NACIONAL': 1, 'SIGLA': 2, 'UNIDAD': 3}


def novedad(nivel, estado, capa, codigo, mensaje, contexto='', **extra):
    return dict(nivel=nivel, estado=estado, sigla='', unidad_id='', unidad='', tipo='', capa=capa, codigo=codigo,
                mensaje=mensaje, contexto=contexto, sigla_sin_datos='', **extra)


# --------------------------------------------------------------------------------------
# Antecedentes: SQL sobre las tablas crudas de SIVICC
# --------------------------------------------------------------------------------------
def construir_sql_sivicc(fuente, filtro, primer_dia, ultimo_dia, siglas=None):
    """Una capa de antecedentes: sigla x unidad x dia de la consulta. Las tablas crudas traen copias de algunas filas,
    por eso `llamadas` y `puntos` cuentan ids distintos y `filas` las filas tal cual."""
    f = FUENTES[fuente]
    fecha, id_ = sc.identifier(f['col_fecha']), sc.identifier(f['col_id'])
    ok = (f'(TRY_CAST(latitud AS double) BETWEEN {sc.LAT_MIN} AND {sc.LAT_MAX} '
          f'AND TRY_CAST(longitud AS double) BETWEEN {sc.LON_MIN} AND {sc.LON_MAX})')
    extra = f'\n  AND {filtro}' if filtro else ''
    extra += f'\n  AND upper(trim(fisicalaborando)) IN ({", ".join(sc.literal(s) for s in sorted(siglas))})' if siglas else ''
    return f"""SELECT
  coalesce(upper(trim(fisicalaborando)), '') AS sigla,
  coalesce(CAST(undelaborando AS varchar), CAST({sc.identifier(f['col_unde'])} AS varchar), '') AS codigo_unidad,
  date_format({fecha}, '%Y-%m-%d') AS dia,
  COUNT(*) AS filas,
  COUNT(DISTINCT {id_}) AS llamadas,
  COUNT(DISTINCT CASE WHEN {ok} THEN {id_} END) AS puntos,
  SUM(CASE WHEN {ok} THEN 0 ELSE 1 END) AS coord_invalida,
  CAST(0 AS bigint) AS hora_invalida,
  CAST(0 AS bigint) AS hora_futura,
  CAST(0 AS bigint) AS desfase_particion,
  date_format(MAX({fecha}), '%Y-%m-%d %H:%i:%s') AS ultima_hora_caso
FROM {sc.identifier(DB_SIVICC)}.{sc.identifier(f['tabla'])}
WHERE {fecha} >= TIMESTAMP '{primer_dia} 00:00:00' AND {fecha} < TIMESTAMP '{ultimo_dia + timedelta(days=1)} 00:00:00'
  AND {id_} IS NOT NULL{extra}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""


def construir_sql_copia_gescov():
    return f"""SELECT date_format(MAX(fecha_creacion), '%Y-%m-%d %H:%i:%s') AS max_creacion
FROM {sc.identifier(DB_DISEC)}.{sc.identifier(TABLA_COPIA_GESCOV)}
"""


def parsear_sivicc(filas):
    mapeadas = []
    for r in filas:
        r = dict(r)
        r['sigla'] = (r.get('sigla') or '').strip() or SIN_SIGLA
        mapeadas.append(r)
    return vs.parsear_filas(mapeadas)


# --------------------------------------------------------------------------------------
# Antecedentes: corridas de V2_SIVICC-migration-to-RDS
# --------------------------------------------------------------------------------------
def evaluar_carga_sivicc(runs, dias, ahora, capas_por_fuente, max_horas=30):
    """Frescura del job que copia cada tabla a RDS. Es idempotente: una corrida exitosa posterior recupera los dias
    anteriores, asi que lo grave es que la ultima carga exitosa sea vieja; los dias sin corrida solo se avisan."""
    novedades, resumen = [], {}
    ahora_utc = ahora.replace(tzinfo=sc.UTC_MENOS_5).astimezone(timezone.utc)
    for fuente, capas in capas_por_fuente.items():
        f = FUENTES[fuente]
        propias = [r for r in runs
                   if (r.get('Arguments') or {}).get('--CAPA', '9') == f['job']
                   and (r.get('Arguments') or {}).get('--DB_TABLE', 'auditoria_cons_imei_V2').lower() == f['rds']
                   and r.get('JobRunState') in sc.TERMINALES]
        exitos = sorted((r for r in propias if r['JobRunState'] == 'SUCCEEDED'), key=lambda r: r['StartedOn'])
        fallidas = sorted((r for r in propias if r['JobRunState'] != 'SUCCEEDED'), key=lambda r: r['StartedOn'])
        con_exito = {r['StartedOn'].astimezone(sc.UTC_MENOS_5).date().isoformat() for r in exitos}
        sin_corrida = [d for d in dias if d not in con_exito]
        horas = (ahora_utc - exitos[-1]['StartedOn']).total_seconds() / 3600 if exitos else None
        resumen[fuente] = dict(exitos=len(exitos), fallidas=len(fallidas), sin_corrida=len(sin_corrida),
                               ultima_hace_h=None if horas is None else round(horas, 1), capas=capas)
        if not exitos:
            msg = f'{JOB_SIVICC} no tiene ninguna corrida exitosa para {f["rds"]} en el historial leido'
            codigo = 'CARGA_RDS_VENCIDA'
        elif horas > max_horas:
            msg = f'la ultima carga exitosa de {f["rds"]} fue hace {horas:.0f} h (limite {max_horas} h): RDS no recibe consultas nuevas'
            codigo = 'CARGA_RDS_VENCIDA'
        else:
            msg, codigo = None, None
        if codigo:
            err = f' Ultimo error: {(fallidas[-1].get("ErrorMessage") or "sin detalle")[:140]}' if fallidas else ''
            for capa in capas:
                novedades.append(novedad('CARGA', vs.FALLA, capa, codigo, msg + '.' + err, f'job {JOB_SIVICC}, CAPA {f["job"]}'))
        elif sin_corrida:
            msg = (f'{f["rds"]}: sin corrida exitosa de {JOB_SIVICC} los dias {", ".join(sin_corrida)}; la siguiente carga exitosa '
                   'recupera esos dias porque el job compara claves, pero hasta entonces RDS va atrasada')
            for capa in capas:
                novedades.append(novedad('CARGA', vs.AVISO, capa, 'CARGA_RDS_DIAS_SIN_CORRIDA', msg, f'job {JOB_SIVICC}, CAPA {f["job"]}'))
    return novedades, resumen


# --------------------------------------------------------------------------------------
# Planes GESCOV: logs de la tarea diaria y mapeo de unidades
# --------------------------------------------------------------------------------------
def parsear_log_gescov(texto):
    """Resumen de una ejecucion de la tarea a partir de su log; None si el log esta vacio (expirado)."""
    if not (texto or '').strip():
        return None

    def num(patron):
        m = re.search(patron, texto)
        return int(m.group(1).replace(',', '')) if m else None

    enr = re.search(r'(\d+) registros enriquecidos - (\d+) descartados', texto)
    rango = re.search(r'Rango fechas: (\d{4}-\d{2}-\d{2}(?: [\d:]+)?) - (\d{4}-\d{2}-\d{2}(?: [\d:]+)?)', texto)
    error = re.search(r'ERROR EN MIGRACI\S+: (.{0,200})', texto)
    desde = re.search(r'fecha_inicio >= (\d{4}-\d{2}-\d{2})', texto)
    return dict(desde=desde.group(1) if desde else None, extraidos=num(r'(\d+) registros extra'),
                enriquecidos=int(enr.group(1)) if enr else None, descartados=int(enr.group(2)) if enr else None,
                insertados=num(r'(\d+) registros insertados'), eliminados=num(r'(\d+) registros eliminados'),
                total=num(r'Total en tabla: ([\d,]+)'), min_inicio=rango.group(1) if rango else None,
                max_inicio=rango.group(2) if rango else None, completada='COMPLETADA' in texto,
                error=error.group(1).strip() if error else None)


def leer_ejecuciones_gescov(logs, grupo, desde):
    """Una ejecucion por stream de CloudWatch: cuando arranco y, si el log sigue vigente, su resumen."""
    streams, token = [], None
    while True:
        kw = dict(logGroupName=grupo, orderBy='LastEventTime', descending=True)
        if token:
            kw['nextToken'] = token
        r = logs.describe_log_streams(**kw)
        streams += r['logStreams']
        token = r.get('nextToken')
        ultimo = streams[-1].get('lastEventTimestamp') if streams else None
        if not token or ultimo is None or datetime.fromtimestamp(ultimo / 1000, sc.UTC_MENOS_5).date() < desde - timedelta(days=2):
            break
    ejecuciones = []
    for s in streams:
        if not s.get('firstEventTimestamp'):
            continue
        inicio = datetime.fromtimestamp(s['firstEventTimestamp'] / 1000, sc.UTC_MENOS_5).replace(tzinfo=None)
        if inicio.date() < desde - timedelta(days=2):
            continue
        eventos, token = [], None
        while True:
            kw = dict(logGroupName=grupo, logStreamName=s['logStreamName'], startFromHead=True)
            if token:
                kw['nextToken'] = token
            e = logs.get_log_events(**kw)
            eventos += [x['message'] for x in e['events']]
            if not e['events'] or e.get('nextForwardToken') == token:
                break
            token = e['nextForwardToken']
        ejecuciones.append(dict(inicio=inicio, stream=s['logStreamName'], detalle=parsear_log_gescov('\n'.join(eventos))))
    return sorted(ejecuciones, key=lambda x: x['inicio'])


def evaluar_gescov(ejecuciones, dias, ahora, max_descartados_pct=8.0, min_volumen_pct=40.0):
    """La tarea corre cada dia a la 01:00 y carga los puntos con fecha_inicio >= ayer. Una corrida que falta o falla deja
    para siempre sin cargar los puntos del dia anterior (la del dia siguiente solo pide desde su 'ayer')."""
    por_dia = {}
    for e in ejecuciones:
        por_dia.setdefault(e['inicio'].date(), []).append(e)
    detalladas = [e['detalle'] for e in ejecuciones if e['detalle'] and e['detalle']['completada'] and e['detalle']['extraidos']]
    mediana = statistics.median(d['extraidos'] for d in detalladas) if detalladas else 0
    novedades = []
    resumen = dict(dias=len(dias), con_ejecucion=0, sin_ejecucion=0, con_detalle=0, sin_detalle=0, fallidas=0, pendientes=0,
                   mediana_extraidos=mediana)
    hoy = ahora.date()
    for c in dias:
        x = date.fromisoformat(c)
        corrida_dia = x + timedelta(days=1)
        ejs = por_dia.get(corrida_dia)
        if not ejs:
            if corrida_dia >= hoy and ahora.hour < 3:
                resumen['pendientes'] += 1
                continue
            resumen['sin_ejecucion'] += 1
            novedades.append(novedad('CARGA', vs.FALLA, GESCOV, 'GESCOV_SIN_EJECUCION',
                                     f'No hay ninguna ejecucion de la migracion diaria el {corrida_dia} (01:00): los puntos con fecha_inicio {c} '
                                     'no se cargaron a v_planes_programados_historico y la corrida siguiente no los recupera',
                                     'ECS pon-prod-addcapas-migrations-gescov-daily-task'))
            continue
        resumen['con_ejecucion'] += 1
        ej = ejs[-1]
        d = ej['detalle']
        if d is None:
            resumen['sin_detalle'] += 1
            continue
        resumen['con_detalle'] += 1
        ctx = f'ejecucion {ej["inicio"]:%Y-%m-%d %H:%M}'
        if d['error'] or not d['completada']:
            resumen['fallidas'] += 1
            novedades.append(novedad('CARGA', vs.FALLA, GESCOV, 'GESCOV_EJECUCION_FALLIDA',
                                     f'La migracion del {corrida_dia} no termino: {d["error"] or "el log no llega al mensaje final"}. '
                                     f'Los puntos con fecha_inicio {c} pueden faltar en el mapa', ctx))
            continue
        if d['extraidos'] == 0:
            novedades.append(novedad('CARGA', vs.FALLA, GESCOV, 'GESCOV_SIN_REGISTROS',
                                     f'La migracion del {corrida_dia} extrajo 0 puntos de Oracle (fecha_inicio >= {d["desde"]})', ctx))
        elif mediana and d['extraidos'] < mediana * min_volumen_pct / 100:
            novedades.append(novedad('CARGA', vs.AVISO, GESCOV, 'GESCOV_VOLUMEN_BAJO',
                                     f'La migracion del {corrida_dia} extrajo {d["extraidos"]} puntos, el {round(100 * d["extraidos"] / mediana)}% '
                                     f'de la mediana ({round(mediana)})', ctx))
        if d['extraidos'] and d['descartados'] is not None and 100 * d['descartados'] / d['extraidos'] > max_descartados_pct:
            novedades.append(novedad('CARGA', vs.AVISO, GESCOV, 'GESCOV_DESCARTES_ALTOS',
                                     f'La migracion del {corrida_dia} descarto {d["descartados"]} de {d["extraidos"]} puntos '
                                     f'({round(100 * d["descartados"] / d["extraidos"], 1)}%, limite {max_descartados_pct:g}%) por no poder asignarles estacion o unidad '
                                     '(estaciones.csv): no aparecen en el mapa', ctx))
        if d['max_inicio'] and date.fromisoformat(d['max_inicio'][:10]) < x:
            novedades.append(novedad('CARGA', vs.FALLA, GESCOV, 'GESCOV_DIA_NO_CARGADO',
                                     f'Tras la migracion del {corrida_dia} el mayor fecha_inicio de la tabla es {d["max_inicio"]}: '
                                     f'los puntos del {c} no estan cargados', ctx))
    # coherencia del total entre ejecuciones consecutivas: total = total previo + enriquecidos - eliminados
    previas = [e for e in ejecuciones if e['detalle'] and e['detalle']['completada'] and e['detalle']['total'] is not None]
    for ant, act in zip(previas, previas[1:]):
        a, b = ant['detalle'], act['detalle']
        if (act['inicio'].date() - ant['inicio'].date()).days != 1 or b['enriquecidos'] is None or b['eliminados'] is None:
            continue
        esperado = a['total'] + b['enriquecidos'] - b['eliminados']
        if b['total'] != esperado:
            novedades.append(novedad('CARGA', vs.AVISO, GESCOV, 'GESCOV_TOTAL_INCONSISTENTE',
                                     f'Tras la migracion del {act["inicio"].date()} la tabla tiene {b["total"]:,} filas y por las corridas deberian ser '
                                     f'{esperado:,} (diferencia {b["total"] - esperado:+,}): otra carga u otro borrado toco la tabla',
                                     f'ejecucion {act["inicio"]:%Y-%m-%d %H:%M}'))
    if resumen['sin_detalle']:
        novedades.append(novedad('CARGA', vs.AVISO, GESCOV, 'GESCOV_LOGS_EXPIRADOS',
                                 f'{resumen["sin_detalle"]} de {resumen["con_ejecucion"]} ejecuciones de la ventana no tienen log: el grupo {LOG_GESCOV} '
                                 'retiene solo 7 dias. Solo se confirma que la tarea arranco; ampliar la retencion permitiria auditar mas de una semana',
                                 LOG_GESCOV))
    return novedades, resumen


def cargar_estaciones(ruta):
    with open(ruta, encoding='utf-8-sig', newline='') as f:
        filas = list(csv.DictReader(f))
    requeridas = {'nombre_clean', 'sigla_fisica', 'unidad_id'}
    if not filas or not requeridas <= set(filas[0]):
        raise sc.Blocked(f'{ruta} debe tener las columnas {", ".join(sorted(requeridas))}.')
    return [dict(nombre=r['nombre_clean'].strip(), sigla=r['sigla_fisica'].strip().upper(), id=sc.normalizar_id(r['unidad_id'])) for r in filas
            if sc.normalizar_id(r['unidad_id'])]


def evaluar_mapeo_gescov(estaciones, arbol):
    """estaciones.csv traduce 'ESTACION-SIGLA' a codigo_unidad. Si apunta a una unidad que el mapa no muestra (o de otra
    sigla), esos planes salen al elegir la sigla pero no la unidad. Devuelve (novedades, estaciones del catalogo sin entrada, resumen)."""
    novedades, ids_csv = [], {e['id'] for e in estaciones}
    for e in estaciones:
        u = arbol.por_id.get(e['id'])
        if u is None:
            codigo, msg = 'GESCOV_MAPEO_A_UNIDAD_INEXISTENTE', f'el id {e["id"]} no existe en el catalogo de unidades'
        elif e['id'] not in arbol.visibles:
            codigo, msg = 'GESCOV_MAPEO_A_UNIDAD_OCULTA', f'el id {e["id"]} ({u.nombre}) existe pero el mapa no lo muestra (tipo {u.tipo} filtrado o sin poligono)'
        elif u.sigla != e['sigla']:
            codigo, msg = 'GESCOV_MAPEO_SIGLA_DISTINTA', f'el id {e["id"]} pertenece a la sigla {u.sigla} en el catalogo, no a {e["sigla"]}'
        else:
            continue
        novedades.append(dict(novedad('UNIDAD', vs.AVISO, GESCOV, codigo, f'estaciones.csv mapea {e["nombre"]} ({e["sigla"]}): {msg}; '
                                     'sus planes GESCOV aparecen al elegir la sigla pero no al elegir la unidad', 'estaciones.csv'),
                              sigla=e['sigla'], unidad_id=e['id'], unidad=e['nombre'], tipo=u.tipo if u else ''))
    sin_entrada = sorted((arbol.por_id[i] for i in arbol.visibles if arbol.por_id[i].tipo == '7.0' and i not in ids_csv),
                         key=lambda u: (u.sigla, u.nombre))
    estaciones_catalogo = sum(1 for i in arbol.visibles if arbol.por_id[i].tipo == '7.0')
    if sin_entrada:
        ejemplos = ', '.join(f'{u.nombre} ({u.sigla})' for u in sin_entrada[:5])
        novedades.append(novedad('CARGA', vs.AVISO, GESCOV, 'GESCOV_ESTACIONES_SIN_ENTRADA',
                                 f'{len(sin_entrada)} de {estaciones_catalogo} estaciones del catalogo no tienen entrada en estaciones.csv ({len(estaciones)} filas): '
                                 f'si tuvieran planes GESCOV, el proceso los descartaria. Ej.: {ejemplos}',
                                 'solo afecta a estaciones con planes GESCOV; lista completa en gescov_estaciones_sin_entrada_*.csv'))
    resumen = dict(filas=len(estaciones), con_problema=len(novedades) - (1 if sin_entrada else 0),
                   estaciones_catalogo=estaciones_catalogo, sin_entrada=len(sin_entrada))
    return novedades, sin_entrada, resumen


def evaluar_copia_athena_gescov(max_creacion, ahora, max_dias=3):
    if not max_creacion:
        return []
    ultima = datetime.fromisoformat(max_creacion)
    if (ahora - ultima).days > max_dias:
        return [novedad('CARGA', vs.AVISO, GESCOV, 'COPIA_ATHENA_GESCOV_DESACTUALIZADA',
                        f'{DB_DISEC}.{TABLA_COPIA_GESCOV} (copia de GESCOV en Athena) llega solo hasta fecha_creacion {max_creacion}, '
                        f'{(ahora - ultima).days} dias atras. No afecta al mapa (que usa RDS), pero impide validar GESCOV desde Athena',
                        'replicacion DMS/Glue de DISEC')]
    return []


# --------------------------------------------------------------------------------------
# Salida y programa
# --------------------------------------------------------------------------------------
def ordenar(novedades):
    novedades.sort(key=lambda n: (-vs.ORDEN[n['estado']], RANGO_NIVEL.get(n['nivel'], 9)))
    return novedades


def imprimir_extra(extra, res_sivicc, res_gescov, res_mapeo):
    print('\n=== CARGA DE ANTECEDENTES A RDS (job V2_SIVICC-migration-to-RDS) ===')
    if res_sivicc is None:
        print('  (no evaluada)')
    for fuente, r in (res_sivicc or {}).items():
        print(f'  {fuente:<9} corridas exitosas {r["exitos"]} | fallidas {r["fallidas"]} | dias sin corrida exitosa {r["sin_corrida"]} | '
              f'ultima exitosa hace {r["ultima_hace_h"]} h')
    for n in (x for x in extra if x['codigo'].startswith('CARGA_RDS')):
        print(f'  [{n["estado"]}] {CORTOS[n["capa"]]} | {n["codigo"]}: {n["mensaje"]}')
    print('\n=== PLANES GESCOV (tarea diaria ECS -> v_planes_programados_historico) ===')
    if res_gescov is None:
        print('  (no evaluada)')
    else:
        print(f'  Dias revisados {res_gescov["dias"]} | con ejecucion {res_gescov["con_ejecucion"]} (log legible {res_gescov["con_detalle"]}, '
              f'sin log {res_gescov["sin_detalle"]}) | sin ejecucion {res_gescov["sin_ejecucion"]} | fallidas {res_gescov["fallidas"]} | '
              f'mediana de puntos extraidos {round(res_gescov["mediana_extraidos"])}')
    if res_mapeo:
        print(f'  estaciones.csv: {res_mapeo["filas"]} filas | {res_mapeo["con_problema"]} apuntan a una unidad que el mapa no muestra | '
              f'{res_mapeo["sin_entrada"]} de {res_mapeo["estaciones_catalogo"]} estaciones del catalogo sin entrada')
    for n in (x for x in extra if x['capa'] == GESCOV and x['nivel'] == 'CARGA'):
        print(f'  [{n["estado"]}] {n["codigo"]}: {n["mensaje"]}')
    nacionales = [x for x in extra if x['nivel'] == 'NACIONAL']
    print('\n=== DIAS ATIPICOS A NIVEL NACIONAL ===')
    for n in nacionales:
        print(f'  [{n["estado"]}] {CORTOS[n["capa"]]}: {n["mensaje"]}')
    if not nacionales:
        print('  (ninguno)')


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile', help='Perfil AWS (opcional)')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA, help='Cuenta esperada; aborta si no coincide')
    p.add_argument('--capas', nargs='+', choices=list(CAPAS), default=list(CAPAS), help='Capas a validar (todas por defecto)')
    p.add_argument('--sigla', nargs='+', help='Limita las capas de antecedentes a estas siglas (ej. MEPAS MEBOG)')
    p.add_argument('--catalogo', default=sc.CATALOGO_S3, help='CSV de estructura de unidades: ruta local o s3:// (por defecto el del backend)')
    p.add_argument('--sin-catalogo', action='store_true', help='Sin catalogo: solo siglas y unidades observadas; sin revisar estaciones.csv')
    p.add_argument('--estaciones', default=str(ESTACIONES_CSV), help='estaciones.csv de la migracion GESCOV (copia del repo pon-iapol-capas)')
    p.add_argument('--ventana-dias', type=int, default=15, help='Dias completos a validar y de la ventana previa (15)')
    p.add_argument('--fecha-fin', help='Ultimo dia de la ventana (YYYY-MM-DD); por defecto ayer')
    p.add_argument('--min-base', type=int, default=20, help='Filas de la ventana previa para exigir datos (20)')
    p.add_argument('--caida-pct', type=float, default=50, help='Aviso de caida de puntos vs ventana previa (50)')
    p.add_argument('--racha-dias', type=int, default=2, help='Dias seguidos sin filas al cierre para avisar (2)')
    p.add_argument('--min-esperado-falla', type=float, default=5,
                   help='Filas esperadas en una racha sin datos para que una sigla sea FALLA y no AVISO (5)')
    p.add_argument('--max-invalidos-pct', type=float, default=1, help='Calidad: porcentaje tolerado (1)')
    p.add_argument('--dia-atipico-pct', type=float, default=40, help='Dia nacional atipico si es menor a este %% de la mediana (40)')
    p.add_argument('--max-horas-carga', type=float, default=30, help='Horas maximas desde la ultima carga exitosa a RDS (30)')
    p.add_argument('--max-descartados-pct', type=float, default=8, help='GESCOV: %% de puntos descartados que ya es anormal (8)')
    p.add_argument('--sin-carga', action='store_true', help='No leer historiales de jobs, logs ni consultar la copia de GESCOV')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'),
                   help='Destino de resultados de Athena, s3://bucket/prefijo/ (o variable ATHENA_OUTPUT_S3); obligatorio si el workgroup no define uno')
    p.add_argument('--timeout', type=int, default=900, help='Segundos de espera de Athena (900)')
    p.add_argument('--solo-sql', action='store_true', help='Genera el SQL sin ejecutarlo')
    p.add_argument('--sin-esquema', action='store_true', help='No consulta Glue para validar columnas (con --solo-sql no usa AWS)')
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true', help='No escribe archivos')
    p.add_argument('--max-lineas', type=int, default=60, help='Unidades a listar en consola (60)')
    p.add_argument('--detallado', action='store_true', help='Imprime tambien el SQL')
    return p


def validar_args(p, a):
    if not 1 <= a.ventana_dias <= 31:
        p.error('--ventana-dias debe estar entre 1 y 31')
    if min(a.min_base, a.racha_dias, a.min_esperado_falla, a.max_horas_carga) <= 0:
        p.error('--min-base, --racha-dias, --min-esperado-falla y --max-horas-carga deben ser mayores que cero')
    if not all(0 <= x <= 100 for x in (a.caida_pct, a.max_invalidos_pct, a.dia_atipico_pct, a.max_descartados_pct)):
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


def ejecutar(a):
    ahora = sc.ahora_colombia()
    v = vs.calcular_ventana(ahora, a.ventana_dias, a.fecha_fin_d)
    primer_dia, ultimo_dia = date.fromisoformat(v.todos[0]), date.fromisoformat(v.todos[-1])
    marca = ahora.strftime('%Y-%m-%d_%H-%M-%S')
    umbrales = vs.Umbrales(a.min_base, a.caida_pct, a.racha_dias, 3, a.max_invalidos_pct, a.min_esperado_falla)
    capas_sivicc = [c for c in a.capas if CAPAS[c]['origen'] == 'sivicc']
    hay_gescov = GESCOV in a.capas
    usa_aws = not (a.solo_sql and a.sin_esquema and (a.sin_catalogo or not a.catalogo.startswith('s3://')))
    sesion, ident = None, None
    if usa_aws:
        sesion = sc.crear_sesion(a.profile, a.region)
        ident = sc.verificar_identidad(sesion, a.cuenta)
        print(f'[INFO] Cuenta {ident["Account"]} | {ident["Arn"]} | region {a.region}')
    print(f'[INFO] Ventana {v.dias[0]} a {v.dias[-1]} | previa {v.base[0]} a {v.base[-1]}'
          f'{" | hoy parcial " + v.parcial if v.parcial else ""}')

    arbol = None
    if not a.sin_catalogo:
        unidades, formato = sc.cargar_catalogo(a.catalogo, sesion)
        arbol = sc.Arbol(unidades)
        print(f'[INFO] Catalogo {a.catalogo}: {len(unidades)} filas ({formato}); {len(arbol.visibles)} unidades visibles, '
              f'{len(arbol.siglas_con_territorio())} siglas con territorio')
    else:
        print('[AVISO] Sin catalogo: no se detectan unidades ni siglas sin ningun dato; solo las observadas.')

    glue = sesion.client('glue', config=sc.BOTO_CONFIG) if (sesion and not a.sin_esquema) else None
    sqls = {}
    for fuente in sorted({CAPAS[c]['fuente'] for c in capas_sivicc}):
        if glue:
            f = FUENTES[fuente]
            cols, _, _ = sc.columnas_tabla(glue, DB_SIVICC, f['tabla'])
            faltan = [c for c in CAMPOS_COMUNES + [f['col_id'], f['col_fecha'], f['col_unde']] if c not in cols]
            if faltan:
                raise sc.Blocked(f'{DB_SIVICC}.{f["tabla"]} no tiene las columnas {", ".join(faltan)}. Disponibles: {", ".join(sorted(cols))}.')
    for capa in capas_sivicc:
        d = CAPAS[capa]
        sqls[capa] = construir_sql_sivicc(d['fuente'], d['filtro'], primer_dia, ultimo_dia, a.siglas)
    if hay_gescov and not a.sin_carga:
        sqls['_copia_gescov'] = construir_sql_copia_gescov()
    if glue:
        print('[INFO] Esquema Glue verificado: ' + ', '.join(sorted({FUENTES[CAPAS[c]['fuente']]['tabla'] for c in capas_sivicc})))

    salida = Path(a.salida)
    if not a.no_export:
        salida.mkdir(parents=True, exist_ok=True)
        (salida / f'consultas_gescov_antecedentes_{marca}.sql').write_text('\n;\n'.join(f'-- {c}\n{s}' for c, s in sqls.items()), encoding='utf-8')
    if a.solo_sql or a.detallado:
        for capa, sql in sqls.items():
            print(f'\n-- {capa}\n{sql}')
    if a.solo_sql:
        print('[INFO] --solo-sql: no se ejecuto nada en Athena.')
        return 0

    corridas, ejecuciones = None, None
    if not a.sin_carga:
        if capas_sivicc:
            try:
                corridas = sc.leer_corridas(sesion.client('glue', config=sc.BOTO_CONFIG), JOB_SIVICC, date.fromisoformat(v.dias[0]))
                print(f'[INFO] Historial de {JOB_SIVICC}: {len(corridas)} corridas' + (f' desde {corridas[-1]["StartedOn"]:%Y-%m-%d}' if corridas else ''))
            except (ClientError, BotoCoreError) as exc:
                print(f'[AVISO] No se pudo leer el historial de {JOB_SIVICC} ({sc.detalle_error(exc)}); esa carga queda sin evaluar.')
        if hay_gescov:
            try:
                ejecuciones = leer_ejecuciones_gescov(sesion.client('logs', config=sc.BOTO_CONFIG), LOG_GESCOV, date.fromisoformat(v.dias[0]))
                print(f'[INFO] Logs de GESCOV: {len(ejecuciones)} ejecuciones desde {ejecuciones[0]["inicio"]:%Y-%m-%d}' if ejecuciones else
                      '[AVISO] El grupo de logs de GESCOV no tiene ejecuciones.')
            except (ClientError, BotoCoreError) as exc:
                print(f'[AVISO] No se pudieron leer los logs de GESCOV ({sc.detalle_error(exc)}); falta logs:DescribeLogStreams/GetLogEvents.')

    obs, stats, copia = {}, {}, None
    if sqls:
        athena = sc.Athena(sesion, DB_SIVICC, a.workgroup, a.output_s3, a.timeout)
        print(f'[INFO] Ejecutando {len(sqls)} consulta(s) en Athena (workgroup {a.workgroup}); Ctrl+C las cancela.')
        for nombre, (filas, info) in athena.ejecutar(sqls).items():
            escaneado = info.get('Statistics', {}).get('DataScannedInBytes', 0)
            stats[nombre] = dict(grupos=len(filas), bytes_escaneados=escaneado, query_id=info.get('QueryExecutionId'))
            if nombre == '_copia_gescov':
                copia = filas[0]['max_creacion'] if filas else None
                continue
            obs[nombre] = parsear_sivicc(filas)
            print(f'[INFO] {CAPAS[nombre]["tabla"]}: {len(filas)} grupos, {escaneado / 1024 ** 2:.0f} MiB escaneados')
            if not filas:
                print(f'[AVISO] {CAPAS[nombre]["tabla"]} no devolvio filas para la ventana; revisa fechas, siglas y columnas.')

    filas_siglas, filas_unidades, novedades = [], [], []
    if capas_sivicc:
        filas_siglas, filas_unidades, novedades = vs.analizar(obs, arbol, v, umbrales, capas_sivicc, a.siglas, TABLAS)
        novedades = vs.marcar_sin_sigla(filas_siglas, novedades, {SIN_SIGLA}, 'consultas', 'CONSULTAS_SIN_SIGLA', 'sigla')
    extra = vs.dias_atipicos(obs, v, capas_sivicc, a.dia_atipico_pct / 100)
    res_sivicc = res_gescov = res_mapeo = None
    sin_entrada = []
    if corridas is not None:
        capas_por_fuente = {}
        for c in capas_sivicc:
            capas_por_fuente.setdefault(CAPAS[c]['fuente'], []).append(c)
        carga, res_sivicc = evaluar_carga_sivicc(corridas, v.dias, ahora, capas_por_fuente, a.max_horas_carga)
        extra += carga
    if ejecuciones is not None:
        carga, res_gescov = evaluar_gescov(ejecuciones, v.dias, ahora, a.max_descartados_pct)
        extra += carga
    if hay_gescov and arbol:
        try:
            carga, sin_entrada, res_mapeo = evaluar_mapeo_gescov(cargar_estaciones(a.estaciones), arbol)
            extra += carga
        except (OSError, sc.Blocked) as exc:
            print(f'[AVISO] No se pudo revisar estaciones.csv ({exc}); indica la ruta con --estaciones.')
    if hay_gescov and not a.sin_carga:
        extra += evaluar_copia_athena_gescov(copia, ahora)
    todas = ordenar(extra + novedades)

    if capas_sivicc:
        vs.imprimir(filas_siglas, filas_unidades, novedades, capas_sivicc, v, a.max_lineas, CORTOS)
    imprimir_extra(extra, res_sivicc, res_gescov, res_mapeo)

    if not a.no_export:
        if capas_sivicc:
            vs.escribir_csv(salida / f'resumen_siglas_{marca}.csv', filas_siglas)
            vs.escribir_csv(salida / f'resumen_unidades_{marca}.csv', filas_unidades)
            vs.escribir_csv(salida / f'diario_{marca}.csv', vs.filas_diario(obs, arbol, v, TABLAS))
        vs.escribir_csv(salida / f'novedades_{marca}.csv', todas,
                        ['nivel', 'estado', 'sigla', 'unidad_id', 'unidad', 'tipo', 'capa', 'codigo', 'mensaje', 'contexto', 'sigla_sin_datos'])
        if sin_entrada:
            vs.escribir_csv(salida / f'gescov_estaciones_sin_entrada_{marca}.csv',
                            [dict(sigla=u.sigla, unidad_id=u.id, unidad=u.nombre) for u in sin_entrada])
        meta = dict(generado=ahora.isoformat(timespec='seconds'), cuenta=ident and ident['Account'], ventana=vars(v), umbrales=vars(umbrales),
                    capas={c: CAPAS[c]['tabla'] for c in a.capas}, catalogo=None if a.sin_catalogo else a.catalogo, consultas=stats,
                    carga_sivicc=res_sivicc, gescov=res_gescov, estaciones=res_mapeo)
        (salida / f'meta_gescov_antecedentes_{marca}.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
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
