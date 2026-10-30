#!/usr/bin/env python3
"""Valida Optimizacion/ en S3: nombres, pares, turnos y contenido GeoJSON.

CloudShell: python3 validador_optimizacion_cloudshell.py --sigla MEPER --unidad 51716
Por defecto revisa la semana actual de Bogota y la siguiente, exclusivamente.
Solo lee resultados; escribe dos informes CSV en ValidationReports/08-MonitorOptimizacion/.
La referencia JSON es opcional y describe observaciones, no un contrato de entrega.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timedelta, timezone
import gzip
import json
import math
from pathlib import Path
import re
import sys
import tempfile
from threading import Lock
import time
from urllib.parse import urlparse
from uuid import uuid4

BUCKET = 'pon-prod-ml-iapol'
OWNER = '926162397524'
REGION = 'us-east-1'
SCRIPT_VERSION = '2026-10-30.4'
ROOT_PREFIX = 'PrediccionMdP'
REPORTS = 's3://pon-prod-ml-iapol/ValidationReports/08-MonitorOptimizacion/'
BOGOTA = timezone(timedelta(hours=-5))
# Cambiar POLICY al modificar reglas de contenido: invalida revisiones antiguas en cache.
POLICY = 'optimizacion-2026-10-29-v1'
PATTERN = re.compile(r'^Optimizacion_(?P<unidad>\d+)_(?P<fecha>\d{8})_(?P<turno>[123])_F_(?P<clase>B|CP)(?P<indice>\d{2,})\.geojson$')
LEVELS = {'INFO': 0, 'OK': 0, 'AVISO': 1, 'PENDIENTE': 2, 'NO EVALUADO': 3, 'INCOMPLETO': 3, 'FALLA': 4}
GENERAL_FIELDS = ('tipo_registro', 'semana', 'sigla', 'unidad', 'estado', 'cobertura_listado',
    'archivos', 'archivos_esperados_min', 'archivos_esperados_max', 'base_esperados',
    'comparacion_referencia', 'unidades_observadas', 'unidades_referencia',
    'unidades_sin_referencia', 'unidades_referencia_no_observadas',
    'nombres_validos', 'nombres_invalidos', 'archivos_vacios',
    'turnos_observados', 'turnos_esperados', 'pares_completos',
    'pares_incompletos', 'indices', 'contenido_ok', 'contenido_con_hallazgos',
    'contenido_pendiente', 'lecturas_nuevas', 'lecturas_cache', 'ultima_modificacion', 'control', 'detalle')
DETAIL_FIELDS = ('semana', 'sigla', 'unidad', 'archivo', 'ruta_s3', 'fecha', 'turno', 'clase',
    'indice', 'tamano_bytes', 'ultima_modificacion', 'nombre_estado', 'contenido_estado',
    'origen_revision', 'features', 'tipos_geometria', 'geometrias_nulas', 'anillos_invalidos',
    'coordenadas_invalidas', 'unidad_distinta', 'cad_distinto', 'intensidad_min', 'intensidad_max',
    'estado', 'detalle')


class ReadFailure(Exception):
    """Una lectura parcial se conserva en los informes, nunca se convierte en cero."""


class BudgetStop(Exception):
    pass


def worst(states):
    return max(states, key=lambda x: LEVELS[x], default='OK')


def validar_fecha(value):
    if not re.fullmatch(r'\d{8}', value):
        raise ValueError('Fecha invalida; usar YYYYMMDD')
    return datetime.strptime(value, '%Y%m%d').date()


def semana_actual(now=None):
    today = (now or datetime.now(BOGOTA)).astimezone(BOGOTA).date()
    return (today - timedelta(days=today.weekday())).strftime('%Y%m%d')


def semana_siguiente(week):
    return (validar_fecha(week) + timedelta(days=7)).strftime('%Y%m%d')


def calendario(week):
    monday = validar_fecha(week)
    if monday.weekday() != 0:
        raise ValueError('La carpeta semanal debe corresponder a un lunes')
    return {
        ((monday + timedelta(days=d)).strftime('%Y%m%d'), str(turn))
        for turn in (1, 2, 3) for d in range(-1 if turn == 1 else 0, 6 if turn == 1 else 7)}


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--desde', default=semana_actual(), metavar='YYYYMMDD', help='Lunes inicial; default semana actual de Bogota')
    p.add_argument('--hasta', metavar='YYYYMMDD', help='Lunes final; default el lunes siguiente a --desde: solo dos semanas')
    p.add_argument('--sigla', type=str.upper)
    p.add_argument('--unidad', help='Unidad numerica; requiere --sigla')
    p.add_argument('--region', default=REGION)
    p.add_argument('--perfil', help='Perfil AWS opcional; usa la sesion normal por defecto')
    p.add_argument('--solo-metadata', action='store_true', help='No lee GeoJSON; contenido queda NO EVALUADO')
    p.add_argument('--sin-cache', action='store_true', help='Relee contenido aunque su version local este revisada')
    p.add_argument('--max-lecturas', type=int, default=1000, help='GET nuevos como maximo por ejecucion; default 1000')
    p.add_argument('--max-segundos', type=int, default=180, help='Presupuesto entre llamadas, no limite del predictor; default 180')
    p.add_argument('--max-mib', type=int, default=8, help='Maximo por archivo GeoJSON; default 8 MiB')
    p.add_argument('--trabajadores', type=int, default=6, help='Unidades en paralelo; default 6')
    p.add_argument('--referencia', type=Path, help='JSON de patrones observados; default junto al .py, si existe')
    p.add_argument('--ciclo-cerrado', action='store_true', help='Entrega confirmada: pares/turnos faltantes en unidades con archivos pasan de PENDIENTE a FALLA')
    p.add_argument('--salida', type=Path, default=Path('salida'))
    p.add_argument('--sin-subir', action='store_true', help='Solo informes locales; no escribe en S3')
    p.add_argument('--reportes-s3', default=REPORTS)
    p.add_argument('--mostrar-todas', action='store_true', help='Tabla por unidad; default resumen por sigla')
    a = p.parse_args(argv)
    a.hasta_explicita = a.hasta is not None
    try:
        calendario(a.desde)
        if a.hasta is None:
            a.hasta = semana_siguiente(a.desde)
        calendario(a.hasta)
        if a.hasta < a.desde:
            raise ValueError('--hasta no puede ser anterior a --desde')
        if a.sigla and not re.fullmatch(r'[A-Z0-9_-]{1,30}', a.sigla):
            raise ValueError('Sigla invalida')
        if a.unidad and (not a.sigla or not re.fullmatch(r'\d{1,20}', a.unidad)):
            raise ValueError('--unidad debe ser numerica y requiere --sigla')
        if not 1 <= a.max_lecturas <= 1000000 or not 10 <= a.max_segundos <= 3600:
            raise ValueError('Limites: --max-lecturas 1..1000000; --max-segundos 10..3600')
        if not 1 <= a.max_mib <= 32 or not 1 <= a.trabajadores <= 16:
            raise ValueError('Limites: --max-mib 1..32; --trabajadores 1..16')
        report_destination(a.reportes_s3)
    except ValueError as exc:
        p.error(str(exc))
    return a


def report_destination(uri):
    parsed = urlparse(uri)
    prefix = parsed.path.strip('/')
    if (parsed.scheme != 's3' or parsed.netloc != BUCKET or not prefix.startswith('ValidationReports/')
            or '..' in prefix.split('/') or parsed.query or parsed.fragment):
        raise ValueError('Los informes deben quedar en s3://pon-prod-ml-iapol/ValidationReports/<carpeta>/')
    return parsed.netloc, prefix + '/'


def stamp(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat()
    return str(value or '')


class Budget:
    def __init__(self, seconds, reads):
        self.deadline = time.monotonic() + seconds
        self.limit, self.reads = reads, 0
        self.lock = Lock()

    def tick(self):
        if time.monotonic() >= self.deadline:
            raise BudgetStop('Presupuesto de tiempo agotado; repetir el comando para continuar')

    def reserve(self):
        with self.lock:
            self.tick()
            if self.reads >= self.limit:
                raise BudgetStop('Limite de lecturas nuevas; repetir el comando para continuar')
            self.reads += 1

    def stop(self):
        self.deadline = 0


class Reader:
    def __init__(self, s3, budget, errors):
        self.s3, self.budget, self.errors = s3, budget, errors

    def pages(self, prefix):
        self.budget.tick()
        try:
            pages = self.s3.get_paginator('list_objects_v2').paginate(
                Bucket=BUCKET, Prefix=prefix, Delimiter='/', ExpectedBucketOwner=OWNER)
            iterator = iter(pages)
            while True:
                self.budget.tick()
                try:
                    page = next(iterator)
                except StopIteration:
                    return
                yield page
        except self.errors as exc:
            raise ReadFailure(error_code(exc)) from exc

    def get(self, obj, limit):
        if not obj.get('ETag'):
            raise ReadFailure('Objeto sin ETag; no se vincula metadata con contenido')
        self.budget.reserve()
        try:
            response = self.s3.get_object(Bucket=BUCKET, Key=obj['Key'],
                IfMatch=obj['ETag'], ExpectedBucketOwner=OWNER)
            body = response['Body']
            try:
                if (response.get('ETag') != obj['ETag'] or response.get('ContentLength') != obj['Size']
                        or stamp(response.get('LastModified')) != stamp(obj.get('LastModified'))):
                    raise ReadFailure('Objeto cambio entre listado y lectura; repetir revision')
                raw = body.read(limit + 1)
            finally:
                body.close()
            if len(raw) > limit:
                raise ReadFailure('Archivo supera el presupuesto de bytes')
            if len(raw) != obj['Size']:
                raise ReadFailure('Cuerpo incompleto; no se evalua el contenido')
            return raw
        except self.errors as exc:
            raise ReadFailure(error_code(exc)) from exc


def error_code(exc):
    response = getattr(exc, 'response', {})
    return response.get('Error', {}).get('Code', type(exc).__name__)


def children(reader, prefix):
    for page in reader.pages(prefix):
        for item in page.get('CommonPrefixes', []):
            name = item['Prefix'][len(prefix):].rstrip('/')
            if name and '/' not in name:
                yield name


def discover(reader, a):
    """Conserva controles de descubrimiento; no interpreta ausencia como lista vacia sana."""
    candidates, controls, weeks = set(), [], []
    future_unobserved = set()
    last_week = a.hasta or semana_siguiente(a.desde)
    automatic_window = not getattr(a,'hasta_explicita',bool(a.hasta))
    def found(prefix, scope):
        result = set()
        try:
            for name in children(reader, prefix):
                result.add(name)
        except (ReadFailure, BudgetStop) as exc:
            controls.append(check('', '', '', scope, 'INCOMPLETO', str(exc)))
        return sorted(result)
    years = found(ROOT_PREFIX + '/', 'Descubrimiento de anios')
    for year in years:
        if not re.fullmatch(r'\d{4}', year) or year < a.desde[:4] or year > last_week[:4]:
            continue
        for week in found(f'{ROOT_PREFIX}/{year}/', 'Descubrimiento de semanas ' + year):
            if not re.fullmatch(r'\d{8}', week) or week[:4] != year or week < a.desde or week > last_week:
                continue
            try:
                calendario(week)
            except ValueError:
                controls.append(check(week, '', '', 'Carpeta semanal', 'AVISO', 'Nombre semanal fuera del calendario de lunes'))
                continue
            weeks.append(week)
    for week in sorted(weeks):
        prefix = f'{ROOT_PREFIX}/{week[:4]}/{week}/'
        automatic_future = week > a.desde and automatic_window
        siglas = found(prefix, 'Descubrimiento de siglas ' + week)
        if a.sigla:
            siglas = [a.sigla] if a.sigla in siglas else []
            if not siglas:
                if automatic_future:
                    future_unobserved.add(week)
                    continue
                controls.append(check(week, a.sigla, '', 'Sigla solicitada', 'AVISO', 'Sin carpeta de esta sigla; elegibilidad y entrega no confirmadas'))
                if a.unidad:
                    candidates.add((week, a.sigla, a.unidad))
        for cad in siglas:
            if not re.fullmatch(r'[A-Z0-9_-]{1,30}', cad):
                controls.append(check(week, cad, '', 'Sigla', 'AVISO', 'Nombre no admitido; carpeta no recorrida'))
                continue
            units = found(prefix + cad + '/', 'Descubrimiento de unidades ' + week + '/' + cad)
            if a.unidad and a.unidad not in units:
                if automatic_future:
                    future_unobserved.add(week)
                    continue
                candidates.add((week, cad, a.unidad))
            for unit in units:
                if not re.fullmatch(r'\d{1,20}', unit):
                    controls.append(check(week, cad, unit, 'Unidad', 'AVISO', 'Nombre no numerico; carpeta no recorrida'))
                elif not a.unidad or unit == a.unidad:
                    candidates.add((week, cad, unit))
    if future_unobserved:
        controls.append(check('',a.sigla or '',a.unidad or '', 'Semana siguiente sin carpeta seleccionada','PENDIENTE',
            'Sin carpeta de la sigla/unidad seleccionada en la semana siguiente: '
            + ', '.join(sorted(future_unobserved))
            + '. No se declara fallo de entrega; se conserva evidencia previa de cache si existe'))
    if not weeks:
        controls.append(check('', a.sigla or '', a.unidad or '', 'Semanas disponibles', 'INCOMPLETO', 'No se encontraron semanas en el rango o no se pudieron enumerar'))
    elif a.desde not in weeks:
        controls.append(check(a.desde, '', '', 'Semana inicial', 'PENDIENTE', 'No existe carpeta observada; no se conoce entrega obligatoria'))
    if weeks and automatic_window and last_week not in weeks:
        controls.append(check(last_week,a.sigla or '',a.unidad or '', 'Semana siguiente','PENDIENTE',
            'Todavia no se observa la carpeta semanal; esta parte del alcance no se ha podido revisar'))
    return sorted(candidates), controls, sorted(weeks)


def check(week, cad, unit, name, state, detail):
    return dict(tipo_registro='CONTROL', semana=week, sigla=cad, unidad=unit,
        estado=state, control=name, detalle=detail)


def slot_key(week, date, turn):
    return f'{(validar_fecha(date) - validar_fecha(week)).days}/{turn}'


def load_reference(path):
    if not path.exists():
        return {}, 'Sin JSON de referencia; pares/turnos y contenido siguen evaluandose'
    data = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(data,dict):
        raise ValueError('Referencia debe ser un objeto JSON')
    if data.get('version') != 1 or data.get('bucket') != BUCKET or data.get('prefijo') != ROOT_PREFIX:
        raise ValueError('JSON de referencia no corresponde a este validador/bucket')
    profiles = data.get('perfiles')
    units = data.get('unidades')
    if not isinstance(profiles, dict) or not isinstance(units, dict):
        raise ValueError('Referencia sin perfiles/unidades validos')
    for unit, profile in units.items():
        if not re.fullmatch(r'[A-Z0-9_-]{1,30}/\d{1,20}', unit) or profile not in profiles:
            raise ValueError('Unidad/perfil invalido en referencia')
    for profile in profiles.values():
        if not isinstance(profile, dict):
            raise ValueError('Perfil de referencia invalido')
        for key, options in profile.items():
            if not re.fullmatch(r'-?\d+/[123]', key) or not isinstance(options, list) or not options:
                raise ValueError('Turno de referencia invalido')
            for indices in options:
                if not isinstance(indices, list) or not indices or any(not isinstance(n, int) or isinstance(n, bool) or n < 1 for n in indices):
                    raise ValueError('Indices invalidos en referencia')
    return {unit: profiles[profile] for unit, profile in units.items()}, 'Referencia observada: ' + ', '.join(data.get('semanas_base', []))


def metadata(objects, week, cad, unit, complete, reference=None, closed=False):
    """Reglas de presencia separadas de la integridad del contenido."""
    expected = calendario(week)
    rows, controls, items = [], [], defaultdict(list)
    technical = 0
    invalid_names = set()
    for obj in objects:
        key = obj['Key'];name = key.rsplit('/', 1)[-1]
        if not name:
            continue
        if name in {'_SUCCESS', '_metadata', '_common_metadata'}:
            technical += 1
            continue
        row = dict(semana=week, sigla=cad, unidad=unit, archivo=name,
            ruta_s3='s3://' + BUCKET + '/' + key, tamano_bytes=obj['Size'],
            ultima_modificacion=stamp(obj.get('LastModified')), nombre_estado='OK',
            contenido_estado='NO EVALUADO', origen_revision='PENDIENTE', estado='OK', detalle='')
        match = PATTERN.fullmatch(name)
        problems = []
        if not match:
            problems.append('Nombre no cumple Optimizacion_UNIDAD_YYYYMMDD_TURNO_F_Bnn/CPnn.geojson')
        else:
            date, turn, kind, index = match['fecha'], match['turno'], match['clase'], int(match['indice'])
            row.update(fecha=date, turno=turn, clase=kind, indice=index)
            if match['unidad'] != unit:
                problems.append('Unidad del nombre distinta de la carpeta')
            try:
                validar_fecha(date)
                if (date, turn) not in expected:
                    problems.append('Fecha/turno fuera del calendario observado de esta semana')
            except ValueError:
                problems.append('Fecha de nombre invalida')
            if index < 1:
                problems.append('Indice debe ser positivo')
            if not problems:
                items[(date, turn, kind, index)].append(row)
        if problems:
            invalid_names.add(name)
        name_state = 'FALLA' if problems else 'OK'
        if obj['Size'] == 0:
            problems.append('Archivo de cero bytes')
        if problems:
            row.update(nombre_estado=name_state, estado='FALLA', detalle='; '.join(problems))
        rows.append(row)
    if technical:
        controls.append(check(week,cad,unit,'Objetos tecnicos','INFO',f'{technical} marcadores tecnicos excluidos de los productos GeoJSON'))
    for group in items.values():
        if len(group) > 1:
            for row in group:
                row.update(nombre_estado='FALLA', estado='FALLA', detalle='Duplicidad semantica de fecha/turno/clase/indice')
                invalid_names.add(row['archivo'])
    slots = defaultdict(lambda: {'B': set(), 'CP': set()})
    for date, turn, kind, index in items:
        slots[(date, turn)][kind].add(index)
    missing_state = 'FALLA' if closed else 'PENDIENTE'
    if complete:
        if not rows:
            controls.append(check(week, cad, unit, 'Archivos observados', 'AVISO',
                'Sin archivos de optimizacion. No se confirma que esta unidad deba producirlos; no se declara falla de entrega'))
        else:
            absent = sorted(expected - slots.keys())
            if absent:
                controls.append(check(week, cad, unit, 'Cobertura por turnos', missing_state,
                    f'{len(absent)} de 21 fechas/turnos sin archivos: ' + '; '.join(d + '/' + t for d, t in absent)))
    else:
        controls.append(check(week, cad, unit, 'Cobertura por turnos', 'INCOMPLETO', 'Listado parcial: no se calculan ausencias globales'))
    unpaired = 0
    for (date, turn), kinds in sorted(slots.items()):
        for index in sorted(kinds['B'] ^ kinds['CP']):
            unpaired += 1
            absent_kind = 'CP' if index in kinds['B'] else 'B'
            controls.append(check(week, cad, unit, 'Pareja B/CP', missing_state if complete else 'INCOMPLETO',
                f'{date} turno {turn}: falta {absent_kind}{index:02d}; no se exige que exista el indice 01'))
    if complete and reference and rows:
        changed = []
        for (date, turn), kinds in sorted(slots.items()):
            observed = sorted(kinds['B'] | kinds['CP'])
            allowed = reference.get(slot_key(week, date, turn))
            if allowed and observed not in allowed:
                changed.append(f'{date}/{turn}: indices=' + ','.join(f'{i:02d}' for i in observed))
        if changed:
            controls.append(check(week, cad, unit, 'Variantes frente a referencia', 'AVISO',
                'Cambio respecto a semanas observadas; no es contrato de entrega. ' + '; '.join(changed)))
    elif not reference:
        controls.append(check(week, cad, unit, 'Variantes frente a referencia', 'AVISO',
            'Unidad sin referencia historica disponible; no se inventa un numero obligatorio de variantes'))
    bounds = reference_count(reference, week)
    summary = dict(tipo_registro='RESUMEN', semana=week, sigla=cad, unidad=unit,
        cobertura_listado='LEIDA' if complete else 'INCOMPLETO', archivos=len(rows),
        archivos_esperados_min=bounds[0] if bounds else '',
        archivos_esperados_max=bounds[1] if bounds else '',
        base_esperados='REFERENCIA_HISTORICA' if bounds else 'SIN_REFERENCIA_COMPLETA',
        nombres_validos=sum(r['nombre_estado'] == 'OK' for r in rows),
        nombres_invalidos=len(invalid_names),
        archivos_vacios=sum(r['tamano_bytes'] == 0 for r in rows),
        turnos_observados=len(slots), turnos_esperados=21,
        pares_completos=sum(len(k['B'] & k['CP']) for k in slots.values()),
        pares_incompletos=unpaired,
        indices=';'.join(f'{i:02d}' for i in sorted({i for k in slots.values() for values in k.values() for i in values})),
        ultima_modificacion=max((r['ultima_modificacion'] for r in rows), default=''))
    return summary, controls, rows


def reference_count(profile, week):
    """Rango historico de productos B/CP; requiere las 21 fechas/turnos de la semana."""
    if not profile:
        return None
    bounds = [0, 0]
    for date, turn in calendario(week):
        options = profile.get(slot_key(week, date, turn))
        if not options:
            return None
        sizes = [2 * len(set(indices)) for indices in options]
        bounds[0] += min(sizes)
        bounds[1] += max(sizes)
    return tuple(bounds)


def summarize_cads(summaries, a, reference, candidates, controls):
    """Una fila por CAD/semana; la referencia nunca se convierte en contrato de entrega."""
    scoped_reference = {
        key: profile for key, profile in reference.items()
        if (not a.sigla or key.split('/')[0] == a.sigla)
        and (not a.unidad or key.split('/')[1] == a.unidad)}
    cads = {r['sigla'] for r in summaries} | {c[1] for c in candidates}
    cads.update(key.split('/')[0] for key in scoped_reference)
    if a.sigla:
        cads.add(a.sigla)
    grouped = defaultdict(list)
    for row in summaries:
        grouped[(row['semana'], row['sigla'])].append(row)
    candidate_units = defaultdict(set)
    for week, cad, unit in candidates:
        candidate_units[(week, cad)].add(unit)
    result = []
    week = a.desde
    while week <= (a.hasta or semana_siguiente(a.desde)):
        for cad in sorted(cads):
            rows = grouped[(week, cad)]
            observed = {r['unidad'] for r in rows}
            profiles = {key.split('/')[1]: profile for key, profile in scoped_reference.items()
                        if key.split('/')[0] == cad}
            ranges = {unit: reference_count(profile, week) for unit, profile in profiles.items()}
            known = {unit: bounds for unit, bounds in ranges.items() if bounds is not None}
            unknown = observed - known.keys()
            absent = known.keys() - observed
            applicable = [c for c in controls
                          if (not c.get('semana') or c['semana'] == week)
                          and (not c.get('sigla') or c['sigla'] == cad)]
            listed = (all(r['cobertura_listado'] == 'LEIDA' for r in rows)
                      and not (candidate_units[(week, cad)] - observed)
                      and not any(c['estado'] == 'INCOMPLETO' for c in applicable))
            minimum = sum(b[0] for b in known.values()) if known else ''
            maximum = sum(b[1] for b in known.values()) if known else ''
            count = sum(r['archivos'] for r in rows)
            extra_files = sum(r['archivos'] for r in rows if r['unidad'] in unknown)
            notices = [r['estado'] for r in rows] + [c['estado'] for c in applicable]
            if not listed:
                comparison = 'NO EVALUABLE: LISTADO INCOMPLETO'
                notices.append('INCOMPLETO')
            elif not known:
                comparison = 'SIN REFERENCIA COMPLETA'
                notices.append('AVISO')
            elif extra_files or len(known) != len(profiles):
                comparison = 'REFERENCIA PARCIAL'
                notices.append('AVISO')
            elif count < minimum:
                comparison = 'POR DEBAJO DE REFERENCIA'
                notices.append('AVISO')
            elif count > maximum:
                comparison = 'POR ENCIMA DE REFERENCIA'
                notices.append('AVISO')
            else:
                comparison = 'COINCIDE CON REFERENCIA' if minimum == maximum else 'DENTRO DEL RANGO DE REFERENCIA'
            detail = ('Esperados estimados con 2 archivos (B/CP) por indice y fecha/turno de cada unidad '
                      'en el JSON. No es contrato de entrega ni prueba de cobertura o calidad. '
                      f'{len(unknown)} unidades observadas sin referencia completa; '
                      f'{len(absent)} unidades de referencia sin informe de unidad. '
                      f'{extra_files} archivos en unidades sin referencia completa.')
            if len(known) != len(profiles):
                detail += f' {len(profiles)-len(known)} perfiles sin las 21 fechas/turnos; excluidos de los esperados.'
            if not listed:
                detail += ' Cantidades observadas parciales; no se calculan faltantes.'
            summary = dict(tipo_registro='RESUMEN_SIGLA', semana=week, sigla=cad, unidad='',
                estado=worst(notices), cobertura_listado='LEIDA' if listed else 'INCOMPLETO',
                archivos=count, archivos_esperados_min=minimum, archivos_esperados_max=maximum,
                base_esperados='REFERENCIA_HISTORICA' if known and len(known) == len(profiles)
                    else 'REFERENCIA_HISTORICA_PARCIAL' if known else 'SIN_REFERENCIA_COMPLETA',
                comparacion_referencia=comparison, unidades_observadas=len(observed),
                unidades_referencia=len(known), unidades_sin_referencia=len(unknown),
                unidades_referencia_no_observadas=len(absent), detalle=detail)
            for name in ('nombres_validos', 'nombres_invalidos', 'archivos_vacios',
                         'pares_completos', 'pares_incompletos', 'contenido_ok',
                         'contenido_con_hallazgos', 'contenido_pendiente', 'lecturas_nuevas', 'lecturas_cache'):
                summary[name] = sum(r.get(name, 0) for r in rows)
            result.append(summary)
        week = semana_siguiente(week)
    return result


def numeric(value):
    return (isinstance(value, int) and not isinstance(value, bool)
        or isinstance(value, float) and math.isfinite(value))


def unit_id(value):
    return str(int(value)) if numeric(value) and int(value) == value else str(value).strip()


def inspect_geojson(raw, cad, unit, index):
    """Inspeccion estructural; no interpreta el significado estadistico ni ejecuta codigo."""
    errors, notices = Counter(), Counter()
    def invalid_constant(value):
        raise ValueError('Constante JSON no finita')
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Clave JSON repetida')
            result[key] = value
        return result
    try:
        data = json.loads(raw.decode('utf-8-sig'), parse_constant=invalid_constant, object_pairs_hook=unique_pairs)
    except (ValueError, UnicodeError, RecursionError):
        return dict(contenido_estado='FALLA', estado='FALLA', detalle='JSON ilegible, ambiguo, no finito o demasiado anidado')
    if not isinstance(data, dict) or data.get('type') != 'FeatureCollection' or not isinstance(data.get('features'), list):
        return dict(contenido_estado='FALLA', estado='FALLA', detalle='Se requiere FeatureCollection con features en lista')
    geometries = Counter();invalid_rings=invalid_coords=null_geometries=wrong_unit=wrong_cad=0
    intensities=[]
    stack=[data];elements=0
    while stack:
        value=stack.pop();elements+=1
        if elements > 1000000:
            return dict(contenido_estado='NO EVALUADO', estado='INCOMPLETO', detalle='Contenido supera presupuesto de elementos')
        if isinstance(value, float) and not math.isfinite(value):
            errors['Numeros JSON no finitos'] += 1
        elif isinstance(value, dict):stack.extend(value.values())
        elif isinstance(value, list):stack.extend(value)
    def position(p):
        return (isinstance(p,list) and 2 <= len(p) <= 3 and all(numeric(v) for v in p)
            and -180 <= p[0] <= 180 and -90 <= p[1] <= 90)
    for feature in data['features']:
        if not isinstance(feature,dict) or feature.get('type')!='Feature' or 'properties' not in feature or 'geometry' not in feature:
            errors['Feature sin estructura requerida']+=1;continue
        props=feature['properties']
        if props is not None and not isinstance(props,dict):
            errors['properties no es objeto/null']+=1;continue
        props=props or {}
        if props.get('id_unidadbase') is None:notices['id_unidadbase ausente/nulo']+=1
        elif unit_id(props['id_unidadbase'])!=unit:wrong_unit+=1
        if 'CAD' in props and props['CAD'] is not None and str(props['CAD']).strip()!=cad:wrong_cad+=1
        if 'intensity_val' not in props:notices['intensity_val ausente']+=1
        for field in ('intensity_val','sum_delito_area'):
            if field not in props:continue
            val=props[field]
            if not numeric(val):notices[field+' no numerico/nulo']+=1
            else:
                if val < 0:notices[field+' negativo; regla de negocio por confirmar']+=1
                if field=='intensity_val':intensities.append(val)
        geometry=feature['geometry']
        if geometry is None:
            null_geometries+=1;notices['Geometria nula; revisar utilidad del resultado']+=1;continue
        if not isinstance(geometry,dict) or 'type' not in geometry:
            errors['Geometria mal formada']+=1;continue
        kind=geometry['type']
        if not isinstance(kind,str):
            errors['Tipo de geometria invalido']+=1;continue
        geometries[kind]+=1
        coords=geometry.get('coordinates')
        if kind not in ('Polygon','MultiPolygon'):
            notices['Geometria distinta de Polygon/MultiPolygon; revision parcial']+=1;continue
        polygons=[coords] if kind=='Polygon' else coords
        if not isinstance(polygons,list) or not polygons:
            errors['Coordenadas poligonales vacias/invalidas']+=1;continue
        for polygon in polygons:
            if not isinstance(polygon,list) or not polygon:
                invalid_rings+=1;continue
            for ring in polygon:
                if not isinstance(ring,list):invalid_rings+=1;continue
                if len(ring)<4 or ring[0]!=ring[-1]:invalid_rings+=1
                invalid_coords+=sum(not position(p) for p in ring)
    if not data['features']:notices['FeatureCollection vacia']+=1
    if len(data['features'])!=index:notices['Cantidad de features distinta del indice; regla observada sin contrato']+=1
    if wrong_unit:errors['id_unidadbase distinto de la ruta']+=wrong_unit
    if wrong_cad:errors['CAD distinto de la ruta']+=wrong_cad
    if invalid_rings:errors['Anillos abiertos/cortos o invalidos']+=invalid_rings
    if invalid_coords:errors['Coordenadas invalidas/no finitas/fuera de rango']+=invalid_coords
    state='FALLA' if errors else 'AVISO' if notices else 'OK'
    coverage='NO EVALUADO' if any('revision parcial' in k for k in notices) else state
    return dict(contenido_estado=coverage, estado=state if coverage!='NO EVALUADO' or errors else 'INCOMPLETO',
        features=len(data['features']),tipos_geometria=';'.join(f'{k}:{v}' for k,v in sorted(geometries.items())),
        geometrias_nulas=null_geometries,anillos_invalidos=invalid_rings,coordenadas_invalidas=invalid_coords,
        unidad_distinta=wrong_unit,cad_distinto=wrong_cad,
        intensidad_min=min(intensities) if intensities else '',intensidad_max=max(intensities) if intensities else '',
        detalle='; '.join(f'{k} ({v})' for collection in (errors,notices) for k,v in collection.items()) or 'Estructura, identificadores, poligonos y numericos revisados; sin prueba topologica completa')


def atomic_json(path, data):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.'+uuid4().hex+'.tmp')
    try:
        temporary.write_text(json.dumps(data,ensure_ascii=False,separators=(',',':')),encoding='utf-8')
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def cache_load(path):
    if not path.exists():return {},'',False
    try:
        data=json.loads(path.read_text())
        if not isinstance(data,dict):raise ValueError('estructura')
        if data.get('policy')!=POLICY or data.get('bucket')!=BUCKET:return {},'Cache anterior; se vuelve a evaluar',False
        if not isinstance(data.get('archivos'),dict):raise ValueError('estructura')
        for name,entry in data['archivos'].items():
            if (not isinstance(entry,dict) or not isinstance(entry.get('huella'),list)
                    or not isinstance(entry.get('resultado'),dict)
                    or entry['resultado'].get('estado') not in LEVELS
                    or entry['resultado'].get('contenido_estado') not in LEVELS):
                raise ValueError('entrada de cache invalida')
        count=data.get('archivos_observados',0)
        observed=(data.get('tuvo_archivos') is True or bool(data['archivos'])
            or isinstance(count,int) and not isinstance(count,bool) and count>0)
        return data['archivos'],'',observed
    except (ValueError,OSError,TypeError):
        return {},'Cache ilegible; no se reutiliza',False


def fingerprint(obj):
    return [POLICY,obj['Size'],stamp(obj.get('LastModified')),obj.get('ETag','')]


def list_unit(reader, candidate):
    """Inventario sin GET: se completa para todas las unidades antes de leer GeoJSON."""
    week,cad,unit=candidate
    prefix=f'{ROOT_PREFIX}/{week[:4]}/{week}/{cad}/{unit}/Optimizacion/'
    objects=[];controls=[];complete=True
    try:
        for page in reader.pages(prefix):
            objects.extend(o for o in page.get('Contents',[]) if o['Key'].startswith(prefix) and '/' not in o['Key'][len(prefix):])
            if page.get('CommonPrefixes'):
                controls.append(check(week,cad,unit,'Subcarpetas','AVISO','Hay subcarpetas no evaluadas; alcance limitado a hijos directos'))
    except (ReadFailure,BudgetStop) as exc:
        complete=False;controls.append(check(week,cad,unit,'Listado S3','INCOMPLETO',str(exc)))
    return dict(objects=objects,controls=controls,complete=complete)


def prepare_listing(reader, candidate, temporary_root):
    """Guarda metadata comprimida en /tmp para no retener el inventario nacional en RAM."""
    listing=list_unit(reader,candidate)
    week,cad,unit=candidate
    path=temporary_root/week/cad/(unit+'.json.gz')
    path.parent.mkdir(parents=True,exist_ok=True)
    with gzip.open(path,'wt',encoding='utf-8') as handle:
        json.dump(listing,handle,default=stamp,separators=(',',':'))
    return path,listing['complete']


def inspect_unit(reader, candidate, a, cache_root, reference, listing_path=None):
    week,cad,unit=candidate
    cache_path=cache_root/week/cad/(unit+'.json')
    cached,cache_note,observed_before=cache_load(cache_path);next_cache={}
    if listing_path is None:
        listing=list_unit(reader,candidate)
    else:
        try:
            with gzip.open(listing_path,'rt',encoding='utf-8') as handle:
                listing=json.load(handle)
        except (OSError,ValueError) as exc:
            raise ReadFailure('Inventario temporal ilegible; no se declara ausencia de archivos') from exc
    objects,controls,complete=listing['objects'],listing['controls'],listing['complete']
    summary,checks,rows=metadata(objects,week,cad,unit,complete,reference.get(cad+'/'+unit),a.ciclo_cerrado)
    controls.extend(checks)
    if cache_note:controls.append(check(week,cad,unit,'Cache local','AVISO',cache_note))
    obj_by_name={o['Key'].rsplit('/',1)[-1]:o for o in objects}
    counts=Counter();last_save=0
    def save():
        preserved={name:entry for name,entry in cached.items() if not complete or name in obj_by_name}
        preserved.update(next_cache)
        reviewed=complete and (a.solo_metadata or all(r['estado']=='FALLA' or r['contenido_estado']!='NO EVALUADO' for r in rows))
        atomic_json(cache_path,dict(policy=POLICY,bucket=BUCKET,archivos=preserved,
            revision_completa=reviewed,solo_metadata=a.solo_metadata,
            archivos_observados=len(rows),tuvo_archivos=observed_before or bool(rows)))
    try:
        for row in rows:
            if row['estado']=='FALLA':
                row['contenido_estado']='NO EVALUADO'
                row['origen_revision']='NOMBRE_INVALIDO' if row['nombre_estado']=='FALLA' else 'ARCHIVO_VACIO'
                continue
            obj=obj_by_name[row['archivo']];old=cached.get(row['archivo'],{})
            result=None
            if a.solo_metadata:
                result=dict(contenido_estado='NO EVALUADO',estado='NO EVALUADO',detalle='Modo solo metadata; contenido no consultado')
                row['origen_revision']='SOLO_METADATA'
                if old:next_cache[row['archivo']]=old
            elif not a.sin_cache and obj.get('ETag') and old.get('huella')==fingerprint(obj) and isinstance(old.get('resultado'),dict):
                result=old['resultado'];row['origen_revision']='CACHE';counts['cache']+=1
                next_cache[row['archivo']]=old
            elif obj['Size']>a.max_mib*1024**2:
                result=dict(contenido_estado='NO EVALUADO',estado='INCOMPLETO',detalle='Archivo supera --max-mib; contenido pendiente')
                row['origen_revision']='LIMITE_TAMANO'
            else:
                try:
                    raw=reader.get(obj,a.max_mib*1024**2)
                    result=inspect_geojson(raw,cad,unit,row['indice']);row['origen_revision']='GET';counts['nuevas']+=1
                    next_cache[row['archivo']]=dict(huella=fingerprint(obj),resultado=result)
                    if counts['nuevas']-last_save>=25:
                        save();last_save=counts['nuevas']
                except (ReadFailure,BudgetStop) as exc:
                    result=dict(contenido_estado='NO EVALUADO',estado='INCOMPLETO',detalle=str(exc))
                    row['origen_revision']='LECTURA_PENDIENTE'
                    if old:next_cache[row['archivo']]=old
            row.update(result)
    finally:
        try:
            save()
        except OSError:
            controls.append(check(week,cad,unit,'Cache local','INCOMPLETO','No se pudo guardar cache; siguientes ejecuciones repetiran lecturas'))
    summary.update(contenido_ok=sum(r['contenido_estado']=='OK' for r in rows),
        contenido_con_hallazgos=sum(r['contenido_estado'] in ('AVISO','FALLA') for r in rows),
        contenido_pendiente=sum(r['estado']!='FALLA' and r['contenido_estado']=='NO EVALUADO' for r in rows),
        lecturas_nuevas=counts['nuevas'],lecturas_cache=counts['cache'])
    for name,count in (('Archivos con nombre/integridad defectuosos',sum(r['estado']=='FALLA' for r in rows)),
                       ('Contenido pendiente',summary['contenido_pendiente'])):
        if count:
            controls.append(check(week,cad,unit,name,'FALLA' if name.startswith('Archivos') else 'NO EVALUADO',f'{count} archivos; detalle en detalle.csv'))
    summary['estado']=worst([r['estado'] for r in rows]+[r['estado'] for r in controls])
    summary['detalle']='Evidencia observada; presencia y contenido no certifican calidad del algoritmo'
    return summary,controls,rows


def csv_writer(path, fields):
    handle=path.open('w',encoding='utf-8-sig',newline='')
    writer=csv.DictWriter(handle,fieldnames=fields,extrasaction='ignore');writer.writeheader()
    return handle,writer


def print_table(summaries, detailed=False):
    groups=defaultdict(list)
    for row in summaries:groups[(row['semana'],row['sigla'],row['unidad']) if detailed else (row['sigla'],)].append(row)
    header=('Semana / CAD / Unidad' if detailed else 'CAD','Unidad-semana','Listados',
        'Archivos','GeoJSON OK','GeoJSON obs.','Pendientes','Control')
    table=[]
    for key,rs in sorted(groups.items()):
        listed=sum(r['cobertura_listado']=='LEIDA' for r in rs)
        def count(name):
            total=sum(r.get(name,0) for r in rs)
            return str(total) if listed==len(rs) else '>='+str(total) if total else 'N/V'
        table.append(('/'.join(key),str(len(rs)),f'{listed}/{len(rs)}',count('archivos'),
            count('contenido_ok'),count('contenido_con_hallazgos'),count('contenido_pendiente'),
            worst(r['estado'] for r in rs)))
    widths=[max([len(header[i])]+[len(r[i]) for r in table]) for i in range(len(header))]
    print(' | '.join(v.ljust(w) for v,w in zip(header,widths)))
    print('-+-'.join('-'*w for w in widths))
    for row in table:print(' | '.join(v.ljust(w) for v,w in zip(row,widths)))


def candidate_priority(candidate, cache_root, metadata_only):
    """Primero unidades pendientes; evita repetir siempre las primeras al agotarse el tiempo."""
    week,cad,unit=candidate
    path=cache_root/week/cad/(unit+'.json')
    try:
        data=json.loads(path.read_text())
        reviewed=(isinstance(data,dict) and data.get('bucket')==BUCKET
            and data.get('policy')==POLICY and data.get('revision_completa') is True
            and (metadata_only or data.get('solo_metadata') is False))
    except (OSError,ValueError):
        reviewed=False
    return (1 if reviewed else 0,week,cad,unit)


def run(reader, a, folder, cache_root, reference):
    candidates,global_controls,weeks=discover(reader,a)
    # Una carpeta observada antes que desaparezca tambien se vuelve a consultar.
    for week in weeks:
        for path in (cache_root/week).glob('*/*.json'):
            cad,unit=path.parent.name,path.stem
            if re.fullmatch(r'[A-Z0-9_-]{1,30}',cad) and re.fullmatch(r'\d{1,20}',unit) and (not a.sigla or cad==a.sigla) and (not a.unidad or unit==a.unidad):
                _,cache_note,observed=cache_load(path)
                if observed:
                    candidates.append((week,cad,unit))
                elif cache_note and (week,cad,unit) not in candidates:
                    global_controls.append(check(week,cad,unit,'Cache local','AVISO',cache_note+'; no acredita archivos previos de esta unidad'))
    candidates=sorted(set(candidates),key=lambda c:candidate_priority(c,cache_root,a.solo_metadata))
    global_controls.append(check('','','','Alcance','INFO',
        'Solo unidades observadas, solicitadas o conservadas en cache; sin plan esperado no identifica todas las ausencias. Referencia historica no acredita elegibilidad'))
    if not candidates:
        global_controls.append(check('',a.sigla or '',a.unidad or '','Unidades para revisar','INCOMPLETO','No hay unidades revisables; no se declara salud'))
    summaries=[];cad_summaries=[];interrupted=False;future_map={}
    gh,gw=csv_writer(folder/'general.csv',GENERAL_FIELDS);dh,dw=csv_writer(folder/'detalle.csv',DETAIL_FIELDS)
    try:
        gw.writerows(global_controls)
        with tempfile.TemporaryDirectory(prefix='optimizacion_inventario_') as temporary:
            temporary_root=Path(temporary);listings={}
            executor=ThreadPoolExecutor(max_workers=a.trabajadores)
            try:
                print(f'[INFO] Fase 1/2: listar {len(candidates)} unidades-semana; sin leer contenido.',flush=True)
                future_map={executor.submit(prepare_listing,reader,c,temporary_root):c for c in candidates}
                last_progress=time.monotonic();complete_listings=0
                try:
                    for future in as_completed(future_map):
                        candidate=future_map.pop(future)
                        path,complete=future.result();listings[candidate]=path
                        complete_listings+=int(complete)
                        if time.monotonic()-last_progress>=20:
                            print(f'[INFO] Listados preparados: {len(listings)}/{len(candidates)}; completos: {complete_listings}',flush=True)
                            last_progress=time.monotonic()
                    print(f'[INFO] Fase 1/2 terminada: {complete_listings}/{len(candidates)} listados completos.',flush=True)
                    print('[INFO] Fase 2/2: revisar GeoJSON y reutilizar cache; los totales del inventario se conservan.',flush=True)
                    future_map={executor.submit(inspect_unit,reader,c,a,cache_root,reference,listings[c]):c for c in candidates}
                    last_progress=time.monotonic()
                    for future in as_completed(future_map):
                        future_map.pop(future)
                        summary,controls,rows=future.result()
                        summaries.append(summary);gw.writerow(summary);gw.writerows(controls);dw.writerows(rows)
                        if time.monotonic()-last_progress>=20:
                            print(f'[INFO] Unidades-semana revisadas: {len(summaries)}/{len(candidates)}; GET nuevos: {reader.budget.reads}',flush=True)
                            gh.flush();dh.flush();last_progress=time.monotonic()
                except KeyboardInterrupt:
                    interrupted=True;reader.budget.stop()
                    for future in future_map:future.cancel()
                    global_controls.append(check('','','','Ejecucion','INCOMPLETO','Interrumpida; conservar cache y repetir comando'))
                    gw.writerow(global_controls[-1])
            finally:
                executor.shutdown(wait=True,cancel_futures=interrupted)
        observed={(r['semana'],r['sigla'],r['unidad']) for r in summaries}
        omitted=len(set(candidates)-observed)
        if omitted:
            global_controls.append(check('','','','Cobertura de unidades','INCOMPLETO',f'{omitted} unidades-semana no incluidas en este informe'))
            gw.writerow(global_controls[-1])
        cad_summaries=summarize_cads(summaries,a,reference,candidates,global_controls)
        gw.writerows(cad_summaries)
    finally:
        gh.close();dh.close()
    context=dict(captura_bogota=datetime.now(BOGOTA).isoformat(),policy=POLICY,version=SCRIPT_VERSION,
        desde=a.desde,hasta=a.hasta or semana_siguiente(a.desde),semanas=weeks,unidades_descubiertas=len(candidates),
        unidades_con_informe=len(summaries),unidades_listado_completo=sum(r['cobertura_listado']=='LEIDA' for r in summaries),
        lecturas_nuevas=reader.budget.reads,
        ciclo_cerrado=a.ciclo_cerrado,solo_metadata=a.solo_metadata,
        controles_generales=global_controls,resumen_siglas=cad_summaries,cache=str(cache_root),
        listado_antes_contenido=True)
    atomic_json(folder/'contexto.json',context)
    states={r['estado'] for r in summaries+cad_summaries+global_controls}
    code=130 if interrupted else 1 if 'FALLA' in states else 3 if states & {'INCOMPLETO','NO EVALUADO'} else 0
    return summaries,context,code


def upload(s3, a, folder):
    bucket,prefix=report_destination(a.reportes_s3)
    prefix+=folder.parent.name+'/'+folder.name+'/'
    for name in ('general.csv','detalle.csv'):
        with (folder/name).open('rb') as body:
            s3.upload_fileobj(body,bucket,prefix+name,ExtraArgs={
                'ExpectedBucketOwner':OWNER,'ContentType':'text/csv; charset=utf-8'})
    return 's3://'+bucket+'/'+prefix


def blocked_reports(folder, message):
    """Una lectura que no arranca tambien deja un control explicito en los CSV."""
    gh,gw=csv_writer(folder/'general.csv',GENERAL_FIELDS)
    try:
        gw.writerow(check('','','','Inicio de revision','INCOMPLETO',message))
    finally:
        gh.close()
    dh,_=csv_writer(folder/'detalle.csv',DETAIL_FIELDS);dh.close()
    atomic_json(folder/'contexto.json',dict(estado='INCOMPLETO',detalle=message))


def main(argv=None):
    a=arguments(argv)
    now=datetime.now(BOGOTA)
    folder=a.salida.expanduser()/'optimizacion'/f'{now:%Y-%m-%d}'/f'{now:%H-%M-%S_%f}'
    try:
        folder.mkdir(parents=True,exist_ok=False)
    except OSError:
        print('[BLOQUEADO] No se pudo crear el directorio local de informes',file=sys.stderr)
        return 3
    cache_root=a.salida.expanduser()/'optimizacion'/'.estado'
    reference_path=a.referencia or Path(__file__).resolve().with_name('referencia_optimizacion.json')
    try:
        reference,note=load_reference(reference_path)
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except (ValueError,OSError,ImportError) as exc:
        print('[BLOQUEADO] '+type(exc).__name__+': revisar referencia local o disponibilidad de boto3',file=sys.stderr)
        blocked_reports(folder,type(exc).__name__+': revisar referencia local o boto3; datos no evaluados')
        return 3
    errors=(BotoCoreError,ClientError)
    try:
        session=boto3.Session(profile_name=a.perfil,region_name=a.region)
        config=Config(connect_timeout=5,read_timeout=20,max_pool_connections=a.trabajadores,
            retries={'mode':'standard','total_max_attempts':3})
        account=session.client('sts',config=config).get_caller_identity()['Account']
        if account!=OWNER:
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan resultados',file=sys.stderr)
            blocked_reports(folder,'Cuenta distinta de la esperada; datos no evaluados')
            return 3
        s3=session.client('s3',config=config)
        print(f'OPTIMIZACION | v{SCRIPT_VERSION} | {BUCKET} | Semanas {a.desde} a {a.hasta} | Solo lectura de resultados',flush=True)
        print('[INFO] '+note,flush=True)
        print(f'[INFO] Presupuesto: {a.max_segundos} s entre llamadas y {a.max_lecturas} GET nuevos; cache local reutilizable.',flush=True)
        reader=Reader(s3,Budget(a.max_segundos,a.max_lecturas),errors)
        summaries,context,code=run(reader,a,folder,cache_root,reference)
        print_table(summaries,a.mostrar_todas)
        print('GeoJSON obs.: contenido con AVISO/FALLA. N/V o >=: conteos desconocidos o parciales por listado incompleto.')
        global_issues=[c for c in context['controles_generales'] if c['estado'] not in ('INFO','OK')]
        for control in global_issues[:3]:
            scope='/'.join(control.get(k,'') for k in ('semana','sigla','unidad') if control.get(k))
            print(f"[{control['estado']}] {scope+' | ' if scope else ''}{control['control']}: {control['detalle']}")
        if len(global_issues)>3:
            print(f'[INFO] Otros {len(global_issues)-3} controles generales en general.csv')
        print(f"Siglas: {len({r['sigla'] for r in summaries})} | Unidades distintas: {len({(r['sigla'],r['unidad']) for r in summaries})} | Unidades-semana: {len(summaries)}")
        print(f"Listados de unidad completos: {context['unidades_listado_completo']}/{len(summaries)}")
        print(f"Contenido: {sum(r['contenido_ok']+r['contenido_con_hallazgos'] for r in summaries)} revisados; "
              f"{sum(r['lecturas_cache'] for r in summaries)} desde cache; GET nuevos intentados: {context['lecturas_nuevas']}")
        if context['unidades_listado_completo']<len(summaries) or (not a.solo_metadata and any(r['contenido_pendiente'] for r in summaries)):
            print('[INFO] Repetir el comando para continuar las lecturas pendientes; conserva .estado/.')
        elif a.solo_metadata:
            print('[INFO] Solo metadata: para evaluar contenido, ejecutar sin --solo-metadata. Cache anterior conservada.')
        print('Faltantes sin cierre confirmado: PENDIENTE. Sin archivos no demuestra que la unidad deba producir optimizacion.')
        print(f'Informes locales: {folder}/general.csv y detalle.csv',flush=True)
        print('general.csv: filtrar tipo_registro=RESUMEN_SIGLA para esperados de referencia y encontrados por semana.')
        if not a.sin_subir:
            try:
                print('[S3] '+upload(s3,a,folder),flush=True)
            except errors+(OSError,) as exc:
                print('[INCOMPLETO] No se terminaron de subir los CSV: '+error_code(exc)+'; copias locales conservadas',file=sys.stderr)
                context['subida_s3']='INCOMPLETO';atomic_json(folder/'contexto.json',context)
                if code not in (1,130):code=3
        return code
    except errors+(ReadFailure,OSError) as exc:
        print('[BLOQUEADO] '+error_code(exc)+'; revisar sesion/permisos o archivos locales',file=sys.stderr)
        if not (folder/'general.csv').exists():
            blocked_reports(folder,error_code(exc)+'; datos no evaluados')
        return 3
    except KeyboardInterrupt:
        print('[INCOMPLETO] Interrumpido; conservar .estado y repetir el comando',file=sys.stderr)
        return 130


if __name__=='__main__':
    sys.exit(main())
