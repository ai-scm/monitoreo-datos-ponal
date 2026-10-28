#!/usr/bin/env python3
"""Consulta manual del job Glue de informes de anticipaciones; solo lectura.

CloudShell: python3 ~/monitor_glue_job_informes.py
Una tabla y un unico informe_glue_job_informes.csv local, con RESUMEN/DETALLE/CONTROL/CONTEXTO.
No ejecuta jobs, no modifica AWS ni valida contenido o entrega de informes.
Exit: 0 lectura completa (puede haber fallos observados); 1 recurso ausente;
2 argumentos; 3 lectura incompleta/bloqueo; 130 interrupcion.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
import textwrap


JOBS = {
    'pon-prod-xcrime-informes-anticipaciones': 'Informes de anticipaciones',
}
BOGOTA = timezone(timedelta(hours=-5))
FAILURES = {'FAILED', 'TIMEOUT', 'ERROR', 'EXPIRED'}
ACTIVE = {'STARTING', 'RUNNING', 'WAITING', 'STOPPING'}
KNOWN_STATES = FAILURES | ACTIVE | {'STOPPED', 'SUCCEEDED'}
ERROR_TYPES = ('AccessDeniedException', 'EntityNotFoundException', 'ResourceNotFoundException',
    'ThrottlingException', 'OutOfMemoryError', 'AnalysisException', 'ModuleNotFoundError',
    'ImportError', 'KeyError', 'ValueError', 'TypeError', 'RuntimeError', 'ConnectionError',
    'TimeoutError', 'FileNotFoundError', 'NoSuchKey', 'Py4JJavaError')
LIMITS = [
    'Solo STS/GetJobRuns; no ejecuta jobs, crea alarmas, lee S3 ni cambia configuracion.',
    'Nombres iniciales del inventario del 22-sep-2026; existencia actual sujeta a consulta real.',
    'Por defecto, muestra limitada por job; --historial-completo agota las paginas disponibles sin limite de runs.',
    'La lectura paginada no es una captura atomica: el estado puede cambiar durante la consulta.',
    'Ventana: inicios o fines dentro del periodo, mas ejecuciones aun abiertas aunque iniciaran antes.',
    'Ultima ejecucion puede estar fuera de ventana; su estado se separa de los conteos de la ventana.',
    'Estado SUCCEEDED no certifica contenido correcto, disponibilidad ni entrega del informe.',
    'No correlaciona automaticamente el job con la Lambda de informes ni con una ejecucion de prediccion.',
    'Sin calendario/SLA confirmado: no clasifica ausencia de ejecuciones como disparo perdido ni impone umbrales de duracion.',
    'Inicio/fin y tiempo transcurrido son distintos de ExecutionTime (segundos de recursos Glue).',
    'No exporta Arguments, politicas, conexiones ni mensajes completos; solo clases/categorias de error reconocidas.',
    'Reportes locales por ejecucion; sin almacenamiento continuo ni envio automatico de alertas.'
]


def aware(value):
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def stamp(value):
    return value.astimezone(BOGOTA).isoformat() if aware(value) else ''


def duration(start, end):
    return round((end-start).total_seconds(), 3) if aware(start) and aware(end) and end >= start else None


def error_hint(message):
    text = str(message or '')
    found = [name for name in ERROR_TYPES if re.search(r'(?<!\w)'+re.escape(name)+r'(?!\w)', text)]
    category = re.search(r'Error Category:\s*([A-Z_]{1,80})', text)
    if category:
        found.append(category.group(1))
    return ', '.join(found) or ('ErrorMessage presente; revisar el run en Glue' if text else '')


def severity(state):
    return 'FALLA' if state in FAILURES else 'AVISO' if state == 'STOPPED' or state not in KNOWN_STATES else 'INFO'


def collect_runs(glue, name, limit):
    """No cortar por antiguedad: un run antiguo puede seguir abierto o finalizar en ventana."""
    rows = []
    pagination = {'PageSize': 200}
    if limit is not None:
        pagination.update(MaxItems=limit+1, PageSize=min(limit+1, 200))
    # Conservar solo metadata necesaria, evitando acumular Arguments/politicas del historial.
    fields = ('Id', 'Attempt', 'JobRunState', 'StartedOn', 'CompletedOn', 'ExecutionTime', 'ErrorMessage')
    for page in glue.get_paginator('get_job_runs').paginate(JobName=name,
            PaginationConfig=pagination):
        rows.extend({key: run[key] for key in fields if key in run} for run in page.get('JobRuns', []))
    return (rows, False) if limit is None else (rows[:limit], len(rows) > limit)


def inspect_job(glue, name, now, start, limit, errors):
    role = JOBS.get(name, 'Job configurado')
    notes, details = [], []
    summary = dict(job=name, rol=role, run_id='', estado='NO DISPONIBLE', control='INFO',
        inicio='', fin='', duracion_segundos=None, duracion_tipo='', execution_time_segundos=None,
        estados_ventana='', ejecuciones_ventana=None, fallas_ventana=None,
        ultimo_exito_observado='', error='')
    def note(state, text):
        notes.append(dict(job=name, rol=role, control=state, detalle=text))
    try:
        raw, truncated = collect_runs(glue, name, limit)
    except errors as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
        missing = code in ('EntityNotFoundException', 'ResourceNotFoundException')
        summary.update(control='AUSENTE' if missing else 'INCOMPLETO')
        note(summary['control'], 'GetJobRuns: '+code)
        return summary, details, notes, dict(recurso=name, lectura=summary['control'], codigo=code)
    if truncated:
        note('INCOMPLETO', f'Historial limitado a {limit} runs; hay mas disponibles. No se certifica cobertura completa de la ventana')
    runs, seen = [], set()
    for run in raw:
        key = (run.get('Id'), run.get('Attempt'))
        if key[0] and key in seen:
            continue
        if key[0]:
            seen.add(key)
        runs.append(run)
    counts, success_ends = Counter(), []
    for index, run in enumerate(runs):
        state = run.get('JobRunState', 'DESCONOCIDO')
        began, ended = run.get('StartedOn'), run.get('CompletedOn')
        active = state in ACTIVE
        valid = aware(began) and began <= now and (ended is None or aware(ended) and began <= ended <= now)
        if not active and ended is None:
            note('INCOMPLETO', 'Run '+str(run.get('Id', '?'))+': estado terminal sin CompletedOn; fin y cobertura temporal no verificables')
        if active and ended is not None:
            note('INCOMPLETO', 'Run '+str(run.get('Id', '?'))+': estado abierto con CompletedOn; revisar captura')
        if not valid:
            note('INCOMPLETO', 'Run '+str(run.get('Id', '?'))+': tiempos ausentes, incoherentes o futuros')
        inside = valid and (began >= start or aware(ended) and ended >= start or active)
        if inside:
            counts[state] += 1
        if valid and state == 'SUCCEEDED' and aware(ended):
            success_ends.append(ended)
        # Nunca inventar duracion final de un run terminal que no tiene CompletedOn.
        seconds = duration(began, now if active and ended is None else ended) if valid else None
        kind = 'HASTA CAPTURA' if active and ended is None else 'INICIO A FIN' if seconds is not None else 'NO EVALUADA'
        hint = error_hint(run.get('ErrorMessage'))
        run_control = severity(state)
        if index == 0 or inside or not valid:
            details.append(dict(job=name, rol=role, run_id=run.get('Id', ''), estado=state,
                control=run_control, inicio=stamp(began), fin=stamp(ended), duracion_segundos=seconds,
                duracion_tipo=kind, execution_time_segundos=run.get('ExecutionTime'),
                intento=run.get('Attempt'), en_ventana='SI' if inside else 'NO' if valid else 'NO EVALUADO',
                es_ultima='SI' if index == 0 else 'NO', error=hint))
        if (inside or index == 0) and run_control != 'INFO':
            scope = 'en ventana' if inside else 'ultima observada fuera de ventana'
            note(run_control, f"Run {run.get('Id', '?')}: {state}; {scope}; inicio={stamp(began)}"+('; '+hint if hint else ''))
        if index == 0:
            summary.update(run_id=run.get('Id', ''), estado=state, inicio=stamp(began), fin=stamp(ended),
                duracion_segundos=seconds, duracion_tipo=kind,
                execution_time_segundos=run.get('ExecutionTime'), error=hint)
    if not runs:
        summary.update(estado='SIN EJECUCIONES')
        note('INFO', 'Glue no devolvio runs; no se infiere que haya faltado una ejecucion esperada')
    elif not counts:
        note('INFO', 'No se observaron ejecuciones en ventana ni abiertas; no hay frecuencia esperada configurada')
    summary.update(estados_ventana=json.dumps(dict(sorted(counts.items())), ensure_ascii=False),
        ejecuciones_ventana=sum(counts.values()), fallas_ventana=sum(counts[s] for s in FAILURES),
        ultimo_exito_observado=stamp(max(success_ends)) if success_ends else '')
    levels = {n['control'] for n in notes}
    summary['control'] = 'INCOMPLETO' if 'INCOMPLETO' in levels else 'FALLA' if 'FALLA' in levels else 'AVISO' if 'AVISO' in levels else 'INFO'
    context = dict(recurso=name, rol=role, lectura='PARCIAL' if 'INCOMPLETO' in levels else 'LEIDA',
        runs_recibidos=len(raw), runs_distintos=len(runs), truncado=truncated,
        modo_historial='COMPLETO' if limit is None else 'LIMITADO',
        estados_ventana=dict(counts), ejecuciones_ventana=sum(counts.values()),
        ultimo_exito_observado=summary['ultimo_exito_observado'])
    return summary, details, notes, context


def pretty_duration(seconds, kind):
    if seconds is None:
        return 'N/V'
    seconds = int(seconds)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f'{hours}h {minutes:02}m {seconds:02}s'+(' *' if kind == 'HASTA CAPTURA' else '')


def print_table(rows):
    widths = (40, 13, 19, 19, 15, 31, 20, 11)
    def render(values):
        cells = [textwrap.wrap(str(value), width=width, break_on_hyphens=False) or ['-']
                 for value, width in zip(values, widths)]
        for i in range(max(map(len, cells))):
            print(' | '.join((cell[i] if i < len(cell) else '').ljust(width) for cell, width in zip(cells, widths)))
    render(('Job', 'Ultimo estado', 'Inicio Bogota', 'Fin Bogota', 'Duracion', 'Estados en ventana', 'Error ultimo', 'Control'))
    print('-+-'.join('-'*width for width in widths))
    for row in rows:
        counts = json.loads(row['estados_ventana']) if row['estados_ventana'] else None
        text = '; '.join(f'{key}={value}' for key, value in counts.items()) if counts else 'SIN EJECUCIONES' if counts == {} else 'NO EVALUADO'
        render((row['job'], row['estado'], row['inicio'][:19], row['fin'][:19],
                pretty_duration(row['duracion_segundos'], row['duracion_tipo']), text, row['error'] or '-', row['control']))


def write_report(folder, summaries, details, notes, context):
    path = folder/'informe_glue_job_informes.csv'
    fields = ['tipo_registro', 'job', 'rol', 'run_id', 'estado', 'control', 'inicio', 'fin',
        'duracion_segundos', 'duracion_tipo', 'execution_time_segundos', 'intento', 'en_ventana',
        'es_ultima', 'estados_ventana', 'ejecuciones_ventana', 'fallas_ventana',
        'ultimo_exito_observado', 'error', 'detalle', 'campo', 'valor']
    def leaves(value, field='', job='GENERAL'):
        if isinstance(value, dict) and value:
            job = value.get('recurso', job)
            for key, item in value.items():
                yield from leaves(item, field+'/'+str(key), job)
        elif isinstance(value, list) and value:
            for i, item in enumerate(value):
                yield from leaves(item, field+'/'+str(i), job)
        else:
            yield dict(tipo_registro='CONTEXTO', job=job, campo=field, valor=json.dumps(value, ensure_ascii=False))
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for kind, rows in (('RESUMEN', summaries), ('DETALLE', details), ('CONTROL', notes)):
            writer.writerows(dict(tipo_registro=kind, **row) for row in rows)
        writer.writerows(leaves(context))
    return path


def inspect(session, config, identity, args, errors):
    now = datetime.now(timezone.utc)
    start = now-timedelta(days=args.dias)
    glue = session.client('glue', config=config)
    summaries, details, notes, resources = [], [], [], []
    for name in dict.fromkeys(args.job or JOBS):
        summary, job_details, job_notes, resource = inspect_job(glue, name, now, start,
            None if args.historial_completo else args.max_ejecuciones, errors)
        summaries.append(summary)
        details.extend(job_details)
        notes.extend(job_notes)
        resources.append(resource)
    folder = Path(args.salida).expanduser()/'informes_glue'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    context = dict(cuenta=identity['Account'], region=args.region, captura_utc=now.isoformat(),
        ventana_utc=dict(desde=start.isoformat(), hasta=now.isoformat()), configuracion=vars(args),
        recursos=resources, limitaciones=LIMITS)
    report = write_report(folder, summaries, details, notes, context)
    print(f'JOB GLUE DE INFORMES | Solo lectura | {args.region} | Ventana: {args.dias} dias')
    print_table(summaries)
    print('* Duracion hasta captura para ejecucion abierta; sin umbrales. N/V = no verificable.')
    print('Ultimo estado puede ser anterior a la ventana. SUCCEEDED no certifica contenido ni entrega del informe.')
    print(f'Informe: {report} | Filtrar tipo_registro=RESUMEN para la vista principal.')
    return 3 if any(s['control'] == 'INCOMPLETO' for s in summaries) else 1 if any(s['control'] == 'AUSENTE' for s in summaries) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--dias', type=int, default=8, help='Ventana de ejecuciones: 1..365 dias')
    parser.add_argument('--max-ejecuciones', type=int, default=1000, help='Maximo de runs recientes por job: 1..10000; default 1000')
    parser.add_argument('--historial-completo', action='store_true',
        help='Agota las paginas de runs; ignora --max-ejecuciones. Puede tardar mas; conserva la ventana --dias')
    parser.add_argument('--job', action='append', help='Nombre completo Glue; repetir para consultar otros jobs de informes. Sustituye el nombre inicial')
    parser.add_argument('--salida', default='salida')
    args = parser.parse_args()
    if not 1 <= args.dias <= 365 or not 1 <= args.max_ejecuciones <= 10000:
        parser.error('--dias: 1..365; --max-ejecuciones: 1..10000')
    if any(not name or len(name) > 255 or any(c in name for c in '\n\r\t') for name in args.job or []):
        parser.error('Nombres de job no vacios, hasta 255 caracteres, sin saltos de linea')
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Ejecutar en CloudShell con boto3/botocore.', file=sys.stderr)
        return 3
    try:
        config = Config(connect_timeout=5, read_timeout=20, retries={'total_max_attempts': 3, 'mode': 'standard'})
        session = boto3.Session(region_name=args.region)
        identity = session.client('sts', config=config).get_caller_identity()
        if identity['Account'] != args.cuenta:
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan jobs.', file=sys.stderr)
            return 3
        return inspect(session, config, identity, args, (BotoCoreError, ClientError))
    except (BotoCoreError, ClientError, OSError, ValueError, csv.Error) as exc:
        print('[BLOQUEADO] '+type(exc).__name__+'; revisar sesion/permisos y destino local.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Consulta interrumpida; no se ejecuto ni modifico ningun job.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
