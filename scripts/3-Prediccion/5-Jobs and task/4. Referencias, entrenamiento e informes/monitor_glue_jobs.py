#!/usr/bin/env python3
"""Monitor de jobs Glue de referencia, entrenamiento e informes; solo lectura.

CloudShell: python3 ~/monitor_glue_jobs.py --historial-completo
Un informe_glue_jobs.csv con RESUMEN/DETALLE/CONTROL/CONTEXTO.
No ejecuta jobs, modifica AWS ni envia alertas. No requiere otros archivos locales.
Exit: 0 lectura completa sin fallas; 1 FALLA/AUSENTE; 2 argumentos;
3 cobertura parcial/error; 130 interrupcion. FALLA y cobertura son independientes.
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


GROUPS = {
    'referencia': {
        'pon-prod-xcrime-update-fuentes-internas': 'Fuentes internas',
        'pon-prod-xcrime-update-estructura-nal': 'Estructura nacional',
        'pon-prod-xcrime-update-geometrias-nal': 'Geometrias',
    },
    'entrenamiento': {
        'pon_prod_xcrime_train_models_job': 'Entrenamiento',
        'pon_prod_xcrime_homologacion_job': 'Homologacion',
        'pon_prod_xcrime_consume_models_job': 'Consumo',
    },
    'informes': {
        'pon-prod-xcrime-informes-anticipaciones': 'Informes',
    },
}
BOGOTA = timezone(timedelta(hours=-5))
FAILURES = {'FAILED', 'TIMEOUT', 'ERROR', 'EXPIRED'}
ACTIVE = {'STARTING', 'RUNNING', 'WAITING', 'STOPPING'}
TERMINAL = FAILURES | {'SUCCEEDED', 'STOPPED'}
ERROR_TYPES = (
    'AccessDeniedException', 'EntityNotFoundException', 'ResourceNotFoundException',
    'ThrottlingException', 'OutOfMemoryError', 'AnalysisException', 'ModuleNotFoundError',
    'ImportError', 'KeyError', 'IndexError', 'ValueError', 'TypeError', 'RuntimeError',
    'ConnectionError', 'TimeoutError', 'FileNotFoundError', 'NoSuchKey', 'Py4JJavaError',
)
LIMITS = [
    'Solo STS/GetJobRuns; no ejecuta jobs, modifica recursos ni envia alertas.',
    'Lectura paginada, no atomica; el estado puede cambiar entre paginas.',
    'Por defecto hasta 1000 runs por job; historial completo agota las paginas disponibles.',
    'No corta por antiguedad: un run antiguo puede seguir abierto o terminar dentro de la ventana.',
    'Ventana incluye inicios o fines en el periodo y runs aun abiertos, aunque iniciaran antes.',
    'Ultimo estado es historico observado: puede estar fuera de ventana. FALLA se basa en la ventana.',
    'Cobertura y resultado son independientes: una lectura parcial no oculta una FALLA conocida.',
    'Sin calendario/SLA confirmado, ausencia de runs no demuestra un disparo perdido.',
    'Duraciones informativas, sin umbrales. ExecutionTime no es duracion de inicio a fin.',
    'SUCCEEDED no certifica calidad, vigencia, entrega ni uso de modelos/datos/informes.',
    'No confirma dependencias ni orden real entre estos jobs y el predictor.',
    'Exporta metadata seleccionada y clase/categoria de error; no Arguments ni mensajes completos.',
    'Reporte local por consulta, sin estado persistente, programacion ni notificaciones.',
    'Historial completo puede tardar; esta CLI aun no tiene presupuesto de tiempo para Lambda.',
]


def select_jobs(groups=None, names=None):
    """Una unica lista de recursos, sin consultas ni efectos laterales."""
    catalog = {
        name: dict(job=name, grupo=group, rol=role)
        for group, jobs in GROUPS.items() for name, role in jobs.items()
    }
    if names:
        return [catalog.get(name, dict(job=name, grupo='personalizado', rol='Job configurado'))
                for name in dict.fromkeys(names)]
    selected = set(groups or ['todos'])
    return [job for job in catalog.values() if 'todos' in selected or job['grupo'] in selected]


def aware(value):
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def stamp(value):
    return value.astimezone(BOGOTA).isoformat() if aware(value) else ''


def error_hint(message):
    text = str(message or '')
    found = [name for name in ERROR_TYPES
             if re.search(r'(?<!\w)' + re.escape(name) + r'(?!\w)', text)]
    category = re.search(r'Error Category:\s*([A-Z_]{1,80})(?![A-Z_])', text)
    if category:
        found.append(category.group(1))
    return ', '.join(found) or ('ErrorMessage presente; revisar run en Glue' if text else '')


def read_runs(glue, name, limit, sdk_errors):
    """Frontera AWS: conserva lo leido si otra pagina falla; omite datos sensibles.

    Se convierten errores SDK en evidencia de cobertura por recurso para poder
    continuar con los otros jobs. No se captura un error de programacion.
    """
    rows = []
    pagination = {'PageSize': 200}
    if limit is not None:
        pagination.update(MaxItems=limit + 1, PageSize=min(limit + 1, 200))
    missing_errors = (glue.exceptions.EntityNotFoundException,)
    try:
        for page in glue.get_paginator('get_job_runs').paginate(
                JobName=name, PaginationConfig=pagination):
            for run in page.get('JobRuns', []):
                if limit is not None and len(rows) == limit:
                    return rows, True, None
                row = {key: run[key] for key in (
                    'Id', 'Attempt', 'JobRunState', 'StartedOn', 'CompletedOn', 'ExecutionTime'
                ) if key in run}
                row['error'] = error_hint(run.get('ErrorMessage'))
                rows.append(row)
    except missing_errors:
        return rows, False, dict(cobertura='PARCIAL' if rows else 'AUSENTE',
                                 codigo='EntityNotFoundException')
    except sdk_errors as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
        # El codigo SDK es metadata, nunca se exporta el mensaje de la excepcion.
        code = str(code)
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9.]{0,100}', code):
            code = type(exc).__name__
        return rows, False, dict(cobertura='PARCIAL' if rows else 'ERROR', codigo=code)
    return rows, False, None


def evaluate_times(run, now, start):
    """Distingue pertenencia a ventana de integridad de los tiempos."""
    began, ended = run.get('StartedOn'), run.get('CompletedOn')
    state = run.get('JobRunState', 'DESCONOCIDO')
    active = state in ACTIVE
    start_ok = aware(began) and began <= now
    end_ok = aware(ended) and ended <= now and (not start_ok or ended >= began)
    problems = []
    if not start_ok:
        problems.append('StartedOn ausente, sin zona horaria o futuro')
    if ended is not None and not end_ok:
        problems.append('CompletedOn sin zona horaria, futuro o anterior a StartedOn')
    if state in TERMINAL and ended is None:
        problems.append('Estado terminal sin CompletedOn')
    if active and ended is not None:
        problems.append('Estado abierto con CompletedOn')
    if active or start_ok and began >= start or end_ok and ended >= start:
        membership = 'SI'
    else:
        membership = 'NO EVALUADO' if problems else 'NO'
    seconds = None
    if not problems:
        if active:
            seconds = round((now - began).total_seconds(), 3)
        elif end_ok:
            seconds = round((ended - began).total_seconds(), 3)
    kind = ('HASTA CAPTURA' if active else 'INICIO A FIN') if seconds is not None else 'NO EVALUADA'
    return membership, seconds, kind, problems


def evaluate_job(job, rows, truncated, read_error, now, start, limit):
    notes, details, seen = [], [], set()
    summary = dict(**job, run_id='', estado='NO DISPONIBLE', control='INFO',
        cobertura='COMPLETA', inicio='', fin='', duracion_segundos=None,
        duracion_tipo='NO EVALUADA', execution_time_segundos=None, en_ventana='NO EVALUADO',
        estados_ventana='', ejecuciones_ventana=None, fallas_ventana=None,
        ultimo_exito_observado='', error='')

    def note(control, text):
        notes.append(dict(**job, control=control, detalle=text))

    if truncated:
        summary['cobertura'] = 'PARCIAL'
        note('INCOMPLETO', f'Historial limitado a {limit} runs; hay mas disponibles')
    if read_error:
        summary['cobertura'] = read_error['cobertura']
        note('AUSENTE' if read_error['cobertura'] == 'AUSENTE' else 'INCOMPLETO',
             'GetJobRuns: ' + read_error['codigo'])
    counts, success_ends = Counter(), []
    unique_count = 0
    for run in rows:
        key = (run.get('Id'), run.get('Attempt'))
        if key[0] and key in seen:
            continue
        if key[0]:
            seen.add(key)
        latest = unique_count == 0  # GetJobRuns devuelve los mas recientes primero.
        unique_count += 1
        state = run.get('JobRunState', 'DESCONOCIDO')
        membership, seconds, kind, problems = evaluate_times(run, now, start)
        if not key[0]:
            problems.append('Run sin Id; no se puede deduplicar')
        if problems:
            summary['cobertura'] = 'PARCIAL'
            note('INCOMPLETO', 'Run ' + str(key[0] or '?') + ': ' + '; '.join(problems))
        inside = membership == 'SI'
        if inside:
            counts[state] += 1
        if not problems and state == 'SUCCEEDED':
            success_ends.append(run['CompletedOn'])
        run_control = ('FALLA' if state in FAILURES else 'AVISO'
                       if state == 'STOPPED' or state not in TERMINAL | ACTIVE else 'INFO')
        detail = dict(**job, run_id=key[0] or '', estado=state, control=run_control,
            inicio=stamp(run.get('StartedOn')), fin=stamp(run.get('CompletedOn')),
            duracion_segundos=seconds, duracion_tipo=kind,
            execution_time_segundos=run.get('ExecutionTime'), intento=run.get('Attempt'),
            en_ventana=membership, es_ultima='SI' if latest else 'NO', error=run['error'])
        # Solo retiene el detalle util: ventana, ultima observada o metadata incompleta.
        if latest or inside or problems:
            details.append(detail)
        if run_control != 'INFO' and (inside or membership == 'NO EVALUADO'):
            level = run_control if inside else 'AVISO'
            note(level, f'Run {key[0] or "?"}: {state}; en_ventana={membership}'
                 + ('; ' + run['error'] if run['error'] else ''))
        elif latest and membership == 'NO' and run_control != 'INFO':
            note('INFO', f'Ultima observada: {state}, fuera de ventana; no es una falla de la ventana')
        if latest:
            summary.update({key: detail[key] for key in (
                'run_id', 'estado', 'inicio', 'fin', 'duracion_segundos', 'duracion_tipo',
                'execution_time_segundos', 'en_ventana', 'error')})
    if rows:
        summary.update(estados_ventana=json.dumps(dict(sorted(counts.items())), ensure_ascii=False),
            ejecuciones_ventana=sum(counts.values()), fallas_ventana=sum(counts[s] for s in FAILURES),
            ultimo_exito_observado=stamp(max(success_ends)) if success_ends else '')
        if not counts:
            note('INFO', 'Sin ejecuciones observadas en ventana; sin calendario esperado configurado')
    elif not read_error:
        summary.update(estado='SIN EJECUCIONES', estados_ventana='{}',
                       ejecuciones_ventana=0, fallas_ventana=0)
        note('INFO', 'Glue no devolvio runs; no se infiere un disparo perdido')
    levels = {row['control'] for row in notes}
    summary['control'] = ('FALLA' if 'FALLA' in levels else 'AUSENTE' if 'AUSENTE' in levels
                          else 'AVISO' if 'AVISO' in levels else 'NO EVALUADO'
                          if summary['cobertura'] != 'COMPLETA' else 'INFO')
    for detail in details:
        detail['cobertura'] = summary['cobertura']
    for row in notes:
        row['cobertura'] = summary['cobertura']
    resource = dict(**job, cobertura=summary['cobertura'], runs_recibidos=len(rows),
        runs_distintos=unique_count, truncado=truncated,
        codigo_error_lectura=read_error['codigo'] if read_error else '',
        modo_historial='COMPLETO' if limit is None else 'LIMITADO')
    return summary, details, notes, resource


def collect_report(glue, jobs, now, days=8, max_runs=1000, sdk_errors=()):
    """Nucleo reutilizable: recibe cliente/reloj; devuelve datos sin CLI ni archivos.

    max_runs=None agota el historial disponible. No presupone un runtime Lambda.
    """
    if not aware(now) or not 1 <= days <= 365 or max_runs is not None and not 1 <= max_runs <= 10000:
        raise ValueError('Reloj con zona horaria, dias 1..365 y max_runs 1..10000 o None')
    start = now - timedelta(days=days)
    report = dict(resumen=[], detalle=[], controles=[], contexto=dict(
        captura_utc=now.astimezone(timezone.utc).isoformat(),
        ventana_utc=dict(desde=start.astimezone(timezone.utc).isoformat(),
                         hasta=now.astimezone(timezone.utc).isoformat()),
        recursos=[], limitaciones=LIMITS))
    for job in {item['job']: item for item in jobs}.values():
        rows, truncated, error = read_runs(glue, job['job'], max_runs, sdk_errors)
        summary, details, notes, resource = evaluate_job(job, rows, truncated, error, now, start, max_runs)
        report['resumen'].append(summary)
        report['detalle'].extend(details)
        report['controles'].extend(notes)
        report['contexto']['recursos'].append(resource)
    return report


def exit_code(report):
    rows = report['resumen']
    if any(row['cobertura'] in ('PARCIAL', 'ERROR') for row in rows):
        return 3
    return 1 if any(row['control'] in ('FALLA', 'AUSENTE') for row in rows) else 0


def pretty_duration(seconds, kind):
    if seconds is None:
        return 'N/V'
    hours, seconds = divmod(int(seconds), 3600)
    minutes, seconds = divmod(seconds, 60)
    return f'{hours}h {minutes:02}m {seconds:02}s' + (' *' if kind == 'HASTA CAPTURA' else '')


def print_table(rows):
    widths = (13, 43, 13, 19, 19, 14, 25, 18, 11, 9)

    def render(values):
        cells = [textwrap.wrap(str(value), width=width, break_on_hyphens=False) or ['-']
                 for value, width in zip(values, widths)]
        for i in range(max(map(len, cells))):
            print(' | '.join((cell[i] if i < len(cell) else '').ljust(width)
                             for cell, width in zip(cells, widths)))

    render(('Grupo', 'Job', 'Ultimo estado', 'Inicio Bogota', 'Fin Bogota', 'Duracion',
            'Estados en ventana', 'Error ultimo', 'Control', 'Cobertura'))
    print('-+-'.join('-' * width for width in widths))
    for row in rows:
        counts = json.loads(row['estados_ventana']) if row['estados_ventana'] else None
        text = ('; '.join(f'{key}={value}' for key, value in counts.items()) if counts
                else 'SIN EJECUCIONES' if counts == {} else 'NO EVALUADO')
        render((row['grupo'], row['job'], row['estado'], row['inicio'][:19], row['fin'][:19],
                pretty_duration(row['duracion_segundos'], row['duracion_tipo']), text,
                row['error'], row['control'], row['cobertura']))


def write_report(folder, report):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / 'informe_glue_jobs.csv'
    fields = ['tipo_registro', 'grupo', 'job', 'rol', 'run_id', 'estado', 'control',
        'cobertura', 'inicio', 'fin', 'duracion_segundos', 'duracion_tipo',
        'execution_time_segundos', 'intento', 'en_ventana', 'es_ultima', 'estados_ventana',
        'ejecuciones_ventana', 'fallas_ventana', 'ultimo_exito_observado', 'error',
        'detalle', 'campo', 'valor']

    def leaves(value, field='', job='GENERAL'):
        if isinstance(value, dict) and value:
            job = value.get('job', job)
            for key, item in value.items():
                yield from leaves(item, field + '/' + str(key), job)
        elif isinstance(value, list) and value:
            for i, item in enumerate(value):
                yield from leaves(item, field + '/' + str(i), job)
        else:
            yield dict(tipo_registro='CONTEXTO', job=job, campo=field,
                       valor=json.dumps(value, ensure_ascii=False))

    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for kind, key in (('RESUMEN', 'resumen'), ('DETALLE', 'detalle'), ('CONTROL', 'controles')):
            writer.writerows(dict(tipo_registro=kind, **row) for row in report[key])
        writer.writerows(leaves(report['contexto']))
    return path


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--dias', type=int, default=8, help='Ventana: 1..365 dias')
    parser.add_argument('--max-ejecuciones', type=int, default=1000, help='Limite por job: 1..10000')
    parser.add_argument('--historial-completo', action='store_true', help='Agota paginas; ignora --max-ejecuciones')
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument('--grupo', action='append', choices=['todos', *GROUPS],
                           help='Default todos; repetir para seleccionar varios grupos')
    selection.add_argument('--job', action='append', help='Nombre completo; repetir. Sustituye el catalogo')
    parser.add_argument('--salida', default='salida', help='Directorio base del informe local')
    args = parser.parse_args(argv)
    if not 1 <= args.dias <= 365 or not 1 <= args.max_ejecuciones <= 10000:
        parser.error('--dias: 1..365; --max-ejecuciones: 1..10000')
    if any(not name.strip() or len(name) > 255 or any(c in name for c in '\n\r\t')
           for name in args.job or []):
        parser.error('Nombres de job no vacios, hasta 255 caracteres, sin saltos de linea')
    return args


def run_cli(session, config, args, sdk_errors):
    """Adaptador CLI; STS se verifica antes de consultar Glue."""
    identity = session.client('sts', config=config).get_caller_identity()
    if identity['Account'] != args.cuenta:
        print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan jobs.', file=sys.stderr)
        return 3
    now = datetime.now(timezone.utc)
    report = collect_report(session.client('glue', config=config), select_jobs(args.grupo, args.job),
        now, days=args.dias, max_runs=None if args.historial_completo else args.max_ejecuciones,
        sdk_errors=sdk_errors)
    report['contexto'].update(cuenta=identity['Account'], region=args.region, configuracion=vars(args))
    folder = Path(args.salida).expanduser() / 'glue_jobs' / now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    path = write_report(folder, report)
    print(f'JOBS GLUE | Solo lectura | {args.region} | Ventana: {args.dias} dias')
    print_table(report['resumen'])
    print('* Duracion hasta captura; sin umbrales. Ultimo estado puede ser anterior a la ventana.')
    print('Control: fallas de la ventana. Cobertura: integridad de lectura; parcial no oculta fallas conocidas.')
    print('SUCCEEDED no certifica datos/modelos/informes. Sin calendario esperado ni envio automatico de alertas.')
    print(f'Informe: {path} | Filtrar tipo_registro=RESUMEN para la vista principal.')
    return exit_code(report)


def main(argv=None):
    args = parse_args(argv)
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Ejecutar en CloudShell con boto3/botocore.', file=sys.stderr)
        return 3
    try:
        config = Config(connect_timeout=5, read_timeout=20,
                        retries={'total_max_attempts': 3, 'mode': 'standard'})
        session = boto3.Session(region_name=args.region)
        return run_cli(session, config, args, (BotoCoreError, ClientError))
    except (BotoCoreError, ClientError, OSError, ValueError, csv.Error) as exc:
        print('[BLOQUEADO] ' + type(exc).__name__ + '; revisar sesion/permisos y destino local.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Consulta interrumpida; no se ejecuto ni modifico ningun job.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
