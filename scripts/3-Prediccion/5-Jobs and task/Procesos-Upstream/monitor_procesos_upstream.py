#!/usr/bin/env python3
"""Monitor manual upstream PONAL: ECS de limpieza, crawler, jobs de referencia y DynamoDB.

Solo lectura AWS. No arranca tareas/crawlers/jobs ni lee items de DynamoDB.
Una tabla y un CSV local con RESUMEN, DETALLE, CONTROL y CONTEXTO.
Exit 0 consulta completa (puede haber avisos/fallos observados); 1 recurso ausente;
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


CLUSTER = 'pon-prod-xcrime-datacleaning-cluster'
CRAWLER = 'pon-prod-xcrime-datacleaning-glue-crawler'
JOBS = ('pon-prod-xcrime-update-fuentes-internas',
        'pon-prod-xcrime-update-estructura-nal', 'pon-prod-xcrime-update-geometrias-nal')
TABLES = ('pon_prod_llamada_siglas_historico', 'pon_prod_comparendo_siglas_historico', 'pon_prod_delito_siglas_historico')
BOGOTA = timezone(timedelta(hours=-5))
JOB_FAILURES = {'FAILED', 'TIMEOUT', 'ERROR', 'EXPIRED'}
ERROR_MARKERS = ('AccessDeniedException', 'EntityNotFoundException', 'ResourceNotFoundException',
    'ThrottlingException', 'OutOfMemoryError', 'CannotPullContainerError',
    'CannotStartContainerError', 'ResourceInitializationError', 'ModuleNotFoundError',
    'ImportError', 'AnalysisException', 'ConnectionError')
LIMITS = [
    'Solo lectura; no crea alarmas, habilita Container Insights ni modifica recursos.',
    'ECS: snapshot RUNNING/STOPPED visibles, no historial completo de la ventana ni de la semana.',
    'Crawler: configuracion actual y LastCrawl, no historial de todos los crawls.',
    'Jobs: muestra acotada de ejecuciones recientes; ventana incluye inicios, fines y ejecuciones aun abiertas.',
    'Ningun estado SUCCEEDED o salida cero certifica calidad/freshness de datos ni ejecucion de predicciones.',
    'Sin umbrales de duracion/antiguedad: no hay frecuencia operativa confirmada para estos recursos.',
    'DynamoDB: describe tablas y consulta CloudWatch; no usa Scan, Query ni GetItem.',
    'ThrottledRequests por TableName+Operation; eventos Read/Write separados por tabla y GSI, sin sumarlos con solicitudes.',
    'ListMetrics descubre series recientes (aprox. ultimas dos semanas); puede omitir series inactivas, especialmente con ventanas mas amplias.',
    'SIN DATOS / SIN SERIES no equivale a cero throttling, falta de actividad ni salud certificada.',
    'Horas de recursos en Bogota; metricas agregadas por hora en la ventana, sin relacion causal inferida entre recursos.',
    'No consulta logs de aplicacion ni verifica datos/archivos de salida. No configura notificaciones ni guarda historial continuo.'
]


def when(value):
    return value.astimezone(BOGOTA).isoformat() if isinstance(value, datetime) and value.tzinfo else ''


def elapsed(start, end):
    if isinstance(start, datetime) and isinstance(end, datetime) and start.tzinfo and end.tzinfo and end >= start:
        return round((end-start).total_seconds(), 3)
    return None


def error_hint(message):
    text = str(message or '')
    markers = [m for m in ERROR_MARKERS if m in text]
    category = re.search(r'Error Category:\s*([A-Z_]{1,80})', text)
    if category:
        markers.append(category.group(1))
    return ', '.join(markers) or ('Mensaje de error presente; revisar recurso en AWS' if text else '')


class Reader:
    def __init__(self, errors):
        self.errors = errors
        self.issues = []
        self.controls = []

    def read(self, resource, operation, call, missing=False):
        try:
            return call()
        except self.errors as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
            kind = 'AUSENTE' if missing and code in ('ResourceNotFoundException', 'EntityNotFoundException', 'ClusterNotFoundException') else 'INCOMPLETO'
            self.issues.append(dict(recurso=resource, operacion=operation, error=code, tipo=kind))
            return None

    def flag(self, resource, name, state, detail):
        self.controls.append(dict(recurso=resource, control=name, estado=state, detalle=detail))

    def status(self, resource):
        issues = [i for i in self.issues if i['recurso'] == resource]
        if any(i['tipo'] == 'INCOMPLETO' for i in issues):
            return 'INCOMPLETO'
        if issues:
            return 'AUSENTE'
        states = {c['estado'] for c in self.controls if c['recurso'] == resource}
        return 'FALLA' if 'FALLA' in states else 'AVISO' if 'AVISO' in states else 'INFO'

    def partial(self, resource, operation, reason):
        self.issues.append(dict(recurso=resource, operacion=operation, error=reason, tipo='INCOMPLETO'))


def listing(client, operation, key, limit, **kwargs):
    rows = []
    for page in client.get_paginator(operation).paginate(**kwargs, PaginationConfig={'MaxItems': limit+1}):
        rows.extend(page.get(key, []))
    return rows[:limit], len(rows) > limit


def task_result(task, essential):
    state = task.get('lastStatus', 'DESCONOCIDO')
    stop_code = task.get('stopCode', '')
    hint = error_hint(task.get('stoppedReason'))
    exits = {c['name']: c.get('exitCode') for c in task.get('containers', [])}
    if stop_code == 'TaskFailedToStart' or any(m in hint for m in
            ('OutOfMemoryError', 'CannotPullContainerError', 'CannotStartContainerError', 'ResourceInitializationError')):
        return 'FALLA', stop_code or hint
    problems = []
    for container in task.get('containers', []):
        name = container['name']
        if container.get('healthStatus') == 'UNHEALTHY':
            problems.append((('FALLA' if essential.get(name) is True else 'AVISO'), 'UNHEALTHY: '+name))
        if container.get('exitCode') not in (None, 0):
            problems.append((('FALLA' if essential.get(name) is True else 'AVISO'), 'Salida no cero: '+name))
    if problems:
        severity = 'FALLA' if any(p[0] == 'FALLA' for p in problems) else 'AVISO'
        return severity, '; '.join(p[1] for p in problems)
    if state == 'STOPPED':
        if stop_code in ('UserInitiated', 'ServiceSchedulerInitiated', 'SpotInterruption', 'TerminationNotice'):
            return 'AVISO', 'Parada reportada: '+stop_code
        required = [name for name, flag in essential.items() if flag]
        if not required or any(exits.get(name) is None for name in required):
            return 'AVISO', 'Parada con codigos esenciales incompletos'
        return 'INFO', 'Esenciales con salida cero; datos no validados'
    return 'INFO', 'Estado observado: '+state


def inspect_ecs(ecs, reader, args, now, details):
    name = args.cluster
    response = reader.read(name, 'DescribeClusters', lambda: ecs.describe_clusters(clusters=[name], include=['SETTINGS']), missing=True)
    if response is None:
        return dict(estado='NO DISPONIBLE'), {}
    clusters = response.get('clusters', [])
    if not clusters:
        reader.issues.append(dict(recurso=name, operacion='DescribeClusters', error='Cluster no devuelto', tipo='AUSENTE'))
        return dict(estado='NO DISPONIBLE'), {}
    for failure in response.get('failures', []):
        reader.partial(name, 'DescribeClusters', str(failure.get('reason', 'Failure')))
    cluster = clusters[0]
    settings = {s['name']: s.get('value') for s in cluster.get('settings', [])}
    insights = settings.get('containerInsights', 'NO INFORMADO')
    if cluster.get('status') != 'ACTIVE':
        reader.flag(name, 'Estado cluster', 'AVISO', 'Estado='+cluster.get('status', '?'))
    if insights not in ('enabled', 'enhanced'):
        reader.flag(name, 'Container Insights', 'AVISO', 'Configuracion observada='+insights+'; no se modifica')
    arns = []
    for desired in ('RUNNING', 'STOPPED'):
        result = reader.read(name, 'ListTasks:'+desired, lambda desired=desired: listing(ecs, 'list_tasks', 'taskArns', args.max_tareas, cluster=name, desiredStatus=desired))
        if result is not None:
            found, truncated = result
            arns.extend(found)
            if truncated:
                reader.partial(name, 'ListTasks:'+desired, 'Limite por estado alcanzado')
    arns = list(dict.fromkeys(arns))
    tasks, definitions = [], {}
    for offset in range(0, len(arns), 100):
        response = reader.read(name, 'DescribeTasks', lambda offset=offset: ecs.describe_tasks(cluster=name, tasks=arns[offset:offset+100]))
        if response is None:
            continue
        tasks.extend(response.get('tasks', []))
        for failure in response.get('failures', []):
            reader.partial(name, 'DescribeTasks', str(failure.get('reason', 'Failure')))
    for task in tasks:
        arn = task.get('taskDefinitionArn', '')
        if arn not in definitions:
            response = reader.read(name, 'DescribeTaskDefinition', lambda arn=arn: ecs.describe_task_definition(taskDefinition=arn))
            definitions[arn] = {c['name']: c.get('essential', True) for c in response.get('taskDefinition', {}).get('containerDefinitions', [])} if response else {}
        severity, outcome = task_result(task, definitions[arn])
        if severity != 'INFO':
            reader.flag(name, 'Tarea '+task['taskArn'].rsplit('/', 1)[-1], severity, outcome)
        stopped = task.get('lastStatus') == 'STOPPED'
        started, ended = task.get('startedAt'), task.get('stoppedAt')
        details.append(dict(tipo='ECS tarea', recurso=name, estado=task.get('lastStatus', '?'),
            control=severity, inicio=when(started), fin=when(ended), duracion_segundos=elapsed(started, ended if stopped else now),
            detalle=json.dumps(dict(task_arn=task['taskArn'], stop_code=task.get('stopCode', ''),
                resultado=outcome, error=error_hint(task.get('stoppedReason')),
                contenedores=[dict(nombre=c['name'], exit_code=c.get('exitCode'), health=c.get('healthStatus'),
                    error=error_hint(c.get('reason'))) for c in task.get('containers', [])]), ensure_ascii=False)))
    counts = dict(Counter(t.get('lastStatus', '?') for t in tasks))
    return dict(estado=cluster.get('status'), container_insights=insights, tareas_visibles=counts,
                fallas_observadas=sum(d['control'] == 'FALLA' for d in details if d['tipo'] == 'ECS tarea' and d['recurso'] == name)), counts


def inspect_crawler(glue, reader, name, details):
    response = reader.read(name, 'GetCrawler', lambda: glue.get_crawler(Name=name), missing=True)
    if response is None:
        return dict(estado='NO DISPONIBLE')
    crawler = response['Crawler']
    last = crawler.get('LastCrawl', {})
    result = last.get('Status', 'SIN EJECUCION INFORMADA')
    severity = 'FALLA' if result == 'FAILED' else 'AVISO' if result == 'CANCELLED' else 'INFO'
    if severity != 'INFO':
        reader.flag(name, 'Ultimo crawl', severity, result+'; inicio='+when(last.get('StartTime')))
    details.append(dict(tipo='Crawler ultima ejecucion', recurso=name, estado=result, control=severity,
        inicio=when(last.get('StartTime')), fin='', detalle=json.dumps(dict(error=error_hint(last.get('ErrorMessage')),
            grupo_logs=last.get('LogGroup', ''), stream_logs=last.get('LogStream', '')), ensure_ascii=False)))
    return dict(estado=crawler.get('State'), ultimo_estado=result, ultimo_inicio=when(last.get('StartTime')),
                schedule=crawler.get('Schedule', {}))


def inspect_job(glue, reader, name, args, start, now, details):
    result = reader.read(name, 'GetJobRuns', lambda: listing(glue, 'get_job_runs', 'JobRuns', args.max_ejecuciones, JobName=name), missing=True)
    if result is None:
        return dict(estado='NO DISPONIBLE')
    runs, truncated = result
    if truncated:
        reader.partial(name, 'GetJobRuns', 'Muestra limitada; puede faltar historial de la ventana')
    selected = []
    for run in runs:
        began, ended = run.get('StartedOn'), run.get('CompletedOn')
        if not isinstance(began, datetime) or not began.tzinfo:
            reader.partial(name, 'GetJobRuns', 'Ejecucion sin StartedOn interpretable')
            continue
        if began > now or ended is not None and (not isinstance(ended, datetime) or not ended.tzinfo):
            reader.partial(name, 'GetJobRuns', 'Tiempos de ejecucion no interpretables')
            continue
        active = run.get('JobRunState') in ('STARTING', 'RUNNING', 'WAITING', 'STOPPING')
        if began >= start or isinstance(ended, datetime) and ended >= start or active:
            selected.append(run)
            state = run.get('JobRunState', 'DESCONOCIDO')
            severity = 'FALLA' if state in JOB_FAILURES else 'AVISO' if state == 'STOPPED' else 'INFO'
            if severity != 'INFO':
                reader.flag(name, 'Job run '+run.get('Id', '?'), severity, state+'; inicio='+when(began))
            details.append(dict(tipo='Glue ejecucion', recurso=name, estado=state, control=severity,
                inicio=when(began), fin=when(ended), duracion_segundos=elapsed(began, ended if ended else now) if active or ended else None,
                detalle=json.dumps(dict(id=run.get('Id'), intento=run.get('Attempt'),
                    execution_time=run.get('ExecutionTime'), error=error_hint(run.get('ErrorMessage'))), ensure_ascii=False)))
    last = runs[0] if runs else {}
    return dict(estado=last.get('JobRunState', 'SIN EJECUCIONES'), ultimo_inicio=when(last.get('StartedOn')),
        ultimo_fin=when(last.get('CompletedOn')), muestra_leida=len(runs), truncada=truncated,
        estados_ventana=dict(Counter(r.get('JobRunState', '?') for r in selected)),
        fallas_ventana=sum(r.get('JobRunState') in JOB_FAILURES for r in selected))


def metric(cw, reader, name, metric_name, dimensions, start, end):
    response = reader.read(name, 'Metrica:'+metric_name, lambda: cw.get_metric_statistics(
        Namespace='AWS/DynamoDB', MetricName=metric_name, Dimensions=dimensions,
        StartTime=start, EndTime=end, Period=3600, Statistics=['Sum']))
    points = response.get('Datapoints', []) if response is not None else []
    values = [p['Sum'] for p in points if 'Sum' in p]
    value = sum(values) if values else None
    if value is not None and value > 0:
        reader.flag(name, metric_name, 'AVISO', f'{value:g} observados; dimensiones='+json.dumps(dimensions))
    return dict(metrica=metric_name, dimensiones=dimensions, valor=value,
        lectura='ERROR' if response is None else 'LEIDA' if values else 'SIN DATOS', muestras=len(values))


def aggregate(metrics):
    values = [m['valor'] for m in metrics if m['valor'] is not None]
    return sum(values) if values else None


def count_text(value):
    return f'{value:g}' if value is not None else 'SIN DATOS'


def inspect_table(ddb, cw, reader, name, args, start, now):
    response = reader.read(name, 'DescribeTable', lambda: ddb.describe_table(TableName=name), missing=True)
    if response is None:
        return dict(estado='NO DISPONIBLE')
    table = response['Table']
    base = [dict(Name='TableName', Value=name)]
    indexes = [i['IndexName'] for i in table.get('GlobalSecondaryIndexes', [])]
    metrics = []
    for index in [None]+indexes:
        dims = base+([dict(Name='GlobalSecondaryIndexName', Value=index)] if index else [])
        metrics.extend(metric(cw, reader, name, m, dims, start, now) for m in ('ReadThrottleEvents', 'WriteThrottleEvents'))
    found = reader.read(name, 'ListMetrics:ThrottledRequests', lambda: listing(cw, 'list_metrics', 'Metrics',
        100, Namespace='AWS/DynamoDB', MetricName='ThrottledRequests', Dimensions=base))
    requests = []
    seen = set()
    if found is not None:
        series, truncated = found
        if truncated:
            reader.partial(name, 'ListMetrics', 'Mas de 100 series; solicitudes parcialmente evaluadas')
        for series_item in series:
            dimensions = series_item.get('Dimensions', [])
            key = tuple(sorted((d['Name'], d['Value']) for d in dimensions))
            if key in seen or {d['Name'] for d in dimensions} != {'TableName', 'Operation'}:
                continue
            if dict(key).get('TableName') != name:
                continue
            seen.add(key)
            requests.append(metric(cw, reader, name, 'ThrottledRequests', dimensions, start, now))
    state = table.get('TableStatus')
    if state != 'ACTIVE':
        reader.flag(name, 'Estado tabla', 'AVISO', 'Estado observado='+str(state))
    return dict(estado=state, modo=table.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED'),
        indices=indexes, metricas=metrics+requests, solicitudes_limitadas=aggregate(requests),
        operaciones_con_serie=len(requests),
        eventos_lectura=aggregate([m for m in metrics if m['metrica'] == 'ReadThrottleEvents']),
        eventos_escritura=aggregate([m for m in metrics if m['metrica'] == 'WriteThrottleEvents']))


def print_table(rows):
    fields, widths = ('tipo', 'recurso', 'estado', 'resumen', 'control'), (12, 47, 24, 55, 11)
    def render(values):
        cells = [textwrap.wrap(str(v), width=w, break_on_hyphens=False) or ['-'] for v, w in zip(values, widths)]
        for i in range(max(map(len, cells))):
            print(' | '.join((c[i] if i < len(c) else '').ljust(w) for c, w in zip(cells, widths)))
    render(('Tipo', 'Recurso', 'Estado observado', 'Resultado / evidencia', 'Control'))
    print('-+-'.join('-'*w for w in widths))
    for row in rows:
        render([row[k] for k in fields])


def write_report(folder, summaries, details, reader, context):
    path = folder/'informe_procesos_upstream.csv'
    fields = ['tipo_registro', 'tipo', 'recurso', 'estado', 'control', 'resumen', 'inicio', 'fin',
              'duracion_segundos', 'detalle', 'campo', 'valor']
    def leaves(value, field='', resource='GENERAL'):
        if isinstance(value, dict) and value:
            resource = value.get('recurso', resource)
            for key, item in value.items():
                yield from leaves(item, field+'/'+str(key), resource)
        elif isinstance(value, list) and value:
            for i, item in enumerate(value):
                yield from leaves(item, field+'/'+str(i), resource)
        else:
            yield dict(tipo_registro='CONTEXTO', recurso=resource, campo=field, valor=json.dumps(value, ensure_ascii=False))
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(tipo_registro='RESUMEN', **r) for r in summaries)
        writer.writerows(dict(tipo_registro='DETALLE', **r) for r in details)
        writer.writerows(dict(tipo_registro='CONTROL', **r) for r in reader.controls)
        writer.writerows(leaves(context))
    return path


def inspect(session, config, identity, args, errors):
    now = datetime.now(timezone.utc)
    start = now-timedelta(days=args.dias)
    reader = Reader(errors)
    ecs = session.client('ecs', config=config)
    glue = session.client('glue', config=config)
    ddb = session.client('dynamodb', config=config)
    cw = session.client('cloudwatch', config=config)
    summaries, details, resources = [], [], []
    def add(kind, name, data, summary):
        resources.append(dict(recurso=name, tipo=kind, **data))
        summaries.append(dict(tipo=kind, recurso=name, estado=data.get('estado', '?'), resumen=summary, control=reader.status(name)))
    cluster, counts = inspect_ecs(ecs, reader, args, now, details)
    add('ECS limpieza', args.cluster, cluster, 'Tareas visibles='+str(counts)+'; fallas observadas='+str(cluster.get('fallas_observadas', 'N/V'))+'; Insights='+str(cluster.get('container_insights', 'N/V')))
    crawler = inspect_crawler(glue, reader, args.crawler, details)
    add('Glue crawler', args.crawler, crawler, 'Ultimo='+crawler.get('ultimo_estado', 'N/V')+'; inicio='+crawler.get('ultimo_inicio', 'N/V'))
    for name in dict.fromkeys(args.job or JOBS):
        job = inspect_job(glue, reader, name, args, start, now, details)
        add('Glue job', name, job, 'Estados en ventana='+str(job.get('estados_ventana', 'N/V'))+'; ultimo inicio='+job.get('ultimo_inicio', 'N/V'))
    for name in dict.fromkeys(args.tabla or TABLES):
        table = inspect_table(ddb, cw, reader, name, args, start, now)
        add('DynamoDB', name, table, 'Observado: solicitudes limitadas='+count_text(table.get('solicitudes_limitadas'))+
            '; eventos lectura='+count_text(table.get('eventos_lectura'))+'; escritura='+count_text(table.get('eventos_escritura')))
    folder = Path(args.salida).expanduser()/'procesos_upstream'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    report = write_report(folder, summaries, details, reader, dict(cuenta=identity['Account'], region=args.region,
        captura_utc=now.isoformat(), ventana_utc=dict(desde=start.isoformat(), hasta=now.isoformat()),
        configuracion=vars(args), recursos=resources, lecturas_incompletas=reader.issues, limitaciones=LIMITS))
    print(f'PROCESOS UPSTREAM | Solo lectura | {args.region} | Ventana jobs/metricas: {args.dias} dias')
    print_table(summaries)
    print('ECS: solo tareas aun visibles. Crawler: solo ultima ejecucion. SIN DATOS no equivale a cero.')
    print('FALLA/AVISO describen evidencia observada; INFO no certifica datos actualizados. Sin limites de duracion.')
    print(f'Informe: {report} | Filtrar tipo_registro=RESUMEN para la vista principal.')
    return 3 if any(i['tipo'] == 'INCOMPLETO' for i in reader.issues) else 1 if reader.issues else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--dias', type=int, default=8)
    parser.add_argument('--cluster', default=CLUSTER)
    parser.add_argument('--crawler', default=CRAWLER)
    parser.add_argument('--job', action='append', help='Nombre job; repetir. Sustituye la lista inicial')
    parser.add_argument('--tabla', action='append', help='Nombre tabla; repetir. Sustituye la lista inicial')
    parser.add_argument('--max-tareas', type=int, default=100, help='Maximo por estado RUNNING/STOPPED')
    parser.add_argument('--max-ejecuciones', type=int, default=200, help='Maximo de runs recientes por job')
    parser.add_argument('--salida', default='salida')
    args = parser.parse_args()
    if not 1 <= args.dias <= 31 or not 1 <= args.max_tareas <= 1000 or not 1 <= args.max_ejecuciones <= 10000:
        parser.error('Limites: dias 1..31; tareas 1..1000; ejecuciones 1..10000')
    names = [args.cluster, args.crawler]+(args.job or [])+(args.tabla or [])
    if any(not name or len(name) > 255 or any(c in name for c in '\n\r\t') for name in names):
        parser.error('Nombres de recurso no vacios, hasta 255 caracteres y sin saltos de linea')
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
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan recursos.', file=sys.stderr)
            return 3
        return inspect(session, config, identity, args, (BotoCoreError, ClientError))
    except (BotoCoreError, ClientError, OSError, ValueError, csv.Error) as exc:
        print('[BLOQUEADO] '+type(exc).__name__+'; revisar sesion/permisos y destino local.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Consulta interrumpida; no se modificaron recursos AWS.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
