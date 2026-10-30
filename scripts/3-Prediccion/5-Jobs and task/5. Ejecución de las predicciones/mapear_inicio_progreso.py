#!/usr/bin/env python3
"""Monitor ECS de inicio y ejecucion de predicciones (un solo archivo).

CloudShell: python3 mapear_inicio_progreso.py
Consulta ECS, STS y CloudWatch. Solo escribe su CSV de monitoreo en S3; no lee predicciones.
Los tiempos son informativos: no hay umbrales estandar de duracion por sigla.
Incluye tareas con desiredStatus RUNNING (pueden estar PENDING) y STOPPED visibles.
La captura de detenidas no es un historial completo. Busca sigla en parametros,
etiquetas y referencias explicitas de logs, sin inferirla por el cluster compartido.
Salida: tareas.csv/contexto.json locales y tareas.csv en ValidationReports/05-MonitorInicioProgreso/.
Logs RUNNING y metricas: ultimos 60 minutos. Logs STOPPED: vida de la tarea ECS.
End Script es cierre reportado de subunidad, no prueba de finalizacion global.
Exit: 0 sin FALLA (puede haber AVISO); 1 error confirmado de tarea/contenedor;
2 argumentos; 3 consulta incompleta/error AWS; 130 interrupcion.
"""
import argparse
import ast
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import sys
import textwrap


DEFAULT_CLUSTER = 'pon-prod-xcrime-predicciones-cluster'
DEFAULT_FAMILY = 'pon-prod-xcrime-prediccion-definition'
BOGOTA = timezone(timedelta(hours=-5))
REPORTS_BUCKET = 'pon-prod-ml-iapol'
REPORTS_PREFIX = 'ValidationReports/05-MonitorInicioProgreso/'


def upload_csv_reports(session, config, folder):
    """Publica solo los CSV de esta ejecucion; mantiene todos los archivos locales."""
    files = sorted(folder.glob('*.csv'))
    if not files:
        return {}
    # Las carpetas locales se crean con fecha y hora de Bogota.
    datetime.strptime(folder.parent.name, '%Y-%m-%d')
    datetime.strptime(folder.name, '%H-%M-%S_%f')
    key_root = REPORTS_PREFIX + folder.parent.name + '/' + folder.name + '/'
    s3 = session.client('s3', config=config)
    locations = {}
    for path in files:
        key = key_root + path.name
        with path.open('rb') as body:
            s3.upload_fileobj(body, REPORTS_BUCKET, key,
                              ExtraArgs={'ContentType': 'text/csv; charset=utf-8'})
        locations[path.name] = 's3://' + REPORTS_BUCKET + '/' + key
    print('[S3] CSV guardados en s3://' + REPORTS_BUCKET + '/' + key_root, flush=True)
    return locations

RANK = {'OK': 0, 'AVISO': 1, 'FALLA': 2}


def sigla_key(key):
    return re.sub(r'[^a-z]', '', str(key).lower()) in ('sigla', 'siglas', 'siglafisica')


def sigla_values(value):
    if isinstance(value, (list, tuple)):
        return set().union(*(sigla_values(v) for v in value)) if value else set()
    if not isinstance(value, str):
        return set()
    values = re.split(r'[,;\s]+', value.strip().upper())
    return {v for v in values if re.fullmatch(r'[A-Z][A-Z0-9]{2,11}', v)
            and v not in {'NONE', 'NULL', 'UNKNOWN', 'DESCONOCIDA', 'TODAS', 'TODOS', 'ALL'}}


def structured_siglas(value, depth=0):
    """Solo campos explicitos sigla; no exporta el payload que los contiene."""
    if depth > 8:
        return set()
    result = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if sigla_key(key):
                result.update(sigla_values(child))
            elif isinstance(child, (dict, list, tuple)):
                result.update(structured_siglas(child, depth+1))
    elif isinstance(value, (list, tuple)):
        for child in value:
            result.update(structured_siglas(child, depth+1))
    elif isinstance(value, str) and len(value) <= 65536 and value.lstrip().startswith(('{', '[')):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            try:
                parsed = ast.literal_eval(value)
            except (ValueError, SyntaxError, TypeError, RecursionError):
                return result
        result.update(structured_siglas(parsed, depth+1))
    return result


def task_sigla_evidence(task, container_definitions=()):
    evidence = set()
    def record(values, source):
        evidence.update((value, source) for value in values)
    for tag in task.get('tags', []):
        if sigla_key(tag.get('key')):
            record(sigla_values(tag.get('value')), 'etiqueta de tarea')
    defaults = {c['name']: c for c in container_definitions}
    overrides = {c['name']: c for c in task.get('overrides', {}).get('containerOverrides', [])}
    for container_name in sorted(set(defaults) | set(overrides)):
        container = overrides.get(container_name, {})
        # ECS reemplaza valores por nombre exacto, incluso con valor vacio.
        environment = {v['name']: v.get('value', '')
                       for v in defaults.get(container_name, {}).get('environment', [])}
        replaced = {v['name'] for v in container.get('environment', [])}
        environment.update({v['name']: v.get('value', '') for v in container.get('environment', [])})
        for name, value in environment.items():
            source = ('variable de ejecucion' if name in replaced else 'variable de definicion')
            if sigla_key(name) or name.upper() == 'CAD':
                # CAD es el nombre confirmado en el entorno de estas tareas.
                record(sigla_values(value), source + ' (' + name + ')')
            elif not re.search(r'secret|token|password|credential|key', name, re.I):
                record(structured_siglas(value), 'campo estructurado en ' + source)
        command = container.get('command', [])
        for i, token in enumerate(command):
            record(structured_siglas(token), 'parametro estructurado de ejecucion')
            flag, sep, value = token.partition('=')
            if flag.startswith('--') and sigla_key(flag):
                record(sigla_values(value if sep else command[i+1] if i+1 < len(command) else ''),
                       'argumento de ejecucion')
    return [{'sigla': value, 'fuente': source} for value, source in sorted(evidence)]


def assign_sigla(row, task, observations, container_definitions=()):
    evidence = task_sigla_evidence(task, container_definitions)
    direct = sorted({e['sigla'] for e in evidence})
    refs = sorted({s for o in observations for s in o.get('siglas_referenciadas', [])})
    row.update(sigla='NO IDENTIFICADA', sigla_estado='SIN EVIDENCIA',
               sigla_fuente='', siglas_referenciadas_logs=', '.join(refs),
               sigla_evidencias=json.dumps(evidence, ensure_ascii=False))
    if len(direct) == 1:
        row.update(sigla=direct[0], sigla_estado='CONFIGURADA EN TAREA',
                   sigla_fuente='; '.join(sorted({e['fuente'] for e in evidence})))
        if set(refs) - set(direct):
            row['sigla_estado'] = 'CONFIGURADA; OTRAS REFERENCIAS EN LOGS'
    elif len(direct) > 1:
        row.update(sigla='MULTIPLES', sigla_estado='SIN ATRIBUCION UNICA',
                   sigla_fuente=', '.join(direct))
    elif refs:
        row.update(sigla='SOLO REFERENCIAS', sigla_estado='NO CONFIRMADA', sigla_fuente='logs: ' + ', '.join(refs))


def analyze_logs(events):
    """Indicios conservadores; no equiparar texto de error con proceso fallido.

    No conservar mensajes completos: pueden incluir datos del negocio o secretos.
    Solo se extraen etiquetas explicitas y contadores estructurados conocidos.
    """
    result = dict(eventos=len(events), errores=0, reintentos=0,
                  ultima_emision_colombia='', etapa='', avance='', categorias={}, siglas_referenciadas=[],
                  cierres_reportados=0)
    categories = Counter()
    referenced = set()
    latest = max((e.get('timestamp', 0) for e in events), default=0)
    if latest:
        result['ultima_emision_colombia'] = local_time(datetime.fromtimestamp(latest/1000, timezone.utc))
    for event in sorted(events, key=lambda e: e.get('timestamp', 0)):
        message = str(event.get('message', ''))
        result['cierres_reportados'] += len(re.findall(r'\bEnd\s+Script\s*!', message, re.I))
        try:
            data = json.loads(message)
        except (ValueError, TypeError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        referenced.update(structured_siglas(message))
        for match in re.finditer(r'''(?i)(?<![\w])sigla(?:_fisica)?['"]?\s*[:=]\s*['"]?([A-Z][A-Z0-9]{2,11})(?=[\s'",;}\]]|$)''', message):
            referenced.update(sigla_values(match.group(1)))
        level = str(data.get('level', data.get('levelname', data.get('severity', '')))).upper()
        # Evitar coincidencias vagas como "error_rate=0" o "no errors".
        error = (level in ('ERROR', 'FATAL', 'CRITICAL') or
                 bool(re.search(r'(?:^|\s|\[)(?:ERROR|FATAL|CRITICAL)(?:\]|\s*[:|]|\s)', message)) or
                 'Traceback (most recent call last)' in message or
                 bool(re.match(r'^\s*[\w.]+(?:Error|Exception):', message)))
        retry = bool(re.search(r'\b(?:retrying|reintentando)\b', message, re.I))
        result['errores'] += int(error)
        result['reintentos'] += int(retry)
        if error:
            if 'Traceback (most recent call last)' in message:
                categories['traceback'] += 1
            elif re.search(r'\b(?:AccessDenied|AccessDeniedException)\b', message):
                categories['acceso'] += 1
            elif re.search(r'\b(?:ThrottlingException|TooManyRequestsException)\b', message):
                categories['throttling'] += 1
            else:
                categories['error_aplicacion'] += 1
        stage = data.get('etapa', data.get('stage'))
        if isinstance(stage, str) and re.fullmatch(r'[\w .:/-]{1,80}', stage):
            result['etapa'] = stage
        done, total = data.get('unidades_completadas'), data.get('unidades_total')
        if (isinstance(done, int) and not isinstance(done, bool) and
                isinstance(total, int) and not isinstance(total, bool) and 0 <= done <= total and total > 0):
            result['avance'] = f'{done}/{total} unidades reportadas'
    result['categorias'] = dict(categories)
    result['siglas_referenciadas'] = sorted(referenced)
    return result


def read_container_logs(client, group, stream, start, end, limit):
    events = []
    for page in client.get_paginator('filter_log_events').paginate(
            logGroupName=group, logStreamNames=[stream],
            startTime=int(start.timestamp()*1000), endTime=int(end.timestamp()*1000),
            PaginationConfig={'MaxItems': limit+1, 'PageSize': min(limit+1, 10000)}):
        events.extend(page.get('events', []))
    result = analyze_logs(events[:limit])
    result.update(grupo=group, stream=stream, truncado=len(events) > limit,
                  desde_utc=start.isoformat(), hasta_utc=end.isoformat())
    return result


def task_log_window(task, start, now):
    """Leer una tarea detenida aunque todos sus logs sean anteriores a la ventana."""
    began = utc_time(task.get('createdAt')) or utc_time(task.get('startedAt'))
    stopped = utc_time(task.get('stoppedAt'))
    if task.get('lastStatus') == 'STOPPED' and began and stopped and began <= stopped <= now:
        return began, min(stopped + timedelta(seconds=1), now)
    return start, now


def read_task_metrics(client, cluster, task_id, start, end):
    """Solo series de ESTA tarea, nunca promedios del cluster o contenedores."""
    result = []
    for name in ('CpuUtilized', 'MemoryUtilized'):
        series = []
        for page in client.get_paginator('list_metrics').paginate(
                Namespace='ECS/ContainerInsights', MetricName=name,
                Dimensions=[{'Name': 'ClusterName', 'Value': cluster}, {'Name': 'TaskId', 'Value': task_id}],
                PaginationConfig={'MaxItems': 21}):
            series.extend(page.get('Metrics', []))
        if len(series) > 20:
            raise ValueError('Mas de 20 series por metrica/tarea; cobertura incompleta')
        for metric in series:
            dims = {d['Name']: d['Value'] for d in metric['Dimensions']}
            if dims.get('TaskId') != task_id or dims.get('ClusterName') != cluster or 'ContainerName' in dims:
                continue
            response = client.get_metric_statistics(
                Namespace=metric['Namespace'], MetricName=name, Dimensions=metric['Dimensions'],
                StartTime=start, EndTime=end, Period=60, Statistics=['Average', 'Maximum'])
            points = sorted(response.get('Datapoints', []), key=lambda p: p['Timestamp'])
            if points:
                last = points[-1]
                result.append(dict(metrica=name, dimensiones=dims, muestras=len(points),
                                   ultimo_promedio=last.get('Average'), ultimo_maximo=last.get('Maximum'),
                                   unidad=last.get('Unit', ''), timestamp=last['Timestamp'].isoformat()))
    return result


def apply_telemetry(row, containers, observations, metrics, problems):
    row.update(estado_ecs_control=row['estado'], motivo_ecs=row['motivo'],
               eventos_log=sum(o.get('eventos', 0) for o in observations),
               indicios_error=sum(o.get('errores', 0) for o in observations),
               indicios_reintento=sum(o.get('reintentos', 0) for o in observations),
               cierres_subunidad_reportados=sum(o.get('cierres_reportados', 0) for o in observations),
               logs_cobertura='INCOMPLETA' if any(p.startswith('Logs') for p in problems) else 'LEIDA',
               logs_detalle=json.dumps(observations, ensure_ascii=False),
               metricas_detalle=json.dumps(metrics, ensure_ascii=False),
               metricas_cobertura=('ERROR LECTURA' if any(p.startswith('Metricas:') for p in problems)
                                   else 'DISPONIBLES' if metrics else 'SIN DATOS'),
               avance='NO VERIFICABLE', cpu='', memoria='',
               telemetria_limitaciones='; '.join(problems))
    evidence = [f'{o["contenedor"]}: ' + ' / '.join(filter(None, (o.get('etapa'), o.get('avance'))))
                for o in observations if o.get('etapa') or o.get('avance')]
    if evidence:
        row['avance'] = '; '.join(evidence)
    elif row['eventos_log']:
        row['avance'] = (f'{row["cierres_subunidad_reportados"]} cierres de subunidad reportados'
                         if row['cierres_subunidad_reportados'] else 'LOGS PRESENTES; sin avance explicito')
    latest_metrics = {}
    for metric in sorted(metrics, key=lambda m: (m['timestamp'], json.dumps(m['dimensiones'], sort_keys=True))):
        latest_metrics[metric['metrica']] = metric
    for metric in latest_metrics.values():
        value = metric['ultimo_promedio']
        if isinstance(value, (int, float)) and math.isfinite(value):
            field = 'cpu' if metric['metrica'] == 'CpuUtilized' else 'memoria'
            # CpuUtilized se publica en unidades de CPU; MemoryUtilized en MB.
            unit = 'CPU units' if field == 'cpu' else 'MB'
            row[field] = f'{value:.1f} {unit}'
    notices = []
    if row['indicios_error']:
        notices.append(f'{row["indicios_error"]} eventos con indicios de error; revisar logs')
    if row['indicios_reintento']:
        notices.append(f'{row["indicios_reintento"]} eventos de reintento')
    if problems:
        notices.append('Lectura de logs/metricas incompleta')
    if not containers or any(c.get('estado') != 'LEIDO' for c in observations):
        row['logs_cobertura'] = 'NO DISPONIBLE' if not row['eventos_log'] else 'PARCIAL'
        notices.append('Logs no disponibles para todos los contenedores')
    if notices:
        if RANK[row['estado']] < RANK['AVISO']:
            row['estado'] = 'AVISO'
        row['motivo'] += '; ' + '; '.join(notices)


def utc_time(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    return value.astimezone(timezone.utc)


def seconds(start, end):
    return round((end-start).total_seconds(), 3) if start is not None and end is not None else None


def local_time(value):
    return value.astimezone(BOGOTA).strftime('%Y-%m-%d %H:%M:%S') if value else ''


def duration_text(value):
    if value is None or value < 0:
        return '-'
    minutes, secs = divmod(int(value), 60)
    days, remainder = divmod(minutes, 24*60)
    hours, minutes = divmod(remainder, 60)
    return f'{days}d {hours:02}h {minutes:02}m' if days else f'{hours:02}h {minutes:02}m {secs:02}s'


def task_row(task, essential, now):
    """Clasificacion de evidencia ECS, sin deducir progreso ni demoras."""
    state = task.get('lastStatus', 'DESCONOCIDO')
    created, started = utc_time(task.get('createdAt')), utc_time(task.get('startedAt'))
    stopped, execution_end = utc_time(task.get('stoppedAt')), utc_time(task.get('executionStoppedAt'))
    end = (execution_end or stopped) if state == 'STOPPED' else (execution_end or now)
    startup_end = (started or stopped) if state == 'STOPPED' else (started or now)
    startup = seconds(created, startup_end)
    runtime = seconds(started, end)
    notes = []
    def note(status, message):
        notes.append((status, message))
    code = task.get('stopCode') or ''
    stopped_reason = str(task.get('stoppedReason') or '')
    containers = task.get('containers', [])
    exits = []
    essential_exits = []
    known_names = set(essential) if essential is not None else set()
    for c in containers:
        name, exit_code = c.get('name', '?'), c.get('exitCode')
        is_essential = essential.get(name) if name in known_names else None
        exits.append(f'{name}:{exit_code if exit_code is not None else "-"}')
        if is_essential:
            essential_exits.append(exit_code)
        if exit_code is not None and exit_code != 0:
            if is_essential:
                note('FALLA', f'Contenedor esencial {name} termino con codigo {exit_code}')
            else:
                label = 'no esencial' if is_essential is False else 'esencialidad no confirmada'
                note('AVISO', f'Contenedor {name} ({label}) termino con codigo {exit_code}')
        if c.get('healthStatus') == 'UNHEALTHY':
            note('FALLA' if is_essential else 'AVISO', f'Health check UNHEALTHY: {name}')
        reason = str(c.get('reason') or '')
        if reason.startswith(('OutOfMemoryError', 'CannotPullContainerError', 'ResourceInitializationError', 'CannotStartContainerError')):
            note('FALLA', f'{name}: {reason}')
    if task.get('healthStatus') == 'UNHEALTHY':
        note('FALLA', 'Health check de tarea UNHEALTHY')
    if code == 'TaskFailedToStart':
        note('FALLA', 'ECS informa TaskFailedToStart')
    if stopped_reason.startswith(('OutOfMemoryError', 'CannotPullContainerError', 'ResourceInitializationError', 'CannotStartContainerError')):
        note('FALLA', stopped_reason)
    if state == 'STOPPED':
        if code in ('UserInitiated', 'ServiceSchedulerInitiated', 'SpotInterruption', 'TerminationNotice'):
            note('AVISO', f'Parada/interrupcion reportada: {code}')
        elif code not in ('', 'EssentialContainerExited', 'TaskFailedToStart'):
            note('AVISO', f'Motivo de parada a revisar: {code}')
        if not started and code != 'TaskFailedToStart':
            note('AVISO', 'Detenida sin inicio registrado; no se confirma ejecucion')
        if not stopped:
            note('AVISO', 'STOPPED sin stoppedAt interpretable')
        required = {name for name, flag in (essential or {}).items() if flag}
        exits_known = {c.get('name') for c in containers if c.get('exitCode') is not None}
        if not essential_exits or any(value is None for value in essential_exits) or required - exits_known:
            note('AVISO', 'Sin codigo de salida confirmado de todos los contenedores esenciales')
    elif state != 'RUNNING':
        note('AVISO', f'Estado transitorio observado: {state}; sin umbral de tiempo')
    if state != 'STOPPED' and task.get('desiredStatus') == 'STOPPED':
        note('AVISO', 'ECS ya solicito detener la tarea; cierre aun no completado')
    if state == 'RUNNING' and not started:
        note('AVISO', 'RUNNING sin startedAt interpretable')
    if startup is None:
        note('AVISO', 'Tiempo de arranque/espera no calculable')
    for value, label in ((startup, 'arranque'), (runtime, 'ejecucion')):
        if value is not None and value < 0:
            note('AVISO', f'Tiempos incoherentes en {label}')
    if not notes:
        note('OK', 'Finalizada sin error ECS' if state == 'STOPPED' else 'Ejecutando sin error ECS')
    result = max((status for status, _ in notes), key=RANK.get)
    arn = task.get('taskArn', '')
    return dict(tarea=arn.rsplit('/', 1)[-1], task_arn=arn,
                cluster=task.get('clusterArn', '').rsplit('/', 1)[-1],
                definicion=task.get('taskDefinitionArn', '').rsplit('/', 1)[-1],
                estado_ecs=state, estado_deseado=task.get('desiredStatus', ''),
                creada_colombia=local_time(created), inicio_colombia=local_time(started),
                fin_colombia=local_time(stopped), arranque_o_espera_segundos=startup,
                ejecucion_segundos=runtime, codigos_salida='; '.join(exits),
                stop_code=code, motivo_parada=stopped_reason,
                health_status=task.get('healthStatus', 'UNKNOWN'),
                estado=result, motivo='; '.join(message for _, message in notes),
                detalle_contenedores=json.dumps([{k: c.get(k) for k in ('name', 'lastStatus', 'exitCode', 'reason', 'healthStatus')}
                                                for c in containers], ensure_ascii=False))


def collect_tasks(ecs, cluster, family, limit, read_errors=()):
    seen, tasks, issues = set(), [], []
    for desired in ('RUNNING', 'STOPPED'):
        arns = []
        for page in ecs.get_paginator('list_tasks').paginate(
                cluster=cluster, family=family, desiredStatus=desired,
                PaginationConfig={'MaxItems': limit+1, 'PageSize': min(limit+1, 100)}):
            arns.extend(page.get('taskArns', []))
        if len(arns) > limit:
            issues.append(f'INCOMPLETO: mas de {limit} tareas para desiredStatus={desired}; aumentar --max-tareas')
        selected = [arn for arn in arns[:limit] if arn not in seen]
        seen.update(selected)
        for start in range(0, len(selected), 100):
            try:
                response = ecs.describe_tasks(cluster=cluster, tasks=selected[start:start+100], include=['TAGS'])
            except read_errors:
                # Si no se autoriza leer etiquetas, conservar el monitoreo ECS.
                issues.append('No se pudieron leer etiquetas de tareas; se reintenta DescribeTasks sin TAGS')
                response = ecs.describe_tasks(cluster=cluster, tasks=selected[start:start+100])
            for failure in response.get('failures', []):
                issues.append('DescribeTasks: ' + str(failure.get('arn', '')) + ' ' + str(failure.get('reason', 'sin motivo')))
            tasks.extend(response.get('tasks', []))
    return tasks, issues


def print_rows(rows):
    headers = ('Tarea*', 'Sigla', 'Estado ECS', 'Arr./espera', 'Ejecucion', 'Salida**', 'Control', 'Motivo')
    widths = (12, 17, 12, 12, 13, 8, 7, 35)
    def render(cells):
        wrapped = [textwrap.wrap(' '.join(str(v).split()), width=w) or [''] for v, w in zip(cells, widths)]
        for i in range(max(map(len, wrapped))):
            print(' | '.join((c[i] if i < len(c) else '').ljust(w) for c, w in zip(wrapped, widths)))
    print()
    render(headers)
    print('-+-'.join('-'*width for width in widths))
    for row in rows:
        codes = [part.rsplit(':', 1)[-1] for part in row['codigos_salida'].split('; ') if part]
        render((row['tarea'][:12], row['sigla'], row['estado_ecs'], duration_text(row['arranque_o_espera_segundos']),
                duration_text(row['ejecucion_segundos']), ','.join(codes) or '-', row['estado'], row['motivo']))
    if not rows:
        print('(Sin tareas visibles de la familia seleccionada; no demuestra falta de ejecuciones programadas.)')
    print('* ID abreviado; ARN completo en CSV. ** Codigos de salida; nombres de contenedores en CSV.')
    print('Inicio/espera: creacion hasta inicio; si no inicio, hasta parada o captura. Ejecucion: tiempo desde startedAt.')
    for row in rows:
        print(f'Sigla {row["tarea"][:12]}: {row["sigla"]} | {row["sigla_estado"]}' +
              (f' | {row["sigla_fuente"]}' if row['sigla_fuente'] else ''))


def print_telemetry(rows, window):
    print(f'\nTELEMETRIA | Activas: ultimos {window} minutos; logs STOPPED: entre creacion y parada ECS')
    widths = (12, 23, 7, 16, 14, 40)
    def render(values):
        print(' | '.join(textwrap.shorten(str(v), width=w, placeholder='...').ljust(w)
                         for v, w in zip(values, widths)))
    render(('Tarea', 'Eventos / lectura logs', 'Err/Ret', 'CPU*', 'Memoria*', 'Actividad / avance reportado'))
    print('-+-'.join('-'*w for w in widths))
    for row in rows:
        render((row['tarea'][:12], f'{row["eventos_log"]} / {row["logs_cobertura"]}',
                f'{row["indicios_error"]}/{row["indicios_reintento"]}',
                row['cpu'] or 'SIN DATOS', row['memoria'] or 'SIN DATOS', row['avance']))
    print('* Ultimo promedio de 1 minuto disponible, por tarea; detalle y hora en CSV. Sin umbrales de consumo.')
    print('Indicios en logs son AVISO, no confirman fallo. Sin logs/metricas no se infiere bloqueo.')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cluster', default=DEFAULT_CLUSTER)
    parser.add_argument('--familia', default=DEFAULT_FAMILY)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--max-tareas', type=int, default=500, help='Limite de lectura por estado deseado; no es umbral de duracion')
    parser.add_argument('--ventana-minutos', type=int, default=60, help='Ventana de logs/metricas, no umbral de demora (default: 60)')
    parser.add_argument('--max-eventos', type=int, default=1000, help='Eventos por contenedor en la ventana; truncamiento explicito')
    parser.add_argument('--salida', default='salida')
    parser.add_argument('--no-export', action='store_true', help='No genera archivos locales ni sube CSV a S3')
    parser.add_argument('--detallado', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.max_tareas <= 10000:
        parser.error('--max-tareas debe estar entre 1 y 10000')
    if not 1 <= args.ventana_minutos <= 1440:
        parser.error('--ventana-minutos debe estar entre 1 y 1440')
    if not 1 <= args.max_eventos <= 10000:
        parser.error('--max-eventos debe estar entre 1 y 10000')
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Ejecutar en CloudShell con boto3/botocore disponibles.', file=sys.stderr)
        return 3
    try:
        session = boto3.Session(region_name=args.region)
        config = Config(connect_timeout=5, read_timeout=20, retries={'total_max_attempts': 3, 'mode': 'standard'})
        identity = session.client('sts', config=config).get_caller_identity()
        if identity['Account'] != args.cuenta:
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan tareas.', file=sys.stderr)
            return 3
        ecs = session.client('ecs', config=config)
        cluster_response = ecs.describe_clusters(clusters=[args.cluster])
        if cluster_response.get('failures') or not cluster_response.get('clusters'):
            print(f'[BLOQUEADO] Cluster no encontrado/accesible: {args.cluster}', file=sys.stderr)
            return 3
        print(f'INICIO Y EJECUCION | ECS | {args.cluster}', flush=True)
        print(f'Familia: {args.familia} | Cuenta: {identity["Account"]} | Region: {args.region}')
        print('Consultando ECS, logs y metricas disponibles por tarea...', flush=True)
        tasks, issues = collect_tasks(ecs, args.cluster, args.familia, args.max_tareas, (ClientError, BotoCoreError))
        definitions, log_definitions, environment_definitions = {}, {}, {}
        for arn in sorted({task['taskDefinitionArn'] for task in tasks if task.get('taskDefinitionArn')}):
            try:
                definition = ecs.describe_task_definition(taskDefinition=arn)['taskDefinition']
                # Solo en memoria: no incluir variables completas en CSV/contexto.
                environment_definitions[arn] = [
                    {'name': c['name'], 'environment': c.get('environment', [])}
                    for c in definition.get('containerDefinitions', [])]
                definitions[arn] = {c['name']: c.get('essential', True) for c in definition.get('containerDefinitions', [])}
                log_definitions[arn] = []
                for c in definition.get('containerDefinitions', []):
                    log = c.get('logConfiguration', {})
                    options = log.get('options', {})
                    log_definitions[arn].append(dict(nombre=c['name'], driver=log.get('logDriver'),
                        grupo=options.get('awslogs-group'), region=options.get('awslogs-region', args.region),
                        prefijo=options.get('awslogs-stream-prefix')))
            except (ClientError, BotoCoreError) as exc:
                # Conservar la captura de tareas si no se puede confirmar esencialidad.
                issues.append(f'No se pudo describir {arn}: {type(exc).__name__}')
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=args.ventana_minutos)
        log_clients = {}
        cloudwatch = session.client('cloudwatch', config=config)
        rows = []
        for number, task in enumerate(tasks, 1):
            if number == 1 or number % 10 == 0:
                print(f'Leyendo telemetria: tarea {number}/{len(tasks)}...', flush=True)
            arn = task.get('taskDefinitionArn')
            row = task_row(task, definitions.get(arn), now)
            log_start, log_end = task_log_window(task, start, now)
            observations, problems = [], []
            containers = log_definitions.get(arn, [])
            for c in containers:
                observation = dict(contenedor=c['nombre'], estado='NO_CONFIGURADO')
                if c['driver'] == 'awslogs' and c['grupo'] and c['prefijo']:
                    try:
                        if c['region'] not in log_clients:
                            log_clients[c['region']] = session.client('logs', region_name=c['region'], config=config)
                        stream = f'{c["prefijo"]}/{c["nombre"]}/{row["tarea"]}'
                        observation.update(read_container_logs(log_clients[c['region']], c['grupo'], stream,
                                                               log_start, log_end, args.max_eventos))
                        observation['estado'] = 'LEIDO'
                        if observation['truncado']:
                            observation['estado'] = 'TRUNCADO'
                            problems.append(f'Logs {c["nombre"]}: supera --max-eventos; se leyeron los primeros eventos de la ventana')
                    except (ClientError, BotoCoreError) as exc:
                        # Aislar la fuente fallida: ECS y otros contenedores siguen siendo evaluables.
                        error = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                        observation.update(estado='ERROR_LECTURA', error=error)
                        problems.append(f'Logs {c["nombre"]}: {error}')
                observations.append(observation)
            metrics = []
            try:
                metrics = read_task_metrics(cloudwatch, args.cluster.rsplit('/', 1)[-1], row['tarea'], start, now)
            except (ClientError, BotoCoreError, ValueError) as exc:
                error = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                problems.append(f'Metricas: {error}')
            if not containers:
                problems.append('Logs: definicion no disponible o sin contenedores')
            apply_telemetry(row, containers, observations, metrics, problems)
            assign_sigla(row, task, observations, environment_definitions.get(arn, ()))
            issues.extend(f'{row["tarea"][:12]}: {problem}' for problem in problems)
            rows.append(row)
        rows.sort(key=lambda row: (-RANK[row['estado']], row['estado_ecs'], row['tarea']))
        print_rows(rows)
        print_telemetry(rows, args.ventana_minutos)
        for issue in issues:
            print('[INCOMPLETO] ' + issue)
        states = Counter(row['estado_ecs'] for row in rows)
        controls = Counter(row['estado'] for row in rows)
        print(f'\nTareas: {len(rows)}' + (' | ' + ' | '.join(f'{key}: {count}' for key, count in sorted(states.items())) if states else ''))
        print(f'Controles por tarea: {controls["OK"]} OK | {controls["AVISO"]} AVISO | {controls["FALLA"]} FALLA')
        print('Duraciones informativas, sin limites estandar. RUNNING/OK no demuestra avance ni calidad de prediccion.')
        print('STOPPED: solo tareas visibles en ECS. Historial por streams en mapear_modelos_dependencias.py --historial-logs.')
        print('End Script no prueba cierre global. Sigla segun evidencia; sin consultas a DynamoDB/S3.')
        print('CPU/memoria requieren series existentes de Container Insights por TaskId; SIN DATOS no significa cero consumo.')
        if not args.no_export:
            folder = Path(args.salida)/'inicio_progreso'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
            folder.mkdir(parents=True, exist_ok=True)
            with (folder/'tareas.csv').open('w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ['tarea', 'estado_ecs', 'estado', 'motivo'])
                writer.writeheader()
                writer.writerows(rows)
            context = {'captura_utc': now.isoformat(), 'configuracion': vars(args), 'cuenta': identity['Account'],
                       'cluster': {key: cluster_response['clusters'][0].get(key) for key in ('clusterArn', 'status', 'runningTasksCount', 'pendingTasksCount')},
                       'definiciones_esencialidad': definitions, 'configuracion_logs': log_definitions,
                       'ventana_telemetria_utc': {'desde': start.isoformat(), 'hasta': now.isoformat()},
                       'completo': not issues, 'limitaciones': issues,
                       'telemetria_disponible_en_todas_las_tareas': bool(rows) and all(
                           r['logs_cobertura'] == 'LEIDA' and r['metricas_cobertura'] == 'DISPONIBLES' for r in rows),
                       'tareas': rows,
                       'alcance': 'ECS y telemetria existente en ventana acotada. Avance solo si la aplicacion lo reporta; indicios de error no prueban fallo. No valida S3 ni solicitudes sin tarea, no conserva eventos ocurridos entre capturas.'}
            (folder/'contexto.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
            print(f'Reportes locales: {folder}/tareas.csv y contexto.json')
            upload_csv_reports(session, config, folder)
        if args.detallado:
            for row in rows:
                print(json.dumps(row, ensure_ascii=False, indent=2))
        return 3 if issues else 1 if controls['FALLA'] else 0
    except (ClientError, BotoCoreError, OSError) as exc:
        print(f'[BLOQUEADO] {type(exc).__name__}: {exc}', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Lectura interrumpida; ninguna tarea fue detenida.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
