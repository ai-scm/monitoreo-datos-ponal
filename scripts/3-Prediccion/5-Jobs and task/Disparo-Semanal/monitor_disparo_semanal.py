#!/usr/bin/env python3
"""Mapeo manual EventBridge -> Lambda, solo lectura. No invoca funciones ni crea alarmas.

CloudShell: python3 ~/monitor_disparo_semanal.py
Consulta reglas del bus default, sus targets y las Lambdas esperadas/enlazadas.
Metricas de los ultimos 8 dias, agregadas por recurso; no certifican el lanzamiento ECS.
Salida: una tabla y un unico informe_disparo_semanal.csv local con resumen,
controles, contexto y evidencias. Logs acotados, sin exportar mensajes.
Exit: 0 consultas completas (puede haber avisos); 1 recurso no encontrado;
3 lectura incompleta o bloqueo; 2 argumentos; 130 interrupcion.
"""
import argparse
import ast
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
import textwrap


RULES = ('xcrime_prediccion_siglas_unidades', 'generacion_informes_anticipaciones')
FUNCTIONS = ('pon-prod-xcrime-prediccion-launch-fargate', 'pon_prod_xcrime_informes_anticipaciones')
BOGOTA = timezone(timedelta(hours=-5))
LIMITATIONS = [
    'No invoca Lambda, no ejecuta ECS y no modifica AWS. No crea alarmas, DLQ ni notificaciones.',
    'Metricas agregadas por regla/funcion, no por disparo, sigla ni solicitante; no se correlacionan por proximidad.',
    'Invocations de Lambda incluye todos sus llamadores; no prueba invocacion desde esta regla ni creacion de tareas ECS.',
    'SIN DATOS no equivale a cero, recurso sano, falta de ejecucion o fallo del disparo.',
    'Cron/rate se muestran como estan configurados, en UTC para reglas programadas. No se calcula un domingo esperado.',
    'Estado de configuracion actual y metricas de la ventana pueden corresponder a configuraciones diferentes.',
    'Los targets DLQ y destinos Lambda son referencias configuradas; no se validan permisos, conectividad ni entrega.',
    'Se consulta $LATEST y se enumeran configuraciones asincronas por version/alias; no se certifica la configuracion efectiva de targets calificados.',
    'Logs: lectura acotada de la lanzadora. No interpreta mensajes por proximidad ni convierte ARNs mencionados en tareas creadas.',
    'Solo operation/action/event_name/api=RunTask (o prefijo RunTask) junto con response.tasks aporta creacion reportada. No prueba estado RUNNING.',
    'Count solicitado y failures son declaraciones en logs, no llamadas API unicas ni cantidades esperadas por sigla. Repeticiones pueden alterar sus sumas.',
    'No descubre EventBridge Scheduler, otros buses, reglas desconocidas ni workflows. No conserva historial semanal persistente.'
]
TASK_ARN = re.compile(r'arn:aws(?:-[a-z]+)?:ecs:[a-z0-9-]+:\d{12}:task/[A-Za-z0-9_/-]+')
LOG_ERRORS = ('AccessDeniedException', 'ClientException', 'InvalidParameterException',
    'ResourceNotFoundException', 'ServiceUnavailableException', 'ThrottlingException',
    'NoCredentialsError', 'EndpointConnectionError', 'ConnectTimeoutError',
    'ReadTimeoutError', 'RuntimeError', 'ValueError', 'KeyError', 'TypeError')


def stamp(value):
    return value.astimezone(BOGOTA).isoformat() if isinstance(value, datetime) and value.tzinfo else ''


def function_from_arn(arn, account, region):
    parts = arn.split(':')
    if len(parts) >= 7 and parts[0] == 'arn' and parts[2:6] == ['lambda', region, account, 'function']:
        return parts[6]
    return None


def paginate(client, name, result_key, **kwargs):
    result = []
    for page in client.get_paginator(name).paginate(**kwargs):
        result.extend(page.get(result_key, []))
    return result


class Reader:
    """Frontera de lectura: un permiso ausente no oculta los demas resultados."""
    def __init__(self, errors):
        self.errors = errors
        self.issues = []

    def read(self, resource, operation, call):
        try:
            return call()
        except self.errors as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
            self.issues.append(dict(recurso=resource, operacion=operation, error=code,
                tipo='AUSENTE' if code == 'ResourceNotFoundException' and operation in
                    ('DescribeRule', 'GetFunctionConfiguration') else 'INCOMPLETO'))
            return None


def metric(reader, cw, resource, namespace, name, dimensions, start, end, stat='Sum'):
    response = reader.read(resource, 'metrica:'+name, lambda: cw.get_metric_statistics(
        Namespace=namespace, MetricName=name, Dimensions=dimensions,
        StartTime=start, EndTime=end, Period=3600, Statistics=[stat]))
    if response is None:
        return dict(lectura='ERROR', valor=None, muestras=0, ultima_muestra='')
    points = [p for p in response.get('Datapoints', []) if stat in p]
    if not points:
        return dict(lectura='SIN DATOS', valor=None, muestras=0, ultima_muestra='')
    values = [p[stat] for p in points]
    times = [p['Timestamp'] for p in points if isinstance(p.get('Timestamp'), datetime)]
    return dict(lectura='LEIDA', valor=sum(values) if stat == 'Sum' else max(values),
        muestras=len(points), ultima_muestra=stamp(max(times)) if times else '')


def shown(result):
    value = result.get('valor')
    return f'{value:g}' if value is not None else result.get('lectura', 'SIN DATOS')


def control(rows, resource, name, state, detail):
    rows.append(dict(recurso=resource, control=name, estado=state, detalle=detail))


def add_metric_controls(controls, resource, metrics, failures):
    for name in failures:
        result = metrics[name]
        if result['valor'] is not None and result['valor'] > 0:
            control(controls, resource, name, 'AVISO', f'{shown(result)} eventos observados en la ventana; revisar su origen')
    if metrics['Invocations']['valor'] is None:
        control(controls, resource, 'Actividad', 'INFO', 'Sin evidencia de invocaciones en las metricas consultadas; no demuestra fallo')


def row_status(controls, reader, resource):
    issues = [i for i in reader.issues if i['recurso'] == resource]
    if any(i['tipo'] == 'INCOMPLETO' for i in issues):
        return 'INCOMPLETO'
    if any(i['tipo'] == 'AUSENTE' for i in issues):
        return 'AUSENTE'
    return 'AVISO' if any(c['recurso'] == resource and c['estado'] == 'AVISO' for c in controls) else 'INFO'


def log_object(message):
    """JSON o literal Python acotado, nunca eval ni ejecucion del mensaje."""
    start = message.find('{')
    if start < 0 or len(message) > 65536:
        return {}
    candidate = message[start:].strip()
    try:
        data = json.loads(candidate)
    except (ValueError, RecursionError):
        try:
            data = ast.literal_eval(candidate)
        except (ValueError, SyntaxError, RecursionError, TypeError):
            return {}
    return data if isinstance(data, dict) else {}


def launch_findings(message):
    """Solo hechos del mismo evento; no guarda el texto ni sus payloads."""
    root = log_object(message)
    inner = root.get('message', '')
    payload = inner if isinstance(inner, dict) else log_object(inner) if isinstance(inner, str) else {}
    def is_runtask(data):
        return any(re.sub(r'[^a-z]', '', str(data.get(k, '')).lower()) in
            ('runtask', 'ecsruntask', 'runtaskresponse', 'runtaskrequest')
            for k in ('operation', 'action', 'event_name', 'api'))
    prefixes = [message.split('{', 1)[0], inner.split('{', 1)[0] if isinstance(inner, str) else '']
    explicit = is_runtask(root) or is_runtask(payload) or any(re.search(
        r'(?i)\b(?:RunTask|run_task)\s*(?:response|request|respuesta|solicitud)?\s*[:=]?\s*$', prefix)
        for prefix in prefixes)
    data = payload or root
    response = data.get('response', data.get('result', data))
    request = data.get('request', data)
    findings = []
    if explicit and isinstance(request, dict):
        count = request.get('count')
        if isinstance(count, int) and not isinstance(count, bool) and 1 <= count <= 10:
            findings.append(dict(tipo='Solicitadas declaradas', cantidad=count))
    if explicit and isinstance(response, dict):
        for task in response.get('tasks', []) if isinstance(response.get('tasks'), list) else []:
            arn = task.get('taskArn') if isinstance(task, dict) else None
            if isinstance(arn, str) and TASK_ARN.fullmatch(arn):
                findings.append(dict(tipo='Tarea en respuesta RunTask', arn_tarea=arn))
        for failure in response.get('failures', []) if isinstance(response.get('failures'), list) else []:
            if not isinstance(failure, dict):
                continue
            reason = failure.get('reason', '')
            code = reason if isinstance(reason, str) and re.fullmatch(r'[A-Z][A-Z0-9_:.-]{0,95}', reason) else 'MOTIVO NO PROYECTADO'
            findings.append(dict(tipo='Fallo en respuesta RunTask', codigo=code))
    for arn in sorted(set(TASK_ARN.findall(message))):
        findings.append(dict(tipo='ARN tarea mencionado', arn_tarea=arn))
    codes = [c for c in LOG_ERRORS if re.search(r'(?<!\w)'+c+r'(?!\w)', message)]
    level = str(root.get('level', root.get('log_level', ''))).upper()
    if codes or level in ('ERROR', 'FATAL', 'CRITICAL') or re.search(
            r'(?m)^\s*(?:\[?(?:ERROR|FATAL|CRITICAL)\]?\s|Traceback \(most recent call last\)|Task timed out after)', message):
        findings.append(dict(tipo='Indicio de error en log', codigo=','.join(codes) or 'NIVEL/TRACEBACK/TIMEOUT'))
    start = re.search(r'(?m)^START RequestId:\s*([A-Za-z0-9-]{1,100})\b', message)
    request_id = start.group(1) if start else ''
    if root.get('type') == 'platform.start' and isinstance(root.get('record'), dict):
        candidate = root['record'].get('requestId', '')
        if isinstance(candidate, str) and re.fullmatch(r'[A-Za-z0-9-]{1,100}', candidate):
            request_id = candidate
    return findings, request_id


def read_launch_logs(logs, group, start, end, limit):
    stats = dict(lectura='LEIDA', eventos_leidos=0, inicios_lambda_observados=0,
        arn_tareas_mencionadas=0, arn_tareas_runtask=0, solicitadas_declaradas=None,
        declaraciones_solicitud=0, fallos_runtask_observados=0, eventos_con_error=0,
        limites=[])
    details, seen, starts, mentions, created = [], set(), set(), set(), set()
    total_bytes = 0
    stop = False
    for page in logs.get_paginator('filter_log_events').paginate(logGroupName=group,
            startTime=int(start.timestamp()*1000), endTime=int(end.timestamp()*1000),
            PaginationConfig={'MaxItems': limit+1, 'PageSize': min(limit+1, 1000)}):
        for event in page.get('events', []):
            if stats['eventos_leidos'] >= limit:
                stats['limites'].append('Limite de eventos; se leyeron los primeros de la ventana')
                stop = True
                break
            message = str(event.get('message', ''))
            total_bytes += len(message.encode('utf-8'))
            if total_bytes > 16*1024**2:
                stats['limites'].append('Presupuesto de lectura de 16 MiB alcanzado')
                stop = True
                break
            key = (event.get('logStreamName'), event.get('eventId'))
            if key[1] and key in seen:
                continue
            if key[1]:
                seen.add(key)
            stats['eventos_leidos'] += 1
            if len(message) > 65536:
                if 'Mensaje mayor de 64 KiB omitido' not in stats['limites']:
                    stats['limites'].append('Mensaje mayor de 64 KiB omitido')
                continue
            findings, request_id = launch_findings(message)
            if request_id:
                starts.add(request_id)
            timestamp = event.get('timestamp')
            for finding in findings:
                details.append(dict(instante=stamp(datetime.fromtimestamp(timestamp/1000, timezone.utc))
                    if isinstance(timestamp, (int, float)) else '', evento_id=event.get('eventId', ''),
                    tipo=finding['tipo'], arn_tarea=finding.get('arn_tarea', ''),
                    cantidad=finding.get('cantidad', ''), codigo=finding.get('codigo', '')))
                kind = finding['tipo']
                if kind == 'ARN tarea mencionado':
                    mentions.add(finding['arn_tarea'])
                elif kind == 'Tarea en respuesta RunTask':
                    created.add(finding['arn_tarea'])
                elif kind == 'Solicitadas declaradas':
                    stats['solicitadas_declaradas'] = (stats['solicitadas_declaradas'] or 0)+finding['cantidad']
                    stats['declaraciones_solicitud'] += 1
                elif kind == 'Fallo en respuesta RunTask':
                    stats['fallos_runtask_observados'] += 1
                elif kind == 'Indicio de error en log':
                    stats['eventos_con_error'] += 1
        if stop:
            break
    stats.update(inicios_lambda_observados=len(starts), arn_tareas_mencionadas=len(mentions),
                 arn_tareas_runtask=len(created), lectura='PARCIAL' if stats['limites'] else 'LEIDA')
    return stats, details


def print_table(rows):
    fields = ('tipo', 'recurso', 'configuracion', 'actividad', 'control')
    widths = (11, 46, 35, 42, 10)
    def render(values):
        cells = [textwrap.wrap(str(v), width=w, break_long_words=True, break_on_hyphens=False) or ['-']
                 for v, w in zip(values, widths)]
        for index in range(max(map(len, cells))):
            print(' | '.join((cell[index] if index < len(cell) else '').ljust(width)
                             for cell, width in zip(cells, widths)))
    render(('Tipo', 'Recurso', 'Configuracion / manejo de fallo', 'Metricas / evidencia de la ventana', 'Control'))
    print('-+-'.join('-'*w for w in widths))
    for row in rows:
        render([row[field] for field in fields])


def write_report(folder, summaries, controls, evidence, context):
    """Un CSV filtrable por tipo_registro, sin perder contexto ni evidencia."""
    path = folder/'informe_disparo_semanal.csv'
    fields = ['tipo_registro', 'recurso', 'tipo', 'estado', 'control', 'configuracion',
              'actividad', 'detalle', 'instante', 'evento_id', 'arn_tarea',
              'cantidad', 'codigo', 'campo', 'valor']
    def leaves(value, field='', resource='GENERAL'):
        if isinstance(value, dict) and value:
            resource = value.get('nombre', value.get('recurso', resource))
            for key, item in value.items():
                yield from leaves(item, field+'/'+str(key), resource)
        elif isinstance(value, list) and value:
            for index, item in enumerate(value):
                yield from leaves(item, field+'/'+str(index), resource)
        else:
            yield dict(tipo_registro='CONTEXTO', recurso=resource, campo=field,
                       valor=json.dumps(value, ensure_ascii=False))
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in summaries:
            writer.writerow(dict(tipo_registro='RESUMEN', estado=row['control'], **row))
        for row in controls:
            writer.writerow(dict(tipo_registro='CONTROL', **row))
        # Los controles ya estan en su seccion; el resto se conserva como campos.
        writer.writerows(leaves({key: value for key, value in context.items() if key != 'controles'}))
        for row in evidence:
            writer.writerow(dict(tipo_registro='EVIDENCIA', recurso=row['funcion'],
                                 **{key: value for key, value in row.items() if key != 'funcion'}))
    return path


def message_profile(message, message_depth=0):
    """Estructura proyectada; ningun literal, llamada o mensaje se ejecuta."""
    allowed = {'tasks', 'failures', 'taskArn', 'clusterArn', 'taskDefinitionArn',
        'lastStatus', 'desiredStatus', 'ResponseMetadata', 'HTTPStatusCode',
        'operation', 'action', 'event_name', 'eventName', 'eventSource', 'api',
        'request', 'response', 'result', 'requestParameters', 'responseElements',
        'count', 'message', 'reason', 'detail', 'status', 'statusCode', 'type', 'record',
        'createdAt', 'startedAt', 'stoppedAt'}
    statuses = {'RUNNING', 'PENDING', 'PROVISIONING', 'STOPPED', 'DEPROVISIONING'}
    vocabulary = {word.lower() for word in allowed | statuses | {
        'RunTask', 'run_task', 'DescribeTasks', 'describe_tasks', 'StartTask',
        'OperationModel', 'name', 'body', 'parameters', 'params', 'received',
        'sending', 'created', 'creating', 'launched', 'launching', 'started',
        'starting', 'task', 'tarea', 'tareas', 'creada', 'creadas', 'creado',
        'creados', 'lanzada', 'lanzadas', 'lanzado', 'lanzados', 'respuesta',
        'solicitud', 'success', 'successful', 'successfully', 'INFO', 'DEBUG',
        'ERROR', 'WARN', 'headers', 'b'}}
    def template(text):
        tokens = re.findall(r'[A-Za-z0-9_]+|[{}\[\]():=,.\'"\\;-]', text[:4000])
        masked = [token if token.lower() in vocabulary or len(token) == 1 and not token.isalnum()
                  else '<VALOR>' for token in tokens]
        return re.sub(r'(?:<VALOR>\s*){2,}', '<VALOR> ', ' '.join(masked))[:1200]
    def project(node, field='', depth=0):
        if depth > 6:
            return '[PROFUNDIDAD LIMITADA]'
        if isinstance(node, ast.Dict):
            pairs = [(k.value, v) for k, v in zip(node.keys, node.values)
                     if isinstance(k, ast.Constant) and isinstance(k.value, str) and k.value in allowed]
            return {key: project(value, key, depth+1) for key, value in pairs[:30]} or '[CAMPOS NO PROYECTADOS]'
        if isinstance(node, (ast.List, ast.Tuple)):
            return [project(v, field, depth+1) for v in node.elts[:2]]+(['[MAS ELEMENTOS]'] if len(node.elts) > 2 else [])
        if isinstance(node, ast.Call):
            return '[OBJETO PYTHON NO EJECUTADO]'
        if isinstance(node, ast.Constant):
            value = node.value
            if isinstance(value, str):
                if field == 'message' and message_depth < 2:
                    inner = message_profile(value, message_depth+1)
                    if inner['formato'] in ('OBJETO/LISTA', 'OBJETO SERIALIZADO'):
                        return inner
                if field in ('operation', 'action', 'api', 'eventName', 'event_name') and value in ('RunTask', 'run_task', 'DescribeTasks', 'describe_tasks', 'StartTask', 'ecs.RunTask'):
                    return value
                if field in ('lastStatus', 'desiredStatus', 'status') and value in statuses:
                    return value
                if TASK_ARN.fullmatch(value):
                    return '[ARN TAREA ECS]'
                return '[TEXTO]'
            if field in ('count', 'HTTPStatusCode', 'statusCode') and isinstance(value, int) and not isinstance(value, bool):
                return value
            return '[VALOR]'
        return '[EXPRESION NO EJECUTADA]'
    markers = sorted(set(re.findall(r'\b(?:RunTask|run_task|DescribeTasks|describe_tasks|StartTask|start_task)\b', message)))
    if len(message) > 65536:
        return dict(formato='MENSAJE GRANDE', marcadores=markers, estructura='Mayor de 64 KiB; no analizado')
    # Prueba principios de objeto/lista, omitiendo prefijos de logging. Solo AST.
    positions = [match.start() for match in re.finditer(r'[\[{]', message)][:12]
    # Respuestas como b'{"tasks": ...}' requieren incluir comilla/prefijo bytes.
    positions += [match.start('literal') for match in re.finditer(
        r"(?:^|[\s:\t])(?P<literal>b?['\"]\s*[\[{])", message)][:4]
    for position in positions:
        candidate = message[position:].strip()
        try:
            tree = ast.parse(candidate, mode='eval')
        except (ValueError, SyntaxError, RecursionError):
            continue
        if isinstance(tree.body, (ast.Dict, ast.List, ast.Tuple)):
            return dict(formato='OBJETO/LISTA', marcadores=markers, estructura=project(tree.body))
        if isinstance(tree.body, ast.Constant) and isinstance(tree.body.value, (str, bytes)) and message_depth < 2:
            value = tree.body.value
            if isinstance(value, bytes):
                value = value.decode('utf-8', errors='replace')
            inner = message_profile(value, message_depth+1)
            if inner['formato'] == 'OBJETO/LISTA':
                return dict(formato='OBJETO SERIALIZADO', marcadores=markers or inner['marcadores'], estructura=inner['estructura'])
    return dict(formato='TEXTO', marcadores=markers,
                estructura='Sin objeto/lista interpretable', plantilla=template(message))


def review_log_formats(session, config, identity, args, errors):
    """Recuperar hasta seis eventos del informe por ID y milisegundo exactos."""
    if args.informe:
        files = [Path(args.informe).expanduser()]
    else:
        files = sorted((Path(args.salida).expanduser()/'disparo_semanal').glob('*/*/informe_disparo_semanal.csv'),
                       key=lambda p: p.stat().st_mtime, reverse=True)
    source, evidence, context_rows = None, [], []
    for path in files:
        with path.open(encoding='utf-8-sig', newline='') as handle:
            rows = list(csv.DictReader(handle))
        evidence = [r for r in rows if r.get('tipo_registro') == 'EVIDENCIA' and r.get('evento_id') and r.get('instante')]
        if evidence:
            source = path
            context_rows = [r for r in rows if r.get('tipo_registro') == 'CONTEXTO']
            break
    if source is None:
        raise ValueError('No hay informe con eventos para muestrear; usar --informe RUTA o ejecutar primero el monitor')
    metadata = {r.get('campo'): json.loads(r['valor']) for r in context_rows if r.get('campo') in ('/cuenta', '/region')}
    if metadata.get('/cuenta') != identity['Account'] or metadata.get('/region') != args.region:
        raise ValueError('Cuenta/region del informe no coincide con la sesion')
    groups = {r['recurso']: json.loads(r['valor']) for r in context_rows
              if r.get('campo', '').endswith('/grupo_logs')}
    chosen, seen = [], set()
    # Ambos extremos conservan formatos adyacentes; no se agrupan por cercania.
    order = evidence[:3]+evidence[-3:]
    for row in order:
        key = (row['recurso'], row['evento_id'])
        if key not in seen:
            seen.add(key)
            chosen.append(row)
    logs = session.client('logs', config=config)
    reader = Reader(errors)
    summaries, controls, profiles = [], [], []
    for index, row in enumerate(chosen, 1):
        name = row['recurso']
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', name):
            raise ValueError('Nombre Lambda no interpretable en el informe')
        group = groups.get(name, '/aws/lambda/'+name)
        instant = datetime.fromisoformat(row['instante'])
        if not instant.tzinfo:
            raise ValueError('Evento sin zona horaria en el informe')
        millis = int(round(instant.timestamp()*1000))
        def fetch():
            for page in logs.get_paginator('filter_log_events').paginate(logGroupName=group,
                    startTime=millis, endTime=millis+1,
                    PaginationConfig={'MaxItems': 501, 'PageSize': 100}):
                for event in page.get('events', []):
                    if event.get('eventId') == row['evento_id']:
                        return event
            return None
        event = reader.read(name, 'MuestraEventoPorID', fetch)
        if event is None:
            profile = dict(formato='NO DISPONIBLE', marcadores=[], estructura='No recuperado en la consulta acotada')
        else:
            profile = message_profile(str(event.get('message', '')))
        detail = dict(funcion=name, instante=row['instante'], evento_id=row['evento_id'], **profile)
        profiles.append(detail)
        controls.append(dict(recurso=name, control=f'Formato muestra {index}', estado='INFO' if event else 'NO EVALUADO',
                             detalle=json.dumps(detail, ensure_ascii=False)))
        summaries.append(dict(tipo='Muestra', recurso=name, configuracion=f"Evento {index}; {row['instante']}",
            actividad=profile['formato']+'; marcadores='+(','.join(profile['marcadores']) or 'ninguno')+
                ('; plantilla='+profile['plantilla'][:140] if profile.get('plantilla') else ''),
            control='INFO' if event else 'NO EVAL.'))
    now = datetime.now(timezone.utc)
    folder = Path(args.salida).expanduser()/'disparo_semanal'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    report = write_report(folder, summaries, controls, [], dict(cuenta=identity['Account'], region=args.region,
        modo='Muestra de estructura de eventos; no valida lanzamiento', informe_origen=str(source),
        perfiles=profiles, lecturas_incompletas=reader.issues,
        limites=['Hasta 6 eventos, consulta de 1 ms/501 eventos por muestra; no es lectura completa',
                 'Valores y campos no permitidos omitidos; no se exporta texto original ni se ejecutan objetos Python']))
    print_table(summaries)
    print(f'Informe de formatos: {report}')
    return 3 if reader.issues or any(p['formato'] == 'NO DISPONIBLE' for p in profiles) else 0


def inspect(session, config, identity, args, errors):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=args.dias)
    reader = Reader(errors)
    events = session.client('events', config=config)
    lambdas = session.client('lambda', config=config)
    cw = session.client('cloudwatch', config=config)
    logs = session.client('logs', config=config)
    rules, functions, summaries, controls, log_evidence = [], [], [], [], []
    expected = set(args.funcion or FUNCTIONS)
    function_names = set(expected)
    links = {}
    for name in dict.fromkeys(args.regla or RULES):
        info = reader.read(name, 'DescribeRule', lambda: events.describe_rule(Name=name, EventBusName='default'))
        if info is None:
            summaries.append(dict(tipo='EventBridge', recurso=name, configuracion='No disponible', actividad='NO EVALUADA', control=row_status(controls, reader, name)))
            continue
        targets = reader.read(name, 'ListTargetsByRule', lambda: paginate(events, 'list_targets_by_rule', 'Targets', Rule=name, EventBusName='default'))
        projected_targets = []
        for target in targets or []:
            arn = target.get('Arn', '')
            linked = function_from_arn(arn, identity['Account'], args.region)
            if linked:
                function_names.add(linked)
                links.setdefault(linked, []).append(dict(regla=name, target_arn=arn))
            projected_targets.append(dict(id=target.get('Id', ''), arn=arn,
                dlq=target.get('DeadLetterConfig', {}).get('Arn', ''),
                retry_policy=target.get('RetryPolicy', {}), lambda_local=linked))
        dims = [dict(Name='RuleName', Value=name)]
        metrics = {n: metric(reader, cw, name, 'AWS/Events', n, dims, start, now)
                   for n in ('Invocations', 'FailedInvocations')}
        add_metric_controls(controls, name, metrics, ('FailedInvocations',))
        if info.get('State') not in ('ENABLED', 'ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS'):
            control(controls, name, 'Estado regla', 'AVISO', 'Estado observado: '+info.get('State', 'SIN ESTADO'))
        if targets is not None and not targets:
            control(controls, name, 'Targets', 'AVISO', 'Regla sin targets configurados')
        if targets is not None and any(not t['dlq'] for t in projected_targets):
            control(controls, name, 'DLQ target', 'AVISO', 'Hay targets sin DLQ configurada; no implica entrega fallida')
        if not info.get('ScheduleExpression'):
            control(controls, name, 'Programacion', 'INFO', 'Sin ScheduleExpression; no se interpreta el patron de eventos')
        rules.append(dict(nombre=name, arn=info.get('Arn'), estado=info.get('State'),
            programacion_utc=info.get('ScheduleExpression', ''), tiene_event_pattern=bool(info.get('EventPattern')),
            targets=projected_targets, lectura_targets='LEIDA' if targets is not None else 'INCOMPLETA', metricas=metrics))
        dlq_text = 'NO EVALUADO' if targets is None else f'{sum(bool(t["dlq"]) for t in projected_targets)}/{len(projected_targets)} targets con DLQ'
        summaries.append(dict(tipo='EventBridge', recurso=name,
            configuracion='; '.join((info.get('State', '?'), info.get('ScheduleExpression') or 'Sin programacion', dlq_text)),
            actividad=f"Inv={shown(metrics['Invocations'])}; Fallos={shown(metrics['FailedInvocations'])}",
            control=row_status(controls, reader, name)))
    for name in sorted(function_names):
        info = reader.read(name, 'GetFunctionConfiguration', lambda: lambdas.get_function_configuration(FunctionName=name))
        if info is None:
            summaries.append(dict(tipo='Lambda', recurso=name, configuracion='No disponible', actividad='NO EVALUADA', control=row_status(controls, reader, name)))
            continue
        configs = reader.read(name, 'ListFunctionEventInvokeConfigs', lambda: paginate(lambdas,
            'list_function_event_invoke_configs', 'FunctionEventInvokeConfigs', FunctionName=name))
        projected_configs = [dict(function_arn=c.get('FunctionArn', ''),
            reintentos=c.get('MaximumRetryAttempts'), edad_maxima_segundos=c.get('MaximumEventAgeInSeconds'),
            on_failure=c.get('DestinationConfig', {}).get('OnFailure', {}).get('Destination', '')) for c in configs or []]
        dlq = info.get('DeadLetterConfig', {}).get('TargetArn', '')
        failure_count = sum(bool(c['on_failure']) for c in projected_configs)
        if configs is not None and not dlq and not failure_count:
            control(controls, name, 'Manejo asincrono de fallo', 'AVISO', 'Sin DLQ en $LATEST ni OnFailure en configuraciones enumeradas; no implica fallo de ejecucion')
        if info.get('State') != 'Active' or info.get('LastUpdateStatus') == 'Failed':
            control(controls, name, 'Configuracion Lambda', 'AVISO', 'Estado='+info.get('State', '?')+'; actualizacion='+info.get('LastUpdateStatus', '?'))
        if name in expected and not links.get(name):
            control(controls, name, 'Enlace con regla', 'AVISO', 'Funcion esperada sin target directo observado en las reglas consultadas')
        dims = [dict(Name='FunctionName', Value=name)]
        metrics = {n: metric(reader, cw, name, 'AWS/Lambda', n, dims, start, now)
                   for n in ('Invocations', 'Errors', 'Throttles', 'AsyncEventsDropped', 'DeadLetterErrors', 'DestinationDeliveryFailures')}
        metrics['Duration'] = metric(reader, cw, name, 'AWS/Lambda', 'Duration', dims, start, now, 'Maximum')
        add_metric_controls(controls, name, metrics,
            ('Errors', 'Throttles', 'AsyncEventsDropped', 'DeadLetterErrors', 'DestinationDeliveryFailures'))
        duration = metrics['Duration']['valor']
        timeout = info.get('Timeout')
        ratio = duration/(timeout*1000) if duration is not None and timeout else None
        if ratio is not None and ratio >= args.fraccion_timeout:
            control(controls, name, 'Duration / timeout', 'AVISO',
                f'Maximo observado={duration/1000:.2f}s; timeout actual={timeout}s; proporcion={ratio:.1%}')
        group = info.get('LoggingConfig', {}).get('LogGroup') or '/aws/lambda/'+name
        response = reader.read(name, 'UltimaMarcaLogs', lambda: logs.describe_log_streams(
            logGroupName=group, orderBy='LastEventTime', descending=True, limit=1))
        streams = response.get('logStreams', []) if response else []
        last_log = streams[0].get('lastEventTimestamp') if streams else None
        launch = None
        if name in (args.funcion_lanzadora or [FUNCTIONS[0]]):
            launch = dict(lectura='NO CONSULTADA' if args.sin_logs else 'ERROR', limites=[])
            if not args.sin_logs:
                result = reader.read(name, 'FilterLogEventsLanzadora', lambda: read_launch_logs(
                    logs, group, start, now, args.max_eventos))
                if result is not None:
                    launch, evidence = result
                    log_evidence.extend(dict(funcion=name, **entry) for entry in evidence)
                    if launch['lectura'] == 'PARCIAL':
                        reader.issues.append(dict(recurso=name, operacion='FilterLogEventsLanzadora',
                            error='; '.join(launch['limites']), tipo='INCOMPLETO'))
                    if launch['eventos_con_error'] or launch['fallos_runtask_observados']:
                        control(controls, name, 'Logs lanzadora', 'AVISO',
                            f"Eventos con indicios de error={launch['eventos_con_error']}; fallos RunTask reportados={launch['fallos_runtask_observados']}. No implica fallo de todo el disparo")
                    if not launch['arn_tareas_runtask']:
                        control(controls, name, 'Creacion de tareas', 'INFO',
                            'NO VERIFICABLE: no se encontro respuesta RunTask explicita con ARN de tareas; no significa que no se crearon')
        functions.append(dict(nombre=name, arn=info.get('FunctionArn'), estado=info.get('State'),
            actualizacion=info.get('LastUpdateStatus'), version_configurada=info.get('Version', '$LATEST'),
            timeout_segundos=timeout, runtime=info.get('Runtime', info.get('PackageType', '')),
            dlq_latest=dlq, configuraciones_asincronas=projected_configs,
            lectura_config_asincrona='LEIDA' if configs is not None else 'INCOMPLETA',
            reglas_enlazadas=links.get(name, []), metricas=metrics, duracion_fraccion_timeout=ratio,
            grupo_logs=group, ultima_marca_log=stamp(datetime.fromtimestamp(last_log/1000, timezone.utc)) if last_log is not None else '',
            logs_lanzamiento=launch))
        failure_text = 'OnFailure NO EVALUADO' if configs is None else f'OnFailure={failure_count} configs'
        duration_text = f'{duration/1000:.2f}s' if duration is not None else shown(metrics['Duration'])
        summaries.append(dict(tipo='Lambda', recurso=name,
            configuracion=f"{info.get('State', '?')}; timeout={timeout}s; DLQ latest={'SI' if dlq else 'NO'}; {failure_text}",
            actividad=f"Inv={shown(metrics['Invocations'])}; Errors={shown(metrics['Errors'])}; Throttles={shown(metrics['Throttles'])}; Dmax={duration_text}",
            control=row_status(controls, reader, name)))
        if launch is not None:
            requested = launch.get('solicitadas_declaradas')
            created = launch.get('arn_tareas_runtask')
            summaries.append(dict(tipo='Lanzamiento', recurso=name,
                configuracion=f"Logs={launch['lectura']}; START observados={launch.get('inicios_lambda_observados', 'N/V')}; eventos={launch.get('eventos_leidos', 'N/V')}",
                actividad=f"Solicitadas (logs)={requested if requested is not None else 'N/V'}; ARN en respuesta RunTask={created if created else 'N/V'}; ARN mencionados={launch.get('arn_tareas_mencionadas', 'N/V')}; fallos RunTask={launch.get('fallos_runtask_observados', 'N/V')}; indicios error={launch.get('eventos_con_error', 'N/V')}",
                control='INCOMPLETO' if launch['lectura'] in ('ERROR', 'PARCIAL') else 'NO EVAL.' if launch['lectura'] == 'NO CONSULTADA'
                    else 'AVISO' if launch.get('eventos_con_error') or launch.get('fallos_runtask_observados') else 'INFO'))
    folder = Path(args.salida)/'disparo_semanal'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    context = dict(cuenta=identity['Account'], region=args.region, captura_utc=now.isoformat(),
        ventana_utc=dict(desde=start.isoformat(), hasta=now.isoformat(), periodo_metrica_segundos=3600),
        configuracion=vars(args), reglas=rules, funciones=functions, lecturas_incompletas=reader.issues,
        controles=controls, limitaciones=LIMITATIONS)
    report = write_report(folder, summaries, controls, log_evidence, context)
    print(f'DISPARO SEMANAL | Solo lectura | {args.region} | Ultimos {args.dias} dias')
    print_table(summaries)
    print('INFO = evidencia consultada; AVISO = revisar; INCOMPLETO/AUSENTE = ver informe. SIN DATOS no es cero.')
    print('Metricas por recurso; no certifican el disparo del domingo ni el lanzamiento correcto de Fargate. Cron en UTC.')
    print('N/V = no verificable en logs. ARN mencionado no demuestra creacion; respuesta RunTask reportada no demuestra ejecucion exitosa.')
    print(f'Informe: {report} | Filtrar tipo_registro=RESUMEN para la vista principal.')
    return 3 if any(i['tipo'] == 'INCOMPLETO' for i in reader.issues) else 1 if reader.issues else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--dias', type=int, default=8, help='Ventana de metricas: 1..31 dias; default 8 para incluir el disparo semanal')
    parser.add_argument('--regla', action='append', help='Nombre de regla en bus default; repetir. Sustituye la lista inicial')
    parser.add_argument('--funcion', action='append', help='Nombre base Lambda esperado; repetir. Se agregan targets Lambda locales descubiertos')
    parser.add_argument('--funcion-lanzadora', action='append', help='Funcion ya incluida a la que leer mensajes de lanzamiento; default pon-prod-xcrime-prediccion-launch-fargate')
    parser.add_argument('--revisar-formatos', action='store_true', help='Muestrea hasta 6 eventos del ultimo informe; solo consulta STS/Logs, sin exportar mensajes crudos')
    parser.add_argument('--informe', help='CSV consolidado de origen para --revisar-formatos; por defecto busca el ultimo con evidencia')
    parser.add_argument('--sin-logs', action='store_true', help='Omite mensajes de la lanzadora; conserva consulta de metadata de log streams')
    parser.add_argument('--max-eventos', type=int, default=10000, help='Maximo de eventos por lanzadora en la ventana; 1..100000, lectura total acotada a 16 MiB')
    parser.add_argument('--fraccion-timeout', type=float, default=0.9, help='Aviso por Duration maxima / timeout actual (default 0.9); no es umbral de tareas ECS')
    parser.add_argument('--salida', default='salida')
    args = parser.parse_args()
    if args.informe and not args.revisar_formatos:
        parser.error('--informe requiere --revisar-formatos')
    if args.revisar_formatos and args.sin_logs:
        parser.error('--revisar-formatos requiere consultar logs; quitar --sin-logs')
    if not 1 <= args.dias <= 31 or not 0 < args.fraccion_timeout <= 1:
        parser.error('--dias: 1..31; --fraccion-timeout: mayor de 0 y hasta 1')
    if not 1 <= args.max_eventos <= 100000:
        parser.error('--max-eventos debe estar entre 1 y 100000')
    if (any(not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', n) for n in args.regla or []) or
            any(not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', n) for n in (args.funcion or []) + (args.funcion_lanzadora or []))):
        parser.error('Reglas y funciones: nombres base de hasta 64 caracteres, sin ARN ni alias')
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
        if args.revisar_formatos:
            return review_log_formats(session, config, identity, args, (BotoCoreError, ClientError))
        return inspect(session, config, identity, args, (BotoCoreError, ClientError))
    except (BotoCoreError, ClientError, OSError, ValueError, csv.Error) as exc:
        # No imprimir respuestas completas de AWS, variables, payloads ni credenciales.
        print('[BLOQUEADO] '+type(exc).__name__+'; revisar sesion/permisos y carpeta de salida.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Consulta interrumpida; no se modificaron recursos AWS.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
