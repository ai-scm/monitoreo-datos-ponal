#!/usr/bin/env python3
"""Presencia de datos del periodo esperado en probabilidaddelito.

Sin opciones: consulta esquema Glue; no ejecuta Athena ni declara datos vigentes.
--ejecutar requiere columna, significado de fecha y periodo explicitamente indicados.
--revisar-fuente-s3 consulta metadata de los objetos de la ubicacion Glue; sin Athena.
Un unico CSV local y copia S3. No modifica datos de origen; Athena escribe resultados internos.
"""
import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import textwrap
import time
from urllib.parse import urlsplit
import uuid


BOGOTA = timezone(timedelta(hours=-5))
REPORTS_BUCKET = 'pon-prod-ml-iapol'
REPORTS_PREFIX = 'ValidationReports/07-MonitorFreshnessProbabilidaddelito/'


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

OUTPUT_S3 = 's3://' + REPORTS_BUCKET + '/' + REPORTS_PREFIX
FIELDS = ('total', 'fechas_no_interpretables', 'primera_fecha', 'ultima_fecha',
          'registros_periodo', 'dias_con_datos', 'primera_fecha_periodo', 'ultima_fecha_periodo')
LIMITS = [
    'Esquema Glue y UpdateTime son metadata; no prueban recencia de los datos.',
    'La fecha y su significado deben confirmarse; no se deducen del nombre de una columna.',
    'Periodo explicito e inclusivo por fecha calendario almacenada; no se impone antiguedad estandar.',
    'Un SELECT agregado lee la columna temporal de toda la tabla; no hay poda de particiones configurada.',
    'Presencia en periodo no certifica cobertura de todas las siglas/unidades/dias ni calidad de prediccion.',
    'Fecha objetivo puede ser futura; no se confunde con fecha de generacion o ingestion.',
    'No valida archivos de prediccion, contenido de informes ni ejecuciones de jobs.',
    'Sin alertas automaticas. Un CSV local y copia en ValidationReports/07-MonitorFreshnessProbabilidaddelito/; Athena puede escribir intermediarios S3.',
    'LastModified S3 prueba modificacion del objeto; no fecha de negocio, generacion del contenido ni actualizacion completa.',
    'Objetos S3 candidatos son referencias por nombre/tamano; no se confirma que Athena los consuma ni se lee contenido.',
]


class ReadBlocked(RuntimeError):
    pass


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--cuenta', default='926162397524')
    p.add_argument('--database', default='pon-prod-mdp')
    p.add_argument('--tabla', default='probabilidaddelito')
    p.add_argument('--fecha', help='Columna exacta confirmada del esquema')
    p.add_argument('--tipo-fecha', choices=('objetivo', 'generacion', 'dato'),
                   help='Significado confirmado de la columna, no inferido por el script')
    p.add_argument('--desde', type=date.fromisoformat, help='Primer dia esperado YYYY-MM-DD, inclusivo')
    p.add_argument('--hasta', type=date.fromisoformat, help='Ultimo dia esperado YYYY-MM-DD, inclusivo')
    p.add_argument('--formato-fecha', help='Formato Athena date_parse para texto, por ejemplo %%Y%%m%%d')
    mode = p.add_mutually_exclusive_group()
    mode.add_argument('--ejecutar', action='store_true', help='Ejecutar un SELECT agregado en Athena')
    mode.add_argument('--solo-sql', action='store_true', help='Guardar SQL en el CSV sin ejecutarlo')
    mode.add_argument('--revisar-fuente-s3', action='store_true',
                      help='Listar metadata de objetos bajo la ubicacion Glue, sin leer contenido ni ejecutar Athena')
    p.add_argument('--max-objetos', type=int, default=10000, help='Limite de objetos S3 listados: 1..1000000; default 10000')
    p.add_argument('--listado-completo', action='store_true', help='Agotar paginas S3; ignora --max-objetos')
    p.add_argument('--workgroup', default=os.getenv('ATHENA_WORKGROUP', 'primary'))
    p.add_argument('--output-s3', default=os.getenv('ATHENA_OUTPUT_S3') or OUTPUT_S3,
                   help='Raiz para intermediarios Athena; se agrega fecha/hora/_athena/')
    p.add_argument('--timeout', type=int, default=600, help='Espera maxima de consulta, segundos (1..3600)')
    p.add_argument('--salida', default='salida')
    a = p.parse_args()
    if a.ejecutar or a.solo_sql:
        if not all((a.fecha, a.tipo_fecha, a.desde, a.hasta)):
            p.error('--ejecutar/--solo-sql requieren --fecha, --tipo-fecha, --desde y --hasta')
    if bool(a.desde) != bool(a.hasta) or a.desde and a.hasta < a.desde:
        p.error('Indicar --desde y --hasta juntos, en orden cronologico')
    if not 1 <= a.timeout <= 3600:
        p.error('--timeout debe estar entre 1 y 3600')
    if not 1 <= a.max_objetos <= 1000000:
        p.error('--max-objetos debe estar entre 1 y 1000000')
    if a.listado_completo and not a.revisar_fuente_s3:
        p.error('--listado-completo requiere --revisar-fuente-s3')
    if a.revisar_fuente_s3 and any((a.fecha, a.tipo_fecha, a.desde, a.hasta, a.formato_fecha)):
        p.error('La revision S3 solo observa metadata; no admite opciones de fecha de datos')
    for value in (a.database, a.tabla, a.fecha, a.workgroup):
        if value is not None and (not value.strip() or len(value) > 255 or any(c in value for c in '\n\r\t')):
            p.error('Nombres no vacios de hasta 255 caracteres, sin saltos de linea')
    output = urlsplit(a.output_s3)
    if output.scheme != 's3' or not output.netloc or output.query or output.fragment:
        p.error('--output-s3 debe ser una ruta s3://bucket/prefijo/')
    if a.formato_fecha and (not a.fecha or len(a.formato_fecha) > 100):
        p.error('--formato-fecha requiere --fecha y admite hasta 100 caracteres')
    return a


def schema_rows(table, resource):
    rows = []
    for source, columns in (('COLUMNA', table.get('StorageDescriptor', {}).get('Columns', [])),
                            ('PARTICION', table.get('PartitionKeys', []))):
        for col in columns:
            rows.append(dict(tipo_registro='ESQUEMA', recurso=resource, campo=col['Name'],
                             tipo=col['Type'], resultado=source, control='INFO'))
    return rows


def build_sql(a, table):
    columns = {r['campo']: r['tipo'] for r in schema_rows(table, '')}
    if a.fecha not in columns:
        raise ReadBlocked('La columna --fecha no existe en el esquema consultado')
    kind = columns[a.fecha].lower()
    if not (kind == 'date' or kind in ('string', 'varchar', 'char') or kind.startswith(('varchar(', 'char(', 'timestamp'))):
        raise ReadBlocked('Tipo temporal no admitido; confirmar la representacion antes de consultar')
    if 'time zone' in kind:
        raise ReadBlocked('Timestamp con zona: requiere confirmar conversion de zona; no se asume')
    col = identifier(a.fecha)
    if a.formato_fecha:
        if not kind.startswith(('string', 'varchar', 'char')):
            raise ReadBlocked('--formato-fecha solo se aplica a columnas de texto')
        expr = f'CAST(TRY(date_parse(CAST({col} AS varchar), {literal(a.formato_fecha)})) AS date)'
    else:
        expr = f'COALESCE(TRY_CAST({col} AS date), CAST(TRY_CAST({col} AS timestamp) AS date))'
    period = f"fecha BETWEEN DATE '{a.desde.isoformat()}' AND DATE '{a.hasta.isoformat()}'"
    return f'''WITH datos AS (
 SELECT {expr} AS fecha
 FROM {identifier(a.database)}.{identifier(a.tabla)}
)
SELECT count(*) AS total,
       count_if(fecha IS NULL) AS fechas_no_interpretables,
       CAST(min(fecha) AS varchar) AS primera_fecha,
       CAST(max(fecha) AS varchar) AS ultima_fecha,
       count_if({period}) AS registros_periodo,
       count(DISTINCT fecha) FILTER (WHERE {period}) AS dias_con_datos,
       CAST(min(fecha) FILTER (WHERE {period}) AS varchar) AS primera_fecha_periodo,
       CAST(max(fecha) FILTER (WHERE {period}) AS varchar) AS ultima_fecha_periodo
FROM datos'''


def execute_query(athena, sql, a, context, errors):
    output = a.output_s3.rstrip('/') + '/' + context['carpeta_ejecucion'] + '/_athena/'
    context['athena'] = info = dict(output_solicitado=output)
    response = athena.start_query_execution(QueryString=sql,
        QueryExecutionContext={'Database': a.database, 'Catalog': 'AwsDataCatalog'},
        WorkGroup=a.workgroup, ClientRequestToken=str(uuid.uuid4()),
        ResultConfiguration={'OutputLocation': output},
        ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': False}})
    query_id = response['QueryExecutionId']
    info['query_id'] = query_id
    finished = False
    deadline = time.monotonic() + a.timeout
    try:
        while True:
            query = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
            state = query['Status']['State']
            info.update(estado=state, estadisticas=query.get('Statistics', {}),
                        output_efectivo=query.get('ResultConfiguration', {}).get('OutputLocation', ''),
                        error_athena=query['Status'].get('AthenaError', {}).get('ErrorType'))
            if state in ('FAILED', 'CANCELLED'):
                finished = True
                raise ReadBlocked(f'Athena {state}; revisar QueryExecutionId={query_id}')
            if state == 'SUCCEEDED':
                finished = True
                break
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise ReadBlocked(f'Tiempo de espera agotado; QueryExecutionId={query_id}')
            time.sleep(min(2, remaining))
    finally:
        if not finished:
            try:
                athena.stop_query_execution(QueryExecutionId=query_id)
                info['cancelacion'] = 'SOLICITADA; estado final no confirmado'
            except errors as exc:
                info['cancelacion'] = 'NO CONFIRMADA: '+error_code(exc)
    records, names = [], None
    for page in athena.get_paginator('get_query_results').paginate(QueryExecutionId=query_id,
            PaginationConfig={'PageSize': 10, 'MaxItems': 3}):
        result = page['ResultSet']
        page_names = [c['Name'] for c in result['ResultSetMetadata']['ColumnInfo']]
        if tuple(page_names) != FIELDS or names is not None and page_names != names:
            raise ReadBlocked('Columnas inesperadas en resultado Athena; no se evalua recencia')
        names = page_names
        records.extend([[cell.get('VarCharValue') for cell in row.get('Data', [])]
                        for row in result.get('Rows', [])])
    if records and records[0] == list(FIELDS):
        records = records[1:]
    if len(records) != 1 or len(records[0]) != len(FIELDS):
        raise ReadBlocked('Resultado agregado ausente o inesperado; no se declara cero')
    return dict(zip(FIELDS, records[0]))


def evaluate(result, a, resource):
    numbers = {key: int(result[key]) for key in ('total', 'fechas_no_interpretables', 'registros_periodo', 'dias_con_datos')}
    total, invalid, present, days = (numbers[key] for key in ('total', 'fechas_no_interpretables', 'registros_periodo', 'dias_con_datos'))
    if min(numbers.values()) < 0 or invalid+present > total or days > present or days > (a.hasta-a.desde).days+1:
        raise ReadBlocked('Conteos agregados incoherentes; no se evalua presencia')
    for key in ('primera_fecha', 'ultima_fecha', 'primera_fecha_periodo', 'ultima_fecha_periodo'):
        if result[key] is not None:
            date.fromisoformat(result[key])
    all_min, all_max = result['primera_fecha'], result['ultima_fecha']
    p_min, p_max = result['primera_fecha_periodo'], result['ultima_fecha_periodo']
    if bool(all_min) != bool(all_max) or bool(p_min) != bool(p_max):
        raise ReadBlocked('Faltan extremos de fechas en resultado agregado')
    if (total-invalid > 0) != bool(all_min and all_max) or (present > 0) != bool(p_min and p_max) or bool(present) != bool(days):
        raise ReadBlocked('Fechas y conteos no concuerdan; no se evalua presencia')
    if all_min and (all_min > all_max or p_min and not all_min <= p_min <= p_max <= all_max):
        raise ReadBlocked('Extremos de fechas incoherentes')
    if p_min and not a.desde.isoformat() <= p_min <= p_max <= a.hasta.isoformat():
        raise ReadBlocked('Fecha agregada fuera del periodo configurado')
    if present:
        control, detail = ('AVISO' if invalid else 'INFO'), 'Hay registros en el periodo; no certifica cobertura completa'
    elif invalid:
        control, detail = 'INCOMPLETO', 'Sin fechas interpretables en periodo; las fechas invalidas impiden confirmar ausencia'
    else:
        control, detail = 'FALLA', 'Sin registros en el periodo esperado configurado; no identifica la causa'
    rows = [dict(tipo_registro='RESUMEN', recurso=resource, campo='Presencia en periodo',
                 resultado=f'{present} registros; {days} dias con datos', control=control, detalle=detail),
            dict(tipo_registro='RESUMEN', recurso=resource, campo='Rango de fechas observado',
                 resultado=f'{all_min or "sin datos"} a {all_max or "sin datos"}', control='INFO'),
            dict(tipo_registro='RESUMEN', recurso=resource, campo='Calidad temporal',
                 resultado=f'{invalid}/{total} fechas nulas/no interpretables', control='AVISO' if invalid else 'INFO')]
    return rows, dict(result, **numbers)


def error_code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)


def review_s3(s3, table, a, resource, rows, context):
    location = table.get('StorageDescriptor', {}).get('Location') or ''
    uri = urlsplit(location)
    if uri.scheme != 's3' or not uri.netloc or uri.query or uri.fragment or not uri.path.endswith('/') or uri.path == '/':
        raise ReadBlocked('Ubicacion Glue no es una carpeta S3 explicita; confirmar antes de listar')
    prefix = uri.path.lstrip('/')
    limit = None if a.listado_completo else a.max_objetos
    pagination = {'PageSize': 1000}
    if limit is not None:
        pagination['MaxItems'] = limit+1
    context['fuente_s3'] = info = dict(ubicacion=location, lectura='INCOMPLETA',
        limite=limit, truncado=False, objetos=0, tecnicos=0, candidatos_vacios=0,
        candidatos_con_contenido=0, bytes_candidatos=0, metadata_invalida=0,
        primera_modificacion=None, ultima_modificacion=None, ultimos_observados=[])
    recent, seen = [], set()
    pages = s3.get_paginator('list_objects_v2').paginate(Bucket=uri.netloc,
        Prefix=prefix, ExpectedBucketOwner=a.cuenta, PaginationConfig=pagination)
    objects = (obj for page in pages for obj in page.get('Contents', []))
    for index, obj in enumerate(objects):
        if limit is not None and index >= limit:
            info['truncado'] = True
            break
        key, size, changed = obj.get('Key'), obj.get('Size'), obj.get('LastModified')
        if not isinstance(key, str) or not key.startswith(prefix) or key in seen:
            raise ReadBlocked('Clave S3 inesperada o repetida; listado no confirmado completo')
        seen.add(key)
        info['objetos'] += 1
        relative = key[len(prefix):]
        technical = key.endswith('/') or any(part.startswith(('_', '.')) for part in relative.split('/') if part)
        valid_size = isinstance(size, int) and not isinstance(size, bool) and size >= 0
        valid_time = isinstance(changed, datetime) and changed.tzinfo is not None and changed.utcoffset() is not None
        valid = valid_size and valid_time
        changed_text = changed.astimezone(BOGOTA).isoformat() if valid_time else None
        category = 'METADATA_NO_EVALUABLE' if not valid else 'TECNICO' if technical else 'CANDIDATO_VACIO' if size == 0 else 'CANDIDATO_CON_CONTENIDO'
        rows.append(dict(tipo_registro='OBJETO_S3', recurso=location, campo=key, tipo=category,
                         resultado=changed_text or 'Fecha no interpretable', control='INFO' if valid else 'INCOMPLETO',
                         valor=json.dumps(dict(bytes=size if valid_size else None, modificado=changed_text))))
        if not valid:
            info['metadata_invalida'] += 1
            continue
        if technical:
            info['tecnicos'] += 1
        elif size == 0:
            info['candidatos_vacios'] += 1
        else:
            info['candidatos_con_contenido'] += 1
            info['bytes_candidatos'] += size
            utc = changed.astimezone(timezone.utc)
            first = info['primera_modificacion']
            last = info['ultima_modificacion']
            info['primera_modificacion'] = min(first, utc) if first else utc
            info['ultima_modificacion'] = max(last, utc) if last else utc
            recent.append((utc, key, size))
            recent.sort(reverse=True)
            del recent[5:]
            # Mantener evidencia util incluso si una pagina posterior falla.
            info['ultimos_observados'] = [dict(clave=k, bytes=b, modificado=t.astimezone(BOGOTA).isoformat()) for t, k, b in recent]
    complete = not info['truncado'] and not info['metadata_invalida']
    info['lectura'] = 'LEIDA' if complete else 'INCOMPLETA'
    state = 'INFO' if complete else 'INCOMPLETO'
    def summary(field, result, control=state):
        rows.append(dict(tipo_registro='RESUMEN', recurso=resource, campo=field, resultado=result, control=control))
    summary('Fuente S3', location)
    summary('Cobertura listado S3', f'{info["objetos"]} objetos; '+('paginas agotadas' if complete else 'lectura parcial'))
    summary('Candidatos con contenido', f'{info["candidatos_con_contenido"]} objetos; {info["bytes_candidatos"]} bytes')
    summary('Vacios / tecnicos', f'{info["candidatos_vacios"]} candidatos vacios; {info["tecnicos"]} objetos tecnicos')
    last = info['ultima_modificacion']
    summary('Ultima modificacion observada', last.astimezone(BOGOTA).isoformat() if last else 'Sin candidatos no vacios con fecha valida')
    for index, item in enumerate(info['ultimos_observados'], 1):
        summary(f'Objeto reciente observado #{index}', item['modificado']+' | '+item['clave'][len(prefix):])
    if not info['candidatos_con_contenido']:
        rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Contenido fuente',
                         resultado='No se observaron candidatos no vacios; no se leyeron filas',
                         control='AVISO' if complete else 'INCOMPLETO'))
    rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Actualidad de los datos',
                     resultado='LastModified no confirma periodo del contenido; frecuencia de actualizacion no confirmada',
                     control='NO EVALUADO'))


def write_report(folder, rows, context):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder/'informe_freshness_probabilidaddelito.csv'
    fields = ('tipo_registro', 'recurso', 'campo', 'tipo', 'resultado', 'control', 'detalle', 'valor')
    def leaves(value, field=''):
        if isinstance(value, dict) and value:
            for key, item in value.items():
                yield from leaves(item, field+'/'+str(key))
        elif isinstance(value, list) and value:
            for i, item in enumerate(value):
                yield from leaves(item, field+'/'+str(i))
        else:
            yield dict(tipo_registro='CONTEXTO', campo=field, valor=json.dumps(value, ensure_ascii=False, default=str))
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        writer.writerows(leaves(context))
    return path


def print_table(rows):
    widths = (10, 36, 58, 13)
    def render(values):
        cells = [textwrap.wrap(str(v), width=w, break_on_hyphens=False) or ['-'] for v, w in zip(values, widths)]
        for i in range(max(map(len, cells))):
            print(' | '.join((c[i] if i < len(c) else '').ljust(w) for c, w in zip(cells, widths)))
    render(('Tipo', 'Campo / control', 'Resultado', 'Control'))
    print('-+-'.join('-'*w for w in widths))
    for row in rows:
        if row['tipo_registro'] in ('RESUMEN', 'ESQUEMA', 'CONTROL'):
            render((row['tipo_registro'], row['campo'], row.get('tipo') or row.get('resultado', ''), row['control']))


def inspect(session, config, a, errors):
    now = datetime.now(timezone.utc)
    run_folder = now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    resource = a.database+'.'+a.tabla
    rows = []
    context = dict(cuenta=a.cuenta, region=a.region, captura_utc=now.isoformat(), recurso=resource,
                   carpeta_ejecucion=run_folder, configuracion=vars(a), limitaciones=LIMITS)
    code = 0
    try:
        glue = session.client('glue', config=config)
        table = glue.get_table(CatalogId=a.cuenta, DatabaseName=a.database, Name=a.tabla)['Table']
        context['tabla'] = dict(tipo=table.get('TableType'), ubicacion=table.get('StorageDescriptor', {}).get('Location'),
                               actualizacion_metadata=table.get('UpdateTime'), particiones=table.get('PartitionKeys', []))
        rows.append(dict(tipo_registro='RESUMEN', recurso=resource, campo='Tabla Glue', resultado='ENCONTRADA: '+resource, control='INFO'))
        rows.extend(schema_rows(table, resource))
        if a.revisar_fuente_s3:
            review_s3(session.client('s3', config=config), table, a, resource, rows, context)
            # Observar archivos no sustituye el control del periodo de los datos.
            code = 3
        elif a.ejecutar or a.solo_sql:
            sql = build_sql(a, table)
            rows.append(dict(tipo_registro='SQL', recurso=resource, campo='SELECT agregado', valor=sql))
            if a.solo_sql:
                rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Datos del periodo',
                                 resultado='SQL guardado; consulta no ejecutada', control='NO EVALUADO'))
                code = 3
            else:
                result = execute_query(session.client('athena', config=config), sql, a, context, errors)
                checks, aggregate = evaluate(result, a, resource)
                rows.extend(checks)
                context['agregados'] = aggregate
        else:
            rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Datos del periodo',
                             resultado='Pendiente: confirmar columna, significado de fecha y periodo esperado', control='NO EVALUADO'))
            code = 3
    except KeyboardInterrupt:
        rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Consulta', resultado='Interrumpida; datos no evaluados', control='INCOMPLETO'))
        code = 130
    except ReadBlocked as exc:
        rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Consulta', resultado=str(exc), control='INCOMPLETO'))
        code = 3
    except errors as exc:
        err = error_code(exc)
        missing = err == 'EntityNotFoundException' and 'tabla' not in context
        rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Consulta', resultado=err,
                         control='AUSENTE' if missing else 'INCOMPLETO'))
        code = 1 if missing else 3
    except (ValueError, TypeError, KeyError) as exc:
        rows.append(dict(tipo_registro='CONTROL', recurso=resource, campo='Resultado',
                         resultado=type(exc).__name__+'; datos no evaluables', control='INCOMPLETO'))
        code = 3
    report = write_report(Path(a.salida).expanduser()/'freshness_probabilidaddelito'/run_folder, rows, context)
    print(f'PERIODO DE DATOS | {resource} | {a.region}')
    print_table([r for r in rows if not a.revisar_fuente_s3 or r['tipo_registro'] != 'ESQUEMA'])
    print('Metadata/LastModified no prueban periodo del contenido. Presencia no prueba cobertura de todas las unidades.')
    print(f'Informe: {report}')
    upload_csv_reports(session, config, report.parent)
    if code:
        return code
    controls = {r.get('control') for r in rows}
    return 3 if 'INCOMPLETO' in controls else 1 if 'FALLA' in controls else 0


def main():
    a = parse_args()
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Ejecutar en CloudShell con boto3/botocore.', file=sys.stderr)
        return 3
    try:
        config = Config(connect_timeout=5, read_timeout=20, retries={'total_max_attempts': 3, 'mode': 'standard'})
        session = boto3.Session(region_name=a.region)
        account = session.client('sts', config=config).get_caller_identity()['Account']
        if account != a.cuenta:
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan datos.', file=sys.stderr)
            return 3
        return inspect(session, config, a, (BotoCoreError, ClientError))
    except (BotoCoreError, ClientError, OSError) as exc:
        print('[BLOQUEADO] '+error_code(exc)+'; revisar sesion/permisos y destino local.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Consulta interrumpida.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
