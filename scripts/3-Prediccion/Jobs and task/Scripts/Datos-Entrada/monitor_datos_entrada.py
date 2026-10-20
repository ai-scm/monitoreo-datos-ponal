#!/usr/bin/env python3
"""Prueba autocontenida de entradas de prediccion, para AWS CloudShell.

1) python3 monitor_datos_entrada.py
   Busca mdp_limpio en Glue y muestra columnas; no ejecuta consultas Athena.
2) python3 monitor_datos_entrada.py --database BASE --fecha COLUMNA --solo-sql
3) python3 monitor_datos_entrada.py --database BASE --fecha COLUMNA --ejecutar
   Opcionales: --unidad COL --sigla COL --delito COL --latitud COL --longitud COL
   Una unidad: --unidad unidad_base --filtrar-unidad 'CAI MORASURCO'
               --sigla sigla --filtrar-sigla MEPAS
   Desglose diario por fuente: agregar --fuente bd --detalle-diario
   Comparacion 15 contra 15 (una consulta): agregar --comparar-periodos --fuente bd

No cambia tablas ni datos de origen. Athena puede escribir resultados en S3.
Modo --comparar-periodos: UN SELECT para resumen y diario de dos ventanas de 15 dias.
Sin ese modo, se conserva la auditoria historica y su detalle opcional (dos consultas).
Filtrar unidad/sigla no garantiza reducir bytes leidos; LIMIT solo limita grupos.
Usar el workgroup de pruebas y su limite de bytes por consulta si corresponde.
Reportes: general.csv, diario.csv, control.csv en S3 /fecha/hora/ y copia en salida/.
Destino fijo: s3://pon-prod-ml-iapol/ValidationReports/MonitorDatosEntrada/
Resultados internos de Athena en /fecha/hora/_athena/, junto a los tres reportes.
Fechas sin zona: se interpretan en --zona-datos (por defecto America/Bogota).
Si la columna es texto no ISO: --formato-fecha '%d/%m/%Y %H:%i:%s' (Athena).
La fecha del hecho mide recencia de incidentes; no demuestra fecha de ingestion.
Los umbrales iniciales son exploratorios, no compromisos operativos.
Exit: 0 sin FALLA (puede incluir AVISO), 1 FALLA, 2 argumentos, 3 bloqueado/error AWS.
Dependencia: boto3/botocore disponibles normalmente en CloudShell.
"""

import argparse
import csv
import io
import json
import math
import os
from pathlib import Path
import sys
import time
import textwrap
from datetime import datetime, timedelta, timezone


REPORTS_S3 = 's3://pon-prod-ml-iapol/ValidationReports/MonitorDatosEntrada/'


class Blocked(RuntimeError):
    pass


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--region', default=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    p.add_argument('--cuenta', default='926162397524', help='Cuenta esperada; aborta si no coincide')
    p.add_argument('--database', help='Sin este argumento busca la tabla en las bases visibles de Glue')
    p.add_argument('--tabla', default='mdp_limpio')
    p.add_argument('--fecha', help='Columna confirmada de fecha de negocio o ingestion')
    for name in ('unidad', 'sigla', 'delito', 'latitud', 'longitud'):
        p.add_argument('--' + name, help='Columna confirmada; opcional')
    p.add_argument('--filtrar-unidad', help='Valor exacto de unidad, comparado sin espacios exteriores')
    p.add_argument('--filtrar-sigla', help='Valor exacto de sigla, comparado sin espacios exteriores')
    p.add_argument('--fuente', help='Columna de origen, por ejemplo bd')
    p.add_argument('--comparar-periodos', action='store_true', help='Modo acotado: una consulta por unidad/BD, resumen y diario; 15 contra 15 por defecto')
    p.add_argument('--catalogo-unidades', help='CSV local del catalogo entregado; opcional en modo comparar-periodos')
    p.add_argument('--particiones-fecha-confirmadas', action='store_true', help='Solo si year/month/day son particiones y representan la misma fecha que --fecha')
    p.add_argument('--detalle-diario', action='store_true', help='Consulta adicional por fuente y dias completos, para una unidad/sigla')
    p.add_argument('--formato-fecha', help='Formato date_parse de Athena para texto; no formato Python')
    p.add_argument('--zona-datos', choices=['America/Bogota', 'UTC'], default='America/Bogota')
    mode = p.add_mutually_exclusive_group()
    mode.add_argument('--ejecutar', action='store_true', help='Ejecuta la consulta agregada')
    mode.add_argument('--solo-sql', action='store_true', help='Genera SQL sin ejecutarlo; consulta metadata Glue')
    p.add_argument('--workgroup', default=os.getenv('ATHENA_WORKGROUP', 'primary'))
    p.add_argument('--output-s3', default=os.getenv('ATHENA_OUTPUT_S3') or REPORTS_S3, help='Raiz para Athena; se agrega fecha/hora/_athena/. Usar la ruta acordada para agrupar con los reportes')
    p.add_argument('--max-horas', type=float, default=48, help='Umbral exploratorio de recencia (48 h)')
    p.add_argument('--ventana-dias', type=int, default=15, help='Dias completos de CADA periodo (15 por defecto)')
    p.add_argument('--caida-pct', type=float, default=50, help='Aviso de caida porcentual (50)')
    p.add_argument('--min-base', type=int, default=20, help='Minimo de registros anteriores para evaluar caida')
    p.add_argument('--max-invalidos-pct', type=float, default=1, help='Aviso de calidad si supera este porcentaje')
    p.add_argument('--max-grupos', type=int, default=1000, help='Grupos a evaluar; si hay mas, sale incompleto')
    p.add_argument('--timeout', type=int, default=180, help='Segundos de espera antes de solicitar cancelacion')
    p.add_argument('--con-avisos', action='store_true', help='Detalle AVISO en modo historico; en comparacion la tabla siempre incluye todos los estados')
    p.add_argument('--detallado', action='store_true', help='Muestra todos los controles')
    p.add_argument('--no-export', action='store_true', help='No genera reportes locales ni los sube a S3; Athena conserva sus resultados internos')
    p.add_argument('--salida', default='salida')
    return p


def validate(p, a):
    if (a.ejecutar or a.solo_sql) and not (a.database and a.fecha):
        p.error('--ejecutar/--solo-sql requieren --database y --fecha confirmadas')
    if bool(a.latitud) != bool(a.longitud):
        p.error('--latitud y --longitud se deben indicar juntas')
    if not a.comparar_periodos and bool(a.fuente) != a.detalle_diario:
        p.error('--fuente y --detalle-diario se deben indicar juntos')
    if a.comparar_periodos and not (a.unidad and a.sigla and a.fuente):
        p.error('--comparar-periodos requiere --unidad, --sigla y --fuente')
    if a.comparar_periodos and a.delito:
        p.error('La comparacion por unidad/BD no admite --delito; su significado sigue pendiente')
    if (a.catalogo_unidades or a.particiones_fecha_confirmadas) and not a.comparar_periodos:
        p.error('Catalogo y particiones confirmadas requieren --comparar-periodos')
    if a.detalle_diario and not a.comparar_periodos and not (a.unidad and a.sigla and a.filtrar_unidad and a.filtrar_sigla):
        p.error('--detalle-diario requiere filtrar una unidad y su sigla')
    if (a.detalle_diario or a.comparar_periodos) and a.ventana_dias > 31:
        p.error('El detalle diario admite ventanas de hasta 31 dias')
    for field in ('unidad', 'sigla'):
        value = getattr(a, 'filtrar_' + field)
        if value is not None and (not value.strip() or not getattr(a, field)):
            p.error(f'--filtrar-{field} requiere un valor no vacio y --{field} con la columna real')
    if not math.isfinite(a.max_horas) or a.max_horas <= 0:
        p.error('--max-horas debe ser finito y mayor que cero')
    if any(v <= 0 for v in (a.ventana_dias, a.min_base, a.max_grupos, a.timeout)):
        p.error('ventana-dias, min-base, max-grupos y timeout deben ser mayores que cero')
    if any(not math.isfinite(v) or not 0 <= v <= 100 for v in (a.caida_pct, a.max_invalidos_pct)):
        p.error('Los porcentajes deben estar entre 0 y 100')
    if a.output_s3 and not a.output_s3.startswith('s3://'):
        p.error('--output-s3 debe comenzar por s3://')


def discover(glue, a):
    databases = [a.database] if a.database else (
        item['Name'] for page in glue.get_paginator('get_databases').paginate()
        for item in page.get('DatabaseList', []))
    found = []
    for database in databases:
        try:
            table = glue.get_table(DatabaseName=database, Name=a.tabla)['Table']
        except glue.exceptions.EntityNotFoundException:
            continue
        columns = table.get('StorageDescriptor', {}).get('Columns', []) + table.get('PartitionKeys', [])
        partitions = table.get('PartitionKeys', [])
        if not a.comparar_periodos or a.detallado or not (a.ejecutar or a.solo_sql):
            print(f'\n[INFO] {database}.{a.tabla} ({table.get("TableType", "tipo no informado")})')
            print('       Columna | Tipo')
            for col in columns:
                print(f'       {col["Name"]} | {col["Type"]}')
            print('       Particiones declaradas: ' + (', '.join(c['Name'] for c in partitions) or 'ninguna'))
        found.append({'database': database, 'tabla': a.tabla, 'columnas': columns, 'particiones': partitions})
    if not found:
        raise Blocked(f'No se encontro {a.tabla} en las bases consultadas. Revisar region, catalogo y permisos.')
    return found


def validate_columns(schema, a):
    columns = {col['Name']: col['Type'] for col in schema['columnas']}
    for name in ('fecha', 'unidad', 'sigla', 'delito', 'latitud', 'longitud', 'fuente'):
        value = getattr(a, name)
        if value and value not in columns:
            raise Blocked(f'--{name}: columna {value!r} no existe. Usar el nombre exacto del esquema.')
    if 'with time zone' in columns[a.fecha].lower():
        raise Blocked('Fecha con zona horaria: requiere adaptar conversion explicitamente; no se asume zona.')


def input_cte(a):
    """Comparte conversiones y filtros entre las dos consultas."""
    date = identifier(a.fecha)
    date = f'TRY(date_parse(CAST({date} AS varchar), {literal(a.formato_fecha)}))' if a.formato_fecha else f'TRY_CAST({date} AS timestamp)'
    selects = [f'{date} AS fecha']
    for alias, col in [('sigla', a.sigla), ('unidad', a.unidad), ('delito', a.delito), ('fuente', a.fuente)]:
        if col:
            selects.append(f'NULLIF(trim(CAST({identifier(col)} AS varchar)), \'\') AS {alias}')
    if a.latitud:
        selects += [f'TRY_CAST({identifier(a.latitud)} AS double) AS lat',
                    f'TRY_CAST({identifier(a.longitud)} AS double) AS lon']
    sql = 'WITH datos AS (\n SELECT ' + ',\n        '.join(selects)
    sql += f'\n FROM {identifier(a.database)}.{identifier(a.tabla)}'
    filters = []
    for alias, col in [('unidad', a.unidad), ('sigla', a.sigla)]:
        value = getattr(a, 'filtrar_' + alias)
        if value is not None:
            filters.append(f'trim(CAST({identifier(col)} AS varchar)) = {literal(value.strip())}')
    if filters:
        sql += '\n WHERE ' + ' AND '.join(filters)
    return sql + '\n)\n'


def build_sql(a, now):
    ref = now.strftime('%Y-%m-%d %H:%M:%S')
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    recent = (end - timedelta(days=a.ventana_dias)).strftime('%Y-%m-%d %H:%M:%S')
    previous = (end - timedelta(days=2*a.ventana_dias)).strftime('%Y-%m-%d %H:%M:%S')
    end = end.strftime('%Y-%m-%d %H:%M:%S')
    dimensions = [('sigla', a.sigla), ('unidad', a.unidad), ('delito', a.delito)]
    groups = [alias for alias, col in dimensions if col]
    fields = groups + [
        'count(*) AS total',
        'count_if(fecha IS NULL) AS fechas_invalidas',
        f"count_if(fecha > TIMESTAMP '{ref}') AS fechas_futuras",
        f"CAST(max(fecha) FILTER (WHERE fecha <= TIMESTAMP '{ref}') AS varchar) AS ultima_fecha",
        f"count_if(fecha >= TIMESTAMP '{recent}' AND fecha < TIMESTAMP '{end}') AS recientes",
        f"count_if(fecha >= TIMESTAMP '{previous}' AND fecha < TIMESTAMP '{recent}') AS anteriores",
    ]
    if a.latitud:
        fields.append('count_if(lat IS NULL OR lon IS NULL OR NOT (lat BETWEEN -90 AND 90) OR NOT (lon BETWEEN -180 AND 180)) AS coordenadas_invalidas')
    sql = input_cte(a) + 'SELECT ' + ',\n       '.join(fields)
    sql += '\nFROM datos'
    if groups:
        sql += '\nGROUP BY ' + ', '.join(groups)
        sql += '\nORDER BY ' + ', '.join(groups)
    return sql + f'\nLIMIT {a.max_grupos + 1}\n'


def detail_days(a, now):
    end = now.date()
    return [end - timedelta(days=i) for i in range(2*a.ventana_dias, 0, -1)]


def build_detail_sql(a, now):
    """Una fila por fuente historica, incluso si no tuvo hechos en estas semanas."""
    fields = ['fuente', 'count(*) AS total_historico',
              'CAST(max(fecha) AS varchar) AS ultima_fecha_observada']
    for day in detail_days(a, now):
        next_day = day + timedelta(days=1)
        fields.append(f"count_if(fecha >= TIMESTAMP '{day} 00:00:00' AND fecha < TIMESTAMP '{next_day} 00:00:00') AS d_{day:%Y%m%d}")
    return (input_cte(a) + 'SELECT ' + ',\n       '.join(fields) +
            '\nFROM datos\nGROUP BY fuente\nORDER BY fuente' + f'\nLIMIT {a.max_grupos + 1}\n')


def summarize_detail(rows, a, now):
    daily, sources, checks = [], [], []
    days = detail_days(a, now)
    group = f'sigla={a.filtrar_sigla} / unidad={a.filtrar_unidad}'
    if len(rows) > a.max_grupos:
        checks.append(dict(recurso=group, control='Cobertura de fuentes', estado='FALLA',
                           mensaje='Detalle INCOMPLETO: supera --max-grupos. No concluye cobertura total.'))
    if not rows:
        checks.append(dict(recurso=group, control='Detalle por fuente', estado='FALLA',
                           mensaje='Sin registros para la unidad/sigla en la consulta de detalle.'))
    for row in rows[:a.max_grupos]:
        fuente = row.get('fuente', '')
        counts = [int(row[f'd_{day:%Y%m%d}']) for day in days]
        previous, recent = sum(counts[:a.ventana_dias]), sum(counts[a.ventana_dias:])
        drop = round(100*(previous-recent)/previous, 2) if previous else None
        status = 'AVISO' if previous < a.min_base or drop >= a.caida_pct else 'OK'
        reason = ('Base insuficiente para evaluar caida' if previous < a.min_base else
                  'Caida supera umbral exploratorio' if status == 'AVISO' else 'Sin caida sobre umbral')
        source = dict(sigla=a.filtrar_sigla.strip(), unidad=a.filtrar_unidad.strip(), fuente=fuente,
                      total_historico=int(row['total_historico']),
                      ultima_fecha_observada=row.get('ultima_fecha_observada', ''),
                      anteriores=previous, recientes=recent, diferencia=recent-previous,
                      caida_pct=drop, dias_sin_registros=counts.count(0), estado=status, motivo=reason)
        sources.append(source)
        checks.append(dict(recurso=f'{group} / fuente={fuente or "<SIN_VALOR>"}', control='Volumen por fuente', estado=status,
                           mensaje=f'Anterior={previous}; reciente={recent}; diferencia={recent-previous}; caida={drop if drop is not None else "no evaluable"}%; {reason}.'))
        if not fuente:
            checks.append(dict(recurso=group, control='Fuente vacia', estado='AVISO',
                               mensaje=f'{source["total_historico"]} registros historicos sin fuente identificable.'))
        for i, (day, count) in enumerate(zip(days, counts)):
            daily.append(dict(sigla=source['sigla'], unidad=source['unidad'], fuente=fuente,
                              dia=str(day), periodo='anterior' if i < a.ventana_dias else 'reciente',
                              registros=count, observacion='Sin registros; no demuestra falla de carga' if count == 0 else 'Con registros'))
    return daily, sources, checks


def print_detail(daily, sources):
    print('\n[INFO] Comparacion por fuente: anterior -> reciente (dias completos).')
    for row in sources:
        print(f'  Fuente={row["fuente"] or "<SIN_VALOR>"} | historico={row["total_historico"]} | {row["anteriores"]} -> {row["recientes"]} | diferencia={row["diferencia"]} | dias sin registros={row["dias_sin_registros"]}')
    print('\n[INFO] Detalle diario por fuente:')
    for row in daily:
        print(f'  {row["dia"]} | {row["fuente"] or "<SIN_VALOR>"} | {row["periodo"]} | registros={row["registros"]}')
    print('[INFO] Los ceros son dias sin hechos registrados en fecha_hora; no prueban falla de ingestion.')
    print('[INFO] Fuentes identificadas en el historico de esta unidad; no es un catalogo de fuentes esperadas.')


def execute(athena, a, sql):
    verbose = a.detallado or not a.comparar_periodos
    kwargs = dict(QueryString=sql, QueryExecutionContext={'Database': a.database, 'Catalog': 'AwsDataCatalog'}, WorkGroup=a.workgroup)
    if a.output_s3:
        kwargs['ResultConfiguration'] = {'OutputLocation': a.athena_output_s3}
        if verbose:
            print(f'[INFO] Destino solicitado para Athena: {a.athena_output_s3}', flush=True)
    query_id = athena.start_query_execution(**kwargs)['QueryExecutionId']
    if verbose:
        print(f'[INFO] QueryExecutionId: {query_id}', flush=True)
    print('Consultando Athena... (Ctrl+C cancela)' if not verbose else '[INFO] Esperando Athena; Ctrl+C solicita cancelar esta consulta.', flush=True)
    deadline = time.monotonic() + a.timeout
    try:
        while True:
            info = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
            state = info['Status']['State']
            if state in ('FAILED', 'CANCELLED'):
                raise Blocked(f'Athena {state}: {info["Status"].get("StateChangeReason", "sin detalle")}; query={query_id}')
            if state == 'SUCCEEDED':
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f'Tiempo de espera agotado; query={query_id}')
            time.sleep(2)
    except (KeyboardInterrupt, TimeoutError):
        athena.stop_query_execution(QueryExecutionId=query_id)
        print(f'[AVISO] Cancelacion solicitada para {query_id}.', flush=True)
        raise
    records = []
    first = True
    for page in athena.get_paginator('get_query_results').paginate(QueryExecutionId=query_id):
        result = page['ResultSet']
        columns = [col['Name'] for col in result['ResultSetMetadata']['ColumnInfo']]
        for row in result.get('Rows', []):
            if first:
                first = False  # Athena devuelve encabezado solo al inicio de los resultados SELECT.
                continue
            values = [v.get('VarCharValue', '') for v in row.get('Data', [])]
            records.append(dict(zip(columns, values)))
    if verbose:
        print(f'[INFO] Datos escaneados: {info.get("Statistics", {}).get("DataScannedInBytes", 0)/1024**2:.2f} MiB')
    actual = info.get('ResultConfiguration', {}).get('OutputLocation', '')
    if actual and not actual.startswith(a.athena_output_s3):
        print(f'[AVISO] Athena uso otra ubicacion de resultados, revisar configuracion del workgroup: {actual}', flush=True)
    return records, info


def evaluate(rows, a, now):
    checks = []
    def add(group, check, status, message):
        checks.append(dict(recurso=group, control=check, estado=status, mensaje=message))
    if len(rows) > a.max_grupos:
        add('GENERAL', 'Cobertura de grupos', 'FALLA', f'INCOMPLETO: supera {a.max_grupos} grupos. Aumentar --max-grupos; no es un resultado global.')
    if not rows:
        add('GENERAL', 'Datos', 'FALLA', 'La consulta no produjo grupos; no hay datos para el alcance/filtros elegidos. Revisar nombres y sigla.')
    for r in rows[:a.max_grupos]:
        group = ' / '.join(f'{name}={r.get(name) or "<SIN_VALOR>"}' for name in ('sigla', 'unidad', 'delito') if getattr(a, name)) or 'GLOBAL'
        total = int(r['total'])
        if not total:
            add(group, 'Datos', 'FALLA', 'Tabla sin registros.')
            continue
        for name in ('sigla', 'unidad', 'delito'):
            if getattr(a, name) and not r.get(name):
                add(group, 'Dimension ' + name, 'AVISO', f'{total} registros sin {name}.')
        last = r.get('ultima_fecha', '')
        if last:
            age = (now.replace(tzinfo=None) - datetime.fromisoformat(last)).total_seconds()/3600
            status = 'OK' if age <= a.max_horas else 'AVISO' if a.unidad or a.delito else 'FALLA'
            add(group, 'Recencia', status, f'Ultima fecha={last}; edad={age:.1f} h; umbral exploratorio={a.max_horas:g} h. Es fecha de la columna elegida, no necesariamente ingestion.')
        else:
            add(group, 'Recencia', 'FALLA', 'No hay fechas validas no futuras; no se puede medir recencia.')
        for key in ('fechas_invalidas', 'fechas_futuras', 'coordenadas_invalidas'):
            if key not in r:
                continue
            count = int(r[key])
            pct = 100*count/total
            status = 'FALLA' if count == total else 'AVISO' if pct > a.max_invalidos_pct else 'OK'
            add(group, key, status, f'{count}/{total} ({pct:.2f}%); tolerancia exploratoria={a.max_invalidos_pct:g}%.')
        recent, previous = int(r['recientes']), int(r['anteriores'])
        if previous < a.min_base:
            add(group, 'Volumen', 'AVISO', f'No evaluable: base anterior={previous} < {a.min_base}; reciente={recent}. No implica fallo para delitos de baja frecuencia.')
        else:
            drop = 100*(previous-recent)/previous
            add(group, 'Volumen', 'AVISO' if drop >= a.caida_pct else 'OK', f'Ultimos {a.ventana_dias} dias completos={recent}; periodo anterior={previous}; caida={drop:.1f}%.')
    for field, label in [('unidad', 'desglose por unidad'), ('delito', 'desglose por delito'), ('latitud', 'calidad de coordenadas')]:
        if not getattr(a, field):
            add('ALCANCE', label, 'AVISO', 'No evaluado: no se configuro la columna correspondiente.')
    add('ALCANCE', 'Unidades ausentes', 'AVISO', 'Sin catalogo esperado no se detectan unidades/delitos totalmente ausentes de la tabla.')
    return checks


def save_csv(path, rows):
    if rows:
        with path.open('w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def csv_path(prefix, report):
    return prefix.with_name(f'{report}_{prefix.name}.csv')


def execution_s3(prefix, root=REPORTS_S3):
    """Una misma identidad de ejecucion para las consultas y los CSV finales."""
    stamp = prefix.name.removeprefix('datos_entrada_')
    day, clock = stamp.split('_', 1)
    return f'{root.rstrip("/")}/{day}/{clock}/'


def upload_reports(s3, prefix, reports, verbose=True):
    """CSV del monitor en carpeta por ejecucion, separados de los CSV de Athena."""
    # Destino fijo solicitado; nombres no derivados de nombres libres de unidades.
    bucket, key_root = execution_s3(prefix).removeprefix('s3://').split('/', 1)
    locations = {}
    for name, rows in reports.items():
        buffer = io.StringIO(newline='')
        fields = list(rows[0]) if rows else ['sigla', 'unidad', 'bd']
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        key = key_root + name + '.csv'
        with io.BytesIO(buffer.getvalue().encode('utf-8-sig')) as body:
            s3.upload_fileobj(body, bucket, key, ExtraArgs={'ContentType': 'text/csv; charset=utf-8'})
        locations[name] = f's3://{bucket}/{key}'
        if verbose:
            print(f'[S3] Subido: {locations[name]}', flush=True)
    if verbose:
        print(f'[S3] Reportes guardados en s3://{bucket}/{key_root}', flush=True)
    return locations


BDS = ('RNMC', 'SECAD', 'SIEDCO')


def partition_predicate(a, now, schema):
    """Solo poda si el operador confirma semantica Y Glue declara las claves."""
    if not a.particiones_fecha_confirmadas:
        return ''
    keys = {c['Name']: c['Type'] for c in schema.get('particiones', [])}
    if not all(key in keys for key in ('year', 'month', 'day')):
        raise Blocked('Glue no declara year/month/day como particiones. No se puede activar la poda.')
    if any(keys[k].lower() not in ('string', 'int', 'integer', 'bigint', 'smallint', 'tinyint') for k in ('year', 'month', 'day')):
        raise Blocked('Tipos de particion no soportados; revisar el esquema antes de filtrar.')
    clauses = []
    for day in detail_days(a, now):
        fields = []
        for key, value in [('year', day.year), ('month', day.month), ('day', day.day)]:
            if keys[key].lower() == 'string':
                # Aceptar formatos 1 y 01 sin aplicar funciones a la clave.
                options = sorted({str(value), f'{value:02d}'})
                fields.append(f'{identifier(key)} IN ({", ".join(literal(v) for v in options)})')
            else:
                fields.append(f'{identifier(key)} = {value}')
        clauses.append('(' + ' AND '.join(fields) + ')')
    return '(' + ' OR '.join(clauses) + ')'


def build_period_sql(a, now, schema):
    days = detail_days(a, now)
    first, end = str(days[0]), str(now.date())
    cte = input_cte(a)
    pruning = partition_predicate(a, now, schema)
    if pruning:
        # Insertar filtro en la lectura fisica; no asumir que las columnas son fechas.
        cte = cte[:-3] + ('\n AND ' if '\n WHERE ' in cte else '\n WHERE ') + pruning + '\n)\n'
    interval = f"fecha >= TIMESTAMP '{first} 00:00:00' AND fecha < TIMESTAMP '{end} 00:00:00'"
    fields = ['sigla', 'unidad', 'fuente', 'count(*) AS total',
              f'count_if({interval}) AS registros_periodo',
              f'CAST(max(fecha) FILTER (WHERE {interval}) AS varchar) AS ultima_fecha',
              'count_if(fecha IS NULL) AS fechas_invalidas',
              f"count_if(fecha > TIMESTAMP '{now:%Y-%m-%d %H:%M:%S}') AS fechas_futuras"]
    if a.latitud:
        fields.append(f'count_if(({interval}) AND (lat IS NULL OR lon IS NULL OR NOT (lat BETWEEN -90 AND 90) OR NOT (lon BETWEEN -180 AND 180))) AS coordenadas_invalidas')
    for day in days:
        fields.append(f"count_if(fecha >= TIMESTAMP '{day} 00:00:00' AND fecha < TIMESTAMP '{day + timedelta(days=1)} 00:00:00') AS d_{day:%Y%m%d}")
    sql = cte + 'SELECT ' + ',\n       '.join(fields) + '\nFROM datos'
    # Sin fecha de particion confirmada no se pueden atribuir fechas invalidas a estos dias.
    if not pruning:
        sql += '\nWHERE ' + interval
    return sql + f'\nGROUP BY sigla, unidad, fuente\nORDER BY sigla, unidad, fuente\nLIMIT {a.max_grupos + 1}\n'


def catalog_units(a):
    if not a.catalogo_unidades:
        return set()
    with Path(a.catalogo_unidades).open(encoding='utf-8-sig', newline='') as f:
        sample = f.read(12000); f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
        reader = csv.DictReader(f, dialect=dialect)
        required = {'SIGLA_FISICA', 'DEPENDENCIA', 'UNIDAD_ID'}
        if not required.issubset(reader.fieldnames or []):
            raise Blocked('Catalogo requiere SIGLA_FISICA, DEPENDENCIA y UNIDAD_ID.')
        units = set()
        for row in reader:
            if not (row.get('UNIDAD_ID') or '').strip():
                continue  # Excluir niveles de jerarquia sin identificador operativo.
            sigla, unit = (row.get('SIGLA_FISICA') or '').strip(), (row.get('DEPENDENCIA') or '').strip()
            if not sigla or not unit:
                raise Blocked('Catalogo con unidad operativa sin sigla o nombre; corregir antes de cruzar.')
            if a.filtrar_sigla and sigla != a.filtrar_sigla.strip():
                continue
            if a.filtrar_unidad and unit != a.filtrar_unidad.strip():
                continue
            units.add((sigla, unit))
    if not units:
        raise Blocked('El catalogo no contiene unidades con UNIDAD_ID para los filtros indicados.')
    return units


def period_report(rows, a, now, expected):
    """Una fila por sigla/unidad/BD; ausencias son AVISO, no fallos de ingestion."""
    summary, daily, checks = [], [], []
    def check(resource, control, status, message):
        checks.append(dict(recurso=resource, control=control, estado=status, mensaje=message))
    complete = len(rows) <= a.max_grupos
    if not complete:
        check('GENERAL', 'Cobertura de grupos', 'FALLA', 'INCOMPLETO: supera --max-grupos; no inferir unidades ausentes de resultados truncados.')
    data = {(r.get('sigla', ''), r.get('unidad', ''), r.get('fuente', '')): r for r in rows[:a.max_grupos]}
    observed = {(k[0], k[1]) for k in data}
    units = observed | (expected if complete else set())
    if not units and a.filtrar_sigla and a.filtrar_unidad and complete:
        units.add((a.filtrar_sigla.strip(), a.filtrar_unidad.strip()))
    if not units:
        check('GENERAL', 'Datos', 'FALLA', 'No hay grupos ni catalogo esperado para delimitar la cobertura.')
    days = detail_days(a, now)
    quality_dates = a.particiones_fecha_confirmadas
    for sigla, unit in sorted(units):
        sources = (set(BDS) if complete else set()) | {k[2] for k in data if k[:2] == (sigla, unit)}
        for source in sorted(sources):
            r = data.get((sigla, unit, source), {})
            counts = [int(r.get(f'd_{day:%Y%m%d}', 0)) for day in days]
            previous, recent = sum(counts[:a.ventana_dias]), sum(counts[a.ventana_dias:])
            total = previous + recent
            last = r.get('ultima_fecha', '')
            age = round((now.replace(tzinfo=None)-datetime.fromisoformat(last)).total_seconds()/3600, 1) if last else None
            streak = 0
            for value in reversed(counts):
                if value:
                    break
                streak += 1
            resource = f'sigla={sigla} / unidad={unit} / BD={source or "<SIN_VALOR>"}'
            start_index = len(checks)
            check(resource, 'Registros en periodo', 'OK' if total else 'AVISO', f'{total} registros en {len(days)} dias. Cero no demuestra una falla ni que esta BD deba aportar a la unidad.')
            drop = round(100*(previous-recent)/previous, 2) if previous else None
            volume_status = 'AVISO' if previous < a.min_base or drop >= a.caida_pct else 'OK'
            check(resource, 'Volumen', volume_status, f'Anterior={previous}; reciente={recent}; caida={drop if drop is not None else "no evaluable"}%; ' + ('base insuficiente' if previous < a.min_base else 'comparacion evaluable'))
            check(resource, 'Recencia en periodo', 'AVISO' if age is None or age > a.max_horas else 'OK', f'Ultima fecha dentro del periodo={last or "sin registros"}; edad_horas={age}. No es fecha de ingestion.')
            if source not in BDS:
                check(resource, 'BD identificada', 'AVISO', 'BD vacia o no incluida en RNMC/SECAD/SIEDCO; no se descarta del reporte.')
            if not sigla or not unit:
                check(resource, 'Identificacion', 'AVISO', 'Sigla o unidad vacia.')
            if expected and (sigla, unit) not in expected:
                check(resource, 'Catalogo', 'AVISO', 'Unidad observada sin coincidencia exacta en el catalogo seleccionado; revisar nombres/siglas.')
            quality_scope = int(r.get('total', 0))
            for key in ('fechas_invalidas', 'fechas_futuras'):
                if quality_dates:
                    count = int(r.get(key, 0))
                    status = ('FALLA' if count and count == quality_scope else 'AVISO' if quality_scope and 100*count/quality_scope > a.max_invalidos_pct else 'OK')
                    check(resource, key, status, f'{count}/{quality_scope} registros de las particiones seleccionadas.')
            coords = int(r.get('coordenadas_invalidas', 0)) if a.latitud else None
            if coords is not None:
                check(resource, 'Coordenadas en periodo', 'FALLA' if coords and coords == total else 'AVISO' if total and 100*coords/total > a.max_invalidos_pct else 'OK', f'{coords}/{total} nulas o fuera de rango; no comprueba limites geograficos de la unidad.')
            state = max((c['estado'] for c in checks[start_index:]), key={'OK': 0, 'AVISO': 1, 'FALLA': 2}.get)
            summary.append(dict(sigla=sigla, unidad=unit, bd=source,
                                inicio_anterior=str(days[0]), fin_anterior=str(days[a.ventana_dias-1]),
                                inicio_reciente=str(days[a.ventana_dias]), fin_reciente=str(days[-1]),
                                registros_anteriores=previous, registros_recientes=recent,
                                diferencia=recent-previous, caida_pct=drop, comparacion_evaluable=previous >= a.min_base,
                                registros_periodo=total, ultima_fecha_en_periodo=last, edad_horas=age,
                                dias_sin_registros=counts.count(0), dias_sin_registros_recientes=counts[a.ventana_dias:].count(0),
                                racha_sin_registros_al_cierre=streak,
                                fechas_invalidas=int(r.get('fechas_invalidas', 0)) if quality_dates else 'NO_EVALUADO',
                                fechas_futuras=int(r.get('fechas_futuras', 0)) if quality_dates else 'NO_EVALUADO',
                                coordenadas_invalidas=coords,
                                cobertura='COMPLETA' if complete else 'INCOMPLETA', estado=state,
                                motivo='; '.join(c['control'] + ': ' + c['mensaje'] for c in checks[start_index:] if c['estado'] != 'OK')))
            for i, (day, count) in enumerate(zip(days, counts)):
                daily.append(dict(sigla=sigla, unidad=unit, bd=source, fecha=str(day),
                                  periodo='anterior' if i < a.ventana_dias else 'reciente', registros=count))
    if not quality_dates:
        check('ALCANCE', 'Calidad de fechas', 'AVISO', 'NO EVALUADA: filtrar por fecha excluye nulos, no interpretables y futuros. No declarar cero. Requiere particiones de fecha verificadas o auditoria historica.')
    if not a.latitud:
        check('ALCANCE', 'Calidad de coordenadas', 'AVISO', 'No evaluada: faltan columnas de latitud/longitud.')
    if not expected:
        check('ALCANCE', 'Catalogo de unidades', 'AVISO', 'Sin catalogo: solo unidades observadas o la unidad filtrada; no detecta todas las ausentes.')
    return summary, daily, checks


def print_summary_table(summary, a):
    headers = ['Sigla', 'Unidad', 'BD', f'Ant.{a.ventana_dias}d', f'Ult.{a.ventana_dias}d', 'Cambio %', 'Sin datos*', 'Estado']
    widths = [7, 34, 7, 8, 8, 9, 10, 6]
    numeric = {3, 4, 5, 6}
    def line(cells):
        return ' | '.join(str(value).rjust(widths[i]) if i in numeric else str(value).ljust(widths[i]) for i, value in enumerate(cells))
    print('\n' + line(headers))
    print('-+-'.join('-'*width for width in widths))
    for row in summary:
        change = -row['caida_pct'] if row['caida_pct'] is not None else None
        cells = [row['sigla'] or '-', row['unidad'] or '-', row['bd'] or '-',
                 row['registros_anteriores'], row['registros_recientes'],
                 f'{change:+.1f}%' if change is not None else 'N/E',
                 row['racha_sin_registros_al_cierre'], row['estado']]
        # Conservar nombres completos, sin ensanchar ni desalinear la tabla.
        wrapped = [textwrap.wrap(' '.join(str(value).split()), width=widths[i]) or [''] for i, value in enumerate(cells)]
        for index in range(max(map(len, wrapped))):
            print(line([parts[index] if index < len(parts) else '' for parts in wrapped]))
    if not summary:
        print('(Sin filas para mostrar)')
    print('* Sin datos: dias consecutivos al cierre; no demuestra fallo de carga. N/E: base anterior cero.')


def print_unit_summary(summary, incomplete=False):
    priority = {'OK': 0, 'AVISO': 1, 'FALLA': 2}
    units = {}
    for row in summary:
        key = (row['sigla'], row['unidad'])
        units[key] = max(units.get(key, 'OK'), row['estado'], key=priority.get)
    siglas = {sigla for sigla, _ in units if sigla}
    counts = {state: sum(value == state for value in units.values()) for state in priority}
    label = 'RESUMEN PARCIAL' if incomplete else 'RESUMEN'
    print(f'\n{label} | Siglas: {len(siglas)} | Unidades: {len(units)} '
          f'({counts["OK"]} OK / {counts["AVISO"]} AVISO / {counts["FALLA"]} FALLA) '
          f'| Unidad-BD: {len(summary)}')


def run_period_mode(a, now, schema, athena, prefix, identity, s3=None):
    expected = catalog_units(a)
    sql = build_period_sql(a, now, schema)
    days = detail_days(a, now)
    print(f'\nDATOS DE ENTRADA | {a.filtrar_sigla or "Todas las siglas"} | Cuenta {identity["Account"]}')
    print(f'Anterior: {days[0]} a {days[a.ventana_dias-1]} | Reciente: {days[a.ventana_dias]} a {days[-1]}')
    if a.detallado:
        print('[INFO] UNA consulta para resumen, calidad por BD y diario. No consulta registros individuales.')
        print('[INFO] Poda por particiones: ' + ('habilitada por confirmacion explicita' if a.particiones_fecha_confirmadas else 'no habilitada; el filtro temporal no garantiza menor lectura fisica'))
    if a.detallado or a.solo_sql:
        print(sql)
    if prefix:
        prefix.with_suffix('.sql').write_text(sql, encoding='utf-8')
    if a.solo_sql:
        print('[AVISO] SQL generado: datos NO evaluados; Athena no ejecutado.')
        return 0
    records, info = execute(athena, a, sql)
    summary, daily, checks = period_report(records, a, now, expected)
    if prefix:
        save_csv(csv_path(prefix, 'resumen'), summary)
        save_csv(csv_path(prefix, 'diario'), daily)
        save_csv(csv_path(prefix, 'controles'), checks)
        context = {'configuracion': vars(a), 'reloj': now.isoformat(), 'cuenta': identity['Account'],
                   'schema': schema, 'query': info, 'resumen': summary,
                   'cobertura': 'INCOMPLETA' if len(records) > a.max_grupos else 'COMPLETA',
                   'calidad_fechas_evaluada': a.particiones_fecha_confirmadas}
        prefix.with_suffix('.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        if a.detallado:
            print(f'[INFO] Resumen: {csv_path(prefix, "resumen")} | Diario: {csv_path(prefix, "diario")} | Controles: {csv_path(prefix, "controles")}')
        context['reportes_s3'] = upload_reports(s3, prefix, {'general': summary, 'diario': daily, 'control': checks}, verbose=a.detallado)
        prefix.with_suffix('.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print_summary_table(summary, a)
    for check in checks:
        if check['estado'] == 'FALLA' or a.detallado:
            print(f'[{check["estado"]}] {check["recurso"]} | {check["control"]}: {check["mensaje"]}')
    if a.detalle_diario:
        for row in daily:
            print(f'[DIARIO] {row["sigla"]} / {row["unidad"]} / {row["bd"]} | {row["fecha"]}: {row["registros"]}')
    counts = {state: sum(r['estado'] == state for r in checks) for state in ('OK', 'AVISO', 'FALLA')}
    print_unit_summary(summary, incomplete=len(records) > a.max_grupos)
    print(f'Controles: {len(checks)} | {counts["OK"]} OK | {counts["AVISO"]} aviso | {counts["FALLA"]} falla')
    if prefix:
        print(f'Reportes S3: {execution_s3(prefix)}')
    else:
        print('Reportes: exportacion desactivada (--no-export).')
    scope_notes = [c['control'] for c in checks if c['recurso'] == 'ALCANCE']
    if scope_notes:
        print('Alcance pendiente: ' + ', '.join(scope_notes) + '. Detalle en control.csv o --detallado.')
    return 1 if counts['FALLA'] else 0


def main():
    p = parser(); a = p.parse_args(); validate(p, a)
    prefix = None
    try:
        import boto3
        from boto3.exceptions import S3UploadFailedError
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Falta boto3/botocore. Ejecutar en AWS CloudShell.', file=sys.stderr)
        return 3
    try:
        session = boto3.Session(region_name=a.region)
        cfg = Config(connect_timeout=5, read_timeout=30, retries={'total_max_attempts': 3, 'mode': 'standard'})
        identity = session.client('sts', config=cfg).get_caller_identity()
        if a.detallado or not a.comparar_periodos:
            print(f'[INFO] Cuenta={identity["Account"]}; ARN={identity["Arn"]}; region={a.region}')
        if identity['Account'] != a.cuenta:
            raise Blocked('Cuenta distinta de --cuenta; no se consultara el catalogo ni Athena.')
        # Fijar carpeta ANTES de lanzar Athena, incluso con --no-export.
        run_name = 'datos_entrada_' + datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d_%H-%M-%S_%f')
        a.athena_output_s3 = execution_s3(Path(run_name), a.output_s3) + '_athena/'
        if not a.no_export:
            directory = Path(a.salida); directory.mkdir(parents=True, exist_ok=True)
            # Nombres en hora de Colombia, independiente de la zona de los datos/host.
            prefix = directory / run_name
        glue = session.client('glue', config=cfg)
        schema = discover(glue, a)
        if prefix:
            prefix.with_suffix('.json').write_text(json.dumps({'cuenta': identity['Account'], 'region': a.region, 'esquemas': schema, 'configuracion': vars(a)}, ensure_ascii=False, indent=2), encoding='utf-8')
        if not (a.ejecutar or a.solo_sql):
            print('\n[AVISO] Solo descubrimiento: NO se evaluaron datos ni se ejecuto Athena.')
            print('Siguiente prueba: --database BASE --fecha COLUMNA_CONFIRMADA --solo-sql')
            return 0
        validate_columns(schema[0], a)
        zone = timezone.utc if a.zona_datos == 'UTC' else timezone(timedelta(hours=-5))
        now = datetime.now(zone).replace(microsecond=0)
        s3 = session.client('s3', config=cfg) if a.ejecutar and not a.no_export else None
        if a.comparar_periodos:
            return run_period_mode(a, now, schema[0], session.client('athena', config=cfg), prefix, identity, s3)
        sql = build_sql(a, now)
        print(f'\n[INFO] Reloj de comparacion: {now.isoformat()} ({a.zona_datos}).')
        print('[INFO] Consulta agregada del historico; no trae incidentes individuales.')
        print('[INFO] Filtros de unidad/sigla aplican antes de agrupar, pero no garantizan reducir bytes escaneados.')
        for field in ('unidad', 'sigla'):
            value = getattr(a, 'filtrar_' + field)
            if value is not None:
                print(f'[INFO] Filtro {field}: {value.strip()}')
        print(sql)
        if prefix:
            prefix.with_suffix('.sql').write_text(sql, encoding='utf-8')
        detail_sql = build_detail_sql(a, now) if a.detalle_diario else None
        if detail_sql:
            print('[INFO] Detalle: UNA consulta adicional, por fuente y dias; puede generar lectura adicional.')
            print(detail_sql)
            if prefix:
                prefix.with_suffix('.fuentes.sql').write_text(detail_sql, encoding='utf-8')
        if a.solo_sql:
            print('[AVISO] SQL generado: datos NO evaluados; Athena no ejecutado.')
            return 0
        athena = session.client('athena', config=cfg)
        sources, daily = [], []
        records, info = execute(athena, a, sql)
        checks = evaluate(records, a, now)
        # Conservar el resultado general aun si falla la segunda consulta.
        context = {'configuracion': vars(a), 'reloj': now.isoformat(), 'cuenta': identity['Account'], 'query': info, 'agregados': records,
                   'estado_detalle': 'PENDIENTE' if detail_sql else 'NO_SOLICITADO'}
        if prefix:
            save_csv(csv_path(prefix, 'controles'), checks)
            prefix.with_suffix('.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
            print(f'[INFO] Controles: {csv_path(prefix, "controles")} | Contexto: {prefix}.json | Consulta: {prefix}.sql')
        if detail_sql:
            print('[INFO] Iniciando detalle diario y por fuente; timeout aplica a cada consulta.')
            detail_records, detail_info = execute(athena, a, detail_sql)
            daily, sources, source_checks = summarize_detail(detail_records, a, now)
            checks.extend(source_checks)
            print_detail(daily, sources)
            context.update(query_detalle=detail_info, fuentes=sources, diario=daily,
                           estado_detalle='INCOMPLETO' if not detail_records or len(detail_records) > a.max_grupos else 'COMPLETO')
            if prefix:
                save_csv(csv_path(prefix, 'controles'), checks)
                save_csv(csv_path(prefix, 'fuentes'), sources)
                save_csv(csv_path(prefix, 'diario'), daily)
                prefix.with_suffix('.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
                print(f'[INFO] Fuentes: {csv_path(prefix, "fuentes")} | Diario: {csv_path(prefix, "diario")}')
        if prefix:
            reports = {'control': checks}
            if detail_sql:
                reports.update(general=sources, diario=daily)
            context['reportes_s3'] = upload_reports(s3, prefix, reports)
            prefix.with_suffix('.json').write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        for check in checks:
            if check['estado'] == 'FALLA' or a.detallado or (a.con_avisos and check['estado'] == 'AVISO'):
                print(f'[{check["estado"]}] {check["recurso"]} | {check["control"]}: {check["mensaje"]}')
        counts = {state: sum(r['estado'] == state for r in checks) for state in ('OK', 'AVISO', 'FALLA')}
        print(f'\nTOTAL: {len(checks)} | {counts["OK"]} OK | {counts["AVISO"]} aviso | {counts["FALLA"]} falla')
        print('[INFO] Recencia por unidad/delito y caida de volumen requieren interpretar baja frecuencia y carga tardia.')
        return 1 if counts['FALLA'] else 0
    except (ClientError, BotoCoreError, S3UploadFailedError, Blocked, TimeoutError, OSError, csv.Error) as exc:
        print(f'[BLOQUEADO] {type(exc).__name__}: {exc}', file=sys.stderr)
        print('Revisar Glue/Athena/S3 y Lake Formation/KMS si aplican. No se declara salud de los datos.', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Ejecucion interrumpida.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
