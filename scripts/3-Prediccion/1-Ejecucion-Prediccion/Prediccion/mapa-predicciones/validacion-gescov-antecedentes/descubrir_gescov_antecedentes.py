#!/usr/bin/env python3
"""Paso 0 de la validacion GESCOV + Antecedentes: revisa en AWS lo que validar_gescov_antecedentes.py da por supuesto.

  1. Esquema Glue de las tres tablas crudas de SIVICC (personas, vehiculos, IMEI) y de la copia de GESCOV en Athena,
     con las columnas que usan las consultas.
  2. Historial del job V2_SIVICC-migration-to-RDS (que copia las tablas de antecedentes a RDS), con --CAPA y --DB_TABLE.
  3. La tarea diaria de GESCOV: regla de EventBridge, grupo de logs (retencion) y streams de los ultimos dias.
  4. Que estaciones.csv exista y coincida con el tamano que reporta la tarea desplegada.
  5. Opcional (--muestra): 3 filas de cada tabla con Athena.
Solo lectura. Exit: 0 todo presente, 1 faltan columnas requeridas, 3 bloqueado (credenciales/permisos), 130 interrupcion.
"""

import argparse
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

import validar_gescov_antecedentes as vg
import secad_comun as sc
from botocore.exceptions import BotoCoreError, ClientError
from descubrir_secad import mostrar_tabla, _recortar

REGLA_GESCOV = 'pon-prod-addcapas-migrations-gescov-daily-schedule'


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA)
    p.add_argument('--runs', type=int, default=6, help='Ultimas corridas del job de antecedentes a mostrar (6)')
    p.add_argument('--muestra', action='store_true', help='Trae 3 filas de cada tabla con Athena')
    p.add_argument('--estaciones', default=str(vg.ESTACIONES_CSV))
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'), help='Destino de resultados de Athena (o variable ATHENA_OUTPUT_S3)')
    p.add_argument('--timeout', type=int, default=300)
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true')
    return p


def descubrir_tablas(glue):
    informe, falta = {}, False
    objetivos = [(vg.DB_SIVICC, f['tabla'], f'antecedentes {k}', vg.CAMPOS_COMUNES + [f['col_id'], f['col_fecha'], f['col_unde']])
                 for k, f in vg.FUENTES.items()]
    objetivos.append((vg.DB_DISEC, vg.TABLA_COPIA_GESCOV, 'copia de GESCOV en Athena (informativa)', ['fecha_creacion', 'fecha_inicio']))
    for db, tabla, uso, requeridas in objetivos:
        try:
            cols, claves, t = sc.columnas_tabla(glue, db, tabla)
        except sc.Blocked as exc:
            print(f'\n[FALLA] {db}.{tabla}: {exc}')
            falta = True
            continue
        mostrar_tabla(f'{db}.{tabla}  <- {uso}', cols, claves, t)
        faltan = [c for c in requeridas if c not in cols]
        if faltan:
            falta = True
            print(f'  [FALLA] faltan columnas que usa validar_gescov_antecedentes.py: {", ".join(faltan)}')
        else:
            print('  [ OK  ] estan todas las columnas requeridas')
        informe[f'{db}.{tabla}'] = dict(columnas=cols, ubicacion=t.get('StorageDescriptor', {}).get('Location'),
                                        actualizada=str(t.get('UpdateTime')), faltan=faltan)
    return informe, falta


def descubrir_job(glue, a):
    runs = []
    try:
        j = glue.get_job(JobName=vg.JOB_SIVICC)['Job']
        print(f'\n[INFO] Job {vg.JOB_SIVICC} | script {j["Command"]["ScriptLocation"]}')
        for k, v in sorted(j.get('DefaultArguments', {}).items()):
            if k in ('--CAPA', '--DB_TABLE', '--WRITE_MODE', '--SECRET_NAME'):
                print(f'      {k} = {_recortar(v, 120)}')
        for r in sc.leer_corridas(glue, vg.JOB_SIVICC, (sc.ahora_colombia() - timedelta(days=3)).date())[:a.runs * 3]:
            ra = r.get('Arguments') or {}
            runs.append(dict(id=r.get('Id'), estado=r['JobRunState'], inicio=str(r['StartedOn'].astimezone(sc.UTC_MENOS_5)),
                             capa=ra.get('--CAPA'), tabla=ra.get('--DB_TABLE'), error=r.get('ErrorMessage')))
        for r in runs[:a.runs * 3]:
            print(f'      run {r["estado"]:<10} {r["inicio"][:16]} CAPA {r["capa"]} -> {r["tabla"]}{" | " + _recortar(r["error"], 140) if r["error"] else ""}')
    except (ClientError, BotoCoreError) as exc:
        print(f'\n[AVISO] No se pudo leer el job {vg.JOB_SIVICC} ({sc.detalle_error(exc)}); falta glue:GetJob/GetJobRuns.')
    return runs


def descubrir_gescov(sesion, a):
    informe = {}
    try:
        regla = sesion.client('events', config=sc.BOTO_CONFIG).describe_rule(Name=REGLA_GESCOV)
        informe['regla'] = dict(nombre=REGLA_GESCOV, programacion=regla.get('ScheduleExpression'), estado=regla.get('State'))
        print(f'\n[INFO] Regla EventBridge {REGLA_GESCOV}: {regla.get("ScheduleExpression")} ({regla.get("State")}) [UTC]')
    except (ClientError, BotoCoreError) as exc:
        print(f'\n[AVISO] No se pudo leer la regla {REGLA_GESCOV} ({sc.detalle_error(exc)}).')
    try:
        logs = sesion.client('logs', config=sc.BOTO_CONFIG)
        grupo = logs.describe_log_groups(logGroupNamePrefix=vg.LOG_GESCOV)['logGroups']
        retencion = grupo[0].get('retentionInDays') if grupo else None
        desde = (sc.ahora_colombia() - timedelta(days=15)).date()
        ejs = vg.leer_ejecuciones_gescov(logs, vg.LOG_GESCOV, desde)
        con_log = sum(1 for e in ejs if e['detalle'])
        informe['logs'] = dict(grupo=vg.LOG_GESCOV, retencion_dias=retencion, ejecuciones=len(ejs), con_log=con_log)
        print(f'[INFO] Logs {vg.LOG_GESCOV}: retencion {retencion} dias | {len(ejs)} ejecuciones en los ultimos 15 dias, {con_log} con log legible')
        for e in ejs[-3:]:
            d = e['detalle']
            print(f'      {e["inicio"]:%Y-%m-%d %H:%M} ' + (f'extraidos {d["extraidos"]} | descartados {d["descartados"]} | max fecha_inicio {d["max_inicio"]}'
                                                             if d else '(sin log)'))
            if d and d['total'] is not None:
                print(f'          total en tabla {d["total"]:,} | estaciones.csv de la tarea desplegada: ver "registros de enriquecimiento" en el log')
        if retencion and retencion < 15:
            print(f'  [AVISO] La retencion ({retencion} dias) es menor que la ventana de 15 dias: las ejecuciones mas viejas no se pueden auditar.')
    except (ClientError, BotoCoreError) as exc:
        print(f'[AVISO] No se pudieron leer los logs de GESCOV ({sc.detalle_error(exc)}); falta logs:DescribeLogGroups/DescribeLogStreams/GetLogEvents.')
    try:
        n = len(vg.cargar_estaciones(Path(a.estaciones)))
        informe['estaciones_csv'] = dict(ruta=a.estaciones, filas=n)
        print(f'[INFO] estaciones.csv local: {n} filas (la tarea desplegada reporto 2185 el 2026-10-22; si difiere, la copia local no es la desplegada)')
    except (OSError, sc.Blocked) as exc:
        print(f'[AVISO] estaciones.csv no disponible ({exc}); indica la ruta con --estaciones.')
    return informe


def muestra(sesion, a):
    athena = sc.Athena(sesion, vg.DB_SIVICC, a.workgroup, a.output_s3, a.timeout)
    consultas = {f['tabla']: f'SELECT * FROM {sc.identifier(vg.DB_SIVICC)}.{sc.identifier(f["tabla"])} '
                             f'WHERE {sc.identifier(f["col_fecha"])} >= current_date - INTERVAL \'2\' DAY LIMIT 3' for f in vg.FUENTES.values()}
    print('\n[INFO] Muestra de 3 filas (Athena):')
    salida = {}
    for nombre, (filas, _) in athena.ejecutar(consultas).items():
        print(f'  {nombre}:')
        for f in filas:
            print('    ' + ' | '.join(f'{k}={_recortar(v, 30)}' for k, v in f.items()))
        salida[nombre] = filas
    return salida


def main(argv=None):
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding='utf-8', errors='replace')
        except (AttributeError, ValueError):
            pass
    a = parser().parse_args(argv)
    try:
        sesion = sc.crear_sesion(a.profile, a.region)
        ident = sc.verificar_identidad(sesion, a.cuenta)
        print(f'[INFO] Cuenta {ident["Account"]} | {ident["Arn"]} | region {a.region}')
        glue = sesion.client('glue', config=sc.BOTO_CONFIG)
        tablas, falta = descubrir_tablas(glue)
        informe = dict(cuenta=ident['Account'], tablas=tablas, corridas=descubrir_job(glue, a), gescov=descubrir_gescov(sesion, a))
        if a.muestra:
            informe['muestra'] = muestra(sesion, a)
        if not a.no_export:
            salida = Path(a.salida)
            salida.mkdir(parents=True, exist_ok=True)
            ruta = salida / f'descubrimiento_gescov_antecedentes_{sc.ahora_colombia().strftime("%Y-%m-%d_%H-%M-%S")}.json'
            ruta.write_text(json.dumps(informe, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
            print(f'\n[INFO] Informe completo: {ruta}')
        return 1 if falta else 0
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
