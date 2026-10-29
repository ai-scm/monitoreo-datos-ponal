#!/usr/bin/env python3
"""Paso 0 de la validacion RNMC: revisa en AWS lo que validar_rnmc.py da por supuesto. Solo lectura.

  1. Esquema Glue de las tablas de mediaciones (capa_16_1, capa_16_2) y de la vista de origen
     boletin_z1_capas_v2, y si estan las columnas que usan las consultas.
  2. Jobs de Glue que cargan estas capas: RNMC-to-RDS (vista -> public.hechos_z1_v2) y el ETL
     diario de las capas (pon-prod-capas-secad-etl): argumentos y ultimas ejecuciones, con el
     QUERY y la tabla de destino de las corridas manuales.
  3. Opcional (--muestra): 3 filas de cada tabla/vista con Athena.

Uso:  python descubrir_rnmc.py [--profile X] [--muestra]
Exit: 0 todo presente, 1 faltan columnas requeridas, 3 bloqueado (credenciales/permisos), 130 interrupcion.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import validar_rnmc as vr
import secad_comun as sc
from botocore.exceptions import BotoCoreError, ClientError
from descubrir_secad import mostrar_tabla, _recortar


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA)
    p.add_argument('--jobs-patron', default=r'^RNMC-to-RDS$|capas-secad-etl|copy-hechos', help='Regex (sin distinguir mayusculas) sobre el nombre de los jobs')
    p.add_argument('--runs', type=int, default=6, help='Ultimas ejecuciones por job (6)')
    p.add_argument('--muestra', action='store_true', help='Trae 3 filas de cada tabla/vista con Athena')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'), help='Destino de resultados de Athena (o variable ATHENA_OUTPUT_S3)')
    p.add_argument('--timeout', type=int, default=180)
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true')
    return p


def descubrir_tablas(glue):
    informe, faltan_total = {}, False
    objetivos = [(vr.DB_VISTA, vr.VISTA, 'vista de origen de rnmc-rinas, rnmc-incautaciones y comportamientos (via RNMC-to-RDS)', vr.CAMPOS_VISTA)]
    objetivos += [(vr.DB_CAPAS, d['tabla'], d['nombre'], list(vr.COLUMNAS_CAPA.values()) + [d['col_id']])
                  for d in vr.CAPAS.values() if d['origen'] == 'capa']
    for db, tabla, uso, requeridas in objetivos:
        cols, claves, t = sc.columnas_tabla(glue, db, tabla)
        mostrar_tabla(f'{db}.{tabla}  <- {uso}', cols, claves, t)
        faltan = [c for c in requeridas if c not in cols]
        if faltan:
            faltan_total = True
            print(f'  [FALLA] faltan columnas que usa validar_rnmc.py: {", ".join(faltan)}')
        else:
            print('  [ OK  ] estan todas las columnas requeridas por validar_rnmc.py')
        informe[f'{db}.{tabla}'] = dict(columnas=cols, particiones=claves, ubicacion=t.get('StorageDescriptor', {}).get('Location'),
                                        tipo=t.get('TableType'), actualizada=str(t.get('UpdateTime')), faltan=faltan)
    return informe, faltan_total


def descubrir_jobs(glue, a):
    patron = re.compile(a.jobs_patron, re.IGNORECASE)
    salida = []
    try:
        jobs = [j for pag in glue.get_paginator('get_jobs').paginate() for j in pag.get('Jobs', []) if patron.search(j['Name'])]
    except (ClientError, BotoCoreError) as exc:
        print(f'\n[AVISO] No se pudieron listar los jobs de Glue ({sc.detalle_error(exc)}); falta glue:GetJobs.')
        return salida
    print(f'\n[INFO] Jobs de Glue que coinciden con /{a.jobs_patron}/: {len(jobs)}')
    for j in sorted(jobs, key=lambda x: x['Name']):
        args = j.get('DefaultArguments', {})
        print(f'  - {j["Name"]} | script {j.get("Command", {}).get("ScriptLocation", "n/d")} | modificado {j.get("LastModifiedOn", "n/d")}')
        for k in sorted(args):
            if not k.startswith('--enable') and k not in ('--job-language', '--TempDir', '--spark-event-logs-path', '--job-bookmark-option'):
                print(f'      {k} = {_recortar(args[k], 400)}')
        corridas = []
        try:
            for r in glue.get_job_runs(JobName=j['Name'], MaxResults=a.runs).get('JobRuns', []):
                ra = r.get('Arguments') or {}
                inicio = r['StartedOn'].astimezone(sc.UTC_MENOS_5)
                corridas.append(dict(id=r.get('Id'), estado=r.get('JobRunState'), inicio=str(inicio), error=r.get('ErrorMessage'), argumentos=ra))
                print(f'      run {r.get("JobRunState", "?"):<10} {inicio:%Y-%m-%d %H:%M}'
                      f'{" | " + _recortar(r["ErrorMessage"], 160) if r.get("ErrorMessage") else ""}')
                for k in ('--DB_TABLE', '--QUERY', '--query', '--nombre_capa'):
                    if k in ra:
                        print(f'          {k} = {_recortar(ra[k], 260)}')
        except (ClientError, BotoCoreError) as exc:
            print(f'      [AVISO] sin historial de ejecuciones: {sc.detalle_error(exc)}')
        salida.append(dict(nombre=j['Name'], script=j.get('Command', {}).get('ScriptLocation'), argumentos=args, corridas=corridas))
    return salida


def muestra(sesion, a):
    athena = sc.Athena(sesion, vr.DB_CAPAS, a.workgroup, a.output_s3, a.timeout)
    consultas = {f'{vr.DB_VISTA}.{vr.VISTA}': f'SELECT * FROM {sc.identifier(vr.DB_VISTA)}.{sc.identifier(vr.VISTA)} WHERE fecha >= current_date - INTERVAL \'2\' DAY LIMIT 3'}
    consultas.update({f'{vr.DB_CAPAS}.{d["tabla"]}': f'SELECT * FROM {sc.identifier(vr.DB_CAPAS)}.{sc.identifier(d["tabla"])} LIMIT 3'
                      for d in vr.CAPAS.values() if d['origen'] == 'capa'})
    print('\n[INFO] Muestra de 3 filas (Athena):')
    resultados = athena.ejecutar(consultas)
    salida = {}
    for nombre, (filas, _) in resultados.items():
        print(f'  {nombre}:')
        for f in filas:
            print('    ' + ' | '.join(f'{k}={_recortar(v, 40)}' for k, v in f.items()))
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
        tablas, faltan = descubrir_tablas(glue)
        informe = dict(cuenta=ident['Account'], tablas=tablas, jobs=descubrir_jobs(glue, a))
        if a.muestra:
            informe['muestra'] = muestra(sesion, a)
        if not a.no_export:
            salida = Path(a.salida)
            salida.mkdir(parents=True, exist_ok=True)
            ruta = salida / f'descubrimiento_rnmc_{sc.ahora_colombia().strftime("%Y-%m-%d_%H-%M-%S")}.json'
            ruta.write_text(json.dumps(informe, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
            print(f'\n[INFO] Informe completo: {ruta}')
        return 1 if faltan else 0
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
