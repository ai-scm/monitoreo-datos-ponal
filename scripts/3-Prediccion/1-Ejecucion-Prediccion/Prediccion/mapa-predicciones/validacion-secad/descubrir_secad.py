#!/usr/bin/env python3
"""Paso 0 de la validacion SECAD: revisa en AWS lo que validar_secad.py da por supuesto. Solo lectura.

  1. Esquema Glue de capa_11 / capa_12 / capa_13 (columnas, tipos, particiones, ubicacion S3,
     ultima actualizacion) y si estan las columnas que usa el BFF.
  2. Esquema de la tabla cruda (por defecto pon-prod-raw-secad.cadpedidos), solo informativo.
  3. Jobs de Glue relacionados con SECAD / homogenizacion / capas: argumentos (incluido el
     `query` que define la categoria de cada capa) y ultimas ejecuciones.
  4. Opcional (--muestra): 3 filas de cada capa en Athena.

Uso:  python descubrir_secad.py [--profile X] [--muestra]
Exit: 0 todo presente, 1 faltan columnas requeridas, 3 bloqueado (credenciales/permisos), 130 interrupcion.
Los permisos que falten en los pasos 2 y 3 se informan como AVISO y no detienen el resto.
"""

import argparse
import os
import json
import re
import sys
from pathlib import Path

import secad_comun as sc
from botocore.exceptions import BotoCoreError, ClientError


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA)
    p.add_argument('--database', default=sc.DATABASE)
    p.add_argument('--raw-database', default='pon-prod-raw-secad')
    p.add_argument('--raw-tabla', default='cadpedidos')
    p.add_argument('--jobs-patron', default=r'secad|homogen|capas', help='Regex (sin distinguir mayusculas) sobre el nombre de los jobs Glue')
    p.add_argument('--runs', type=int, default=5, help='Ultimas ejecuciones por job (5)')
    p.add_argument('--muestra', action='store_true', help='Trae 3 filas de cada capa con Athena (puede escanear datos)')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'), help='Destino de resultados de Athena, s3://bucket/prefijo/ (o variable ATHENA_OUTPUT_S3); obligatorio si el workgroup no define uno')
    p.add_argument('--timeout', type=int, default=180)
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true')
    return p


def mostrar_tabla(nombre, cols, claves, t):
    sd = t.get('StorageDescriptor', {})
    print(f'\n[INFO] {nombre} ({t.get("TableType", "tipo n/d")})')
    print(f'       Ubicacion : {sd.get("Location", "n/d")}')
    print(f'       Formato   : {sd.get("InputFormat", "n/d").split(".")[-1]} | serde {sd.get("SerdeInfo", {}).get("SerializationLibrary", "n/d").split(".")[-1]}')
    print(f'       Actualizada: {t.get("UpdateTime", "n/d")} | creada {t.get("CreateTime", "n/d")}')
    proyeccion = {k: v for k, v in t.get('Parameters', {}).items() if k.startswith('projection.')}
    if proyeccion:
        print(f'       Partition projection: {len(proyeccion)} parametros')
    print(f'       Particiones: {", ".join(claves) or "(ninguna)"}')
    print('       Columna | Tipo')
    for c, tipo in cols.items():
        print(f'       {c} | {tipo}{"  [particion]" if c in claves else ""}')


def descubrir_capas(glue, a):
    resultado, faltan_total = {}, False
    for capa, (tabla, nombre) in sc.CAPAS.items():
        cols, claves, t = sc.columnas_tabla(glue, a.database, tabla)
        mostrar_tabla(f'{a.database}.{tabla}  <- {capa} ({nombre})', cols, claves, t)
        faltan = [f'{k}={n}' for k, n in sc.COLUMNAS.items() if n not in cols]
        if faltan:
            faltan_total = True
            print(f'  [FALLA] faltan columnas que usa validar_secad.py: {", ".join(faltan)} (usar --col CLAVE=COLUMNA)')
        else:
            print('  [ OK  ] estan todas las columnas requeridas por validar_secad.py')
        print(f'  [INFO] {sc.COLUMNA_LLAMADA}: {"presente (se cuentan llamadas distintas)" if sc.COLUMNA_LLAMADA in cols else "ausente (no se cuentan llamadas)"}')
        for k in ('anio', 'mes', 'dia'):
            if sc.COLUMNAS[k] in cols and sc.COLUMNAS[k] not in claves:
                print(f'  [AVISO] {sc.COLUMNAS[k]} no es clave de particion: el filtro por dia del BFF escaneara mas datos')
        resultado[capa] = dict(tabla=tabla, columnas=cols, particiones=claves, ubicacion=t.get('StorageDescriptor', {}).get('Location'),
                               actualizada=str(t.get('UpdateTime')), faltan=faltan)
    return resultado, faltan_total


def descubrir_raw(glue, a):
    try:
        cols, claves, t = sc.columnas_tabla(glue, a.raw_database, a.raw_tabla)
    except sc.Blocked as exc:
        print(f'\n[AVISO] Tabla cruda {a.raw_database}.{a.raw_tabla}: {exc}')
        return {}
    mostrar_tabla(f'{a.raw_database}.{a.raw_tabla} (cruda, solo informativo)', cols, claves, t)
    return dict(tabla=f'{a.raw_database}.{a.raw_tabla}', columnas=cols, particiones=claves,
                ubicacion=t.get('StorageDescriptor', {}).get('Location'))


def _recortar(texto, n=700):
    texto = ' '.join(str(texto).split())
    return texto if len(texto) <= n else texto[:n] + f'... (+{len(texto) - n} caracteres; completo en el JSON)'


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
            print(f'      {k} = {_recortar(args[k])}')
        corridas = []
        try:
            for r in glue.get_job_runs(JobName=j['Name'], MaxResults=a.runs).get('JobRuns', []):
                ra = r.get('Arguments', {})
                corridas.append(dict(estado=r.get('JobRunState'), inicio=str(r.get('StartedOn')), fin=str(r.get('CompletedOn')),
                                     error=r.get('ErrorMessage'), argumentos=ra))
                print(f'      run {r.get("JobRunState", "?"):<10} {r.get("StartedOn")} -> {r.get("CompletedOn")}'
                      f'{" | " + _recortar(r["ErrorMessage"], 160) if r.get("ErrorMessage") else ""}')
                for k in ('--query', '--nombre_capa'):
                    if k in ra:
                        print(f'          {k} = {_recortar(ra[k])}')
        except (ClientError, BotoCoreError) as exc:
            print(f'      [AVISO] sin historial de ejecuciones: {sc.detalle_error(exc)}')
        salida.append(dict(nombre=j['Name'], script=j.get('Command', {}).get('ScriptLocation'), argumentos=args, corridas=corridas))
    if not jobs:
        print('  (ninguno; ajusta --jobs-patron o revisa permisos)')
    return salida


def muestra(sesion, a):
    athena = sc.Athena(sesion, a.database, a.workgroup, a.output_s3, a.timeout)
    consultas = {capa: f'SELECT * FROM {sc.identifier(a.database)}.{sc.identifier(t)} LIMIT 3' for capa, (t, _) in sc.CAPAS.items()}
    print('\n[INFO] Muestra de 3 filas por capa (Athena):')
    resultados = athena.ejecutar(consultas)
    salida = {}
    for capa, (filas, _) in resultados.items():
        print(f'  {sc.CAPAS[capa][0]}:')
        for f in filas:
            print('    ' + ' | '.join(f'{k}={_recortar(v, 40)}' for k, v in f.items()))
        salida[capa] = filas
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
        capas, faltan = descubrir_capas(glue, a)
        informe = dict(cuenta=ident['Account'], database=a.database, capas=capas, raw=descubrir_raw(glue, a),
                       jobs=descubrir_jobs(glue, a))
        if a.muestra:
            informe['muestra'] = muestra(sesion, a)
        if not a.no_export:
            salida = Path(a.salida)
            salida.mkdir(parents=True, exist_ok=True)
            ruta = salida / f'descubrimiento_secad_{sc.ahora_colombia().strftime("%Y-%m-%d_%H-%M-%S")}.json'
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
