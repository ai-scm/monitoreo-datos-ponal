#!/usr/bin/env python3
"""Paso 0 de la validacion de fuentes internas: revisa en AWS lo que validar_fuentes_internas.py da por supuesto. Solo lectura.

  1. Que cada capa siga registrada en DynamoDB (pon_prod_external_layers) con la clave de S3 que usa el script.
  2. Que cada archivo exista, su tamano, su fecha de modificacion y que su primer registro traiga las propiedades
     (fecha, sigla, unidad) que el script lee.
  3. Que cada vista de origen exista en Glue con las columnas que usa el job (coordenadas, fecha, fecha_actualiza_vm).
  4. El job diario pon-prod-xcrime-update-fuentes-internas (disparador, argumentos y ultimas corridas) y el semanal.
Uso:  python descubrir_fuentes_internas.py [--profile X]
Exit: 0 todo presente, 1 falta algo requerido, 3 bloqueado (credenciales/permisos), 130 interrupcion.
"""

import argparse
import json
import re
import sys
from pathlib import Path

import validar_fuentes_internas as vf
import secad_comun as sc
from botocore.exceptions import BotoCoreError, ClientError
from descubrir_secad import _recortar

TABLA_CAPAS = 'pon_prod_external_layers'
NOMBRE_EN_DYNAMO = {'gescov': 'Gescov', 'mtrs': 'MTRS', 'siedco-i': 'SIEDCO I', 'siedco-ii': 'SIEDCO II', 'siedco-iii': 'SIEDCO III',
                    'simep': 'SIMEP', 'sirap': 'SIRAP', 'iris-p1': 'IRIS P1'}
VISTA_SIEDCO = ('pon_prod_raw_siedco_delitos_temporal', 'vw_delitos_corregido_schema_temporal')


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA)
    p.add_argument('--bucket', default=vf.BUCKET)
    p.add_argument('--runs', type=int, default=6, help='Ultimas corridas por job (6)')
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true')
    return p


def revisar_registro(sesion, informe):
    falta = False
    try:
        ddb = sesion.client('dynamodb', config=sc.BOTO_CONFIG)
        items = [i for pag in ddb.get_paginator('scan').paginate(TableName=TABLA_CAPAS) for i in pag['Items']]
    except (ClientError, BotoCoreError) as exc:
        print(f'\n[AVISO] No se pudo leer {TABLA_CAPAS} ({sc.detalle_error(exc)}); falta dynamodb:Scan.')
        return False
    por_nombre = {i.get('name', {}).get('S', ''): i for i in items}
    print(f'\n[INFO] {TABLA_CAPAS}: {len(items)} capas registradas')
    informe['registro'] = {}
    for capa, nombre in NOMBRE_EN_DYNAMO.items():
        it = por_nombre.get(nombre)
        if not it:
            print(f'  [FALLA] "{nombre}" no esta registrada en {TABLA_CAPAS}: el mapa no la lista')
            falta = True
            continue
        clave = (it.get('key', {}).get('S') or '').strip()
        coincide = clave == vf.CAPAS[capa]['key']
        print(f'  [{" OK  " if coincide else "FALLA"}] {nombre:<11} id={it["id"]["S"][:8]} categoria={it.get("category", {}).get("S", "")[:8]} '
              f'turno={it.get("shift", {}).get("N", "-")} clave={"igual a la del script" if coincide else clave}')
        falta = falta or not coincide
        informe['registro'][capa] = dict(id=it['id']['S'], clave=clave, coincide=coincide)
    return falta


def revisar_archivos(sesion, a, informe):
    falta = False
    s3 = sesion.client('s3', config=sc.BOTO_CONFIG)
    print('\n[INFO] Archivos GeoJSON:')
    informe['archivos'] = {}
    for capa, cfg in vf.CAPAS.items():
        try:
            h = s3.head_object(Bucket=a.bucket, Key=cfg['key'])
            cabeza = s3.get_object(Bucket=a.bucket, Key=cfg['key'], Range='bytes=0-80000')['Body'].read()
        except (ClientError, BotoCoreError) as exc:
            print(f'  [FALLA] {cfg["nombre"]}: {sc.detalle_error(exc)}')
            falta = True
            continue
        props = {}
        for linea in cabeza.split(b'\n'):
            if linea.startswith(vf.INICIO_FEATURE):
                try:
                    props = json.loads(linea.rstrip().rstrip(b',')).get('properties') or {}
                except ValueError:
                    pass
                break
        requeridas = [x for x in (cfg['fecha'], cfg['sigla'], cfg['unidad'][1], cfg['hora']) if x]
        faltan = [x for x in requeridas if x not in props]
        falta = falta or bool(faltan)
        print(f'  [{" OK  " if not faltan else "FALLA"}] {cfg["nombre"]:<11} {h["ContentLength"] / 1024 ** 2:>7.1f} MiB | modificado '
              f'{h["LastModified"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M} | {len(props)} propiedades'
              + (f' | FALTAN: {", ".join(faltan)}' if faltan else ''))
        informe['archivos'][capa] = dict(bytes=h['ContentLength'], modificado=str(h['LastModified']), propiedades=sorted(props), faltan=faltan)
    return falta


def revisar_vistas(sesion, informe):
    falta = False
    glue = sesion.client('glue', config=sc.BOTO_CONFIG)
    print('\n[INFO] Vistas de origen:')
    informe['vistas'] = {}
    vistas = {}
    for capa, cfg in vf.CAPAS.items():
        if 'turno' in cfg:
            vistas[VISTA_SIEDCO] = dict(capas=['siedco-i', 'siedco-ii', 'siedco-iii'], cols=['fecha_hecho', 'latitud', 'longitud', 'hora_24', 'tematica'])
        elif 'vista' in cfg:
            db, tabla = [x.strip('"') for x in cfg['vista'].split('"."')]
            ent = vistas.setdefault((db, tabla), dict(capas=[], cols=['latitud', 'longitud']))
            ent['capas'].append(capa)
            ent['cols'] += [cfg['col_fecha']] + (['fecha_actualiza_vm'] if cfg.get('vm') else [])
    for (db, tabla), ent in vistas.items():
        try:
            cols, _, t = sc.columnas_tabla(glue, db, tabla)
        except sc.Blocked as exc:
            print(f'  [FALLA] {db}.{tabla}: {exc}')
            falta = True
            continue
        faltan = [c for c in ent['cols'] if c not in cols]
        falta = falta or bool(faltan)
        print(f'  [{" OK  " if not faltan else "FALLA"}] {db}.{tabla} ({t.get("TableType")}) <- {", ".join(vf.CAPAS[c]["nombre"] for c in ent["capas"])}'
              + (f' | FALTAN: {", ".join(faltan)}' if faltan else ''))
        informe['vistas'][f'{db}.{tabla}'] = dict(capas=ent['capas'], faltan=faltan, actualizada=str(t.get('UpdateTime')))
    return falta


def revisar_jobs(sesion, a, informe):
    glue = sesion.client('glue', config=sc.BOTO_CONFIG)
    informe['jobs'] = {}
    for job in (vf.JOB_DIARIO, vf.JOB_SEMANAL):
        try:
            j = glue.get_job(JobName=job)['Job']
            print(f'\n[INFO] Job {job} | script {j["Command"]["ScriptLocation"]} | modificado {j.get("LastModifiedOn"):%Y-%m-%d}')
            for t in [t for p in glue.get_paginator('get_triggers').paginate() for t in p['Triggers']
                      if any(x.get('JobName') == job for x in t.get('Actions', []))]:
                print(f'      disparador {t["Name"]}: {t.get("Schedule")} ({t.get("State")}) [UTC]')
            corridas = []
            for r in glue.get_job_runs(JobName=job, MaxResults=a.runs).get('JobRuns', []):
                corridas.append(dict(estado=r['JobRunState'], inicio=str(r['StartedOn'].astimezone(sc.UTC_MENOS_5)), error=r.get('ErrorMessage')))
                print(f'      run {r["JobRunState"]:<10} {r["StartedOn"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M}'
                      f'{" | " + _recortar(r["ErrorMessage"], 140) if r.get("ErrorMessage") else ""}')
            informe['jobs'][job] = corridas
        except (ClientError, BotoCoreError) as exc:
            print(f'\n[AVISO] No se pudo leer el job {job} ({sc.detalle_error(exc)}); falta glue:GetJob/GetJobRuns/GetTriggers.')


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
        informe = dict(cuenta=ident['Account'], bucket=a.bucket)
        falta = revisar_registro(sesion, informe)
        falta = revisar_archivos(sesion, a, informe) or falta
        falta = revisar_vistas(sesion, informe) or falta
        revisar_jobs(sesion, a, informe)
        if not a.no_export:
            salida = Path(a.salida)
            salida.mkdir(parents=True, exist_ok=True)
            ruta = salida / f'descubrimiento_fuentes_internas_{sc.ahora_colombia().strftime("%Y-%m-%d_%H-%M-%S")}.json'
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
