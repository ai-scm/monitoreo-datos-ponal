#!/usr/bin/env python3
"""Validacion de las capas RNMC del mapa por sigla y unidad. Solo lectura; fuente Athena + historial de Glue.

Capas del mapa y de donde sale cada una (pon-iapol-capas/capas_adicionales/app/capas_config.json):
  rnmc-rinas                          RDS public.hechos_z1_v2, articulo=27 y numeral=1
  rnmc-incautaciones                  RDS public.hechos_z1_v2, incautacion=1
  comportamientos-contrarios-...      RDS public.hechos_z1_v2, el resto
  rnmc-mediaciones-situ               Athena pon-prod-capas-iapol.capa_16_1
  rnmc-mediaciones-sala               Athena pon-prod-capas-iapol.capa_16_2
La tabla de RDS la llena a diario el job de Glue RNMC-to-RDS desde la vista de Athena
pon-prod-raw-cnp.boletin_z1_capas_v2. Este script NO consulta RDS: para esas tres capas valida
la vista (lo que debe llegar) y el historial del job (si cada dia de creacion se cargo). Las
mediaciones se validan sobre sus tablas, igual que SECAD.

Hace lo mismo que validar_secad.py (ventana contra ventana previa, rollup por unidad con sus
descendientes, reglas y CSV), con 15 dias por defecto, mas:
  - CARGA_RDS_*: dias de creacion que RNMC-to-RDS no cargo (o cargo dos veces).
  - CARGA_CAPA_*: dias que el ETL pon-prod-capas-secad-etl no cargo a capa_16_1 / capa_16_2.
  - DIA_ATIPICO: dias con un volumen nacional muy inferior a la mediana.
  - HECHOS_SIN_SIGLA: hechos sin sigla ni cuadrante, que el mapa no puede mostrar.
Para las capas de la vista, la unidad se atribuye por cuadrante2 -> catalogo (CUADRANTES_ID); el job
usa el poligono (lat/lon), asi que puede diferir en casos de borde.

Uso:
  python validar_rnmc.py --output-s3 s3://bucket/prefijo/ --profile <perfil>
  python validar_rnmc.py --solo-sql --sin-esquema --sin-catalogo     # SQL sin tocar AWS
Reutiliza el motor de ../validacion-secad (secad_comun.py y validar_secad.py).
Exit: 0 sin FALLA, 1 FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupcion.
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'validacion-secad'))  # motor compartido
import secad_comun as sc  # noqa: E402
import validar_secad as vs  # noqa: E402
from botocore.exceptions import BotoCoreError, ClientError  # noqa: E402

DB_CAPAS = 'pon-prod-capas-iapol'
DB_VISTA = 'pon-prod-raw-cnp'
VISTA = 'boletin_z1_capas_v2'
JOB_CARGA = 'RNMC-to-RDS'
JOB_ETL = 'pon-prod-capas-secad-etl'
TABLA_RDS = 'public.hechos_z1_v2'
SIN_SIGLA = '(SIN_SIGLA)'
# 'NO HA' es SUBSTRING('NO HAY CUADRANTE', 1, 5): la sigla que el ETL de las mediaciones deja a los hechos sin cuadrante
SIN_SIGLAS = {SIN_SIGLA, 'NO HA'}
CAMPOS_VISTA = ['sigla', 'cuadrante2', 'fecha', 'fecha_creacion', 'latitud', 'longitud', 'id_hechos', 'articulo', 'numeral', 'incautacion']

# Orden del menu del mapa. Los filtros reproducen capas_config.json, con las columnas de la vista
# (incautacion es numerica en la vista y texto en RDS).
CAPAS = {
    'rnmc-rinas': dict(nombre='RNMC: Riñas', corto='Rinas', origen='vista', tabla=f'{VISTA} (riñas)',
                       filtro="articulo = '27' AND numeral = '1'"),
    'rnmc-incautaciones': dict(nombre='RNMC: Incautaciones', corto='Incautaciones', origen='vista', tabla=f'{VISTA} (incautaciones)',
                               filtro='incautacion = 1'),
    'rnmc-mediaciones-situ': dict(nombre='RNMC: Mediaciones in situ', corto='Mediac. in situ', origen='capa', tabla='capa_16_1',
                                  col_id='id_hechos'),
    'rnmc-mediaciones-sala': dict(nombre='RNMC: Mediaciones en sala', corto='Mediac. sala', origen='capa', tabla='capa_16_2',
                                  col_id='id_solicitud'),
    'comportamientos-contrarios-convivencia-materializados': dict(
        nombre='Comportamientos contrarios a la convivencia', corto='Comportam.', origen='vista',
        tabla=f'{VISTA} (comportamientos)',
        filtro="NOT (COALESCE(articulo, '') = '27' AND COALESCE(numeral, '') = '1') AND COALESCE(CAST(incautacion AS varchar), '0') <> '1'"),
}
COLUMNAS_CAPA = dict(sigla='sigla', unidad='codigo_unidad', fecha='fecha', lat='latitud', lon='longitud', anio='anio', mes='mes', dia='dia')
TABLAS = {c: d['tabla'] for c, d in CAPAS.items()}
CORTOS = {c: d['corto'] for c, d in CAPAS.items()}


# --------------------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------------------
def construir_sql_vista(filtro, primer_dia, ultimo_dia, siglas=None):
    """Una capa servida desde RDS, leida en su vista de origen: sigla x cuadrante2 x dia del hecho."""
    ok = f'(latitud BETWEEN {sc.LAT_MIN} AND {sc.LAT_MAX} AND longitud BETWEEN {sc.LON_MIN} AND {sc.LON_MAX})'
    filtro_sigla = f'\n  AND sigla IN ({", ".join(sc.literal(s) for s in sorted(siglas))})' if siglas else ''
    return f"""SELECT
  coalesce(upper(sigla), '') AS sigla,
  coalesce(cuadrante2, '') AS cuadrante,
  date_format(fecha, '%Y-%m-%d') AS dia,
  COUNT(*) AS filas,
  COUNT(DISTINCT id_hechos) AS llamadas,
  COUNT(DISTINCT CASE WHEN {ok} THEN id_hechos END) AS puntos,
  SUM(CASE WHEN {ok} THEN 0 ELSE 1 END) AS coord_invalida,
  CAST(0 AS bigint) AS hora_invalida,
  CAST(0 AS bigint) AS hora_futura,
  CAST(0 AS bigint) AS desfase_particion,
  date_format(MAX(fecha), '%Y-%m-%d %H:%i:%s') AS ultima_hora_caso
FROM {sc.identifier(DB_VISTA)}.{sc.identifier(VISTA)}
WHERE fecha >= TIMESTAMP '{primer_dia} 00:00:00' AND fecha < TIMESTAMP '{ultimo_dia + timedelta(days=1)} 00:00:00'
  AND ({filtro}){filtro_sigla}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""


def construir_sql_creaciones(primer_dia, capas_vista):
    """Hechos creados por dia (lo que carga RNMC-to-RDS) y cuantos caen en cada capa."""
    cols = ',\n  '.join(f'SUM(CASE WHEN ({CAPAS[c]["filtro"]}) THEN 1 ELSE 0 END) AS c{i}' for i, c in enumerate(capas_vista))
    return f"""SELECT
  date_format(fecha_creacion, '%Y-%m-%d') AS creado,
  COUNT(*) AS total{',' if cols else ''}
  {cols}
FROM {sc.identifier(DB_VISTA)}.{sc.identifier(VISTA)}
WHERE fecha_creacion >= TIMESTAMP '{primer_dia} 00:00:00'
GROUP BY 1
ORDER BY 1
"""


def parsear_vista(filas, cuadrante_a_unidad):
    """Filas de la vista -> {sigla: {unidad: {dia: Acum}}}. La unidad sale del cuadrante via catalogo."""
    mapeadas = []
    for r in filas:
        r = dict(r)
        r['sigla'] = (r.get('sigla') or '').strip() or SIN_SIGLA
        cuadrante = (r.get('cuadrante') or '').strip()
        sin_cuadrante = not cuadrante or cuadrante.upper() == 'NO HAY CUADRANTE'
        r['codigo_unidad'] = '' if sin_cuadrante else cuadrante_a_unidad.get(cuadrante, cuadrante)
        mapeadas.append(r)
    return vs.parsear_filas(mapeadas)


# --------------------------------------------------------------------------------------
# Carga a RDS (historial del job de Glue)
# --------------------------------------------------------------------------------------
TERMINALES = sc.TERMINALES
fecha_run_utc = sc.fecha_run_utc
leer_corridas = sc.leer_corridas
dias_atipicos = vs.dias_atipicos


def fechas_cubiertas(run, tabla_rds=TABLA_RDS):
    """Dias de creacion que una corrida SUCCEEDED dejo en la tabla de produccion.
    set() si no aplica (fallo, o escribe en otra tabla como hechos_z1_v2_prueba); None si el QUERY manual
    no se puede interpretar con seguridad."""
    args = run.get('Arguments') or {}
    if run.get('JobRunState') != 'SUCCEEDED' or args.get('--DB_TABLE', tabla_rds) != tabla_rds:
        return set()
    q = args.get('--QUERY')
    if q is None or ('current_date' in q and re.search(r"date_add\(\s*'day'\s*,\s*-1", q)):
        return {fecha_run_utc(run) - timedelta(days=1)}  # consulta por defecto: el dia anterior a la corrida
    ini = re.search(r"fecha_creacion\s*>=\s*(?:timestamp|date)\s*'(\d{4}-\d{2}-\d{2})", q, re.I)
    fin = re.search(r"fecha_creacion\s*<\s*(?:timestamp|date)\s*'(\d{4}-\d{2}-\d{2})", q, re.I)
    if ini and fin and not re.search(r'\bAND\b', q[fin.end():], re.I):
        d0, d1 = date.fromisoformat(ini.group(1)), date.fromisoformat(fin.group(1))
        return {d0 + timedelta(days=i) for i in range((d1 - d0).days)}
    return None


def evaluar_carga(runs, creaciones, dias, ahora, capas_vista, tabla_rds=TABLA_RDS):
    """Una novedad por dia de creacion sin carga (FALLA) o cargado mas de una vez (AVISO).
    creaciones: {'YYYY-MM-DD': {'total': n, 'c0': n, ...}} de la vista, para dimensionar lo que falta."""
    cubren, fallos = defaultdict(list), defaultdict(list)
    en_curso, manuales = set(), 0
    for r in runs:
        args = r.get('Arguments') or {}
        estado = r.get('JobRunState')
        if estado not in TERMINALES:
            en_curso.add(fecha_run_utc(r) - timedelta(days=1))
            continue
        cub = fechas_cubiertas(r, tabla_rds)
        if cub is None:
            manuales += 1
        for d in cub or ():
            cubren[d.isoformat()].append(r)
        if estado != 'SUCCEEDED' and args.get('--DB_TABLE', tabla_rds) == tabla_rds and '--QUERY' not in args:
            fallos[(fecha_run_utc(r) - timedelta(days=1)).isoformat()].append(r)
    if not runs:
        return [], dict(revisados=0, sin_carga=0, duplicados=0, pendientes=0, manuales=0)
    primera = min(fecha_run_utc(r) for r in runs) - timedelta(days=1)
    hoy = ahora.date()
    novedades, resumen = [], dict(revisados=0, sin_carga=0, duplicados=0, pendientes=0, manuales=manuales)

    def nombres(c):
        partes = [f'{creaciones[c].get(f"c{i}", 0)} {CAPAS[k]["corto"].lower()}' for i, k in enumerate(capas_vista)] if c in creaciones else []
        total = creaciones.get(c, {}).get('total')
        return (f' Faltan en el mapa {total} hechos (' + ', '.join(partes) + ').') if total is not None else ''

    for c in dias:
        d = date.fromisoformat(c)
        if d < primera:
            continue  # el historial de Glue no llega tan atras
        n = len(cubren.get(c, []))
        if n == 0 and (d in en_curso or (d == hoy - timedelta(days=1) and ahora.hour < 11 and not fallos.get(c))):
            resumen['pendientes'] += 1
            continue
        resumen['revisados'] += 1
        if n == 0:
            resumen['sin_carga'] += 1
            f = sorted(fallos.get(c, []), key=lambda r: r['StartedOn'])
            if f:
                ult = f[-1]
                motivo = (f'{JOB_CARGA} FALLO el {ult["StartedOn"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M} '
                          f'({(ult.get("ErrorMessage") or "sin detalle")[:160]}) y no hay una corrida posterior que cargue ese dia')
                codigo, ctx = 'CARGA_RDS_FALLIDA', f'job {JOB_CARGA}, run {ult.get("Id", "?")}'
            else:
                motivo, codigo, ctx = f'no hay ninguna corrida exitosa de {JOB_CARGA} que cargue ese dia', 'CARGA_RDS_AUSENTE', f'job {JOB_CARGA}'
            novedades.append(dict(nivel='CARGA', estado=vs.FALLA, sigla='', unidad_id='', unidad='', tipo='', capa='TODAS', codigo=codigo,
                                  mensaje=f'Los hechos creados el {c} no estan en {tabla_rds}: {motivo}.{nombres(c)}',
                                  contexto=ctx, sigla_sin_datos=''))
        elif n > 1:
            resumen['duplicados'] += 1
            novedades.append(dict(nivel='CARGA', estado=vs.AVISO, sigla='', unidad_id='', unidad='', tipo='', capa='TODAS',
                                  codigo='CARGA_RDS_DUPLICADA',
                                  mensaje=f'Los hechos creados el {c} se cargaron {n} veces a {tabla_rds} (modo append): posibles filas duplicadas.{nombres(c)}',
                                  contexto=f'job {JOB_CARGA}', sigla_sin_datos=''))
    return novedades, resumen


def dias_de_query(q):
    """Dias que carga un --query del ETL: YEAR/MONTH/DAY explicitos, o una o dos fechas literales
    (el limite superior es exclusivo salvo que use BETWEEN). None si no se puede interpretar."""
    m = re.search(r"\"YEAR\"\s*=\s*'(\d{4})'\s+AND\s+\"MONTH\"\s*=\s*'(\d{1,2})'\s+AND\s+\"DAY\"\s*=\s*'(\d{1,2})'", q or '', re.I)
    if m:
        return {date(int(m.group(1)), int(m.group(2)), int(m.group(3)))}
    fechas = sorted({date.fromisoformat(x) for x in re.findall(r"'(\d{4}-\d{2}-\d{2})", q or '')})
    if len(fechas) == 1:
        return {fechas[0]}
    if len(fechas) == 2 and 0 < (fechas[1] - fechas[0]).days <= 62:
        fin = fechas[1] + timedelta(days=1) if re.search(r'\bBETWEEN\b', q, re.I) else fechas[1]
        return {fechas[0] + timedelta(days=i) for i in range((fin - fechas[0]).days)}
    return None


def evaluar_etl(runs, capa, obs_capa, dias, ahora):
    """Dias de `dias` que el ETL no cargo a la tabla de la capa. El --query dice que dia carga cada corrida
    y --nombre_capa a que tabla. 'No hay datos para procesar' sale como SystemExit y Glue lo marca FALLIDA."""
    tabla = CAPAS[capa]['tabla']
    propias = [r for r in runs if (r.get('Arguments') or {}).get('--nombre_capa') == tabla]
    cubiertos, fallos, sin_interpretar = set(), defaultdict(list), 0
    for r in propias:
        if r.get('JobRunState') not in TERMINALES:
            continue
        cargados = dias_de_query((r.get('Arguments') or {}).get('--query', ''))
        if cargados is None:
            sin_interpretar += 1
        elif r['JobRunState'] == 'SUCCEEDED':
            cubiertos |= cargados
        else:
            for d in cargados:
                fallos[d].append(r)
    resumen = dict(capa=capa, corridas=len(propias), revisados=0, sin_carga=0, sin_interpretar=sin_interpretar)
    if not propias:
        return [], resumen
    primera = min(fecha_run_utc(r) for r in propias) - timedelta(days=1)
    hoy = ahora.date()
    filas_por_dia = defaultdict(int)
    for por_codigo in obs_capa.values():
        for serie in por_codigo.values():
            for dia, a in serie.items():
                filas_por_dia[dia] += a.filas
    novedades = []
    for c in dias:
        d = date.fromisoformat(c)
        if d in cubiertos or d < primera:
            resumen['revisados'] += int(d >= primera)
            continue
        if d == hoy - timedelta(days=1) and ahora.hour < 7 and not fallos.get(d):
            continue  # el ETL corre a las 04:00; todavia puede estar en camino
        resumen['revisados'] += 1
        resumen['sin_carga'] += 1
        f = sorted(fallos.get(d, []), key=lambda r: r['StartedOn'])
        if f:
            ult = f[-1]
            error = (ult.get('ErrorMessage') or 'sin detalle').strip()
            motivo = f'la corrida del {ult["StartedOn"].astimezone(sc.UTC_MENOS_5):%Y-%m-%d %H:%M} FALLO ({error[:120]}) y nadie la repitio'
            if error.startswith('SystemExit'):
                motivo += '; SystemExit es el "No hay datos para procesar" del ETL: la consulta a la fuente devolvio 0 filas ese dia'
            codigo, ctx = 'CARGA_CAPA_FALLIDA', f'job {JOB_ETL}, run {ult.get("Id", "?")}'
        else:
            motivo, codigo, ctx = 'no hay ninguna corrida exitosa que cargue ese dia', 'CARGA_CAPA_AUSENTE', f'job {JOB_ETL}'
        novedades.append(dict(nivel='CARGA', estado=vs.FALLA, sigla='', unidad_id='', unidad='', tipo='', capa=capa, codigo=codigo,
                              mensaje=f'El ETL {JOB_ETL} no cargo el {c} a {tabla}: {motivo}. La tabla tiene {filas_por_dia.get(c, 0)} filas de ese dia.',
                              contexto=ctx, sigla_sin_datos=''))
    return novedades, resumen


def marcar_sin_sigla(filas_siglas, novedades):
    return vs.marcar_sin_sigla(filas_siglas, novedades, SIN_SIGLAS, 'hechos', 'HECHOS_SIN_SIGLA')


# --------------------------------------------------------------------------------------
# Volumen nacional por dia
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Salida adicional y programa
# --------------------------------------------------------------------------------------
RANGO_NIVEL = {'CARGA': 0, 'NACIONAL': 1, 'SIGLA': 2, 'UNIDAD': 3}


def ordenar(novedades):
    novedades.sort(key=lambda n: (-vs.ORDEN[n['estado']], RANGO_NIVEL.get(n['nivel'], 9)))
    return novedades


def imprimir_extra(extra, resumen_carga, resumen_etl=None):
    print('\n=== CARGA A RDS (job RNMC-to-RDS -> public.hechos_z1_v2) ===')
    if resumen_carga is None:
        print('  (no evaluada)')
    else:
        print(f'  Dias de creacion revisados: {resumen_carga["revisados"]} | sin carga: {resumen_carga["sin_carga"]} | '
              f'cargados mas de una vez: {resumen_carga["duplicados"]} | pendientes de hoy: {resumen_carga["pendientes"]}')
        if resumen_carga['manuales']:
            print(f'  ({resumen_carga["manuales"]} corridas con QUERY manual no interpretable: no cuentan como carga)')
    for n in (x for x in extra if x['codigo'].startswith('CARGA_RDS')):
        print(f'  [{n["estado"]}] {n["codigo"]}: {n["mensaje"]}')
    print(f'\n=== CARGA A LAS TABLAS DE MEDIACIONES (ETL {JOB_ETL}) ===')
    if not resumen_etl:
        print('  (no evaluada)')
    for r in resumen_etl or []:
        print(f'  {CORTOS[r["capa"]]}: dias revisados {r["revisados"]} | sin carga {r["sin_carga"]} | corridas del ETL para esta capa {r["corridas"]}'
              f'{" | sin interpretar " + str(r["sin_interpretar"]) if r["sin_interpretar"] else ""}')
    for n in (x for x in extra if x['codigo'].startswith('CARGA_CAPA')):
        print(f'  [{n["estado"]}] {CORTOS[n["capa"]]} | {n["codigo"]}: {n["mensaje"]}')
    atipicos = [x for x in extra if x['nivel'] == 'NACIONAL']
    print('\n=== DIAS ATIPICOS A NIVEL NACIONAL ===')
    for n in atipicos:
        print(f'  [{n["estado"]}] {CORTOS[n["capa"]]}: {n["mensaje"]}')
    if not atipicos:
        print('  (ninguno)')


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--profile', help='Perfil AWS (opcional)')
    p.add_argument('--region', default=sc.REGION)
    p.add_argument('--cuenta', default=sc.CUENTA, help='Cuenta esperada; aborta si no coincide')
    p.add_argument('--capas', nargs='+', choices=list(CAPAS), default=list(CAPAS), help='Capas a validar (todas por defecto)')
    p.add_argument('--sigla', nargs='+', help='Limita a estas siglas (ej. MEPAS MEBOG)')
    p.add_argument('--catalogo', default=sc.CATALOGO_S3, help='CSV de estructura de unidades: ruta local o s3:// (por defecto el del backend)')
    p.add_argument('--sin-catalogo', action='store_true', help='Sin catalogo: solo siglas y unidades/cuadrantes observados en los datos')
    p.add_argument('--ventana-dias', type=int, default=15, help='Dias completos a validar y de la ventana previa (15)')
    p.add_argument('--fecha-fin', help='Ultimo dia de la ventana (YYYY-MM-DD); por defecto ayer')
    p.add_argument('--min-base', type=int, default=20, help='Filas de la ventana previa para exigir datos (20)')
    p.add_argument('--caida-pct', type=float, default=50, help='Aviso de caida de puntos vs ventana previa (50)')
    p.add_argument('--racha-dias', type=int, default=2, help='Dias seguidos sin filas al cierre para avisar (2)')
    p.add_argument('--max-invalidos-pct', type=float, default=1, help='Calidad: porcentaje tolerado (1)')
    p.add_argument('--dia-atipico-pct', type=float, default=40, help='Dia nacional atipico si es menor a este %% de la mediana (40)')
    p.add_argument('--min-esperado-falla', type=float, default=5,
                   help='Filas esperadas en una racha sin datos para que una sigla sea FALLA y no AVISO (5)')
    p.add_argument('--job-carga', default=JOB_CARGA, help=f'Job de Glue que carga la tabla de RDS ({JOB_CARGA})')
    p.add_argument('--job-etl', default=JOB_ETL, help=f'Job de Glue que carga las tablas de mediaciones ({JOB_ETL})')
    p.add_argument('--sin-carga', action='store_true', help='No revisar el historial de los jobs de carga')
    p.add_argument('--workgroup', default='primary')
    p.add_argument('--output-s3', default=os.environ.get('ATHENA_OUTPUT_S3'),
                   help='Destino de resultados de Athena, s3://bucket/prefijo/ (o variable ATHENA_OUTPUT_S3); obligatorio si el workgroup no define uno')
    p.add_argument('--timeout', type=int, default=600, help='Segundos de espera de Athena (600)')
    p.add_argument('--solo-sql', action='store_true', help='Genera el SQL sin ejecutarlo')
    p.add_argument('--sin-esquema', action='store_true', help='No consulta Glue para validar columnas (con --solo-sql no usa AWS)')
    p.add_argument('--salida', default=str(Path(__file__).resolve().parent / 'salida'))
    p.add_argument('--no-export', action='store_true', help='No escribe archivos')
    p.add_argument('--max-lineas', type=int, default=60, help='Unidades a listar en consola (60)')
    p.add_argument('--detallado', action='store_true', help='Imprime tambien el SQL')
    return p


def validar_args(p, a):
    if not 1 <= a.ventana_dias <= 31:
        p.error('--ventana-dias debe estar entre 1 y 31')
    if min(a.min_base, a.racha_dias) < 1 or a.min_esperado_falla <= 0:
        p.error('--min-base, --racha-dias y --min-esperado-falla deben ser mayores que cero')
    if not all(0 <= x <= 100 for x in (a.caida_pct, a.max_invalidos_pct, a.dia_atipico_pct)):
        p.error('Los porcentajes deben estar entre 0 y 100')
    a.fecha_fin_d = None
    if a.fecha_fin:
        try:
            a.fecha_fin_d = datetime.strptime(a.fecha_fin, '%Y-%m-%d').date()
        except ValueError:
            p.error('--fecha-fin debe tener formato YYYY-MM-DD')
        if a.fecha_fin_d >= sc.ahora_colombia().date():
            p.error('--fecha-fin debe ser un dia anterior a hoy (dia completo)')
    if a.output_s3 and not a.output_s3.startswith('s3://'):
        p.error('--output-s3 debe comenzar por s3://')
    a.siglas = {s.upper() for s in a.sigla} if a.sigla else None


def ejecutar(a):
    ahora = sc.ahora_colombia()
    v = vs.calcular_ventana(ahora, a.ventana_dias, a.fecha_fin_d)
    primer_dia, ultimo_dia = date.fromisoformat(v.todos[0]), date.fromisoformat(v.todos[-1])
    marca = ahora.strftime('%Y-%m-%d_%H-%M-%S')
    umbrales = vs.Umbrales(a.min_base, a.caida_pct, a.racha_dias, 3, a.max_invalidos_pct, a.min_esperado_falla)
    capas_vista = [c for c in a.capas if CAPAS[c]['origen'] == 'vista']
    capas_tabla = [c for c in a.capas if CAPAS[c]['origen'] == 'capa']
    usa_aws = not (a.solo_sql and a.sin_esquema and (a.sin_catalogo or not a.catalogo.startswith('s3://')))
    sesion, ident = None, None
    if usa_aws:
        sesion = sc.crear_sesion(a.profile, a.region)
        ident = sc.verificar_identidad(sesion, a.cuenta)
        print(f'[INFO] Cuenta {ident["Account"]} | {ident["Arn"]} | region {a.region}')
    print(f'[INFO] Ventana {v.dias[0]} a {v.dias[-1]} | previa {v.base[0]} a {v.base[-1]}'
          f'{" | hoy parcial " + v.parcial if v.parcial else ""}')

    arbol, cuadrantes = None, {}
    if not a.sin_catalogo:
        unidades, formato = sc.cargar_catalogo(a.catalogo, sesion)
        arbol = sc.Arbol(unidades)
        cuadrantes = arbol.cuadrante_a_unidad()
        print(f'[INFO] Catalogo {a.catalogo}: {len(unidades)} filas ({formato}); {len(arbol.visibles)} unidades visibles, '
              f'{len(arbol.siglas_con_territorio())} siglas con territorio, {len(cuadrantes)} cuadrantes')
        if capas_vista and not cuadrantes:
            print('[AVISO] El catalogo no trae CUADRANTES_ID: las capas de la vista se evaluan solo por sigla y cuadrante, sin unidades.')
    else:
        print('[AVISO] Sin catalogo: no se detectan unidades ni siglas sin ningun dato; solo las observadas.')

    glue = sesion.client('glue', config=sc.BOTO_CONFIG) if (sesion and not a.sin_esquema) else None
    sqls = {}
    if glue and capas_vista:
        cols, _, _ = sc.columnas_tabla(glue, DB_VISTA, VISTA)
        faltan = [c for c in CAMPOS_VISTA if c not in cols]
        if faltan:
            raise sc.Blocked(f'La vista {DB_VISTA}.{VISTA} no tiene las columnas {", ".join(faltan)}. Disponibles: {", ".join(sorted(cols))}.')
    for capa in capas_vista:
        sqls[capa] = construir_sql_vista(CAPAS[capa]['filtro'], primer_dia, ultimo_dia, a.siglas)
    for capa in capas_tabla:
        tabla, cols = CAPAS[capa]['tabla'], None
        col_id = CAPAS[capa]['col_id']
        if glue:
            cols, _, _ = sc.columnas_tabla(glue, DB_CAPAS, tabla)
            faltan = [f'{k}={n}' for k, n in COLUMNAS_CAPA.items() if n not in cols]
            if faltan:
                raise sc.Blocked(f'{DB_CAPAS}.{tabla} no tiene las columnas {", ".join(faltan)}. Disponibles: {", ".join(sorted(cols))}.')
            col_id = col_id if col_id in cols else None
        sqls[capa] = vs.construir_sql(DB_CAPAS, tabla, COLUMNAS_CAPA, [date.fromisoformat(d) for d in v.todos], ahora, a.siglas,
                                      col_id, cols, col_punto=col_id)
    if capas_vista and not a.sin_carga:
        sqls['_creaciones'] = construir_sql_creaciones(primer_dia, capas_vista)
    if glue:
        print('[INFO] Esquema Glue verificado: ' + ', '.join(([f'{VISTA}'] if capas_vista else []) + [CAPAS[c]['tabla'] for c in capas_tabla]))

    salida = Path(a.salida)
    if not a.no_export:
        salida.mkdir(parents=True, exist_ok=True)
        (salida / f'consultas_rnmc_{marca}.sql').write_text('\n;\n'.join(f'-- {c}\n{s}' for c, s in sqls.items()), encoding='utf-8')
    if a.solo_sql or a.detallado:
        for capa, sql in sqls.items():
            print(f'\n-- {capa}\n{sql}')
    if a.solo_sql:
        print('[INFO] --solo-sql: no se ejecuto nada en Athena.')
        return 0

    corridas, corridas_etl = None, None
    if not a.sin_carga:
        glue_h = sesion.client('glue', config=sc.BOTO_CONFIG)
        for job, necesario in ((a.job_carga, capas_vista), (a.job_etl, capas_tabla)):
            if not necesario:
                continue
            try:
                lista = leer_corridas(glue_h, job, primer_dia)
                print(f'[INFO] Historial de {job}: {len(lista)} corridas' + (f' desde {lista[-1]["StartedOn"]:%Y-%m-%d}' if lista else ''))
                if job == a.job_carga:
                    corridas = lista
                else:
                    corridas_etl = lista
            except (ClientError, BotoCoreError) as exc:
                print(f'[AVISO] No se pudo leer el historial de {job} ({sc.detalle_error(exc)}); esa carga queda sin evaluar.')

    athena = sc.Athena(sesion, DB_CAPAS, a.workgroup, a.output_s3, a.timeout)
    print(f'[INFO] Ejecutando {len(sqls)} consulta(s) en Athena (workgroup {a.workgroup}); Ctrl+C las cancela.')
    resultados = athena.ejecutar(sqls)
    obs, stats, creaciones = {}, {}, {}
    for nombre, (filas, info) in resultados.items():
        escaneado = info.get('Statistics', {}).get('DataScannedInBytes', 0)
        stats[nombre] = dict(grupos=len(filas), bytes_escaneados=escaneado, query_id=info.get('QueryExecutionId'))
        if nombre == '_creaciones':
            creaciones = {f['creado']: {k: int(float(x or 0)) for k, x in f.items() if k != 'creado'} for f in filas}
            continue
        obs[nombre] = parsear_vista(filas, cuadrantes) if CAPAS[nombre]['origen'] == 'vista' else vs.parsear_filas(filas)
        print(f'[INFO] {CAPAS[nombre]["tabla"]}: {len(filas)} grupos, {escaneado / 1024 ** 2:.1f} MiB escaneados')
        if not filas:
            print(f'[AVISO] {CAPAS[nombre]["tabla"]} no devolvio filas para la ventana; revisa fechas, siglas y columnas.')

    filas_siglas, filas_unidades, novedades = vs.analizar(obs, arbol, v, umbrales, a.capas, a.siglas, TABLAS)
    extra = dias_atipicos(obs, v, a.capas, a.dia_atipico_pct / 100)
    resumen_carga, resumen_etl = None, None
    if corridas is not None:
        carga, resumen_carga = evaluar_carga(corridas, creaciones, v.dias, ahora, capas_vista)
        extra += carga
    if corridas_etl is not None:
        resumen_etl = []
        for capa in capas_tabla:
            carga, resumen = evaluar_etl(corridas_etl, capa, obs.get(capa, {}), v.dias, ahora)
            extra += carga
            resumen_etl.append(resumen)
    novedades = marcar_sin_sigla(filas_siglas, novedades)
    todas = ordenar(extra + novedades)

    vs.imprimir(filas_siglas, filas_unidades, novedades, a.capas, v, a.max_lineas, CORTOS)
    imprimir_extra(extra, resumen_carga, resumen_etl)

    if not a.no_export:
        vs.escribir_csv(salida / f'resumen_siglas_{marca}.csv', filas_siglas)
        vs.escribir_csv(salida / f'resumen_unidades_{marca}.csv', filas_unidades)
        vs.escribir_csv(salida / f'novedades_{marca}.csv', todas,
                        ['nivel', 'estado', 'sigla', 'unidad_id', 'unidad', 'tipo', 'capa', 'codigo', 'mensaje', 'contexto', 'sigla_sin_datos'])
        vs.escribir_csv(salida / f'diario_{marca}.csv', vs.filas_diario(obs, arbol, v, TABLAS))
        meta = dict(generado=ahora.isoformat(timespec='seconds'), cuenta=ident and ident['Account'], ventana=vars(v), umbrales=vars(umbrales),
                    capas={c: CAPAS[c]['tabla'] for c in a.capas}, catalogo=None if a.sin_catalogo else a.catalogo, consultas=stats,
                    carga=resumen_carga, carga_etl=resumen_etl)
        (salida / f'meta_rnmc_{marca}.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
        print(f'[INFO] Archivos en {salida} (sufijo {marca}); el CSV de novedades es novedades_{marca}.csv')
    hay_falla = any(n['estado'] == vs.FALLA for n in todas) or any(f['estado'] == vs.FALLA for f in filas_siglas + filas_unidades)
    return 1 if hay_falla else 0


def main(argv=None):
    for flujo in (sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding='utf-8', errors='replace')
        except (AttributeError, ValueError):
            pass
    p = parser()
    a = p.parse_args(argv)
    validar_args(p, a)
    try:
        return ejecutar(a)
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
