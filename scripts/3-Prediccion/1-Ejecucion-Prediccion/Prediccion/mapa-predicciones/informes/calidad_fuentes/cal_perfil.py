"""Perfil de calidad de las fuentes crudas (antes del primer ETL propio), ultimos 6 meses."""
import json, sys, concurrent.futures as cf
from aq import q

D, H = '2026-05-03', '2026-11-02'          # ventana inclusiva
DN, HN = 20260503, 20261103                # particiones CDC (un dia extra al final)
PART = ("(cast(year as integer)*10000 + cast(month as integer)*100 + cast(day as integer)) "
        f"BETWEEN {DN} AND {HN} AND year = '2026'")
VENT = lambda f: f"{f} >= TIMESTAMP '{D} 00:00:00' AND {f} < TIMESTAMP '{H} 00:00:00' + INTERVAL '1' DAY"
VACIO = lambda c: f"NULLIF(TRIM(CAST({c} AS VARCHAR)), '') IS NULL"
O = 'ó'

def fecha_multi(c):
    return (f"COALESCE(TRY(date_parse(CAST({c} AS VARCHAR), '%Y-%m-%d %H:%i:%s')), "
            f"TRY(date_parse(CAST({c} AS VARCHAR), '%d/%m/%Y %H:%i:%s')), TRY(date_parse(CAST({c} AS VARCHAR), '%d/%m/%Y')), "
            f"TRY(date_parse(CAST({c} AS VARCHAR), '%Y/%m/%d')), TRY(date_parse(CAST({c} AS VARCHAR), '%Y-%m-%d')))")

FUENTES = {}
def fuente(clave, validador, origen, db, base, extras, completa=None, nota=''):
    FUENTES[clave] = dict(validador=validador, origen=origen, db=db, base=base, extras=extras, completa=completa, nota=nota)

# ---------------- SECAD ----------------
fuente('secad_cadpedidos', 'SECAD', 'pon-prod-raw-secad.cadpedidos (DMS desde las BD Oracle SECAD regionales)', 'pon-prod-raw-secad', f"""
WITH p AS (SELECT *, row_number() OVER (PARTITION BY secad_origen, sitio_graba, nume_llamada ORDER BY "timestamp" DESC) rn
           FROM "pon-prod-raw-secad"."cadpedidos" WHERE {PART}),
cas AS (SELECT DISTINCT secad_origen, codigo FROM "pon-prod-raw-secad"."cadcasos" WHERE COALESCE(op, 'I') <> 'D'),
can AS (SELECT DISTINCT secad_origen, cadpedi_sitiograba, cadpedi_numellamada FROM "pon-prod-raw-secad"."cadpedidos_canales" WHERE {PART})
SELECT CASE WHEN p.sitio_graba IS NULL OR p.nume_llamada IS NULL THEN NULL
            ELSE p.secad_origen || '-' || CAST(p.sitio_graba AS VARCHAR) || '-' || CAST(p.nume_llamada AS VARCHAR) END id,
       p.hora_caso fecha, p.latitud_caso lat, p.longitud_caso lon, UPPER(SUBSTR(p.secad_origen, 11)) grp,
       p.codi_pedido IS NULL e_tipo_nulo,
       (p.codi_pedido IS NOT NULL AND cas.codigo IS NULL) e_tipo_sin_catalogo,
       can.cadpedi_numellamada IS NULL e_sin_canal,
       {VACIO('p.dire_caso')} e_sin_direccion
FROM p LEFT JOIN cas ON cas.secad_origen = p.secad_origen AND cas.codigo = p.codi_pedido
       LEFT JOIN can ON can.secad_origen = p.secad_origen AND can.cadpedi_sitiograba = p.sitio_graba AND can.cadpedi_numellamada = p.nume_llamada
WHERE p.rn = 1 AND COALESCE(p.op, 'I') <> 'D' AND (p.hora_caso IS NULL OR {VENT('p.hora_caso')})""",
    ['e_tipo_nulo', 'e_tipo_sin_catalogo', 'e_sin_canal', 'e_sin_direccion'],
    nota='Ventana por particion de llegada CDC; una fila por llamada (ultima version, sin borrados).')

FM = fecha_multi('fecha_inicial')
fuente('mebog_premierone', 'SECAD', 'raw_mebog.llamadas_mebog (archivos PremierOne de Bogota)', 'raw_mebog', f"""
SELECT no_incidente id, {FM} fecha, latitud lat, longitud lon, 'MEBOG' grp,
       {VACIO(f'"descripci{O}n_tipo"')} e_tipo_nulo, {VACIO(f'"direcci{O}n"')} e_sin_direccion,
       {VACIO('cai')} e_sin_cai
FROM "raw_mebog"."llamadas_mebog" WHERE {VENT(FM)}""",
    ['e_tipo_nulo', 'e_sin_direccion', 'e_sin_cai'],
    completa=f'SELECT count(*) n, count_if({FM} IS NULL) f_ilegible FROM "raw_mebog"."llamadas_mebog"')

FH = fecha_multi('fecha')
fuente('meval_hexagon', 'SECAD', 'pon-prod-hexagon.hexagon (archivos Hexagon de Cali)', 'pon-prod-hexagon', f"""
SELECT cod_incidente id, {FH} fecha, latitud lat, longitud lon, 'MEVAL' grp,
       {VACIO('desc_incidente')} e_tipo_nulo, {VACIO('direccion')} e_sin_direccion, {VACIO('cuadrante')} e_sin_cuadrante
FROM "pon-prod-hexagon"."hexagon" WHERE {VENT(FH)}""",
    ['e_tipo_nulo', 'e_sin_direccion', 'e_sin_cuadrante'],
    completa=f'SELECT count(*) n, count_if({FH} IS NULL) f_ilegible FROM "pon-prod-hexagon"."hexagon"')

# ---------------- RNMC ----------------
SIG_CUAD = ("CASE WHEN regexp_like(SUBSTR(h.cuadrante2, 1, 5), '^[A-Z]{5}$') AND SUBSTR(h.cuadrante2, 1, 5) <> 'NO HA' "
            "THEN SUBSTR(h.cuadrante2, 1, 5) ELSE '(SIN SIGLA)' END")
fuente('rnmc_hechos', 'RNMC', 'pon-prod-raw-cnp.raw_dboper_usr_cnp_cnp_hechos (copia JDBC de la BD RNMC/CNP)', 'pon-prod-raw-cnp', f"""
WITH inf AS (SELECT DISTINCT id_hechos FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_personas" WHERE id_tipo_persona = 350)
SELECT CAST(h.id_hechos AS VARCHAR) id, h.fecha, h.latitud lat, h.longitud lon, {SIG_CUAD} grp,
       ({VACIO('h.cuadrante2')} OR h.cuadrante2 LIKE 'NO HA%') e_sin_cuadrante,
       (h.cnp_art IS NULL OR h.cnp_art = 0) e_sin_comportamiento,
       inf.id_hechos IS NULL e_sin_infractor,
       COALESCE(h.vigente, 0) <> 1 e_no_vigente
FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_hechos" h LEFT JOIN inf ON inf.id_hechos = h.id_hechos
WHERE {VENT('h.fecha')}""",
    ['e_sin_cuadrante', 'e_sin_comportamiento', 'e_sin_infractor', 'e_no_vigente'],
    completa='SELECT count(*) n, count_if(fecha IS NULL) f_ilegible FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_hechos"',
    nota='Alimenta Rinas/Incautaciones/Comportamientos (vista boletin_z1_capas_v2 -> RDS) y Mediaciones in situ (capa_16_1).')

fuente('rnmc_mediaciones_sala', 'RNMC', 'pon-prod-raw-cnp.raw_dboper_usr_cnp_cnp_solici_media', 'pon-prod-raw-cnp', f"""
SELECT CAST(id_solicitud AS VARCHAR) id, fecha_solicitud fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla_fisica), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla_fisica')} e_sin_sigla, {VACIO('cuadrante')} e_sin_cuadrante, COALESCE(vigente, 0) <> 1 e_no_vigente
FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_solici_media" WHERE {VENT('fecha_solicitud')}""",
    ['e_sin_sigla', 'e_sin_cuadrante', 'e_no_vigente'],
    completa='SELECT count(*) n, count_if(fecha_solicitud IS NULL) f_ilegible FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_solici_media"')

# ---------------- Antecedentes (SIVICC) ----------------
for tipo, idc, fc, doc in [('persona', 'id_auditoria_persona', 'fecha', 'identificacion'),
                           ('vehiculo', 'id_vehiculo', 'fecha_creacion', 'placa'),
                           ('imei', 'id_imei', 'fecha_creacion', 'imei')]:
    unidad = 'undelaborando'
    fuente(f'antecedentes_{tipo}', 'GESCOV-ANTECEDENTES', f'pon-prod-raw-sivicc.raw_sivicc_usr_mnvcc_auditoria_cons_{tipo} (copia JDBC de SIVICC/MNVCC)',
           'pon-prod-raw-sivicc', f"""
SELECT CAST({idc} AS VARCHAR) id, {fc} fecha, latitud lat, longitud lon, 'TOTAL' grp,
       ({unidad} IS NULL OR {unidad} = 0) e_sin_unidad, id_cuadrante IS NULL e_sin_cuadrante,
       {VACIO(doc)} e_sin_{doc}, tiene_antecedentes IS NULL e_resultado_nulo
FROM "pon-prod-raw-sivicc"."raw_sivicc_usr_mnvcc_auditoria_cons_{tipo}" WHERE {VENT(fc)}""",
           ['e_sin_unidad', 'e_sin_cuadrante', f'e_sin_{doc}', 'e_resultado_nulo'],
           completa=f'SELECT count(*) n, count_if({fc} IS NULL) f_ilegible FROM "pon-prod-raw-sivicc"."raw_sivicc_usr_mnvcc_auditoria_cons_{tipo}"')

# ---------------- GESCOV / Fuentes internas ----------------
fuente('gescov_evos_planes', 'GESCOV / FUENTES INTERNAS', 'pon-prod-raw-disec."usr_disec.v_evos_planes" (vista materializada Oracle DISEC)', 'pon-prod-raw-disec', f"""
SELECT plan_id || '-' || puntos_id id, fecha_inicio_plan fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla_unidad), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla_unidad')} e_sin_sigla, id_unidad IS NULL e_sin_unidad, {VACIO('estado_cumplimiento')} e_cumplimiento_nulo,
       ({VACIO('plan_id')} OR {VACIO('puntos_id')}) e_id_parcial
FROM "pon-prod-raw-disec"."usr_disec.v_evos_planes" WHERE {VENT('fecha_inicio_plan')}""",
    ['e_sin_sigla', 'e_sin_unidad', 'e_cumplimiento_nulo', 'e_id_parcial'],
    completa='SELECT count(*) n, count_if(fecha_inicio_plan IS NULL) f_ilegible FROM "pon-prod-raw-disec"."usr_disec.v_evos_planes"')

fuente('mtrs', 'FUENTES INTERNAS', 'pon-prod-raw-disec."usr_disec.v_mtrs_riesgo_ubicacion"', 'pon-prod-raw-disec', f"""
SELECT riesgo_id id, fecha_inicio_mesa_tecnica fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla')} e_sin_sigla, {VACIO('delito')} e_sin_delito, {VACIO('cuadrante')} e_sin_cuadrante
FROM "pon-prod-raw-disec"."usr_disec.v_mtrs_riesgo_ubicacion" WHERE {VENT('fecha_inicio_mesa_tecnica')}""",
    ['e_sin_sigla', 'e_sin_delito', 'e_sin_cuadrante'],
    completa='SELECT count(*) n, count_if(fecha_inicio_mesa_tecnica IS NULL OR fecha_inicio_mesa_tecnica < TIMESTAMP \'2000-01-01 00:00:00\' OR fecha_inicio_mesa_tecnica > current_timestamp) f_ilegible FROM "pon-prod-raw-disec"."usr_disec.v_mtrs_riesgo_ubicacion"')

fuente('iris_p1', 'FUENTES INTERNAS', 'pon-prod-raw-disec."usr_disec.v_general_irisp"', 'pon-prod-raw-disec', f"""
SELECT criminalidad_id id, fecha_creacion_irisp1 fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla_unidad), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla_unidad')} e_sin_sigla, {VACIO('nro_cuadrante')} e_sin_cuadrante, {VACIO('delito_principal')} e_sin_delito
FROM "pon-prod-raw-disec"."usr_disec.v_general_irisp" WHERE {VENT('fecha_creacion_irisp1')}""",
    ['e_sin_sigla', 'e_sin_cuadrante', 'e_sin_delito'],
    completa='SELECT count(*) n, count_if(fecha_creacion_irisp1 IS NULL) f_ilegible FROM "pon-prod-raw-disec"."usr_disec.v_general_irisp"')

fuente('simep', 'FUENTES INTERNAS', 'pon-prod-raw-simep-s3.usr_simep_v_simep_autos_arcgis (base de la vista simep_datac)', 'pon-prod-raw-simep-s3', f"""
SELECT auto_id id, fecha_expedicion fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla_fisica), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla_fisica')} e_sin_sigla, id_unidad IS NULL e_sin_unidad, {VACIO('tipo_conducta')} e_sin_conducta
FROM "pon-prod-raw-simep-s3"."usr_simep_v_simep_autos_arcgis" WHERE {VENT('fecha_expedicion')}""",
    ['e_sin_sigla', 'e_sin_unidad', 'e_sin_conducta'],
    completa='SELECT count(*) n, count_if(fecha_expedicion IS NULL OR fecha_expedicion < TIMESTAMP \'2000-01-01 00:00:00\' OR fecha_expedicion > current_timestamp) f_ilegible FROM "pon-prod-raw-simep-s3"."usr_simep_v_simep_autos_arcgis"')

FS = fecha_multi('fecha_hecho')
fuente('siedco', 'FUENTES INTERNAS', 'pon_prod_raw_siedco_delitos_temporal.raw_data_siedco_full_delitos (extraccion SIEDCO)', 'pon_prod_raw_siedco_delitos_temporal', f"""
SELECT CAST(hechos_id AS VARCHAR) id, {FS} fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(sigla_undepertenece), ''), '(SIN SIGLA)') grp,
       {VACIO('sigla_undepertenece')} e_sin_sigla, {VACIO('tematica')} e_sin_tematica, hora_24 IS NULL e_sin_hora,
       {VACIO('perte_dependencia')} e_sin_cuadrante
FROM "pon_prod_raw_siedco_delitos_temporal"."raw_data_siedco_full_delitos" WHERE {VENT(FS)}""",
    ['e_sin_sigla', 'e_sin_tematica', 'e_sin_hora', 'e_sin_cuadrante'],
    completa=f'SELECT count(*) n, count_if({FS} IS NULL) f_ilegible FROM "pon_prod_raw_siedco_delitos_temporal"."raw_data_siedco_full_delitos"',
    nota='Un hecho trae varias filas (una por persona/bien): las filas repetidas por hechos_id no son error.')

fuente('sirap', 'FUENTES INTERNAS', 'pon-prod-raw-geopol.raw_geopol_usr_pmu_actividades (copia JDBC de GEOPOL/PMU)', 'pon-prod-raw-geopol', f"""
SELECT CAST(id_actividad AS VARCHAR) id, fecha_inicio fecha, latitud lat, longitud lon, COALESCE(NULLIF(TRIM(unidad), ''), '(SIN UNIDAD)') grp,
       {VACIO('unidad')} e_sin_unidad, cuadrante_id IS NULL e_sin_cuadrante, {VACIO('descripcion')} e_sin_descripcion
FROM "pon-prod-raw-geopol"."raw_geopol_usr_pmu_actividades" WHERE {VENT('fecha_inicio')}""",
    ['e_sin_unidad', 'e_sin_cuadrante', 'e_sin_descripcion'],
    completa='SELECT count(*) n, count_if(fecha_inicio IS NULL) f_ilegible FROM "pon-prod-raw-geopol"."raw_geopol_usr_pmu_actividades"')


def perfil_sql(f):
    ext = ',\n  '.join(f'count_if({e}) {e}' for e in f['extras'])
    return f"""
WITH b AS ({f['base']}),
c AS (SELECT b.*, TRY_CAST(REPLACE(TRIM(CAST(lat AS VARCHAR)), ',', '.') AS DOUBLE) la,
                  TRY_CAST(REPLACE(TRIM(CAST(lon AS VARCHAR)), ',', '.') AS DOUBLE) lo FROM b),
d AS (SELECT c.*, CASE
   WHEN NULLIF(TRIM(CAST(lat AS VARCHAR)), '') IS NULL OR NULLIF(TRIM(CAST(lon AS VARCHAR)), '') IS NULL THEN 'nula'
   WHEN la IS NULL OR lo IS NULL OR is_nan(la) OR is_nan(lo) THEN 'no_numerica'
   WHEN la = 0 OR lo = 0 THEN 'cero'
   WHEN la BETWEEN -4.3 AND 13.6 AND lo BETWEEN -82.0 AND -66.0 THEN 'ok'
   WHEN lo BETWEEN -4.3 AND 13.6 AND la BETWEEN -82.0 AND -66.0 THEN 'invertida'
   ELSE 'fuera' END cc FROM c)
SELECT grp, count(*) n, count_if(id IS NULL OR TRIM(id) = '') id_nulo, count(DISTINCT id) ids,
  count_if(fecha IS NULL) f_nula, count_if(fecha > current_timestamp) f_futura,
  count_if(cc = 'nula') c_nula, count_if(cc = 'no_numerica') c_no_numerica, count_if(cc = 'cero') c_cero,
  count_if(cc = 'invertida') c_invertida, count_if(cc = 'fuera') c_fuera, count_if(cc = 'ok') c_ok,
  count_if(cc <> 'ok' OR id IS NULL OR TRIM(id) = '' OR fecha IS NULL) critico,
  {ext}
FROM d GROUP BY grp ORDER BY n DESC"""


def correr(clave):
    f = FUENTES[clave]
    sql = perfil_sql(f)
    try:
        filas = q(sql, f['db'], quiet=True)
        res = {'cols': filas[0], 'filas': filas[1:]}
        if f['completa']:
            c = q(f['completa'], f['db'], quiet=True)
            res['completa'] = dict(zip(c[0], c[1]))
    except Exception as e:
        res = {'error': str(e)}
    res['sql'] = sql
    print('listo', clave, res.get('error', ''), file=sys.stderr, flush=True)
    return clave, res


if __name__ == '__main__':
    claves = sys.argv[1:] or list(FUENTES)
    if claves == ['--sql']:
        for k, f in FUENTES.items(): print('--', k, perfil_sql(f))
        sys.exit()
    try:
        out = json.load(open('cal_perfil.json', encoding='utf-8'))
    except Exception:
        out = {}
    with cf.ThreadPoolExecutor(5) as ex:
        for k, r in ex.map(correr, claves):
            out[k] = {**{x: FUENTES[k][x] for x in ('validador', 'origen', 'nota', 'extras')}, **r}
            json.dump(out, open('cal_perfil.json', 'w', encoding='utf-8'), indent=1, ensure_ascii=False)
