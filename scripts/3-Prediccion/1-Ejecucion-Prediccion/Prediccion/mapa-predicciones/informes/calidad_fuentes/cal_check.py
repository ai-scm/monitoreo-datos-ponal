import sys, json
from aq import q, show
from cal_perfil import PART, VENT
sel = sys.argv[1:]
out = {}
if 'secad' in sel:
    out['secad'] = q(f"""
WITH p AS (SELECT *, row_number() OVER (PARTITION BY secad_origen, sitio_graba, nume_llamada ORDER BY "timestamp" DESC) rn
           FROM "pon-prod-raw-secad"."cadpedidos" WHERE {PART}),
can AS (SELECT DISTINCT secad_origen, cadpedi_sitiograba, cadpedi_numellamada FROM "pon-prod-raw-secad"."cadpedidos_canales" WHERE {PART}),
x AS (SELECT UPPER(SUBSTR(p.secad_origen, 11)) sigla, can.cadpedi_numellamada IS NOT NULL con_canal, p.tipo_pedido,
        CASE WHEN NULLIF(TRIM(p.latitud_caso), '') IS NULL OR NULLIF(TRIM(p.longitud_caso), '') IS NULL THEN 'nula'
             WHEN TRY_CAST(p.latitud_caso AS DOUBLE) = 0 OR TRY_CAST(p.longitud_caso AS DOUBLE) = 0 THEN 'cero' ELSE 'con' END cc
      FROM p LEFT JOIN can ON can.secad_origen = p.secad_origen AND can.cadpedi_sitiograba = p.sitio_graba AND can.cadpedi_numellamada = p.nume_llamada
      WHERE p.rn = 1 AND COALESCE(p.op, 'I') <> 'D' AND (p.hora_caso IS NULL OR {VENT('p.hora_caso')}))
SELECT sigla, con_canal, tipo_pedido, cc, count(*) n FROM x GROUP BY 1, 2, 3, 4""", 'pon-prod-raw-secad')
if 'gescov' in sel:
    out['gescov'] = q(f"""SELECT date_format(fecha_inicio_plan, '%Y-%m') m, count(*) n, count_if(NULLIF(TRIM(estado_cumplimiento), '') IS NULL) nulos,
        max(fecha_actualiza_vm) vm FROM "pon-prod-raw-disec"."usr_disec.v_evos_planes" WHERE {VENT('fecha_inicio_plan')} GROUP BY 1 ORDER BY 1""", 'pon-prod-raw-disec')
if 'rnmc' in sel:
    out['rnmc'] = q(f"""WITH h AS (SELECT * FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_hechos" WHERE {VENT('fecha')}),
      d AS (SELECT id_hechos, count(*) c, count(DISTINCT fecha_modifica) fm, count(DISTINCT CAST(latitud AS VARCHAR) || CAST(cuadrante2 AS VARCHAR) || CAST(id_estado AS VARCHAR)) v
            FROM h GROUP BY 1 HAVING count(*) > 1)
      SELECT count(*) ids_dup, sum(c) filas, count_if(fm = 1 AND v = 1) identicas, count_if(fm > 1) distinta_modif FROM d""", 'pon-prod-raw-cnp')
if 'pers' in sel:
    out['pers'] = q(f"""WITH d AS (SELECT id_auditoria_persona, count(*) c, count(DISTINCT CAST(fecha AS VARCHAR) || COALESCE(identificacion, '') || COALESCE(latitud, '')) v
            FROM "pon-prod-raw-sivicc"."raw_sivicc_usr_mnvcc_auditoria_cons_persona" WHERE {VENT('fecha')} GROUP BY 1 HAVING count(*) > 1)
      SELECT count(*) ids_dup, sum(c) filas, count_if(v = 1) identicas, min(id_auditoria_persona), max(id_auditoria_persona) FROM d""", 'pon-prod-raw-sivicc')
if 'hex' in sel:
    out['hex'] = q(f"""WITH h AS (SELECT *, "$path" p FROM "pon-prod-hexagon"."hexagon"),
      d AS (SELECT cod_incidente, count(*) c, count(DISTINCT p) archivos FROM h GROUP BY 1 HAVING count(*) > 1)
      SELECT count(*) ids_dup, sum(c) filas, count_if(archivos > 1) en_varios_archivos FROM d""", 'pon-prod-hexagon')
if 'mebog' in sel:
    out['mebog'] = q("""SELECT date_format(COALESCE(TRY(date_parse(fecha_inicial, '%Y-%m-%d %H:%i:%s')), TRY(date_parse(fecha_inicial, '%d/%m/%Y %H:%i:%s')), TRY(date_parse(fecha_inicial, '%d/%m/%Y'))), '%Y-%m') m,
        count(*) n, count_if(NULLIF(TRIM(no_incidente), '') IS NULL) sin_id, count_if(latitud = 0 OR longitud = 0) cero
        FROM "raw_mebog"."llamadas_mebog" GROUP BY 1 ORDER BY 1""", 'raw_mebog')
    out['mebog_ileg'] = q("""SELECT fecha_inicial, count(*) n FROM "raw_mebog"."llamadas_mebog"
        WHERE COALESCE(TRY(date_parse(fecha_inicial, '%Y-%m-%d %H:%i:%s')), TRY(date_parse(fecha_inicial, '%d/%m/%Y %H:%i:%s')), TRY(date_parse(fecha_inicial, '%d/%m/%Y')), TRY(date_parse(fecha_inicial, '%Y/%m/%d')), TRY(date_parse(fecha_inicial, '%Y-%m-%d'))) IS NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""", 'raw_mebog')
for k, v in out.items():
    json.dump(v, open(f'cal_check_{k}.json', 'w', encoding='utf-8'), ensure_ascii=False)
    if k != 'secad':
        print('==', k); show(v)
