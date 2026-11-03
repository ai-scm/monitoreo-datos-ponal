-- =====================================================================================
-- PremierOne (MEBOG) y Hexagon (MEVAL) NO son bases de datos: el cliente entrega archivos en S3.
--   PremierOne: GUTIC sube un Excel por dia a s3://pon-prod-mebog-iapol/raw-data-premier-one/; el job Glue
--               pon-prod-premier-one-raw-to-parquet lo convierte a
--               s3://pon-prod-llamadas/data/mdp/llamadas-mebog/        -> Athena raw_mebog.llamadas_mebog
--               Para consultar la base de PremierOne (en GUTIC) ver 06_premierone_plantilla_para_gutic.sql
--   Hexagon:    s3://pon-prod-raw-data-unfiltered/data/hexagon/        -> Athena "pon-prod-hexagon".hexagon
-- Se consultan en Athena (consola AWS, region us-east-1, workgroup primary, resultados en
-- s3://aws-athena-query-results-us-east-1-926162397524/). La columna "$path" dice de que archivo
-- viene cada fila: sirve para mostrarle al proveedor el archivo exacto.
-- =====================================================================================

-- ---------- PremierOne (MEBOG) ----------
-- N1. Registros sin numero de incidente y con coordenada 0, por mes
SELECT date_format(COALESCE(TRY(date_parse(fecha_inicial, '%Y-%m-%d %H:%i:%s')),
                            TRY(date_parse(fecha_inicial, '%d/%m/%Y %H:%i:%s')),
                            TRY(date_parse(fecha_inicial, '%d/%m/%Y'))), '%Y-%m') AS mes,
       count(*) AS filas,
       count_if(NULLIF(TRIM(no_incidente), '') IS NULL) AS sin_incidente,
       count_if(latitud = 0 OR longitud = 0)            AS coord_cero
FROM   "raw_mebog"."llamadas_mebog"
GROUP  BY 1 ORDER BY 1;

-- N1. Muestra con el archivo de origen
SELECT "$path" AS archivo, fecha_inicial, no_incidente, "descripción_tipo", "dirección", latitud, longitud
FROM   "raw_mebog"."llamadas_mebog"
WHERE  NULLIF(TRIM(no_incidente), '') IS NULL AND NULLIF(TRIM(fecha_inicial), '') IS NOT NULL
LIMIT  50;

-- N2. Filas vacias (sin fecha): cuantas trae cada archivo
SELECT "$path" AS archivo, count(*) AS filas_vacias
FROM   "raw_mebog"."llamadas_mebog"
WHERE  NULLIF(TRIM(fecha_inicial), '') IS NULL
GROUP  BY 1 ORDER BY 2 DESC LIMIT 50;

-- N3. Incidentes repetidos
SELECT no_incidente, count(*) AS veces, array_agg(DISTINCT "$path") AS archivos
FROM   "raw_mebog"."llamadas_mebog"
WHERE  NULLIF(TRIM(no_incidente), '') IS NOT NULL
GROUP  BY 1 HAVING count(*) > 1 LIMIT 50;

-- ---------- Hexagon (MEVAL) ----------
-- N4. Coordenada 0
SELECT "$path" AS archivo, fecha, cod_incidente, desc_incidente, direccion, cuadrante, latitud, longitud
FROM   "pon-prod-hexagon"."hexagon"
WHERE  latitud = 0 OR longitud = 0
LIMIT  50;

-- N5. Incidentes que llegan en varios archivos (entregas solapadas)
SELECT cod_incidente, count(*) AS veces, array_agg(DISTINCT "$path") AS archivos
FROM   "pon-prod-hexagon"."hexagon"
GROUP  BY 1 HAVING count(DISTINCT "$path") > 1 LIMIT 50;

-- N6. Meses faltantes o casi vacios
SELECT date_format(COALESCE(TRY(date_parse(fecha, '%Y-%m-%d %H:%i:%s')), TRY(date_parse(fecha, '%d/%m/%Y'))), '%Y-%m') AS mes,
       count(*) AS filas
FROM   "pon-prod-hexagon"."hexagon"
GROUP  BY 1 ORDER BY 1;
