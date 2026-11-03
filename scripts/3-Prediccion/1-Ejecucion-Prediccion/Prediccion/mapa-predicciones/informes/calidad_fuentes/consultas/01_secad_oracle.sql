-- =====================================================================================
-- SECAD (una base Oracle por SECAD regional). Esquema USR_CAD.
-- Conexion (ejemplo MEBAR): jdbc:oracle:thin:@//MEBAR.secad.prod.mdp.local:1521/secad123
-- Usuario que usa DMS: USR_MOTPOL (clave en Secrets Manager; pedir usuario de solo lectura al DBA).
-- Cambiar MEBAR por la sigla a revisar (ver la tabla de hosts en LEEME.md).
-- Ventana: 2026-05-03 a 2026-11-02.
-- =====================================================================================
ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';

-- N1. Llamadas ATENDIDAS (tienen canal) sin coordenadas: resumen
SELECT COUNT(*)                                                                          AS atendidas,
       SUM(CASE WHEN TRIM(p.LATITUD_CASO) IS NULL OR TRIM(p.LONGITUD_CASO) IS NULL THEN 1 ELSE 0 END) AS coord_nula,
       SUM(CASE WHEN REGEXP_LIKE(TRIM(p.LATITUD_CASO),  '^-?0+([.,]0+)?$')
                  OR REGEXP_LIKE(TRIM(p.LONGITUD_CASO), '^-?0+([.,]0+)?$') THEN 1 ELSE 0 END)  AS coord_cero
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-05-03' AND p.HORA_CASO < DATE '2026-11-03'
AND    EXISTS (SELECT 1 FROM USR_CAD.CADPEDIDOS_CANALES c
               WHERE c.CADPEDI_SITIOGRABA = p.SITIO_GRABA AND c.CADPEDI_NUMELLAMADA = p.NUME_LLAMADA);

-- N1. Muestra de esas llamadas (ver que tampoco hay coordenada en otra columna)
SELECT p.SITIO_GRABA, p.NUME_LLAMADA, p.HORA_CASO, p.CODI_PEDIDO, p.DIRE_CASO, p.LUGE_CODIGO, p.CODI_BARRIO,
       p.LATITUD_CASO, p.LONGITUD_CASO, p.CADUSUA_USUARIO
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-10-01' AND p.HORA_CASO < DATE '2026-11-03'
AND    (TRIM(p.LATITUD_CASO) IS NULL OR REGEXP_LIKE(TRIM(p.LATITUD_CASO), '^-?0+([.,]0+)?$'))
AND    EXISTS (SELECT 1 FROM USR_CAD.CADPEDIDOS_CANALES c
               WHERE c.CADPEDI_SITIOGRABA = p.SITIO_GRABA AND c.CADPEDI_NUMELLAMADA = p.NUME_LLAMADA)
ORDER  BY p.HORA_CASO DESC
FETCH FIRST 50 ROWS ONLY;

-- N1. Por mes y por operador (quien registra la llamada sin ubicacion)
SELECT TO_CHAR(p.HORA_CASO, 'YYYY-MM') AS mes, p.CADUSUA_USUARIO, COUNT(*) AS sin_coordenadas
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-05-03' AND p.HORA_CASO < DATE '2026-11-03'
AND    (TRIM(p.LATITUD_CASO) IS NULL OR REGEXP_LIKE(TRIM(p.LATITUD_CASO), '^-?0+([.,]0+)?$'))
AND    EXISTS (SELECT 1 FROM USR_CAD.CADPEDIDOS_CANALES c
               WHERE c.CADPEDI_SITIOGRABA = p.SITIO_GRABA AND c.CADPEDI_NUMELLAMADA = p.NUME_LLAMADA)
GROUP  BY TO_CHAR(p.HORA_CASO, 'YYYY-MM'), p.CADUSUA_USUARIO
ORDER  BY 1, 3 DESC;

-- N2. Registros SIN canal (no despachados): no son incidentes, solo para dimensionar
SELECT p.TIPO_PEDIDO, COUNT(*) AS registros,
       SUM(CASE WHEN TRIM(p.LATITUD_CASO) IS NULL OR REGEXP_LIKE(TRIM(p.LATITUD_CASO), '^-?0+([.,]0+)?$') THEN 1 ELSE 0 END) AS sin_coord
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-05-03' AND p.HORA_CASO < DATE '2026-11-03'
AND    NOT EXISTS (SELECT 1 FROM USR_CAD.CADPEDIDOS_CANALES c
                   WHERE c.CADPEDI_SITIOGRABA = p.SITIO_GRABA AND c.CADPEDI_NUMELLAMADA = p.NUME_LLAMADA)
GROUP  BY p.TIPO_PEDIDO;

-- N3. Codigo de caso que no existe en el catalogo CADCASOS
SELECT p.CODI_PEDIDO, COUNT(*) AS llamadas
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-05-03' AND p.HORA_CASO < DATE '2026-11-03'
AND    p.CODI_PEDIDO IS NOT NULL
AND    NOT EXISTS (SELECT 1 FROM USR_CAD.CADCASOS k WHERE k.CODIGO = p.CODI_PEDIDO)
GROUP  BY p.CODI_PEDIDO ORDER BY 2 DESC;

-- N4. Coordenadas fuera de Colombia o invertidas (pocas, pero visibles)
SELECT p.SITIO_GRABA, p.NUME_LLAMADA, p.HORA_CASO, p.LATITUD_CASO, p.LONGITUD_CASO
FROM   USR_CAD.CADPEDIDOS p
WHERE  p.HORA_CASO >= DATE '2026-05-03' AND p.HORA_CASO < DATE '2026-11-03'
AND    TO_NUMBER(REPLACE(TRIM(p.LATITUD_CASO), ',', '.') DEFAULT NULL ON CONVERSION ERROR) <> 0
AND    NOT (TO_NUMBER(REPLACE(TRIM(p.LATITUD_CASO),  ',', '.') DEFAULT NULL ON CONVERSION ERROR) BETWEEN -4.3 AND 13.6
        AND TO_NUMBER(REPLACE(TRIM(p.LONGITUD_CASO), ',', '.') DEFAULT NULL ON CONVERSION ERROR) BETWEEN -82 AND -66);


-- =====================================================================================
-- Alternativa sin acceso a Oracle: replica Aurora PostgreSQL (DMS primer salto, solo lectura)
--   jdbc:postgresql://prod-mdp-ponal-reader.ciyl8uc12rup.us-east-1.rds.amazonaws.com:5432/raw_secad_mebar
--   usuario read_only (el de la conexion Glue "prod-mecal-secad"). DMS crea el esquema con el nombre
--   de origen: usr_cad (si no aparece, probar "USR_CAD" entre comillas).
-- =====================================================================================
-- SELECT count(*) AS atendidas,
--        count(*) FILTER (WHERE nullif(trim(p.latitud_caso), '') IS NULL)          AS coord_nula,
--        count(*) FILTER (WHERE trim(p.latitud_caso) ~ '^-?0+([.,]0+)?$')         AS coord_cero
-- FROM   usr_cad.cadpedidos p
-- WHERE  p.hora_caso >= '2026-05-03' AND p.hora_caso < '2026-11-03'
-- AND    EXISTS (SELECT 1 FROM usr_cad.cadpedidos_canales c
--                WHERE c.cadpedi_sitiograba = p.sitio_graba AND c.cadpedi_numellamada = p.nume_llamada);
