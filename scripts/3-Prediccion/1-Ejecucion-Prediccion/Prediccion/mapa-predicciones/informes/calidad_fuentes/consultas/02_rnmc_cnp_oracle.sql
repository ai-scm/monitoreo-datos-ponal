-- =====================================================================================
-- RNMC / Codigo Nacional de Policia. Base DBOPER, esquema USR_CNP.
-- Conexion (la que usa el job Glue pon-prod-raw-cnp_etl):
--   jdbc:oracle:thin:@//172.28.18.185:1538/DBOPER        (conexion Glue pon-prod-raw-cnp-connection)
-- Alternativa registrada en DMS: cnp.prod.mdp.local:1538 / servicio dboper, usuario USR_CNP.
-- Ventana: 2026-05-03 a 2026-11-02.
-- =====================================================================================

-- N1. Hechos sin coordenadas (la vista de capas los descarta)
SELECT COUNT(*) AS hechos,
       SUM(CASE WHEN h.LATITUD IS NULL OR h.LONGITUD IS NULL THEN 1 ELSE 0 END) AS sin_coord,
       SUM(CASE WHEN h.LATITUD = 0 OR h.LONGITUD = 0 THEN 1 ELSE 0 END)         AS coord_cero
FROM   USR_CNP.CNP_HECHOS h
WHERE  h.FECHA >= DATE '2026-05-03' AND h.FECHA < DATE '2026-11-03';

SELECT h.ID_HECHOS, h.FECHA, h.FECHA_CREACION, h.CUADRANTE2, h.DIRECCION_HECHOS, h.LATITUD, h.LONGITUD, h.FUENTE
FROM   USR_CNP.CNP_HECHOS h
WHERE  h.FECHA >= DATE '2026-05-03' AND h.FECHA < DATE '2026-11-03'
AND    (h.LATITUD IS NULL OR h.LONGITUD IS NULL)
FETCH FIRST 50 ROWS ONLY;

-- N2. Hechos sin cuadrante (quedan como "(SIN SIGLA)": el mapa no los ubica por unidad)
SELECT NVL(SUBSTR(h.CUADRANTE2, 1, 5), '(nulo)') AS prefijo, COUNT(*) AS hechos
FROM   USR_CNP.CNP_HECHOS h
WHERE  h.FECHA >= DATE '2026-05-03' AND h.FECHA < DATE '2026-11-03'
AND    (TRIM(h.CUADRANTE2) IS NULL OR h.CUADRANTE2 LIKE 'NO HA%')
GROUP  BY NVL(SUBSTR(h.CUADRANTE2, 1, 5), '(nulo)');

-- N3. Hechos sin infractor (persona tipo 350): no entran a la vista por el INNER JOIN
SELECT h.ID_HECHOS, h.FECHA, h.CNP_ART, h.CNP_NUM, h.CUADRANTE2
FROM   USR_CNP.CNP_HECHOS h
WHERE  h.FECHA >= DATE '2026-05-03' AND h.FECHA < DATE '2026-11-03'
AND    NOT EXISTS (SELECT 1 FROM USR_CNP.CNP_PERSONAS p WHERE p.ID_HECHOS = h.ID_HECHOS AND p.ID_TIPO_PERSONA = 350);


-- N4. Mediaciones en sala con coordenadas fuera de Colombia o invertidas
SELECT s.ID_SOLICITUD, s.FECHA_SOLICITUD, s.SIGLA_FISICA, s.CUADRANTE, s.LATITUD, s.LONGITUD
FROM   USR_CNP.CNP_SOLICI_MEDIA s
WHERE  s.FECHA_SOLICITUD >= DATE '2026-05-03' AND s.FECHA_SOLICITUD < DATE '2026-11-03'
AND    NOT (s.LATITUD BETWEEN -4.3 AND 13.6 AND s.LONGITUD BETWEEN -82 AND -66);


-- =====================================================================================
-- Mismas novedades sobre la copia en AWS (Athena, base pon-prod-raw-cnp)
-- =====================================================================================
-- SELECT id_hechos, count(*) FROM "pon-prod-raw-cnp"."raw_dboper_usr_cnp_cnp_hechos"
-- WHERE fecha >= TIMESTAMP '2026-05-03 00:00:00' AND fecha < TIMESTAMP '2026-11-03 00:00:00'
-- GROUP BY 1 HAVING count(*) > 1 LIMIT 50;
