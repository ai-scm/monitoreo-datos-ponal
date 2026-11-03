-- =====================================================================================
-- Antecedentes (SIVICC / MNVCC). Base SIVICC, esquema USR_MNVCC.
-- Conexion (job Glue pon-prod-raw-sivicc_etl):
--   jdbc:oracle:thin:@//172.28.18.185:1538/SIVICC        (conexion Glue pon-prod-raw-sivicc-connection;
--                                                          tambien glue_connection_sivicc, usuario USR_MNVCC)
-- Ventana: 2026-05-03 a 2026-11-02. Son tablas muy grandes (decenas de millones de filas por mes):
-- filtrar siempre por fecha.
-- =====================================================================================
ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';

-- N1. Consultas de IMEI sin la unidad que las hizo (17,2 %): no salen al filtrar por unidad
SELECT TO_CHAR(i.FECHA_CREACION, 'YYYY-MM') AS mes, COUNT(*) AS consultas,
       SUM(CASE WHEN i.UNDELABORANDO IS NULL OR i.UNDELABORANDO = 0 THEN 1 ELSE 0 END) AS sin_unidad
FROM   USR_MNVCC.AUDITORIA_CONS_IMEI i
WHERE  i.FECHA_CREACION >= DATE '2026-05-03' AND i.FECHA_CREACION < DATE '2026-11-03'
GROUP  BY TO_CHAR(i.FECHA_CREACION, 'YYYY-MM') ORDER BY 1;

SELECT i.ID_IMEI, i.FECHA_CREACION, i.FUENTE, i.MAQUINA_CREACION, i.USUARIO_CREACION, i.UNDE_MINUTA,
       i.UNDELABORANDO, i.DEPENDENCIA_FUNCIONARIO, i.ID_CUADRANTE
FROM   USR_MNVCC.AUDITORIA_CONS_IMEI i
WHERE  i.FECHA_CREACION >= DATE '2026-10-01' AND i.FECHA_CREACION < DATE '2026-11-03'
AND    i.UNDELABORANDO IS NULL
FETCH FIRST 50 ROWS ONLY;

-- N1b. Que canal (FUENTE) genera las consultas sin unidad
SELECT i.FUENTE, COUNT(*) AS sin_unidad
FROM   USR_MNVCC.AUDITORIA_CONS_IMEI i
WHERE  i.FECHA_CREACION >= DATE '2026-10-01' AND i.FECHA_CREACION < DATE '2026-11-03'
AND    i.UNDELABORANDO IS NULL
GROUP  BY i.FUENTE ORDER BY 2 DESC;

-- N2. Vehiculos sin unidad, sin cuadrante o sin coordenada
SELECT COUNT(*) AS consultas,
       SUM(CASE WHEN v.UNDELABORANDO IS NULL OR v.UNDELABORANDO = 0 THEN 1 ELSE 0 END) AS sin_unidad,
       SUM(CASE WHEN v.ID_CUADRANTE IS NULL THEN 1 ELSE 0 END)                         AS sin_cuadrante,
       SUM(CASE WHEN v.LATITUD IS NULL OR v.LONGITUD IS NULL THEN 1 ELSE 0 END)        AS sin_coord,
       SUM(CASE WHEN v.LATITUD = 0 OR v.LONGITUD = 0 THEN 1 ELSE 0 END)                AS coord_cero
FROM   USR_MNVCC.AUDITORIA_CONS_VEHICULO v
WHERE  v.FECHA_CREACION >= DATE '2026-05-03' AND v.FECHA_CREACION < DATE '2026-11-03';

-- N3. Personas con coordenadas invalidas (LATITUD/LONGITUD son texto en esta tabla)
SELECT p.ID_AUDITORIA_PERSONA, p.FECHA, p.FUENTE, p.LATITUD, p.LONGITUD
FROM   USR_MNVCC.AUDITORIA_CONS_PERSONA p
WHERE  p.FECHA >= DATE '2026-05-03' AND p.FECHA < DATE '2026-11-03'
AND    NOT (NVL(TO_NUMBER(REPLACE(TRIM(p.LATITUD),  ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -4.3 AND 13.6
        AND NVL(TO_NUMBER(REPLACE(TRIM(p.LONGITUD), ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -82 AND -66)
FETCH FIRST 100 ROWS ONLY;

-- N4. Duplicados de personas: en AWS el bloque de IDs 284.684.160 a 285.156.816 esta dos veces.
--     Aqui debe dar 0 filas repetidas (confirma que el duplicado es de nuestra copia).
SELECT COUNT(*) AS filas, COUNT(DISTINCT p.ID_AUDITORIA_PERSONA) AS ids
FROM   USR_MNVCC.AUDITORIA_CONS_PERSONA p
WHERE  p.ID_AUDITORIA_PERSONA BETWEEN 284684160 AND 285156816;
