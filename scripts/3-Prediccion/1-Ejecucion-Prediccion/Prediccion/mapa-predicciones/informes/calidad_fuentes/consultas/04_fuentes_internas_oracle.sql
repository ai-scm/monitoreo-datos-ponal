-- =====================================================================================
-- GESCOV, MTRS e IRIS P1: base DISEC, esquema USR_DISEC
--   jdbc:oracle:thin:@//172.28.18.185:1538/DISEC         (conexiones Glue pon-prod-raw-disec-connection
--                                                          y DISEC_CONNECTION-DIRECT, usuario USR_DISEC)
-- SIMEP: base DBOPER, esquema USR_SIMEP
--   jdbc:oracle:thin:@//172.28.18.185:1538/DBOPER        (conexion Glue pon-prod-raw-simep-connection)
-- SIRAP: base GEOPOL, esquema USR_PMU
--   jdbc:oracle:thin:@//172.28.18.192:1521/GEOPOL        (conexion Glue pon-prod-raw-geopol-connection)
-- SIEDCO: base SIEDCO, tabla USR_ESTA.MOTIVOSPOLICIA
--   jdbc:oracle:thin:@//172.28.4.213:1521/SIEDCO         (conexion Glue pon-prod-raw-siedco-connection)
-- Ventana: 2026-05-03 a 2026-11-02.
-- =====================================================================================
ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';

-- ---------- GESCOV (DISEC) ----------
-- N1. Planes sin estado de cumplimiento, por mes (noviembre es normal: aun no se evalua)
SELECT TO_CHAR(e.FECHA_INICIO_PLAN, 'YYYY-MM') AS mes, COUNT(*) AS puntos,
       SUM(CASE WHEN TRIM(e.ESTADO_CUMPLIMIENTO) IS NULL THEN 1 ELSE 0 END) AS sin_estado,
       MAX(e.FECHA_ACTUALIZA_VM) AS ultimo_refresco_vm
FROM   USR_DISEC.V_EVOS_PLANES e
WHERE  e.FECHA_INICIO_PLAN >= DATE '2026-05-03' AND e.FECHA_INICIO_PLAN < DATE '2026-11-03'
GROUP  BY TO_CHAR(e.FECHA_INICIO_PLAN, 'YYYY-MM') ORDER BY 1;

-- ---------- MTRS (DISEC) ----------
-- N2. Fechas imposibles (ano 0004 o futuras)
SELECT m.RIESGO_ID, m.CODIGO_MTRS, m.SIGLA, m.FECHA_INICIO_MESA_TECNICA, m.FECHA_FIN_MESA_TECNICA
FROM   USR_DISEC.V_MTRS_RIESGO_UBICACION m
WHERE  m.FECHA_INICIO_MESA_TECNICA IS NULL
   OR  m.FECHA_INICIO_MESA_TECNICA < DATE '2000-01-01'
   OR  m.FECHA_INICIO_MESA_TECNICA > SYSDATE;

-- N3. Sin cuadrante
SELECT m.SIGLA, COUNT(*) AS sin_cuadrante
FROM   USR_DISEC.V_MTRS_RIESGO_UBICACION m
WHERE  m.FECHA_INICIO_MESA_TECNICA >= DATE '2026-05-03' AND TRIM(m.CUADRANTE) IS NULL
GROUP  BY m.SIGLA;

-- ---------- IRIS P1 (DISEC) ----------
-- N4. Coordenadas fuera de Colombia, invertidas o no numericas (la vista tambien existe como VM_GENERAL_IRISP)
SELECT g.CRIMINALIDAD_ID, g.SIGLA_UNIDAD, g.FECHA_CREACION_IRISP1, g.LATITUD, g.LONGITUD, g.NRO_CUADRANTE
FROM   USR_DISEC.V_GENERAL_IRISP g
WHERE  g.FECHA_CREACION_IRISP1 >= DATE '2026-05-03'
AND    (TRIM(g.NRO_CUADRANTE) IS NULL
   OR  NOT (NVL(TO_NUMBER(REPLACE(TRIM(g.LATITUD),  ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -4.3 AND 13.6
        AND NVL(TO_NUMBER(REPLACE(TRIM(g.LONGITUD), ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -82 AND -66));

-- ---------- SIMEP (DBOPER) ----------
-- N5. Coordenadas fuera de Colombia y fechas imposibles
SELECT a.AUTO_ID, a.SIGLA_FISICA, a.FECHA_EXPEDICION, a.LATITUD, a.LONGITUD
FROM   USR_SIMEP.V_SIMEP_AUTOS_ARCGIS a
WHERE  a.FECHA_EXPEDICION >= DATE '2026-05-03' AND a.FECHA_EXPEDICION < DATE '2026-11-03'
AND    NOT (NVL(TO_NUMBER(REPLACE(TRIM(a.LATITUD),  ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -4.3 AND 13.6
        AND NVL(TO_NUMBER(REPLACE(TRIM(a.LONGITUD), ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -82 AND -66);

SELECT a.AUTO_ID, a.FECHA_EXPEDICION, a.FECHA_CREACION
FROM   USR_SIMEP.V_SIMEP_AUTOS_ARCGIS a
WHERE  a.FECHA_EXPEDICION IS NULL OR a.FECHA_EXPEDICION < DATE '2000-01-01' OR a.FECHA_EXPEDICION > SYSDATE;

-- ---------- SIRAP (GEOPOL) ----------
-- N6. Actividades con coordenadas fuera de Colombia o invertidas
SELECT x.ID_ACTIVIDAD, x.UNIDAD, x.FECHA_INICIO, x.DIRECCION_MAPA, x.LATITUD, x.LONGITUD
FROM   USR_PMU.ACTIVIDADES x
WHERE  x.FECHA_INICIO >= DATE '2026-05-03' AND x.FECHA_INICIO < DATE '2026-11-03'
AND    NOT (NVL(TO_NUMBER(REPLACE(TRIM(x.LATITUD),  ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -4.3 AND 13.6
        AND NVL(TO_NUMBER(REPLACE(TRIM(x.LONGITUD), ',', '.') DEFAULT NULL ON CONVERSION ERROR), 999) BETWEEN -82 AND -66);

-- ---------- SIEDCO ----------
-- N7. Hechos con coordenada 0. La tabla Oracle es USR_ESTA.MOTIVOSPOLICIA; el job la copia tal cual a
--     raw_data_siedco_full_delitos. Si un nombre de columna no coincide, revisar con DESC USR_ESTA.MOTIVOSPOLICIA.
SELECT m.HECHOS_ID, m.FECHA_HECHO, m.TEMATICA, m.SIGLA_UNDEPERTENECE, m.MUNICIPIO_HECHO, m.LATITUD, m.LONGITUD
FROM   USR_ESTA.MOTIVOSPOLICIA m
WHERE  (m.LATITUD = 0 OR m.LONGITUD = 0)
FETCH FIRST 100 ROWS ONLY;
