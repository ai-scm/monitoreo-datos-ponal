-- =====================================================================================
-- PremierOne (MEBOG): PLANTILLA para que GUTIC la ejecute en la base de PremierOne.
--
-- AWS no tiene conexion a la base de PremierOne: no hay endpoint DMS ni conexion Glue. Lo que llega es un Excel
-- por dia que GUTIC genera con el reporte "10-Mebog-CIAPD2_Automatico" y sube a
-- s3://pon-prod-mebog-iapol/raw-data-premier-one/. Por eso aqui no hay cadena de conexion.
--
-- PremierOne CAD (Motorola) suele correr sobre Microsoft SQL Server, no sobre Oracle. Esta plantilla esta en
-- T-SQL. Antes de usarla hay que pedir a GUTIC:
--   1. el motor y la cadena de conexion (servidor, puerto, base) y un usuario de solo lectura;
--   2. la tabla o vista de la que sale el reporte 10-Mebog-CIAPD2_Automatico.
-- Reemplazar <VISTA_REPORTE_CIAPD2> por esa vista. Los nombres de columna son los encabezados del Excel; si la
-- vista usa otros nombres, ajustarlos.
-- Ventana: 2026-05-03 a 2026-11-02.
-- =====================================================================================

-- N1. Por mes: registros sin numero de incidente y con coordenada en 0 o vacia
SELECT FORMAT(v.[Fecha Inicial], 'yyyy-MM')                                      AS mes,
       COUNT(*)                                                                  AS total_registros,
       SUM(CASE WHEN NULLIF(LTRIM(RTRIM(v.[No Incidente])), '') IS NULL THEN 1 ELSE 0 END) AS sin_numero_de_incidente,
       SUM(CASE WHEN v.[Latitud] = 0 OR v.[Longitud] = 0 THEN 1 ELSE 0 END)      AS coordenada_en_cero,
       SUM(CASE WHEN v.[Latitud] IS NULL OR v.[Longitud] IS NULL THEN 1 ELSE 0 END) AS coordenada_vacia
FROM   <VISTA_REPORTE_CIAPD2> v
WHERE  v.[Fecha Inicial] >= '2026-05-03' AND v.[Fecha Inicial] < '2026-11-03'
GROUP  BY FORMAT(v.[Fecha Inicial], 'yyyy-MM')
ORDER  BY 1;

-- N1. Muestra de registros sin numero de incidente: ver si es una llamada real o una fila de reporte
SELECT TOP 50 v.[Fecha Inicial], v.[No Incidente], v.[Tipo de incidente], v.[Descripción Tipo], v.[Dirección],
       v.[Patrulla], v.[CAI], v.[Latitud], v.[Longitud]
FROM   <VISTA_REPORTE_CIAPD2> v
WHERE  v.[Fecha Inicial] >= '2026-05-03' AND v.[Fecha Inicial] < '2026-11-03'
AND    NULLIF(LTRIM(RTRIM(v.[No Incidente])), '') IS NULL;

-- N2. Coordenada en 0 por tipo de incidente: muestra si el 0 depende del tipo de llamada
SELECT TOP 30 v.[Descripción Tipo], COUNT(*) AS coordenada_en_cero
FROM   <VISTA_REPORTE_CIAPD2> v
WHERE  v.[Fecha Inicial] >= '2026-05-03' AND v.[Fecha Inicial] < '2026-11-03'
AND    (v.[Latitud] = 0 OR v.[Longitud] = 0)
GROUP  BY v.[Descripción Tipo]
ORDER  BY 2 DESC;

-- N3. Incidentes repetidos dentro de la base (si da 0, los repetidos de AWS vienen de entregas solapadas)
SELECT TOP 50 v.[No Incidente], COUNT(*) AS veces
FROM   <VISTA_REPORTE_CIAPD2> v
WHERE  v.[Fecha Inicial] >= '2026-05-03' AND v.[Fecha Inicial] < '2026-11-03'
AND    NULLIF(LTRIM(RTRIM(v.[No Incidente])), '') IS NOT NULL
GROUP  BY v.[No Incidente]
HAVING COUNT(*) > 1;
