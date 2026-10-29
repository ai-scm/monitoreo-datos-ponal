Este es un monitor de frescura/presencia de datos en la tabla de predicciones. Solo lectura, no modifica datos.

Propósito principal
Verificar que datos de predicción (probabilidaddelito) existan en el período esperado:

Consulta esquema Glue
Opcionalmente ejecuta agregado en Athena
Opcionalmente inspecciona metadata S3 subyacente
Funcionalidades clave
1. Descubrimiento de esquema (por defecto)

Lista columnas y tipos de datos
Identifica particiones
Ubicación de datos en S3
Timestamp de actualización Glue
2. Validación de presencia (con --ejecutar)
Ejecuta SELECT agregado que cuenta:

Total de registros
Fechas no interpretables
Rango completo de fechas (primero/último)
Registros en período: conteo dentro de fechas configuradas
Días con datos: cantidad de fechas distintas en período
Rango de fechas dentro del período
3. Inspección S3 (con --revisar-fuente-s3)
Sin leer contenido, lista metadata:

Objetos en la ubicación Glue
LastModified de cada objeto
Tamaño de candidatos no vacíos
Objetos técnicos (_ ._ ) y vacíos
Últimos 5 objetos modificados
4. Análisis de fecha

Soporta columnas tipo date, timestamp, string
Conversión de texto con --formato-fecha (sintaxis Athena)
Interpretación de significado: objetivo / generación / dato
Periodo explícito e inclusivo (DESDE/HASTA)
5. Reporte CSV
Un único informe_freshness_probabilidaddelito.csv:

tipo_registro=ESQUEMA — columnas de la tabla
tipo_registro=RESUMEN — hallazgos principales
tipo_registro=CONTROL — validaciones
tipo_registro=SQL — consulta ejecutada
tipo_registro=OBJETO_S3 — metadata de archivos (si aplica)
tipo_registro=CONTEXTO — metadatos y configuración
Estados de control
INFO: esquema encontrado, presencia confirmada
AVISO: fechas inválidas presentes, vacíos, sin DLQ
FALLA: sin registros en período esperado
INCOMPLETO: lectura parcial, fecha no interpretable
AUSENTE: tabla no existe
Limitaciones importantes
NO valida contenido — solo presencia de registros
NO certifica cobertura: puede haber unidades ausentes
LastModified ≠ fecha de datos — solo prueba modificación del objeto
Periodo explícito: no impone antiguedad estándar
Fecha debe confirmarse: no se deduce del nombre de columna
Presencia ≠ calidad — registros pueden estar corruptos
Sin alertas automáticas — solo CSV local
Modos de uso
Modo	Qué hace
Sin opciones	Consulta esquema Glue; no ejecuta Athena
--ejecutar	Ejecuta agregado Athena con fechas configuradas
--solo-sql	Genera SQL sin ejecutar
--revisar-fuente-s3	Lista metadata de objetos S3 (sin leer contenido)
Opciones de fecha (requeridas para --ejecutar):

--fecha COLUMNA_EXACTA — nombre confirmado en esquema
--tipo-fecha {objetivo,generacion,dato} — significado confirmado
--desde 2026-10-20 — primer día esperado (inclusivo)
--hasta 2026-10-28 — último día esperado (inclusivo)
--formato-fecha '%Y%m%d' — si la columna es texto (sintaxis Athena)
Exit codes: 0 (presencia confirmada), 1 (ausente/falla), 2 (args inválidos), 3 (lectura incompleta/Athena timeout), 130 (interrupción)

Ejemplos:


# Solo esquema
python3 monitor_freshness.py --tabla probabilidaddelito

# Con periodo de datos
python3 monitor_freshness.py --fecha fecha_prediccion --tipo-fecha objetivo \
  --desde 2026-10-20 --hasta 2026-10-28 --ejecutar

# Inspeccionar archivos S3
python3 monitor_freshness.py --revisar-fuente-s3 --listado-completo --max-objetos 50000
Caso de uso: auditoría post-predicción de presencia de resultados, sin validar contenido ni calidad.
