Este script consulta modelos reportados por tareas de predicción y dispone de modos de auditoría de imagen y dependencias. No modifica AWS. La prueba opcional de imports ejecuta librerías en una copia Docker local.

Tabla de todas las siglas de estructura y sus streams
  python3 ~/mapear_modelos_dependencias.py --mapear-streams
Lee SIGLA_PAPA de la estructura actual S3 y cruza sus CAD con la evidencia
ECS/logs. Imprime Sigla/CAD | Stream, sin filtro por una sigla de ejemplo.
Repite CAD cuando hay varios streams y muestra SIN STREAM ASOCIADO cuando
no hay una relacion confirmada. Exporta mapa_siglas_streams.csv y contexto.
Este modo explicito usa S3; el resumen predeterminado conserva su fuente de logs.
El mapa usa CAD de ECS/cache y muestras de hasta 200 mensajes, sin analizar modelos.
Presupuesto por pasada: --max-segundos-mapeo 180; guarda CSV y cache mientras avanza.
Repetirlo continua con las asociaciones guardadas. --reiniciar-mapeo las ignora.
Para ejecutarlo en segundo plano:
  nohup python3 -u ~/mapear_modelos_dependencias.py --mapear-streams > ~/mapa_streams.log 2>&1 &
  tail -f ~/mapa_streams.log
Ver HISTORIAL-LOGS.txt para limites, reanudacion y el permiso logs:GetLogEvents.

Monitoreo de siglas observadas (modo predeterminado)
  python3 ~/mapear_modelos_dependencias.py
Una fila por sigla identificada desde streams conservados y metadata ECS vinculada.
Incluye activas, detenidas y recuperadas de logs, sin filtro por RUNNING.
No lee catalogos ni estructura nacional S3. Las nunca observadas no se identifican.
Por defecto revisa 30 dias, sujeto a la retencion; --dias-logs 8 acota la ventana.
El porcentaje Uniform usa el total de selecciones observadas de la sigla.

Historial por streams (sin depender de tareas visibles en ECS)
  python3 ~/mapear_modelos_dependencias.py --historial-logs --dias-logs 8 --max-eventos 100000
Genera una tabla por sigla y un historial_tareas.csv con RESUMEN_SIGLA y
DETALLE_TAREA. El detalle incluye sigla, modalidad cuando hay
evidencia explícita, estado/códigos ECS recuperables, cierres End Script,
selecciones por modelo, total y porcentaje Uniform observado, pocos datos y
errores. Sin S3/ECR ni mensajes completos. No confunde cierre de subunidad
con éxito global. Ver HISTORIAL-LOGS.txt para cobertura, límites y permisos.
--por-tarea conserva el mapeo anterior.

Modos de inspección (se ejecutan por separado del historial)

Propósito principal
Mapear modelos de predicción, dependencias Python y operaciones de archivo en la imagen Docker del predictor:

Cluster: pon-prod-xcrime-predicciones-cluster
Familia: pon-prod-xcrime-prediccion-definition
Funcionalidades clave
1. Inspección de imagen Docker (ECR)

Descarga capas sin extraerlas (presupuesto de 8 GB descomprimido)
Inventaría archivos sin ejecutar nada
Detecta:
Metadata de paquetes Python (METADATA, PKG-INFO)
Manifiestos (requirements.txt, pyproject.toml, setup.cfg)
Código Python de la aplicación
Archivos de datos (CSV, GeoJSON)
2. Validación de dependencias

Compara requirements.txt extraído vs. metadata en el entorno
Verifica versiones fijadas
Detecta inconsistencias estáticas (no ejecuta imports)
Controles por digest y entorno objetivo
3. Análisis estático de código

Parse AST sin ejecutar código
Detecta operaciones de archivo:
Lecturas: pandas.read_csv(), geopandas.read_file(), numpy.load()
Escrituras: to_csv(), to_file(), S3 get_object()/put_object()
Archivos: open(), download_file(), upload_file()
Rastreo de referencias a S3, rutas estáticas vs. dinámicas
Identificación de clases de modelo declaradas (Counter, KDE, SEEP, Uniform)
4. Rastreo de columnas CSV

Propaga origen de CSV en AST
Detecta qué columnas se leen/usan
No ejecuta, solo analiza estructura de código
5. Reportes locales

mapa.csv — inventario estático
evidencias.csv — referencias encontradas
contexto.json — metadatos y limitaciones
Limitaciones importantes
La inspección estática NO certifica ejecución. El historial separa metadata ECS de declaraciones de logs.
NO valida modelos actualizados o en uso
El análisis AST no ejecuta código. --probar-imports es un modo independiente con ejecución Docker local.
NO resuelve dinámicas — exec(), importlib.import_module() dinámico, despacho por nombre
Referencias ≠ disponibilidad — archivo mencionado puede no existir
Ramas no resueltas — if/else ambiguas
Imports en paquetes vendorizados pueden no detectarse
Límites de cobertura: 100k nodos, 2k referencias, 4 niveles de recursión
Opciones de validación
--validar-entradas — lee cabeceras de CSV
--validar-filas — valida conteos de dos CSVs en S3
--inspeccionar-imagen — descarga capas ECR (más lento)
--probar-imports — ejecuta imports en Docker local (sin red)
Exit codes: 0 (sin diferencias), 1 (diferencia estática encontrada), 2 (args inválidos), 3 (lectura incompleta), 130 (interrupción)

Caso de uso: auditoría pre-predicción de dependencias Python y operaciones de archivo, sin ejecutar código.
