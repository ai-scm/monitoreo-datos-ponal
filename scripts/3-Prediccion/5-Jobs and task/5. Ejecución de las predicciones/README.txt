Este es un monitor de auditoría de tareas ECS (contenedores de predicción). Solo lectura, no modifica nada.

Propósito principal
Verificar inicio y ejecución de tareas ECS en el cluster de predicciones:

pon-prod-xcrime-predicciones-cluster
Familia: pon-prod-xcrime-prediccion-definition
Funcionalidades clave
1. Estado de tareas ECS

Lista tareas con desiredStatus RUNNING y STOPPED
Muestra: ID, estado, creación, inicio, fin, duración
Códigos de salida de contenedores
Health status
2. Identificación de sigla
Extrae sigla (unidad policial) de:

Etiquetas de tarea
Variables de entorno (especialmente CAD)
Argumentos de ejecución (--sigla, --CAD)
Campos estructurados en JSON (logs/parámetros)
Referencias en logs (menciones directas)
3. Análisis de error
Detecta:

Contenedores esenciales con exit code ≠ 0
Health checks UNHEALTHY
Errores de inicio (OutOfMemory, CannotPullContainer, etc.)
Razones de parada (TaskFailedToStart, SpotInterruption, etc.)
4. Telemetría (activas: últimos 60 min por defecto; logs STOPPED: vida de la tarea)

Logs: eventos por contenedor, indicios de error/reintento, etapa de procesamiento, avance reportado
En STOPPED, consulta desde creación hasta parada ECS aunque esté fuera de la
ventana reciente. Los tiempos consultados quedan en logs_detalle del CSV.
Cuenta End Script! como cierres reportados de subunidades; no demuestra que
todas las subunidades o toda la tarea terminaron correctamente.
Métricas CloudWatch: CPU utilizado, memoria utilizada (últimas muestras)
Análisis de estructura JSON en logs (sin guardar mensajes completos)
5. Reportes locales

tareas.csv — tabla completa con todos los detalles
contexto.json — metadatos, configuración, definiciones, limitaciones
Clasificación de estado
OK: finalizada sin error ECS o ejecutando sin error
AVISO: contenedores con exit code, health check fallido, parada sin inicio, sin cobertura de logs/métricas
FALLA: contenedor esencial con error de salida, health check UNHEALTHY, fallo en arranque, Out of Memory
Limitaciones importantes
No valida predicción — RUNNING ≠ predicción correcta
Avance solo si se reporta en logs; sin reporte, "NO VERIFICABLE"
Indicios de error ≠ fallo confirmado
Sin datos de logs/métricas ≠ bloqueo
STOPPED es captura reciente, no historial completo
Para tareas que ECS ya no conserve, usar mapear_modelos_dependencias.py
--historial-logs. El monitor de ejecución sigue funcionando por sí solo.
Sigla solo de configuración/etiquetas, no validada contra DynamoDB/S3
CPU/memoria requieren Container Insights habilitado; "SIN DATOS" no significa cero consumo
Exit codes: 0 (ok), 1 (falla confirmada), 2 (args inválidos), 3 (error AWS/lectura incompleta), 130 (interrupción)

Opciones útiles:

--max-tareas 1000 — hasta 1000 tareas (por defecto 500)
--ventana-minutos 120 — ampliar ventana de logs/métricas a 2 horas
--max-eventos 5000 — hasta 5000 eventos por contenedor
--detallado — imprime JSON completo de cada tarea
--no-export — solo tabla en consola, sin archivos
