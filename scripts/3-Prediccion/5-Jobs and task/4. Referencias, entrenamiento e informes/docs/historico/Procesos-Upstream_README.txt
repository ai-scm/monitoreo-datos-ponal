Este es un monitor de auditoría del pipeline upstream PONAL — el procesamiento de datos previo a la predicción. Solo lectura, no modifica nada.

Propósito principal
Verificar el estado de los procesos que preparan y actualizan datos de referencia:

ECS: pon-prod-xcrime-datacleaning-cluster (limpieza de datos)
Crawler Glue: pon-prod-xcrime-datacleaning-glue-crawler (cataloga datos)
Jobs Glue: actualización de fuentes internas, estructura nacional, geometrías
DynamoDB: tablas de históricos (llamadas, comparendos, delitos)
Funcionalidades clave
1. ECS de limpieza

Lista tareas RUNNING y STOPPED visibles
Detecta fallos en contenedores esenciales
Valida códigos de salida y health checks
Verifica Container Insights habilitado
2. Crawler Glue

Última ejecución (estado, inicio, logs)
Detecta fallos FAILED/CANCELLED
Verifica activación del catalogo
3. Jobs Glue

Ejecuciones en ventana temporal (últimos 8 días por defecto)
Cuenta estados (SUCCEEDED, FAILED, RUNNING, etc.)
Detecta fallos de inicialización y timeout
Valida duración de ejecución
4. DynamoDB

Estado de tablas (ACTIVE/UPDATING)
Modo de billing (PROVISIONED/PAY_PER_REQUEST)
Throttling: lectura y escritura
ThrottledRequests por operación (Read/Write)
Índices globales y sus métricas
5. Reportes locales

Un único informe_procesos_upstream.csv con:
tipo_registro=RESUMEN — vista principal
tipo_registro=DETALLE — todas las ejecuciones
tipo_registro=CONTROL — validaciones
tipo_registro=CONTEXTO — metadatos
Limitaciones importantes
No certifica calidad de datos — SUCCEEDED ≠ datos correctos
Sin umbrales: no hay frecuencia operativa confirmada
ECS: solo snapshot actual, no historial completo de la semana
Throttling: agregado por tabla+operación, sin causa inferida
SIN DATOS ≠ cero actividad — métricas pueden estar ausentes
Sin logs de aplicación — solo metadatos de AWS
Crawler: solo última ejecución registrada
Estados
FALLA: fallos detectados (TaskFailedToStart, FAILED job, etc.)
AVISO: estado transitorio, salida no cero, throttling, Container Insights deshabilitado
INFO: completado sin error ECS (no valida datos)
Exit codes: 0 (consulta completa), 1 (recurso ausente), 2 (args inválidos), 3 (lectura incompleta), 130 (interrupción)

Opciones útiles:

--dias 15 — ventana de 15 días para jobs y métricas
--max-tareas 200 — hasta 200 tareas visibles por estado
--max-ejecuciones 500 — hasta 500 ejecuciones de job
--tabla nombre_tabla — especificar tabla DynamoDB diferente
Caso de uso: auditoría del pipeline de preparación de datos que alimenta predicciones, sin ejecutar ni validar contenido.
