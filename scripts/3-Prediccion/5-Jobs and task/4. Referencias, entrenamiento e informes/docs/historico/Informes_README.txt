Este es un monitor de auditoría del job Glue de generación de informes. Solo lectura, no ejecuta ni modifica nada.

Propósito principal
Verificar el estado y actividad del job Glue que genera informes de anticipaciones:

pon-prod-xcrime-informes-anticipaciones
Funcionalidades clave
1. Historial de ejecuciones

Consulta runs recientes (últimos 8 días por defecto)
Muestra ID, estado, inicio/fin, duración, intentos, errores
2. Análisis de estado

Ventana: ejecuciones que iniciaron O terminaron en el período
Última ejecución: se reporta separada si está fuera de ventana
Cuenta de estados dentro de ventana
Último éxito observado
3. Detección de errores

Extrae tipos de error en logs
Categorías: AccessDenied, OutOfMemory, ImportError, timeout, FileNotFoundError, etc.
Marca severidad: INFO/AVISO/FALLA
4. Reportes CSV
Un único informe_glue_job_informes.csv con filas por tipo:

tipo_registro=RESUMEN — vista principal
tipo_registro=DETALLE — todos los runs consultados
tipo_registro=CONTROL — validaciones y advertencias
tipo_registro=CONTEXTO — metadatos y configuración
Limitaciones importantes
SUCCEEDED ≠ informe correcto — no valida contenido ni entrega
No correlaciona automáticamente con la Lambda de informes ni con predicción
Sin SLA: no sabe si debería haber ejecutado
ExecutionTime ≠ duración real (inicio a fin)
Runs abiertos: duración = "HASTA CAPTURA"
Lectura no atómica (estado puede cambiar durante consulta)
Exit codes: 0 (ok), 1 (job no existe), 2 (args inválidos), 3 (lectura incompleta), 130 (interrupción)

Diferencia principal con monitor_entrenamiento.py: este monitorea UN SOLO job específico en lugar de 3 jobs de ML. Es más simple pero con la misma estructura.
