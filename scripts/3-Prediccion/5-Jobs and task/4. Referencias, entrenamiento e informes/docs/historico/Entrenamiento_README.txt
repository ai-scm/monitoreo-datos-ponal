Este es un monitor de auditoría de jobs Glue de ML (entrenamiento, homologación, consumo). Solo lectura, no ejecuta ni modifica nada.

Propósito principal
Verificar el estado y la actividad de los 3 jobs principales de entrenamiento de modelos de predicción:

pon_prod_xcrime_train_models_job — Entrenamiento
pon_prod_xcrime_homologacion_job — Homologación
pon_prod_xcrime_consume_models_job — Consumo
Funcionalidades clave
1. Historial de ejecuciones (job runs)

Consulta runs recientes (últimos 8 días por defecto)
Muestra ID, estado, inicio/fin, duración, intentos
Categoriza estados: SUCCEEDED, FAILED, RUNNING, STOPPED, etc.
2. Análisis de estado

Ventana: ejecuciones que iniciaron O terminaron en el período
Última ejecución: puede estar fuera de ventana (se reporta separado)
Cuenta de estados dentro de ventana vs. total
Último éxito observado
3. Detección de errores

Extrae tipo de error si está presente en logs
Categorías: AccessDenied, OutOfMemory, ImportError, timeout, conexión, etc.
Marca severidad: INFO/AVISO/FALLA
4. Reportes CSV
Un único informe_entrenamiento.csv con filas por tipo:

tipo_registro=RESUMEN — vista principal (1 fila/job)
tipo_registro=DETALLE — todos los runs consultados
tipo_registro=CONTROL — validaciones y advertencias
tipo_registro=CONTEXTO — metadatos y configuración
Limitaciones importantes (intencionadas)
SUCCEEDED ≠ modelo válido — no verifica calidad ni que se haya actualizado
No certifica dependencia entre estos jobs ni relación con predicción semanal
Sin SLA: no sabe si una ejecución debería haber ocurrido
ExecutionTime (segundos Glue) ≠ duración real (inicio a fin)
Runs abiertos: duración = "HASTA CAPTURA"
Historial paginado; lectura no atómica (estado puede cambiar durante consulta)
Exit codes: 0 (ok), 1 (recurso ausente), 2 (args inválidos), 3 (lectura incompleta), 130 (interrupción)

Opciones útiles:

--dias 30 — ventana de 30 días
--historial-completo — agota todas las páginas disponibles
--max-ejecuciones 5000 — hasta 5000 runs por job
