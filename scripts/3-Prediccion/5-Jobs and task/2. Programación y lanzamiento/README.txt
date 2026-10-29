Este es un monitor de auditoría del disparo semanal de predicciones en AWS (EventBridge → Lambda → ECS). Es solo lectura, no invoca ni modifica nada.

Propósito principal
Verificar que el flujo programado semanal (xcrime_prediccion_siglas_unidades e informes_anticipaciones) se dispare correctamente y lance tareas ECS.

Funcionalidades clave
1. Mapeo EventBridge → Lambda

Consulta reglas en el bus default
Lista targets (Lambdas) enlazados
Valida DLQ, retry policy, estado de reglas
2. Métricas de CloudWatch (últimos 8 días, por defecto)

EventBridge: invocaciones y fallos por regla
Lambda: invocaciones, errores, throttles, drop de eventos asincronos, fallos en DLQ
Duration máxima vs. timeout configurado
3. Análisis de logs de Lambda

Lee eventos de la función "lanzadora" (pon-prod-xcrime-prediccion-launch-fargate)
Busca llamadas RunTask explícitas a ECS
Detecta:
ARNs de tareas creadas (en respuesta RunTask)
Errores y excepciones
Fallos reportados en RunTask
4. Dos modos

Modo	Qué hace
--revisar-formatos	Muestrea hasta 6 eventos reales del informe anterior; analiza su estructura sin ejecutar
Default	Inspección completa: reglas + Lambdas + métricas + logs
5. Reportes CSV
Un único informe_disparo_semanal.csv con filas filtradas por:

tipo_registro=RESUMEN — vista principal (reglas y Lambdas)
tipo_registro=CONTROL — validaciones (OK/AVISO/INCOMPLETO/AUSENTE)
tipo_registro=EVIDENCIA — eventos de log (ARNs, errores, etc.)
tipo_registro=CONTEXTO — metadatos y configuración
Importantes limitaciones (intencionadas)
No certifica que la tarea ECS se lanzó; solo que RunTask fue llamado
No ejecuta la regla ni invoca Lambda
ARN mencionado ≠ tarea creada ≠ tarea en RUNNING
Métricas sin causa: "SIN DATOS" ≠ fallo
Logs acotados a 16 MiB, sin exportar mensajes crudos
Exit codes: 0 (ok), 1 (falla encontrada), 3 (error AWS o lectura incompleta), 130 (interrución)
