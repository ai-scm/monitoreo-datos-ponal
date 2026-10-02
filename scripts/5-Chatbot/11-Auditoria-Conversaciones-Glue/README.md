# 11 — Auditoría de Conversaciones (Glue) (`monitor_auditoria_conversaciones_glue.py`)

Corre **a diario**. **Ojo con el nombre original**: este punto se llamaba
"Sincronización Glue", pero fue un **choque de nombres**. **No existe ningún Glue que
sincronice la Knowledge Base** del chatbot — esa "sincronización" la hace **Bedrock
mismo** (Step Function → Lambda → `StartIngestionJob`), y la cubre el **punto 10**.

El único recurso **Glue real** del dominio chatbot es el job de **auditoría de
conversaciones**, que exporta el historial del chat (**DynamoDB → Parquet**) para
auditoría. No tiene relación alguna con cargar documentos a la KB. Por eso la carpeta
se renombró de `11-Sincronizacion-Glue` a `11-Auditoria-Conversaciones-Glue`.

**Solo lectura.** No dispara nada.

## Qué revisa

Un único recurso: el job `pon-prod-expertopol-conversation-audit-glue-full-refresh` y
su Glue Trigger.

1. **Identidad AWS** (`sts:GetCallerIdentity`).
2. **Glue Trigger** `conversation-audit-glue-full-refresh` (`glue.get_trigger`):
   `State == ACTIVATED` = OK; cualquier otro = AVISO. Es `SCHEDULED`, **cron diario
   02:00 UTC**. Verificado con la API correcta (no asumido por el nombre — lección de
   `daily_rds` en RNMC, que resultó ser Glue Trigger y no EventBridge).
3. **Último run del job** (`glue.get_job_runs`), mismo patrón que `monitor_jobs.py`:
   - **OK** — `SUCCEEDED` dentro de la ventana (`--horas`, 24 por defecto).
   - **AVISO** — `SUCCEEDED` fuera de ventana · en curso · sin corridas.
   - **FALLA** — `FAILED`/`TIMEOUT`/`ERROR`/`STOPPED`.

## Clasificación de errores

El job tiene **10/10 corridas recientes exitosas**, así que **no** tiene un patrón de
fallo propio todavía. Arranca con el **catálogo genérico de Glue** (reutilizado del
dominio Base-Datos) leído por Logs Insights: códigos `ORA-#####`, OOM, ExecutorLost,
`TASK_WRITE_FAILED`, AccessDenied en S3, Py4J, y problemas con la tabla DynamoDB
origen. Si aparece un fallo real con un patrón específico, se agrega aquí.

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_auditoria_conversaciones_glue.py
python3 monitor_auditoria_conversaciones_glue.py --horas 48
python3 monitor_auditoria_conversaciones_glue.py --con-avisos
python3 monitor_auditoria_conversaciones_glue.py --sin-insights
python3 monitor_auditoria_conversaciones_glue.py --no-export
```

### Flags

| Flag | Qué hace |
|------|----------|
| `--horas N` | Ventana en horas (default 24). |
| `--sin-insights` | Omite la clasificación de errores por Logs Insights. |
| `--con-avisos` | Muestra los AVISOS en el resumen (el CSV siempre los incluye). |
| `--detallado` | Salida larga sin spinner. |
| `--no-export` | No escribe el CSV. |

## Permisos IAM mínimos

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetTrigger",
    "glue:GetJobRuns",
    "logs:StartQuery",
    "logs:GetQueryResults",
    "logs:StopQuery",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

## Salida

- **Consola**: resumen `TOTAL: N | x OK | y aviso | z falla` + detalle de lo crítico.
- **CSV**: `salida/auditoria_conversaciones_<YYYYmmdd_HHMMSS>.csv` con columnas
  `recurso, ubicacion, estado, mensaje, accion_sugerida, tipo_error_detectado`.
- **Exit code**: `0` todo OK · `1` si falló · `2` args inválidos.

## Nota (choque de nombres)

Si buscas un pipeline **Glue → S3 → KB**, no existe. La ingesta de la KB la maneja
Bedrock (ver el punto 10, `monitor_carga_kb.py`). Este script es exclusivamente para el
job de auditoría de conversaciones del chat.
