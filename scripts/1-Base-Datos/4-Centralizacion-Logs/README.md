# 4 — Centralización de Logs (`monitor_logs.py`)

Corre **a diario**. En esta cuenta **no existe hoy un pipeline de centralización de
logs**: 0 subscription filters, 0 CloudWatch Logs destinations, y ningún bucket S3 /
Firehose que reciba logs de Glue o DMS. Por eso este script **no verifica un destino
cruzado**; hace **higiene de los log groups** del pipeline (Glue/DMS/Lambda),
**detecta errores recientes e identifica de qué recurso vienen** (servicio, tipo,
nombre, ARN) —clasificando la causa en los Glue jobs— y reporta la ausencia de
centralización como un bloque **informativo**.

**Solo lectura.** No escribe ni configura nada (solo consulta y corre Logs Insights).

## Qué revisa

1. **Identidad AWS** (`sts:GetCallerIdentity`).
2. **Higiene de log groups** descubiertos **dinámicamente** (paginado completo en
   cada corrida; nunca se cablea el conteo ni los nombres, solo los prefijos):
   - `/aws-glue/jobs/` (incluye el suelto `pon-prod-expertopol-conversation-audit-glue`)
   - `/aws-glue/python-jobs/`
   - `/aws-glue/sessions/`
   - `/aws-glue/crawlers`
   - `/aws-glue/column-statistics`
   - `/aws-glue/testconnection/` (se **agregan** en una sola fila)
   - `dms-tasks-`
   
   Más una **lista explícita de 9 Lambdas del pipeline** (hoy invisibles) bajo
   `/aws/lambda/`: `pon-prod-Monitor-Glue-Jobs-DPU`,
   `pon-prod-disec-launch-extract-crawler`, `pon-prod-dms-rows-dynamo`,
   `pon-prod-dms-update-trigger`, `resumeFailedTasksDms`,
   `pon-prod-tablero-alerta-delitos-athena-update`, `glue-usage`,
   `athenafederatedcatalog_athena_ih2iqmeq`,
   `athenafederatedcatalog_pon_prod_dynamo_tables`. (No todo `/aws/lambda/*`, que son
   cientos e irrelevantes.)
   
   Por cada grupo evalúa:
   - **Actividad**: último evento dentro de la ventana (`--horas`, 24 por defecto).
   - **Retención**: `null` (nunca expira) o menor al mínimo (30 días por defecto).
   - **Tamaño sin retención**: `storedBytes` por encima del umbral (5 GB por defecto).
3. **Errores + localización del recurso (todos los servicios)**: para los grupos
   **candidatos** con actividad reciente —`/aws-glue/jobs/error`,
   `/aws-glue/python-jobs/error`, `/aws-glue/crawlers`, `/aws-glue/sessions/*`,
   `dms-tasks-*` y las lambdas del pipeline— corre CloudWatch Logs Insights (24 h). Si
   hay líneas de error → **FALLA**, e **identifica de qué recurso vino**:
   
   | Log group | Servicio | Tipo | Nombre (de dónde) | ARN |
   |-----------|----------|------|-------------------|-----|
   | `/aws-glue/jobs/error` | AWS Glue | Job (ETL/Spark) | `JobName` del payload | `...:job/<n>` |
   | `/aws-glue/python-jobs/error` | AWS Glue | Job (Python Shell) | `JobName` / stream | `...:job/<n>` |
   | `/aws-glue/crawlers` | AWS Glue | Crawler | el `@logStream` | `...:crawler/<n>` |
   | `/aws-glue/sessions/*` | AWS Glue | Interactive Session | el `@logStream` | — |
   | `dms-tasks-*` | AWS DMS | Replication task | nombre del log group | — (usa resource-id) |
   | `/aws/lambda/<n>` | AWS Lambda | Function | el `<n>` del log group | `...:function:<n>` |
   
   Además, **solo para los Glue jobs** (donde hay evidencia real), **clasifica la
   causa** por regex (ORA-17002, AccessDenied S3/Athena, TASK_WRITE_FAILED,
   GlueBootstrap, bugs de código, etc.). Para crawlers, DMS, sessions y Lambda
   **identifica el recurso pero no inventa la causa** (muestra el renglón crudo).
   Cuando hay `JobName` + `JobRunId`, arma también un **deeplink** a la consola.
4. **Cobertura de logging DMS**: tareas con `Logging.EnableLogging == false` (AVISO).
5. **Centralización** (informativo): cuenta subscription filters en una muestra de
   grupos; si hay 0, lo señala como brecha de arquitectura a resolver aparte.

Los grupos `/aws-glue/testconnection/*` (63 y subiendo, ruido) se **resumen en una
sola fila** con el conteo de cuántos están fuera de umbral.

> **ARN de DMS**: no se arma porque el ARN real de una replication task usa un
> resource-id interno, no el nombre; dar uno construido sería falso. El nombre del log
> group (`dms-tasks-<nombre>`) basta para ubicar la tarea en la consola.

## Clasificación

- **OK** — grupo activo, con retención adecuada y tamaño bajo control.
- **AVISO** — sin actividad reciente · retención `null` o < 30 d · tamaño > 5 GB sin
  retención · tareas DMS sin logging · sin pipeline de centralización · lambda del
  pipeline sin log group.
- **FALLA** — un grupo candidato (Glue job/crawler/session, DMS task o lambda del
  pipeline) tiene **actividad reciente Y líneas de error reales** en la ventana. Se
  identifica servicio/tipo/nombre/ARN; para Glue jobs además se clasifica la causa.
  También FALLA ante errores de acceso/listado de la API.

> Las brechas de higiene (retención, tamaño, actividad) **no** fuerzan fallo diario:
> un grupo sin actividad o sin retención no es un incidente, es configuración. El
> **exit code** es `1` solo cuando hay un error real clasificado en un grupo de error
> o un fallo de acceso. Esto diferencia señal (algo roto) de ruido (higiene).

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_logs.py                 # higiene con ventana de 24 h
python3 monitor_logs.py --con-avisos    # muestra todos los avisos de higiene
python3 monitor_logs.py --horas 48
python3 monitor_logs.py --no-export     # no genera CSV
```

### Flags

| Flag | Qué hace |
|------|----------|
| `--horas N` | Ventana de actividad en horas (default 24; env `LOGS_ACTIVITY_WINDOW_HOURS`). |
| `--sin-insights` | Omite la clasificación de errores por Logs Insights (solo higiene). |
| `--con-avisos` | Muestra los AVISOS en el resumen (el CSV siempre los incluye). |
| `--detallado` | Salida larga, sin spinner de progreso. |
| `--no-export` | No escribe el CSV. |

### Umbrales (env vars)

| Variable | Default | Qué controla |
|----------|---------|--------------|
| `LOGS_ACTIVITY_WINDOW_HOURS` | `24` | Ventana para considerar un grupo "vivo". |
| `LOGS_MIN_RETENTION_DAYS` | `30` | Retención mínima aceptable. |
| `LOGS_SIZE_WARN_GB` | `5` | Tamaño que dispara AVISO si el grupo no tiene retención. |

## Permisos IAM mínimos

```json
{
  "Effect": "Allow",
  "Action": [
    "logs:DescribeLogGroups",
    "logs:DescribeLogStreams",
    "logs:DescribeSubscriptionFilters",
    "logs:StartQuery",
    "logs:GetQueryResults",
    "logs:StopQuery",
    "dms:DescribeReplicationTasks",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

## Salida

- **Consola**: resumen `TOTAL: N | x OK | y aviso | z falla` + detalle (todo con
  `--con-avisos`).
- **CSV**: `salida/logs_<YYYYmmdd_HHMMSS>.csv` con columnas `recurso, ubicacion,
  estado, mensaje, accion_sugerida, tipo_error_detectado, servicio, tipo_recurso,
  recurso_nombre, origen`. Las cinco últimas solo se llenan en filas de FALLA por
  error: `tipo_error_detectado` (clasificación, solo Glue jobs), `servicio` (Glue/DMS/
  Lambda), `tipo_recurso` (Job/Crawler/Task/Function/Session), `recurso_nombre` y
  `origen` (texto completo con servicio, tipo, nombre, run y ARN).
- **Exit code**: `0` todo OK · `1` si hay un error clasificado en un grupo de error o
  un fallo de acceso · `2` args inválidos.

## Tabla de clasificación de errores Glue

Patrones (regex) validados con evidencia real sobre `/aws-glue/jobs/error` y
`/aws-glue/python-jobs/error`. Si una línea matchea → FALLA con causa y acción:

| Regex | Causa | Acción |
|-------|-------|--------|
| `ORA-17002` | Oracle cerró la conexión en el handshake JDBC | Reintentar con backoff; revisar listener/VPN Oracle |
| `ORA-\d{5}` | Otro error Oracle | Buscar código específico; escalar a DBA si se repite |
| `s3:(PutObject\|ListBucket).*AccessDenied` | Rol IAM sin permisos S3 sobre el bucket destino | Revisar política IAM del rol del job |
| `TASK_WRITE_FAILED` | Spark envuelve un fallo de escritura (suele ser el AccessDenied de arriba) | Mirar el "Caused by" anidado |
| `ConnectException: Connection refused.*GlueBootstrap` | Fallo de red executor→driver, infra transitoria | Reintentar; vigilar si aumenta |
| `argparse\.ArgumentError.*conflicting option` | Bug: argumento choca con uno que Glue inyecta | Fix de código, renombrar argumento |
| `athena:StartQueryExecution.*AccessDeniedException` | Rol IAM sin permiso de Athena | Otorgar `athena:StartQueryExecution` al rol |
| `InvalidRequestException.*mismatched input` | SQL mal formado hacia Athena | Revisar generación de query en el script origen |
| `RuntimeError: \d+ SIVICC table load\(s\) failed` | Contador agregado; la causa real no está en esa línea | Correlacionar por timestamp con `/aws-glue/jobs/error` |
| `IndexError: index 0 is out of bounds` | DataFrame vacío sin guard | Validar antes de `.values[0]` |
| `TypeError:.*missing 1 required positional argument: 'operation_name'` | Bug en el manejador de excepciones; enmascara el error real | Revisar bloque "During handling of..." |
| *(ninguno)* | Error no catalogado | Revisar el log crudo; agregar el patrón a `PATRONES_ERROR` si se repite |

**Alcance de la query**: Insights corre solo sobre los grupos `*/error` con actividad
reciente (son 2: jobs y python-jobs), no sobre los ~70 grupos sanos, para mantener
bajo el costo/tiempo. Se puede desactivar del todo con `--sin-insights`.

## Nota importante (brecha de negocio)

Si el negocio esperaba que estos logs **sí** estuvieran centralizados en un destino
(S3, OpenSearch, Firehose), eso es una **brecha de arquitectura real** que este
script solo puede *señalar*, no "verificar como rota" (no existe el pipeline a
verificar). Conviene documentarla y decidir aparte si se implementa esa
centralización; `monitor_logs.py` seguirá reportándola como AVISO hasta entonces.
