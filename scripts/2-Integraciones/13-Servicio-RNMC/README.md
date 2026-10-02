# 13 — Servicio RNMC (`monitor_rnmc.py`)

Corre **a diario**. "Servicio RNMC" **no es una API**: no hay API Gateway, endpoint
HTTP, Step Functions ni colas con ese nombre. Es un **pipeline de datos por lotes** con
dos cadenas encadenadas + una tarea DMS puntual. Por eso **no se verifica nada de
HTTP/5xx**; las señales son estado de jobs Glue, métricas de la Lambda disparadora,
estado de reglas EventBridge y de la DMS.

**Solo lectura.** No escribe ni dispara nada.

## La cadena del pipeline

```
S3 (pon-prod-raw-rnmc)
   │  (evento ObjectCreated)
   ▼
pon-prod-launch-homogenize-rnmc-job (Lambda)
   │  lanza
   ▼
pon-prod-homogenize-raw-rnmc-db (Glue, por evento)
   
pon-prod-raw-rnmc-incremental_etl (Glue, frecuente)  → lago de datos

RNMC-to-RDS (Glue, diario 14:00 UTC, trigger daily_rds)
   lee Athena pon-prod-raw-cnp.boletin_z1_capas_v2 → escribe Aurora public.hechos_z1_v2
```

## Recursos y reglas (por naturaleza)

| Recurso | Tipo | Naturaleza | Regla de severidad |
|---------|------|------------|--------------------|
| `pon-prod-raw-rnmc-incremental_etl` | Job Glue | **FRECUENTE** (10+/día) | `FAILED` = FALLA · sin corrida en ventana = AVISO · `SUCCEEDED` = OK |
| `pon-prod-homogenize-raw-rnmc-db` | Job Glue | **EVENTO_S3** (no cron) | `FAILED` = FALLA · **sin corrida en 24 h = INFO** (normal si no llegó archivo) |
| `RNMC-to-RDS` | Job Glue | **DIARIO** (14:00 UTC) | `FAILED` = FALLA · sin corrida del día = AVISO · `SUCCEEDED` = OK |
| `pon-prod-launch-homogenize-rnmc-job` | Lambda | **métricas CloudWatch** | lógica de **cadena rota** (abajo) |
| `prod-data-reload-rnmc1` / `rnmc2` / `prod-data-createview-rnmc` | **EventBridge rule** | scheduling | `State != ENABLED` = AVISO |
| `daily_rds` | **Glue Trigger** | scheduling | `State != ACTIVATED` = AVISO |
| `rnmc-prod-source-to-rnmc-prod-target-fullload-2` | DMS | puntual | `stopped` = **INFO** (migración terminada), nunca FALLA |

> **Dos mecanismos de scheduling distintos**: `daily_rds` **no** es una EventBridge
> rule pese al nombre tipo cron; es un **Glue Trigger** (servicio y API distintos:
> `glue.get_trigger`, estado `ACTIVATED/DEACTIVATED/CREATED`, apunta directo a un job).
> Las `prod-data-*` sí son EventBridge rules (`events.describe_rule`, estado
> `ENABLED/DISABLED`, apuntan a Lambda). El nombre no indica cuál es cuál: se confirma
> con la API correspondiente.

### Lógica de "cadena rota" de la Lambda

La Lambda no tiene `get_job_runs`, así que se mira por métricas CloudWatch
(`AWS/Lambda Invocations`/`Errors`) y se **correlaciona** con el job `homogenize`:

| Escenario | Estado |
|-----------|--------|
| Invocada, 0 errores, `homogenize` con run reciente | **OK** (cadena sana) |
| Invocada, errores ≤ 3, pero `homogenize` **sí** corrió después | **AVISO** (hiccup tolerable) |
| Invocada, errores > 3 **o** (con errores y `homogenize` sin run nuevo pese a la invocación) | **FALLA** (cadena cortada) |
| Sin invocaciones en la ventana | **INFO** (normal: no llegó archivo) |

El umbral de errores es ajustable con `RNMC_LAMBDA_ERROR_THRESHOLD` (default 3). El
caso FALLA cubre explícitamente "la Lambda se rompió y cortó la cadena" (se invocó,
registró errores, y el job que debía disparar no corrió).

## Clasificación de errores (jobs fallidos)

Para los jobs en `FAILED`, lee el log del run con Logs Insights (1 patrón real):

| Regex | Causa | Acción |
|-------|-------|--------|
| `La vista NO contiene la columna requerida: id_hechos` | Schema drift: la vista Athena origen perdió `id_hechos` | Revisar/regenerar `pon-prod-raw-cnp.boletin_z1_capas_v2` |

Si no matchea, "Error no catalogado" con el renglón crudo, **sin inventar**.

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_rnmc.py
python3 monitor_rnmc.py --horas 48
python3 monitor_rnmc.py --con-avisos    # muestra avisos e info
python3 monitor_rnmc.py --sin-insights  # omite clasificación de errores
python3 monitor_rnmc.py --no-export
```

### Flags y env vars

| Flag / Env | Default | Qué hace |
|------------|---------|----------|
| `--horas N` | 24 | Ventana de actividad. |
| `--con-avisos` | — | Muestra AVISOS e INFO (evento S3, DMS, Lambda sin invocaciones). |
| `--sin-insights` | — | Omite la clasificación de errores por Logs Insights. |
| `--detallado` | — | Salida larga sin spinner. |
| `--no-export` | — | No escribe el CSV. |
| `RNMC_LAMBDA_ERROR_THRESHOLD` | 3 | Errores de la Lambda que escalan a FALLA. |

## Permisos IAM mínimos

> **Diferencia respecto a los demás scripts**: este es el primero que lee **métricas
> de CloudWatch** (`cloudwatch:GetMetricStatistics`), porque la Lambda no tiene
> `get_job_runs` y es la única forma de verla. También usa `events:DescribeRule`.

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetJobRuns",
    "glue:GetTrigger",
    "cloudwatch:GetMetricStatistics",
    "events:DescribeRule",
    "dms:DescribeReplicationTasks",
    "logs:StartQuery",
    "logs:GetQueryResults",
    "logs:StopQuery",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

## Salida

- **Consola**: resumen `TOTAL: N | x OK | y aviso/info | z falla` + detalle con la
  naturaleza entre corchetes. Muestra el destino Aurora como dato (no verificado).
- **CSV**: `salida/rnmc_<YYYYmmdd_HHMMSS>.csv` con columnas `recurso, naturaleza,
  ubicacion, estado, mensaje, accion_sugerida, tipo_error_detectado`.
- **Exit code**: `0` todo OK · `1` si algo falló · `2` args inválidos.

## Pendiente / notas

- **Verificación en Aurora** (`pon-prod-db-postgres`, tabla `public.hechos_z1_v2`):
  fuera de v1. Comprobar que la tabla tiene filas del día requeriría credenciales de
  BD y rompería el patrón solo-lectura-boto3. v1 solo mira que el job `RNMC-to-RDS`
  corrió OK (y asume que escribió). Queda como mejora futura.
- **VPN `ofite-bogota-*`**: los túneles compartidos que llevan el tráfico on-prem de
  RNMC muestran estado mixto (algunos DOWN). No es parte de este script, pero si el
  pipeline falla sin causa aparente en los jobs, ese es un buen lugar para mirar. Ver
  `docs-investigacion/VPN-DIAGNOSTICO.md`.
