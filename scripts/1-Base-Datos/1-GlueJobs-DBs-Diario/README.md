# B — Monitor de Jobs de Glue (bases Oracle On-Premise)

`monitor_jobs.py` revisa el **estado del último run** de cada job de Glue de las 8
bases Oracle, clasifica los que fallaron y lo resume. **Solo lectura.**

- **Frecuencia**: diario.
- **Ventana**: últimas 24 h por defecto (`--horas`).
- **No** revisa conexión a las bases (eso es el script **A**) ni tareas DMS (script **C**).

## Qué hace

1. **Identidad** (`sts:GetCallerIdentity`) — con qué rol corres.
2. **Estado del último run** de cada job (`glue:GetJobRuns`, MaxResults=1):
   - `SUCCEEDED` → OK (o AVISO si el último run quedó fuera de la ventana → puede que no haya corrido).
   - `RUNNING`/en curso → AVISO.
   - sin corridas → AVISO.
   - `FAILED`/`TIMEOUT`/`ERROR`/`STOPPED` → **FALLA**.
3. **Clasificación de los fallidos**: para cada run FAILED, lee su log con
   CloudWatch Logs Insights (`logs:StartQuery`/`GetQueryResults`) y clasifica el
   error por **código Oracle** (ORA-XXXXX, con descripción) o **error Glue/Spark**
   (OOM, executor perdido, contenedor terminado, exit code, conexión cerrada, Py4J).

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1
python3 monitor_jobs.py                 # últimas 24 h, solo lo crítico
python3 monitor_jobs.py --horas 48
python3 monitor_jobs.py --con-avisos    # muestra también avisos (RUNNING, sin corridas, fuera de ventana)
python3 monitor_jobs.py --detallado     # salida larga, sin progreso dinámico
python3 monitor_jobs.py --no-export     # sin CSV
```

## Salida

- Consola: progreso mínimo mientras corre y un **RESUMEN** con severidad.
  Por defecto solo se listan los **jobs fallidos** (crítico); los avisos se
  cuentan y van al CSV, y se muestran con `--con-avisos`.
- CSV en `1-GlueJobs-DBs-Diario/salida/jobs_<fecha>.csv`.

## Exit code

- `0` — ningún job falló.
- `1` — al menos un job falló (útil para enganchar a una alerta/cron).
- `2` — argumentos inválidos.

## Permisos IAM (solo lectura)

- `sts:GetCallerIdentity`
- `glue:GetJobRuns`
- `logs:StartQuery`, `logs:GetQueryResults` (para clasificar el error del run fallido)

## Jobs monitoreados

| Base | JOB_NAME |
|------|----------|
| SIVIC | `pon-prod-raw-sivicc_etl` |
| SIEDCO | `pon-prod-raw-siedco_etl` |
| DISEC | `pon-prod-raw-disec_etl` |
| RNMC | `pon-prod-raw-rnmc-incremental_etl` |
| GEOPOL | `pon-prod-raw-geopol_etl` |
| TELEPOL | `pon-prod-raw-telepol_etl` |
| DBOPER | `pon-prod-raw-dboper_etl` |
| ALI | `pon-prod-raw-ali_etl` |

Variables de entorno opcionales: `MONITOR_HORAS` (equivale a `--horas`),
`GLUE_ERROR_LOG_GROUP` (por defecto `/aws-glue/jobs/error`).

---

## Consultas de CloudWatch Logs Insights

El script usa Logs Insights para clasificar el error de un job **cuando su último
run falló**. Aquí quedan documentadas la query que ejecuta el script y otras
relacionadas que puedes correr **manualmente** en la consola.

Cómo correrlas a mano: Consola AWS → CloudWatch → Logs → **Logs Insights** →
selecciona el log group `/aws-glue/jobs/error`, elige el rango de tiempo, pega la
query y **Run query**.

### 1. La que usa el script (error de un run fallido)

Cuando un job aparece `FAILED`, el script toma su `JobRunId` (el `Id` del run) y
lanza esta consulta para traer las líneas de error de **ese** run y clasificarlas.
Reemplaza `<JOB_RUN_ID>` por el id real (ej. `jr_c8062676...`):

```
fields @message
| filter @logStream like /<JOB_RUN_ID>/
| filter @message like /ORA-/ or @message like /Exception/ or @message like /ERROR/
| sort @timestamp desc
| limit 50
```

El script recorre el resultado y clasifica: primero busca un **código Oracle**
(`ORA-XXXXX`, con descripción); si no, un **patrón de Glue/Spark** (OOM, executor
perdido, contenedor terminado, exit code, conexión cerrada, Py4J); si no encaja en
nada, lo reporta como "Error no clasificado" con la primera línea como pista.

### 2. Conteo de errores por código Oracle (todo el log group)

Panorama rápido de qué códigos Oracle aparecen y cuántas veces, agregado en el
servidor (no infla por multi-línea):

```
fields @message
| parse @message /(?<ora>ORA-\d{3,5})/
| filter ispresent(ora)
| stats count(*) as total by ora
| sort total desc
```

### 3. Resolver el JOB_NAME de un JobRunId

El `JobRunId` no dice a qué job/base pertenece; el nombre está en la línea de
arranque. Sirve tanto para Glue 4.0 (`PrepareLaunch`) como 5.0 (`GlueBootstrap`):

```
fields @message, @logStream
| filter @message like /--JOB_NAME/
| parse @message /--JOB_NAME\s+(?<job>\S+)/
| filter ispresent(job)
| limit 5
```

### 4. Ver todo el detalle de un run puntual

Para investigar a fondo un run (todas sus líneas, ordenadas):

```
fields @timestamp, @message
| filter @logStream like /<JOB_RUN_ID>/
| sort @timestamp asc
| limit 200
```

> Nota: el filtro `@message like /ERROR/` de la query 1 puede traer líneas
> informativas que contienen la palabra "error"; por eso el script prioriza primero
> los códigos `ORA-` y los patrones específicos de Glue/Spark antes de caer en
> "Error no clasificado".
