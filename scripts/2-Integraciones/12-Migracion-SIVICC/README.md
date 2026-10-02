# 12 — Migración SIVICC (`monitor_migracion_sivicc.py`)

Corre **a diario**. La "migración SIVICC" **no es una sola cosa**: es un conjunto de
backfills históricos ad-hoc + un ETL que ya mutó a régimen + una carga estática ya
terminada + una tarea DMS dormida, cada recurso en una **etapa (naturaleza)** distinta.
Por eso la severidad se aplica **según la naturaleza** del recurso, no con una regla
única.

**Solo lectura.** No dispara jobs ni tareas.

## Inventario y naturaleza de cada recurso

| Recurso | Tipo | Naturaleza | Regla de severidad |
|---------|------|------------|--------------------|
| `pon-prod-raw-sivicc-armamento-asignaciones-backfill` | Job Glue | **REGIMEN** (ya diario) | `SUCCEEDED` en 24 h = OK · fuera de ventana = AVISO · `FAILED` = FALLA |
| `pon-prod-raw-sivicc-tamir-actividad-backfill` | Job Glue | **BACKFILL** manual | `FAILED` = FALLA · `SUCCEEDED` (aunque viejo) = OK · sin corrida = dato; AVISO si > 90 d |
| `pon-prod-raw-sivicc-tamir-cumplimiento-staging-backfill` | Job Glue | **BACKFILL** manual | igual que el anterior |
| `historico_personas` (S3 + `pon-prod-sivicc-historico-personas-crawler`) | Carga única | **ESTATICO** | OK si no cambió · **crawl reciente = AVISO** (debería estar congelado) |
| `pon-prod-raw-sivicc-dashboard-incremental-scheduler` / `-job` | Jobs Glue | **EXPERIMENTAL** | solo **INFO**, nunca FALLA/AVISO (en desarrollo activo) |
| `pon-prod-sivicc-historico-persona-to-s3-full-load` | Tarea DMS | **DMS_DORMIDA** | **INFO** (ready 0%, nunca corrió a propósito), nunca FALLA |

### Decisiones de diseño clave

- **Backfill manual sin corrida reciente**: por diseño es irregular, así que **no**
  escala a AVISO por defecto. Solo si pasa de **90 días** sin correr (y no migró a
  régimen) → AVISO ("posible backfill olvidado/abandonado"). Ajustable con
  `SIVICC_BACKFILL_STALE_DAYS`. Siempre reporta la antigüedad como dato.
- **Experimental**: `dashboard-incremental-*` se creó/modificó muy reciente y su
  criterio de éxito aún no está definido. Tratarlo como job normal daría FALLAs falsas
  cuando cambie su firma o cadencia. Se deja **solo informativo** hasta que se asiente;
  entonces se promueve a una naturaleza real (REGIMEN o BACKFILL).
- **Estático**: `historico_personas` debe quedar congelado desde 2026-01-09. Un crawl
  reciente ahí es señal **rara** (no buena) → AVISO.

### Falso positivo excluido

`V2_SIVICC-migration-to-RDS` **NO es SIVICC** pese al nombre: mueve datos de **CNP**
hacia Postgres usando el rol compartido `pon-prod-rnmc-migration-role`. Se **excluye**
para no monitorear el pipeline equivocado.

## Clasificación de errores (jobs fallidos)

Para los jobs en `FAILED`, lee el log del run con CloudWatch Logs Insights y clasifica
por regex (3 patrones validados con evidencia real):

| Regex | Causa | Acción |
|-------|-------|--------|
| `java.lang.String cannot be cast to java.lang.Boolean` | Bug de configuración/tipo | Revisar el parámetro que espera booleano |
| `RuntimeError: La extracción no contiene tamir_actividad_id o fecha` | Guard de datos faltantes (extracción vacía) | Verificar la fuente de la extracción |
| `upgrading to Spark >= 3.0` / `o<n>.parquet` | Incompatibilidad de escritura Parquet | Ajustar versión de Spark / opciones Parquet |

Si no matchea, "Error no catalogado" con el renglón crudo, **sin inventar**.

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_migracion_sivicc.py
python3 monitor_migracion_sivicc.py --horas 48
python3 monitor_migracion_sivicc.py --con-avisos    # muestra avisos e info
python3 monitor_migracion_sivicc.py --sin-insights  # omite clasificación de errores
python3 monitor_migracion_sivicc.py --no-export
```

### Flags y env vars

| Flag / Env | Default | Qué hace |
|------------|---------|----------|
| `--horas N` | 24 | Ventana para los jobs de régimen. |
| `--con-avisos` | — | Muestra AVISOS e INFO (backfills, estático, experimental, DMS dormida). |
| `--sin-insights` | — | Omite la clasificación de errores por Logs Insights. |
| `--detallado` | — | Salida larga sin spinner. |
| `--no-export` | — | No escribe el CSV. |
| `SIVICC_BACKFILL_STALE_DAYS` | 90 | Días sin corrida que suben un backfill manual a AVISO. |

## Permisos IAM mínimos

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetJobRuns",
    "glue:GetCrawler",
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
  naturaleza entre corchetes (`[REGIMEN]`, `[BACKFILL]`, etc.).
- **CSV**: `salida/migracion_sivicc_<YYYYmmdd_HHMMSS>.csv` con columnas `recurso,
  naturaleza, ubicacion, estado, mensaje, accion_sugerida, tipo_error_detectado`.
- **Exit code**: `0` todo OK · `1` si algo falló · `2` args inválidos.

## Pendiente / notas

- Cuando `dashboard-incremental-*` se asiente (unas semanas), promoverlo de
  EXPERIMENTAL a REGIMEN o BACKFILL ajustando `JOBS_SIVICC`.
- La DMS dormida se reporta como INFO; si algún día debe migrar, se dispara aparte.
