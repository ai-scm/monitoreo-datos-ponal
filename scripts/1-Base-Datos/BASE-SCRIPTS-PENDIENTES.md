# Base de Datos — Qué tenemos y base para los dos pendientes

Este documento resume **qué hace cada script**, el **patrón/ciclo de vida común** y el
**contrato de salida** de la categoría Base-Datos.

> **Estado: la categoría Base-Datos está COMPLETA.** Los 5 puntos (1, 2, 3, 4, 7) están
> implementados. Este doc conserva el histórico de diseño (incluidas las preguntas que
> en su momento resolvieron el alcance de `3-Refresh-Athena` y `4-Centralizacion-Logs`,
> hoy ya implementados) como referencia para futuros scripts de otras categorías.

---

## 1. Lo que tenemos hoy

Carpeta `1-Base-Datos/`:

| # | Carpeta | Script | Qué monitorea | Frecuencia | Escribe algo |
|---|---------|--------|---------------|------------|--------------|
| 1 | `1-GlueJobs-DBs-Diario/` | `monitor_jobs.py` | Estado del **último run** de los 8 jobs de Glue; clasifica el error de los fallidos leyendo su log | diario | no (solo lectura) |
| 2 | `2-Estados-DMS-Diario/` | `monitor_dms.py` | Estado de las **replication tasks de DMS**; clasifica fallidas/detenidas y relaciona secretos | diario | no (solo lectura) |
| 7 | `7-Conexiones-DBs-DMS/` | `monitor_conexion.py` | **Cambios de IP/host/puerto/credenciales** de las bases + ping TCP opcional | 1–2 semanas | snapshot JSON en `$HOME` |
| 3 | `3-Refresh-Athena/` | `monitor_athena_refresh.py` | **Refresh de bases Athena** vía el último crawl de los crawlers `*_discoverer` | diario | no (solo lectura) |
| 4 | `4-Centralizacion-Logs/` | `monitor_logs.py` | **Higiene de log groups** Glue/DMS + estado de centralización (informativo) | diario | no (solo lectura) |

### Qué hace cada uno con detalle

- **1 — monitor_jobs.py**: recorre un mapa fijo `JOB_NAME -> DatabaseName` (8 jobs
  `pon-prod-raw-*_etl`). Para cada job pide `glue.get_job_runs(MaxResults=1)` (el
  último run). Clasifica: `SUCCEEDED` = OK (AVISO si quedó fuera de la ventana de
  horas), en curso = AVISO, fallo (`FAILED/TIMEOUT/ERROR/STOPPED`) = FALLA. Para los
  fallos lee el log del run con **CloudWatch Logs Insights** sobre
  `/aws-glue/jobs/error` y busca códigos `ORA-#####` (tabla `ORA_DESCRIPCIONES`) o
  patrones Spark/Glue (OOM, ExecutorLost, etc.).

- **2 — monitor_dms.py**: `dms.describe_replication_tasks()` (paginado por `Marker`).
  Las `failed/stopped` se analizan; el resto cuenta como OK. `classify_error()` mapea
  el `LastFailureMessage` a un tipo + severidad (`CRITICAL/HIGH/MEDIUM/INFO`) que luego
  se traduce a `OK/AVISO/FALLA`. Relaciona cada tarea con sus secretos por **sigla**
  (lista `SIGLAS`) y reporta su antigüedad.

- **7 — monitor_conexion.py**: descubre endpoints desde **Glue Connections** (regex del
  JDBC URL) y **DMS endpoints** Oracle. Revisa antigüedad de credenciales en Secrets
  Manager. Guarda un **snapshot JSON en `$HOME`** y lo compara con la corrida anterior
  para detectar cambios de host/puerto/credencial. Ping TCP opcional y auto-detectado
  (solo con ruta on-premise / VPC environment).

---

## 2. Patrón / ciclo de vida común (todos lo cumplen)

Cualquier script nuevo debe seguir **exactamente** este molde:

1. **Cabecera y flags**: docstring explicando qué hace, cuándo corre y uso en
   CloudShell. `argparse` con, como mínimo: `--con-avisos`, `--detallado`,
   `--no-export`.
2. **Config por entorno**: `REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")`
   y un `BOTO_CONFIG = Config(connect_timeout=5, read_timeout=20, retries={"max_attempts": 2})`.
   Umbrales ajustables por variable de entorno.
3. **`check_identity()`**: valida `sts.get_caller_identity()` antes de todo. Si falla,
   imprime resumen y devuelve exit code 1.
4. **Chequeo principal**: lee el recurso (solo lectura por defecto), paginando donde
   aplique, y llama a `record(...)` por cada hallazgo.
5. **Clasificación**: traduce el estado real del recurso a `OK / AVISO / FALLA`
   (+ acción sugerida cuando no es OK).
6. **`print_summary()`**: cabecera, línea `TOTAL: N | x OK | y aviso | z falla`, y
   detalle (solo FALLA por defecto; FALLA+AVISO con `--con-avisos`).
7. **`export()`**: CSV en `salida/` con timestamp, `utf-8-sig`, y pista de descarga
   para CloudShell. Se omite con `--no-export`.
8. **Exit code**: `0` todo OK, `1` si hay alguna FALLA, `2` si args inválidos.

Helpers compartidos que se copian tal cual: `paso()` (spinner/progreso),
`record()`, `_err()`, `_err_detail()`, iconos `ICON`, constantes `OK/WARN/FAIL`.

> Nota: hoy cada script **duplica** estos helpers (no hay `common/`). Mantener esa
> independencia salvo que decidamos lo contrario.

---

## 3. Contrato de salida (lo que deben producir los dos nuevos)

### 3.1 En consola

```
==============================================================================
MONITOR DE <LO QUE SEA> (solo lectura)
==============================================================================
  Region us-east-1  |  <parametros relevantes>

  ✓ Identidad AWS  (cuenta 123456789012)
  ✓ <chequeo>  (N revisados, M con aviso/falla)

==============================================================================
RESUMEN — <TITULO>
==============================================================================
  TOTAL: 10 verificaciones | 8 OK | 1 aviso | 1 falla

  Detalle de lo CRITICO:

  [FALLA] <recurso>
       que pasa : <mensaje>
       donde    : <ubicacion>
       accion   : <accion sugerida>
```

### 3.2 En CSV (`salida/<nombre>_<YYYYmmdd_HHMMSS>.csv`)

Columnas mínimas (idénticas a jobs/conexión):

```
recurso, ubicacion, estado, mensaje, accion_sugerida
```

Si el chequeo tiene una dimensión extra de severidad (como DMS), se añade una
columna propia (ej. `severidad_dms`) pero se conservan las anteriores.

### 3.3 Estados

- **OK**: el recurso corrió/está como se espera.
- **AVISO**: algo a vigilar pero no roto (fuera de ventana, parada manual,
  informativo). Oculto por defecto, visible con `--con-avisos`, siempre en el CSV.
- **FALLA**: roto / requiere acción. Siempre visible. Fuerza exit code 1.

---

## 4. Definición final de los dos puntos (ya implementados)

Ambos quedaron escritos como **un solo script Python por punto**, autocontenido,
siguiendo el molde de la sección 2 y el contrato de la sección 3. Detalle completo
en el `README.md` de cada carpeta.

### 4.1 — `3-Refresh-Athena/monitor_athena_refresh.py` ✅

En la cuenta **no hay refresh nativo programado de Athena**. Lo que mantiene
actualizadas las bases `pon-prod-raw-*` es el **crawl de Glue** de los crawlers
`pon-prod-raw-*_discoverer`. El script verifica el **último crawl** de cada uno.

- **Qué revisa**: `get_crawler → LastCrawl` sobre un **mapa fijo de 8 crawlers**
  (1:1 con los jobs ETL) + el extra `pon-prod-raw-sivicc_discoverer`
  (toggle `--sin-sivicc`).
- **Clasificación**: `SUCCEEDED` en ventana → OK · `SUCCEEDED` fuera de ventana,
  `RUNNING`, sin crawls, o crawler inexistente → AVISO · `FAILED`/`CANCELLED` → FALLA.
- **siedco**: nombre de crawler no confirmado en el barrido → se maneja con AVISO
  ("nombre por confirmar") sin romper. Ajustar en `CRAWLERS` cuando se confirme.
- **Ventana**: 24 h, env `ATHENA_REFRESH_WINDOW_HOURS`.
- **Solo lectura** (`glue:GetCrawler`, `sts`). No dispara crawls.
- **Fuera de v1 (pendiente)**: pipeline Iceberg de sivicc (CTAS+MERGE manual, sin
  orquestador), partición de "homicidios" (S3-triggered).
  `prod-data-reload-database` descartado (es Postgres/Oracle, no Athena).

### 4.2 — `4-Centralizacion-Logs/monitor_logs.py` ✅

En la cuenta **no existe pipeline de centralización** (0 subscription filters, 0
destinations, ningún S3/Firehose recibiendo logs de Glue/DMS). El script hace
**higiene de log groups** y reporta la ausencia de centralización como bloque
**informativo**, no como FALLA.

- **Qué revisa**: `describe_log_groups` por prefijo (`/aws-glue/`, `dms-tasks-`) y por
  cada grupo: actividad reciente, retención y tamaño sin retención. Más cobertura de
  logging DMS (`EnableLogging == false`) y conteo de subscription filters.
- **Clasificación**: todas las higienes son **AVISO** (actividad > 24 h, retención
  `null` o < 30 d, tamaño > 5 GB sin retención, DMS sin logging, 0 centralización).
  **FALLA** reservada para errores de acceso/listado → exit code 1 solo en ese caso.
- **`testconnection/*`** (~40): se **resumen en una sola fila** (ruido).
- **Umbrales (env)**: `LOGS_ACTIVITY_WINDOW_HOURS=24`, `LOGS_MIN_RETENTION_DAYS=30`,
  `LOGS_SIZE_WARN_GB=5`.
- **Solo lectura** (`logs:DescribeLogGroups/LogStreams/SubscriptionFilters`,
  `dms:DescribeReplicationTasks`, `sts`).
- **Brecha de negocio**: si se esperaba centralización real, es una brecha de
  arquitectura a resolver aparte; el script solo la señala (AVISO).

---

## 5. Estado

Los cinco puntos de `1-Base-Datos/` están implementados (1, 2, 3, 4, 7), cada uno
autocontenido con su `README.md`. El foco de desarrollo pendiente pasa a
**Integraciones** y **Chatbot**.
