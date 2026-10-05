# monitoreo-datos-ponal — Scripts de CloudShell

Scripts de monitoreo **independientes y autocontenidos** (cada uno se sube suelto a
CloudShell y se corre con `python3`). Todos de **solo lectura** salvo donde se indique.

**Patrón común a todos**:
- Config por entorno (`AWS_DEFAULT_REGION`, umbrales por env var).
- `check_identity()` (STS) antes de cualquier chequeo.
- Estados homogéneos: `OK` / `AVISO` / `FALLA` (más `INFO`, que se agrupa con AVISO).
- Resumen final con `TOTAL: N | x OK | y aviso | z falla`.
- CSV con timestamp en `salida/`.
- Exit code: `0` todo OK · `1` si hubo FALLA · `2` args inválidos.
- Flags comunes: `--con-avisos`, `--detallado`, `--no-export` (y `--horas` donde aplica).

Cuenta de referencia: **926162397524**, región **us-east-1**.

## Organización por categorías

```text
scripts/
│
├── 1-Base-Datos/
│   ├── 1-GlueJobs-DBs-Diario/        monitor_jobs.py              ✅ implementado
│   ├── 2-Estados-DMS-Diario/         monitor_dms.py               ✅ implementado
│   ├── 3-Refresh-Athena/             monitor_athena_refresh.py    ✅ implementado
│   ├── 4-Centralizacion-Logs/        monitor_logs.py              ✅ implementado
│   ├── 7-Conexiones-DBs-DMS/         monitor_conexion.py          ✅ implementado
│   ├── 15-Frescura-Datos/            monitor_frescura_datos.py    ⛔ diseñado (bloqueado: falta permiso Athena)
│   └── BASE-SCRIPTS-PENDIENTES.md    (doc de diseño de la categoría)
│
├── 2-Integraciones/
│   ├── 12-Migracion-SIVICC/          monitor_migracion_sivicc.py  ✅ implementado
│   ├── 13-Servicio-RNMC/             monitor_rnmc.py              ✅ implementado
│   └── docs-investigacion/           (hallazgos transversales: VPN, arquitectura)
│
├── 3-Prediccion/                     (desarrollo en el otro equipo)
│   ├── 5-Ejecucion-Prediccion/
│   ├── 8-Zonas-Atencion-Fusion/
│   ├── 9-Scripts-Adicionales/
│   └── 14-Anticipacion-Demanda/
│
├── 4-Planta-Nacional/                (desarrollo en el otro equipo)
│   └── 6-Carga-Planta-Nacional/
│
├── 5-Chatbot/
│   ├── 10-Carga-Base-Conocimiento/        monitor_carga_kb.py                     ✅ implementado
│   └── 11-Auditoria-Conversaciones-Glue/  monitor_auditoria_conversaciones_glue.py ✅ implementado
│
└── README.md
```

Cada punto sigue siendo **independiente**. No hay `common/`, `utils/` ni `core/`
compartidos: los helpers (`paso`, `record`, clasificación, etc.) se duplican a
propósito para que cada script sea autocontenido.

## Estado por categoría

| Categoría | Puntos | Estado |
|-----------|--------|--------|
| **1 — Base de Datos** | 1, 2, 3, 4, 7 | ✅ los 5 implementados |
| **1 — Base de Datos (extra)** | 15 | ⛔ diseñado, bloqueado por permiso Athena |
| **2 — Integraciones** | 12, 13 | ✅ los 2 implementados |
| **3 — Predicción** | 5, 8, 9, 14 | 🖥️ en el otro equipo |
| **4 — Planta Nacional** | 6 | 🖥️ en el otro equipo |
| **5 — Chatbot** | 10, 11 | ✅ los 2 implementados |

---

## Catálogo de scripts

### 1 — Base de Datos

| # | Script | Qué monitorea | Frecuencia | Notas |
|---|--------|---------------|------------|-------|
| 1 | `monitor_jobs.py` | Último run de los 8 jobs Glue de las bases; clasifica el error de los fallidos (ORA / Spark) leyendo el log | diario | solo lectura |
| 2 | `monitor_dms.py` | Replication tasks de DMS; clasifica fallidas/detenidas por severidad y relaciona secretos | diario | solo lectura |
| 3 | `monitor_athena_refresh.py` | Refresh de bases Athena vía el último crawl de 12 crawlers (de 68); 56 fuera a propósito | diario | solo lectura |
| 4 | `monitor_logs.py` | Higiene de log groups Glue/DMS/Lambda + clasificación de errores + localización del recurso | diario | solo lectura; usa Logs Insights |
| 7 | `monitor_conexion.py` | Cambios de IP/host/puerto/credenciales de las bases + ping TCP opcional (incluye endpoints fijos SIVICC/RNMC on-premise) | 1–2 semanas | escribe snapshot en `$HOME` |
| 15 | `monitor_frescura_datos.py` | **Frescura de datos** (`MAX(fecha)` vía Athena) de 4 bases confirmadas; 8 pendientes de confirmar | diario | ⛔ **bloqueado**: requiere `athena:*` + `ATHENA_OUTPUT_S3` |

### 2 — Integraciones

| # | Script | Qué monitorea | Frecuencia | Notas |
|---|--------|---------------|------------|-------|
| 12 | `monitor_migracion_sivicc.py` | Backfills/migración SIVICC, clasificados por **naturaleza** (régimen / backfill / estático / experimental / DMS dormida) | diario | solo lectura; excluye `V2_SIVICC-migration-to-RDS` (es CNP) |
| 13 | `monitor_rnmc.py` | Pipeline por lotes RNMC (sin API): jobs Glue, Lambda (cadena rota), reglas EventBridge, Glue Trigger `daily_rds`, DMS | diario | solo lectura; usa `cloudwatch:GetMetricStatistics` |

### 5 — Chatbot

| # | Script | Qué monitorea | Frecuencia | Notas |
|---|--------|---------------|------------|-------|
| 10 | `monitor_carga_kb.py` | Ingesta de las 2 KBs de Bedrock del chatbot (de 4); carga **manual**, no se evalúa frescura | diario | solo lectura; usa `bedrock-agent:*` |
| 11 | `monitor_auditoria_conversaciones_glue.py` | Job Glue de auditoría de conversaciones (DynamoDB→Parquet) + su Glue Trigger | diario | solo lectura; **no** sincroniza la KB (choque de nombres) |

---

## Hallazgos de infraestructura (contexto, no todo se monitorea)

Hallazgos de las investigaciones de la cuenta que **no** necesariamente se traducen en
un chequeo de script (son contexto de arquitectura, dependencias de red o huecos a
construir). Documentados para no re-investigarlos. Detalle en los `docs-investigacion/`.

| Hallazgo | Dónde se documenta | ¿En un script? |
|----------|--------------------|----------------|
| **VPN on-premise crítica** — 7/8 túneles `ofite-bogota-*` caídos; punto único de falla para SIVICC, RNMC y SECAD | `2-Integraciones/docs-investigacion/VPN-ONPREMISE-ESTADO.md` | ⚠️ indirecto: ping TCP de endpoints fijos en el punto 7 (solo en VPC env) |
| **`RNMC-to-RDS` es job de Glue, no DMS** | `2-Integraciones/docs-investigacion/HALLAZGOS-INTEGRACIONES.md` | ✅ ya correcto en `monitor_rnmc.py` |
| **Endpoint DMS huérfano** `pon-prod-db-postgres-instance-1` (sin uso) | idem | ❌ no (sin uso) |
| **Paso `RDS → S3` no existe** (hueco de arquitectura a construir) | idem | ❌ no existe |
| **SECAD** — fan-out de ~60 tareas DMS; `mesan-secad` en `FATAL_ERROR` | idem | ❌ punto futuro (patrón `monitor_dms.py` ampliado) |
| **Lambda PBX rota** `pon-prod-pbx-auditoria-procesados-files` (`NameError`, dominio PBX) | `1-Base-Datos/4-Centralizacion-Logs/docs-investigacion/LAMBDA-PBX-ROTA.md` | ❌ no (otro dominio; sería monitor propio de PBX) |
| **No hay centralización de logs** (0 Kinesis/Firehose/subscription filters) | idem | ✅ `monitor_logs.py` lo reporta como informativo |

### Decisiones pendientes de contexto externo

- **Permiso Athena** (`athena:StartQueryExecution` + S3): necesario para activar el
  punto 15 (frescura de datos). Hoy el rol no lo tiene → el script sale BLOQUEADO.
- **Columnas de fecha de 8 bases**: pendientes de confirmar con el equipo de datos
  antes de ampliar el punto 15 más allá de las 4 confirmadas (SIEDCO, SECAD, ALI, SIMEP).
- **Turnos SIVICC** (validaciones sobre `usr_mnvcc.turno`): en espera; la base
  `pon-prod-raw-sivicc-incremental-shadow` es muy nueva (inestable), mismo criterio que
  los recursos "experimentales".
- **DynamoDB `pon-prod-expertopol-Document-inventory`**: bloqueada por aprobación de
  `describe_table`; cuando se resuelva se decide si alimenta un chequeo del punto 10.

---

## Convenciones de severidad

- **OK** — el recurso corrió/está como se espera.
- **AVISO** — algo a vigilar que no está roto (fuera de ventana, parada manual,
  indexación parcial, retención fuera de rango). Oculto por defecto; visible con
  `--con-avisos`; siempre en el CSV.
- **FALLA** — roto / requiere acción. Siempre visible. Fuerza exit code 1.
- **INFO** — informativo (recurso dormido, estático, experimental). Se agrupa con
  AVISO para la severidad, pero se etiqueta aparte en el mensaje.

Principio transversal aplicado en varios scripts: **un recurso dormido no es un
recurso roto**. Ejemplos: `historico_personas` estático (SIVICC), DMS detenidas
(SIVICC/RNMC), KB inactiva (Chatbot). Se reportan como INFO, no como FALLA.

## Lecciones de diseño aprendidas (aplican a futuros scripts)

- **No asumir el mecanismo de scheduling por el nombre**: `daily_rds` parecía
  EventBridge por el estilo cron, pero es un **Glue Trigger** (`glue.get_trigger`,
  estado `ACTIVATED`). EventBridge usa `events.describe_rule` (estado `ENABLED`). Se
  confirma con la API correcta, no por el nombre.
- **Clasificar errores solo con evidencia real**: los patrones regex de cada script
  salen de errores observados en la cuenta, no inventados. Donde no hay muestra, se usa
  clasificación genérica o "no catalogado — revisar manualmente".
- **La severidad depende de la naturaleza del recurso**: un backfill manual sin
  correr no es lo mismo que un ETL de régimen sin correr; una carga manual (KB) no
  tiene "frescura" que vigilar. Cada script modela esto explícitamente.
- **Nombres de puntos pueden no reflejar la realidad**: "Sincronización Glue" resultó
  no tener ningún Glue sincronizando la KB; se renombró a "Auditoría-Conversaciones-Glue".

## Cómo correrlos (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

# ejemplo: estado de jobs Glue (diario)
python3 monitor_jobs.py

# cualquier script acepta --con-avisos para ver avisos e info, y --no-export
python3 monitor_carga_kb.py --con-avisos
```

Cada carpeta tiene su propio `README.md` con el detalle de flags, permisos IAM
mínimos, salida y ejemplos. Las carpetas `docs-investigacion/` guardan inventarios,
hallazgos y pistas de diagnóstico (p. ej. la nota de VPN en RNMC).

---

Los scripts y artefactos anteriores (lambdas de producción, IAM, docs, tests viejos,
`CloudshellUnificado/`) quedaron en `../scripts-antiguos/` como referencia.
