# C — Monitor de Tareas DMS

`monitor_dms.py` revisa las **replication tasks de DMS**, detecta las que fallaron
o están detenidas, clasifica el error y lo resume. **Solo lectura.**

- **Frecuencia**: diario.
- **No** revisa jobs de Glue (script **B**) ni hace ping a las bases (script **A**).

## Qué hace

1. **Identidad** (`sts:GetCallerIdentity`).
2. **Estado de todas las tareas** (`dms:DescribeReplicationTasks`): las
   `failed`/`stopped` se analizan; el resto (running, etc.) se cuentan como OK.
3. **Clasificación del error** de cada tarea con problema, con tipo + severidad +
   acción sugerida:
   - `AUTHENTICATION`, `FATAL_AFTER_RETRIES` → **CRITICAL** → FALLA
   - `RESOURCE_EXHAUSTION`, `ORACLE_CONNECTION`, `CONNECTIVITY`, `RECOVERABLE_EXHAUSTED` → **HIGH** → FALLA
   - `SCHEMA_MISMATCH`, `DATA_INTEGRITY`, `UNKNOWN` → **MEDIUM** → AVISO
   - `FULL_LOAD_FINISHED`, `MANUAL_STOP` → **INFO** → AVISO
4. **Secretos relacionados**: por la sigla de la tarea (ej. `mebuc`, `siedco`),
   busca el secreto correspondiente y muestra su antigüedad (marca `[>365d]` si
   supera el umbral).

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1
python3 monitor_dms.py                # solo lo crítico (CRITICAL/HIGH)
python3 monitor_dms.py --con-avisos   # incluye MEDIUM/INFO (paradas manuales, full load fin, schema)
python3 monitor_dms.py --detallado    # salida larga
python3 monitor_dms.py --no-export    # sin CSV
```

## Salida

- Consola: progreso mínimo + **RESUMEN** con severidad. Por defecto solo se listan
  las tareas en **CRITICAL/HIGH** (FALLA); los avisos (MEDIUM/INFO) se cuentan y
  van al CSV, y se muestran con `--con-avisos`.
- CSV en `2-estados-DMS-Diario/salida/dms_<fecha>.csv` (incluye la severidad DMS original).

## Exit code

- `0` — ninguna tarea en CRITICAL/HIGH.
- `1` — al menos una tarea crítica (útil para alertas/cron).
- `2` — argumentos inválidos.

## Permisos IAM (solo lectura)

- `sts:GetCallerIdentity`
- `dms:DescribeReplicationTasks`
- `secretsmanager:ListSecrets` (para relacionar tareas con secretos por sigla)

## Nota

El clasificador se basa en el `dms_daily_check_v2` original, adaptado al formato
común (severidad OK/AVISO/FALLA, resumen, CSV, exit code) de los scripts A y B.
Las siglas de bases/unidades están en la constante `SIGLAS` del script; si aparece
una nueva unidad, agrégala ahí.
