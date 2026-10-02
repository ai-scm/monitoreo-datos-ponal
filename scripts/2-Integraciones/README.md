# 2 — Integraciones

Scripts de monitoreo de las **integraciones** del ecosistema. Ambos son pipelines de
datos **por lotes** (no servicios HTTP/API), y ambos resultaron más complejos de lo que
el nombre sugería: en cada uno apareció al menos un recurso que no estaba en el radar
inicial.

**Estado: los 2 puntos implementados.**

| # | Carpeta | Script | Qué monitorea | Frecuencia |
|---|---------|--------|---------------|------------|
| 12 | `12-Migracion-SIVICC/` | `monitor_migracion_sivicc.py` | Backfills/migración de SIVICC | diario |
| 13 | `13-Servicio-RNMC/` | `monitor_rnmc.py` | Pipeline por lotes RNMC (ingesta → homogenize → RDS) | diario |

Ambos son **solo lectura** y siguen el patrón común (ver el `README.md` raíz de
`scripts/`). El detalle de cada uno está en el `README.md` de su carpeta.

## 12 — Migración SIVICC

No es una sola cosa: es un conjunto de recursos en **etapas distintas** (naturalezas),
y la severidad se aplica según la naturaleza de cada uno:

- **REGIMEN** — `armamento-asignaciones-backfill` (ya mutó a ETL diario).
- **BACKFILL** manual — los dos `tamir-*` (FAILED=FALLA; sin correr solo escala a AVISO
  pasados 90 días).
- **ESTATICO** — `historico_personas` (crawl reciente = AVISO; debería estar congelado).
- **EXPERIMENTAL** — `dashboard-incremental-*` (solo INFO, en desarrollo activo).
- **DMS_DORMIDA** — full-load nunca corrida (INFO).

Falso positivo excluido a propósito: `V2_SIVICC-migration-to-RDS` **no es SIVICC** (es
CNP hacia Postgres), se omite para no monitorear el pipeline equivocado.

## 13 — Servicio RNMC

"Servicio" es un nombre engañoso: **no hay API**. Es un pipeline por lotes con jobs
Glue de distinta cadencia, una Lambda disparadora (con lógica de **cadena rota**
correlacionada con el job que dispara), reglas EventBridge y un **Glue Trigger**
(`daily_rds` — confirmado Glue, no EventBridge). El destino final es Aurora Postgres,
cuya verificación directa queda fuera de v1 (rompería el patrón solo-lectura-boto3).

`docs-investigacion/VPN-DIAGNOSTICO.md` documenta los túneles `ofite-bogota-*` como
pista de diagnóstico (fuera del script): si el pipeline se seca sin fallo aparente en
los jobs, ahí es donde mirar.

## Diferencia de permisos

RNMC es el primer script que lee **métricas de CloudWatch**
(`cloudwatch:GetMetricStatistics`) para ver la Lambda disparadora, y usa
`glue:GetTrigger` + `events:DescribeRule`. Ver su README para la política IAM completa.
