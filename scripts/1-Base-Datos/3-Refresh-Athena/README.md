# 3 — Refresh Athena (`monitor_athena_refresh.py`)

Verifica, **a diario**, que las bases Athena `pon-prod-raw-*` se estan
**refrescando**. En esta cuenta no hay refresh nativo programado de Athena: lo que
mantiene actualizadas esas bases es el **crawl de Glue** de los crawlers de la
familia raw (`pon-prod-raw-*_discoverer` y afines). Por eso el script revisa el
**estado del ultimo crawl** de cada uno (`get_crawler → LastCrawl`).

De los **68 crawlers** del inventario de la cuenta, se monitorean **12** (11 fijos +
sivicc toggleable); los otros **56 quedan fuera a propósito** (migraciones/backfill,
BI/reporting, apps de negocio, PBX, GPS, pruebas, downstream de otros equipos).

**Solo lectura.** No dispara crawls ni queries.

## Qué revisa

1. **Identidad AWS** (`sts:GetCallerIdentity`): con qué rol corre.
2. **Último crawl de cada crawler** monitoreado (11 fijos + `sivicc` extra):

| # | Crawler | Base Athena | Nota |
|---|---------|-------------|------|
| 1 | `pon-prod-raw-ali_discoverer` | `pon-prod-raw-ali` | Core (`ali_etl`). Ver nota ali abajo. |
| 2 | `pon-prod-raw-cnp_discoverer` | `pon-prod-raw-cnp` | Core (`cnp_etl`). |
| 3 | `pon-prod-raw-dboper_discoverer` | `pon-prod-raw-dboper` | Core (`dboper_etl`). |
| 4 | `pon-prod-raw-disec_discoverer` | `pon-prod-raw-disec` | Core (`disec_etl`). |
| 5 | `pon-prod-raw-geopol_discoverer` | `pon-prod-raw-geopol` | Core (`geopol_etl`). |
| 6 | `pon-prod-raw-rnmc-incremental_discoverer` | `pon_prod_raw_comparendo` | Core (`rnmc-incremental_etl`). **v1, el real** (no el `_v2`). |
| 7 | `pon-prod-raw-siedco_discoverer` | `pon_prod_siedco_raw` | Core (`siedco_etl`). |
| 8 | `pon-prod-raw-simep_discoverer` | `pon-prod-raw-simep` | Core (`simep_etl`). Suele estar **never run** → AVISO. |
| 9 | `pon-prod-raw-telepol_discoverer` | `pon-prod-raw-telepol` | Familia raw (`telepol_etl`). |
| 10 | `pon-prod-raw-cetus_discoverer` | `pon-prod-raw-cetus` | Familia raw. |
| 11 | `pon-prod-secad-crawler` | `pon-prod-raw-secad` | ✅ Confirmado. Nombre irregular pero es el real de secad (cron diario). |
| extra | `pon-prod-raw-sivicc_discoverer` | `pon-prod-raw-sivicc` | 9no de la familia raw. Toggle `--sin-sivicc`. |

> `pon-prod-raw-cetus_discoverer` y `pon-prod-secad-crawler` están **confirmados** en
> la cuenta (926162397524, us-east-1).

**Nota ali**: en barridos recientes `ali_discoverer` apareció **FAILED** cuando antes
estaba **SUCCEEDED**. El script **lee el estado real en vivo** cada corrida, así que
el veredicto lo da la ejecución, no un valor cableado. Confírmalo al correrlo.

**rnmc v1 vs v2**: se monitorea `pon-prod-raw-rnmc-incremental_discoverer` (v1, base
`pon_prod_raw_comparendo`). El `_v2` es scaffolding abandonado y **no** se monitorea.

**Fuera del mapa a propósito**: `pon-prod-raw-simep-s3` (cataloga la salida de simep
para xcrime/ML, otro equipo/dominio) no entra; no es parte del pipeline raw.

## Alcance: qué se monitorea y qué no

Inventario total de la cuenta: **68 crawlers**. El script vigila **12** (los de la
tabla de arriba) y deja **56 fuera a propósito**. Resumen abajo.

### ✅ Monitoreados (12) — dominio del pipeline raw

Los 11 fijos del mapa + `sivicc` (toggle `--sin-sivicc`). Son los crawlers que
refrescan las bases Athena `pon-prod-raw-*` del pipeline de ingesta. Ver la tabla de
la sección anterior para el detalle de cada uno.

### ❌ No monitoreados (56) — fuera de alcance, con razón

Agrupados por el motivo por el que **no** pertenecen a este monitor:

| Grupo | Nº | Por qué queda fuera | Ejemplos |
|-------|----|--------------------|----------|
| **Scaffolding/abandonado** | 1 | Nunca corrió de verdad; tablas creadas en un repunteo temporal, revertido. | `pon-prod-raw-rnmc-incremental_discoverer_v2` |
| **Downstream de otro equipo** | 1 | Cataloga salida de simep para xcrime/ML; otro dueño y dominio. | `pon-prod-raw-simep-s3` |
| **Migración / histórico / backfill (1 solo uso)** | 9 | Cargas históricas o de migración, no refresh recurrente. | `pon-prod-siedco-datacleaning`, `pon-prod-siedco-raw-2Oct`, `pon-prod-sivicc-historico-personas-crawler`, `pon-prod-disec-bp-onp-iris_discoverer_f1e0644c` |
| **Variantes irisp1 / POC** | 5 | Dominio operacional-directo, distinto del raw lake. | `pon-prod-irisp1-crawler`, `pon-prod-poc-irisp1-2_discoverer_ecaba193`, `pon-prod-iris-craw` |
| **BI / reporting / ML downstream** | 10 | Capa de reportes/BI de IAPOL, no ingesta raw. | `informe_anticipaciones_prediccion`, `pon-prod-estadistica_tablas_crawler`, `pon-prod-probabilidadDelito`, `colegios_ponal_data` |
| **Apps / módulos de negocio** | 10 | Módulos de aplicación propios, dominio distinto. | `pon-prod-expertopol-crawler-DB`, `pon-prod-hexagon-crawler`, `pon-prod-disec-gestion-territorial`, `pon-prod-mtrs` |
| **PBX / telefonía** | 5 | Pipeline de CDR telefónico; candidato a su propio monitor. | `pbx_raw`, `pon-prod-pbx-iapol`, `pon-pord-pbx_actualizado` |
| **QuickSight / usuarios / eventos** | 4 | Ya cubierto por la regla EventBridge `refresh-quicksight-pbx` de BI. | `iapol_users`, `sivicc-users`, `vista-pruebas` |
| **GPS** | 2 | Dominio de rastreo GPS, no raw lake. | `pon-prod-trazas-gps-crawler`, `sivicc_consulta_actual_gps` |
| **Mebog (despacho)** | 2 | Dominio de despacho Mebog, aparte. | `pon-pedidos-mebog`, `raw_mebog` |
| **Prueba / dev explícitos** | 7 | El nombre indica test/prueba o ajuste puntual, no producción. | `test_alias_crawler`, `prueba_sivicc_crawler`, `disec-plano-corregido` |

Verificación: **12 monitoreados + 56 no monitoreados = 68** (cuadra con el total).

> Si en el futuro algún grupo de los de arriba necesita vigilancia (p. ej. PBX),
> lo natural es un monitor **propio** para ese dominio, no mezclarlo aquí: este
> script es específico del refresh del pipeline raw.

## Clasificación

- **OK** — último crawl `SUCCEEDED` dentro de la ventana (`--horas`, 24 por defecto).
- **AVISO** — `SUCCEEDED` pero fuera de la ventana · crawler `RUNNING` ahora · sin
  crawls registrados (never run) · crawler no encontrado en la cuenta.
- **FALLA** — último crawl `FAILED` o `CANCELLED`.

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_athena_refresh.py                 # últimas 24 h
python3 monitor_athena_refresh.py --horas 48
python3 monitor_athena_refresh.py --con-avisos    # muestra también los avisos
python3 monitor_athena_refresh.py --sin-sivicc    # omite el crawler extra de sivicc
python3 monitor_athena_refresh.py --no-export     # no genera CSV
```

### Flags

| Flag | Qué hace |
|------|----------|
| `--horas N` | Ventana en horas (default 24; env `ATHENA_REFRESH_WINDOW_HOURS`). |
| `--sin-sivicc` | No vigila `pon-prod-raw-sivicc_discoverer`. |
| `--con-avisos` | Muestra los AVISOS en el resumen (el CSV siempre los incluye). |
| `--detallado` | Salida larga, sin spinner de progreso. |
| `--no-export` | No escribe el CSV. |

## Permisos IAM mínimos

```json
{
  "Effect": "Allow",
  "Action": ["glue:GetCrawler", "sts:GetCallerIdentity"],
  "Resource": "*"
}
```

## Salida

- **Consola**: resumen con `TOTAL: N | x OK | y aviso | z falla` y el detalle de lo
  crítico (o de todo con `--con-avisos`).
- **CSV**: `salida/athena_refresh_<YYYYmmdd_HHMMSS>.csv` con columnas
  `recurso, ubicacion, estado, mensaje, accion_sugerida`.
- **Exit code**: `0` todo OK · `1` si algún crawler falló · `2` args inválidos.

## Fuera de v1 (pendiente)

- **Pipeline Iceberg de `sivicc`** (CTAS + `MERGE INTO` manual, sin orquestador): no
  se verifica su ejecución, solo el crawl del catálogo. Si más adelante se le pone
  una Step Function, se podría leer su estado como señal limpia de éxito/fallo.
- **Refresh de la partición de "homicidios"** disparado por evento S3
  (`ObjectCreated`): no se verifica en v1.
- **`prod-data-reload-database`**: descartado, no es Athena (es Postgres/Oracle vía
  `psycopg2`/`cx_Oracle`), pertenece a otro dominio.
