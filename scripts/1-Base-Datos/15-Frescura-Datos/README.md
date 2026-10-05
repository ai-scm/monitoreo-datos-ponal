# 15 — Frescura de Datos (`monitor_frescura_datos.py`) — ⛔ BLOQUEADO (diseñado)

Verifica que los **datos** estén frescos (no solo que el crawler corrió): consulta
`MAX(columna_de_fecha)` por tabla vía **Athena** y compara contra un umbral de días.

> ## ⛔ Estado: BLOQUEADO — no puede correr todavía
> Requiere un permiso IAM que el rol de monitoreo **no tiene hoy**:
> `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
> **+** escritura S3 en el bucket de resultados de Athena (`ATHENA_OUTPUT_S3`).
> El script está **escrito y listo**; detecta la falta de permiso y sale con mensaje
> claro (exit code `3`, "BLOQUEADO"), **sin dar falsos OK**. Se activa en cuanto se
> conceda el permiso.

## Diferencia con el punto 3 (Refresh-Athena)

| | Punto 3 (`monitor_athena_refresh.py`) | Punto 15 (este) |
|-|---------------------------------------|-----------------|
| Pregunta | ¿el **crawler** corrió? | ¿los **datos** están frescos? |
| Fuente | metadata del catálogo (`get_crawler`) | contenido real (`MAX(fecha)` vía Athena) |
| Permiso | `glue:GetCrawler` | `athena:*` + S3 (hoy bloqueado) |

Son preguntas distintas; por eso es un **punto aparte**, no un flag del punto 3.

## Alcance v1 — solo 4 columnas de ALTA confianza

Se verifican **solo** las 4 tablas cuya columna de fecha de negocio está **confirmada
con evidencia real**. Escribir SQL con un nombre de columna inventado daría falsos OK
o fallos silenciosos — justo lo que se evita.

| Base | Tabla | Columna | Nota técnica |
|------|-------|---------|--------------|
| SIEDCO | `pon_prod_siedco_raw.raw_siedco_usr_esta_motivospolicia` | `fecha_hecho` | **String**, no timestamp → `date_parse`/`CAST` (ajustar `fmt` al formato real) |
| SECAD | `pon-prod-raw-secad.cadpedidos` | `fecha_grabacion` | Confirmar si la fecha de negocio es `fech_caso` o `fecha_grabacion` |
| ALI | `pon-prod-raw-ali.data_etiquetada` | `fecha_grabacion` | Timestamp real, recordCount confirmado |
| SIMEP | `pon-prod-raw-simep.v_simep_medidas` (vista) | `fecha_expedicion` | Se mide la **vista** (más fresca); alternativa `fecha_medida` |

### Las 8 bases restantes — FUERA de v1 (pendientes de confirmar)

**No se consultan** hasta validar la columna con el equipo de datos. Documentadas en el
código (`PENDIENTES`) y aquí:

- **CNP / DBOPER / RNMC (comparendo)**: las tres apuntan a `cnp_hechos` → posible
  verificación compartida, pero es una decisión de arquitectura de alguien que conozca
  el pipeline.
- **DISEC / SIVICC / GEOPOL**: "tabla primaria" fue inferida por conteo de columnas, no
  por tamaño real (el crawler JDBC nunca llenó recordCount) → puede estar equivocada.
- **TELEPOL**: volcado estático de catálogo Oracle Discoverer 2024 → **no** es
  candidato de frescura; a lo sumo "¿crawleó?" (eso es el punto 3).
- **GEOPOL (detalle)**: 158/159 tablas son metadata de replicación GIS (SDE); solo
  `geo_cartografia_*` tiene fecha de negocio, y sin crawl desde ago-2024 (posible dato
  abandonado).

El script reporta un AVISO informativo recordando que estos grupos no se verifican aún.

## Uso (cuando se conceda el permiso)

```bash
export AWS_DEFAULT_REGION=us-east-1
export ATHENA_OUTPUT_S3=s3://<bucket-resultados-athena>/monitoreo/

python3 monitor_frescura_datos.py
python3 monitor_frescura_datos.py --dias 2        # umbral de rancidez
python3 monitor_frescura_datos.py --con-avisos
python3 monitor_frescura_datos.py --no-export
```

### Flags y env vars

| Flag / Env | Default | Qué hace |
|------------|---------|----------|
| `--dias N` | 2 | Umbral de rancidez: si `MAX(fecha)` es más viejo → FALLA (env `FRESCURA_MAX_DIAS`). |
| `--con-avisos` | — | Muestra los AVISOS en el resumen. |
| `--detallado` | — | Salida larga sin spinner. |
| `--no-export` | — | No escribe el CSV. |
| `ATHENA_OUTPUT_S3` | *(vacío)* | **Obligatorio**: bucket S3 de resultados de Athena. Sin esto, sale BLOQUEADO. |
| `ATHENA_WORKGROUP` | `primary` | Workgroup de Athena. |

## Clasificación

- **OK** — `MAX(fecha)` dentro del umbral (`--dias`).
- **AVISO** — `MAX(fecha)` vacío (tabla sin filas / columna nula) · valor no
  interpretable · nota de las bases pendientes.
- **FALLA** — datos rancios (más viejos que el umbral) · la query falló (columna/tabla
  mal) · **permiso de Athena ausente** (bloqueado).

## Permisos IAM mínimos

> **Diferencia respecto a los demás scripts**: es el primero que **ejecuta queries
> Athena** y por tanto escribe el resultado en S3 (comportamiento normal de Athena).

```json
{
  "Effect": "Allow",
  "Action": [
    "athena:StartQueryExecution",
    "athena:GetQueryExecution",
    "athena:GetQueryResults",
    "glue:GetTable",
    "glue:GetDatabase",
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

(Los `s3:*` solo sobre el bucket de `ATHENA_OUTPUT_S3` y los buckets de datos de las
bases; `glue:GetTable/GetDatabase` porque Athena consulta el catálogo.)

## Salida

- **Consola**: resumen `TOTAL: N | x OK | y aviso | z falla`.
- **CSV**: `salida/frescura_datos_<YYYYmmdd_HHMMSS>.csv` (columnas estándar).
- **Exit code**: `0` todo fresco · `1` datos rancios o query fallida · `2` args ·
  **`3` BLOQUEADO** (falta permiso Athena o `ATHENA_OUTPUT_S3`).

## Pendiente

1. **Conceder el permiso de Athena** al rol de monitoreo (lo de arriba).
2. **Confirmar con el equipo de datos** las columnas de las 8 bases pendientes y la
   duda de SECAD (`fech_caso` vs `fecha_grabacion`) y SIMEP (vista vs cruda).
3. Una vez concedido el permiso, **probar** las 4 confirmadas y ajustar el `fmt` de
   SIEDCO al formato real de su string de fecha.
