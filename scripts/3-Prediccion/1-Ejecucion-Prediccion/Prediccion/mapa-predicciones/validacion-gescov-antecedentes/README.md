# validacion-gescov-antecedentes

Valida los datos de las **siete capas** de Planes GESCOV y Antecedentes del mapa de los últimos **15 días**, con
el mismo patrón de [`../validacion-secad`](../validacion-secad/README.md) y [`../validacion-rnmc`](../validacion-rnmc/README.md).
Solo lectura, para correr en local. Genera al final un **CSV de novedades**.

> **Depende de `../validacion-secad`:** reutiliza su motor (`secad_comun.py` y `validar_secad.py`). No muevas
> esa carpeta sin esta. Estado: 28 pruebas sin AWS y ejecutado contra producción el 2026-10-28
> (cuenta `926162397524`).

## De dónde sale cada capa y qué se puede validar

| Capa del mapa | Origen real (BFF `capas_adicionales`) | Qué valida este script |
|---|---|---|
| Antecedentes: Personas / Vehículos / IMEI (`antecedentes-personas`, `-vehiculos`, `-imei`) | RDS `public.auditoria_cons_{persona,vehiculo,imei}_v2` | **por sigla y unidad**, directo en las tablas crudas de Athena que alimentan RDS, más la frescura del job de carga |
| Antecedentes (Solo positivos) (`…-positivos`) | las mismas tablas, `tiene_antecedentes = true` | ídem, con ese filtro |
| Planes Gescov Programados (`planes-programados-historico`) | RDS `public.v_planes_programados_historico` | **solo el proceso diario y el mapeo de unidades** (ver abajo); no hay datos por sigla/unidad |

**Antecedentes.** El job de Glue `V2_SIVICC-migration-to-RDS` copia cada noche (22:xx) a RDS las tablas de Athena
`pon-prod-raw-sivicc.raw_sivicc_usr_mnvcc_auditoria_cons_{persona,vehiculo,imei}`. Toma `sigla` de
`fisicalaborando` y `codigo_unidad` de `undelaborando` (o de la unidad de la minuta si es nula), **sin etiquetado
por polígono**, descarta filas sin coordenadas y es idempotente (compara claves antes de insertar). Por eso la
validación por unidad sale exacta desde la tabla cruda. Las tablas crudas traen copias de algunas filas (por eso
`filas` puede superar a `llamadas`); `puntos` y `llamadas` cuentan ids distintos.

**Planes GESCOV.** Una tarea ECS (`pon-prod-addcapas-migrations-gescov-daily-task`, regla de EventBridge a las
06:00 UTC = 01:00 Colombia) copia de Oracle DISEC (`V_PLANESPROGRAMADOS`) los puntos con `fecha_inicio >= ayer`,
asigna `codigo_unidad` con `estaciones.csv` (clave `ESTACION-SIGLA`) y **descarta** lo que no mapea. Sus datos
solo existen en RDS: la copia de Athena (`pon-prod-raw-disec.raw_disec_usr_disec_gescov_punto`) está
desactualizada y la tabla `evos_plan` no es consultable. Lo que sí se valida:
- **La tarea diaria**, con su log de CloudWatch: si corrió cada día, si terminó, cuántos puntos extrajo y
  descartó, y si el mayor `fecha_inicio` de la tabla llegó al día esperado. Una ejecución que falta o falla deja
  sin cargar *para siempre* los puntos del día anterior, porque la siguiente solo pide desde su propio "ayer".
- **`estaciones.csv` contra el catálogo del mapa**: entradas que apuntan a una unidad que el mapa no muestra
  (sus planes salen al elegir la sigla pero no la unidad) y estaciones del catálogo sin entrada.

## Uso

```powershell
# 0. Una vez: esquemas, job de carga, regla/logs de GESCOV
python descubrir_gescov_antecedentes.py --profile <perfil>        # --muestra trae 3 filas de cada tabla

# 1. Validar (sin --catalogo lo lee de S3; el workgroup primary exige --output-s3)
$out = "s3://aws-athena-query-results-us-east-1-926162397524/"
python validar_gescov_antecedentes.py --profile <perfil> --output-s3 $out
python validar_gescov_antecedentes.py --sigla MEPAS MEBOG ...                       # antecedentes de algunas siglas
python validar_gescov_antecedentes.py --capas antecedentes-imei planes-programados-historico ...
python validar_gescov_antecedentes.py --estaciones C:\ruta\estaciones.csv ...        # otra copia de estaciones.csv
python validar_gescov_antecedentes.py --sin-carga ...                                # sin historiales ni logs
python validar_gescov_antecedentes.py --solo-sql --sin-esquema --sin-catalogo        # ver el SQL sin tocar AWS
```

Requisitos, catálogo y credenciales: igual que en [`validacion-secad`](../validacion-secad/README.md). Además:
`glue:GetJob/GetJobRuns`, `logs:DescribeLogGroups/DescribeLogStreams/GetLogEvents` sobre
`/ecs/pon-prod-addcapas-migrations-gescov-migration`, `events:DescribeRule` (solo el descubrimiento) y lectura de
las tablas crudas de SIVICC. Sin esos permisos esas partes quedan "sin evaluar" y el resto corre. Cada corrida
escanea unos 5,5 GB en Athena. `estaciones.csv` se busca por defecto en el repo hermano
`pon-iapol-capas/migrations/gescop_migration/daily_update/`; en la primera corrida tenía 2.185 filas, las mismas
que reporta la tarea desplegada.

## Qué reporta

Antecedentes usa las reglas de SECAD/RNMC (`CAIDA_A_CERO`, `RACHA_SIN_DATOS`, `CAIDA_VOLUMEN`, `DIAS_SIN_DATOS`,
`SIN_DATOS_EN_TODAS_LAS_CAPAS`, `COORD_INVALIDA`, `DIA_ATIPICO`, …) con ventana de **15 días** contra los 15 anteriores,
`--min-base 20` y `--min-esperado-falla 5`. `UNIDAD_NO_ASIGNADA` ahora dice **por qué** no hay unidad visible:
*no existe en el catálogo* (el `codigo_unidad` del origen no está en la estructura nacional), *unidad oculta en el
árbol* (tipos 1.0, 308.0, 164.0, 187.0, 6.0, que el mapa no lista) o *de otra sigla*. Nuevos aquí:

| Código | Nivel | Cuándo | Estado |
|---|---|---|---|
| `CARGA_RDS_VENCIDA` | CARGA | la última corrida exitosa de `V2_SIVICC-migration-to-RDS` para esa tabla supera `--max-horas-carga` (30) o no hay | FALLA |
| `CARGA_RDS_DIAS_SIN_CORRIDA` | CARGA | días de la ventana sin corrida exitosa (la siguiente carga los recupera porque compara claves) | AVISO |
| `CONSULTAS_SIN_SIGLA` | SIGLA | consultas con `fisicalaborando` nulo: se cargan a RDS pero el mapa, que filtra por sigla, no las muestra | AVISO |
| `GESCOV_SIN_EJECUCION` | CARGA | no hay ejecución de la tarea el día que cargaría los puntos de un `fecha_inicio` | FALLA |
| `GESCOV_EJECUCION_FALLIDA` | CARGA | la ejecución no llegó a "MIGRACIÓN COMPLETADA" (se cita el error) | FALLA |
| `GESCOV_SIN_REGISTROS` / `GESCOV_VOLUMEN_BAJO` | CARGA | 0 puntos extraídos / menos del 40 % de la mediana | FALLA / AVISO |
| `GESCOV_DIA_NO_CARGADO` | CARGA | tras la ejecución, el mayor `fecha_inicio` de la tabla es anterior al día esperado | FALLA |
| `GESCOV_DESCARTES_ALTOS` | CARGA | más del `--max-descartados-pct` (8 %) de los puntos extraídos se descartan | AVISO |
| `GESCOV_TOTAL_INCONSISTENTE` | CARGA | el total de la tabla ≠ total previo + enriquecidos − eliminados (otra carga o borrado tocó la tabla) | AVISO |
| `GESCOV_LOGS_EXPIRADOS` | CARGA | ejecuciones de la ventana sin log: el grupo retiene solo **7 días** | AVISO |
| `GESCOV_MAPEO_A_UNIDAD_INEXISTENTE` / `_OCULTA` / `_SIGLA_DISTINTA` | UNIDAD | una fila de `estaciones.csv` apunta a una unidad que no existe, que el mapa oculta, o de otra sigla | AVISO |
| `GESCOV_ESTACIONES_SIN_ENTRADA` | CARGA | estaciones (tipo 7.0) del catálogo sin fila en `estaciones.csv`; lista completa en `gescov_estaciones_sin_entrada_*.csv` | AVISO |
| `COPIA_ATHENA_GESCOV_DESACTUALIZADA` | CARGA | la copia de GESCOV en Athena lleva más de 3 días sin actualizarse (no afecta al mapa) | AVISO |

## Salida

En `salida/` (ignorada por git), sufijo con fecha y hora de Colombia: `resumen_siglas`, `resumen_unidades` y `diario`
(solo antecedentes), `consultas_gescov_antecedentes.sql`, `meta_gescov_antecedentes.json`,
`gescov_estaciones_sin_entrada_*.csv` y:

- **`novedades_*.csv`**: el CSV de novedades. Un registro por hallazgo, FALLA primero y por nivel (`CARGA`,
  `NACIONAL`, `SIGLA`, `UNIDAD`). Columnas: `nivel, estado, sigla, unidad_id, unidad, tipo, capa (o TODAS), codigo,
  mensaje, contexto, sigla_sin_datos`. `SIGLA_FUERA_CATALOGO` (siglas sin territorio como DIJIN o escuelas) sale una
  vez por sigla con `capa = TODAS`.

Exit: 0 sin FALLA, 1 con FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupción.

## Qué no hace (y por qué)

- **No lee RDS.** Lo que ve el mapa vive allí (el BFF usa el secreto *administrador* del clúster); abrir esa
  conexión es una decisión de seguridad pendiente. Con un usuario de solo lectura se podrían comparar las
  tablas crudas contra `auditoria_cons_*_v2` y validar GESCOV por sigla y unidad.
- **GESCOV no tiene tabla de datos por sigla/unidad aquí**, por lo ya explicado. El log de CloudWatch
  conserva 7 días: las ejecuciones más viejas de la ventana solo se confirman por su existencia
  (el stream sigue ahí), no por su resultado. Ampliar la retención lo resolvería.
- Para antecedentes no mide `hora_invalida`, `hora_futura` ni `desfase_particion` (sin particiones, `fecha` es
  `timestamp`). Tampoco mide el filtro `>= 2024-01-01` del job.

## Pruebas

```powershell
python -m unittest discover -s tests -v
```

| Archivo | Rol |
|---|---|
| `descubrir_gescov_antecedentes.py` | Paso 0: esquemas, job de carga, regla/logs de GESCOV, `estaciones.csv` |
| `validar_gescov_antecedentes.py` | Validador: SQL por capa, cargas, tarea de GESCOV, mapeo de estaciones, informe y CSV |
| `tests/test_gescov_antecedentes.py` | Pruebas sin AWS |
