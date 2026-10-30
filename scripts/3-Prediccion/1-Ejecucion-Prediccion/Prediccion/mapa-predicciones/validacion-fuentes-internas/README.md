# validacion-fuentes-internas

Valida las **fuentes internas** del mapa (Gescov, MTRS, SIEDCO I/II/III, SIMEP, SIRAP e IRIS P1) de los últimos
**15 días**, con el mismo patrón de [`../validacion-secad`](../validacion-secad/README.md),
[`../validacion-rnmc`](../validacion-rnmc/README.md) y
[`../validacion-gescov-antecedentes`](../validacion-gescov-antecedentes/README.md). Solo lectura, para correr en local.
Genera al final un **CSV de novedades**.

> **Depende de `../validacion-secad`:** reutiliza su motor (`secad_comun.py` y `validar_secad.py`). No muevas esa
> carpeta sin esta. Estado: 32 pruebas sin AWS y ejecutado contra producción el 2026-10-29 (cuenta `926162397524`).

## Cómo se sirven (distinto a las otras capas)

```
vistas de Athena ──(job de Glue pon-prod-xcrime-update-fuentes-internas, a diario 04:30)──► un GeoJSON nacional por capa
  en s3://xcrime-ml-ponal-dev-mdp/Fuentes_externas_geojson/  ──► backend (GET /externalLayers/get/{id}?unidadID=…)
  recorta por la caja (bbox) del polígono de la unidad ──► el front filtra por fecha y por polígono
```

La lista de capas está en DynamoDB (`pon_prod_external_layers`, categoría de fuentes internas). **La unidad se asigna
por coordenadas**, no por un código: por eso aquí sigla y unidad salen de las *propiedades* de cada registro, que es una
**aproximación** de lo que dibuja el mapa. El job descarta los registros sin coordenadas numéricas y reescribe el archivo
completo cada día.

| Capa | Archivo (`Fuentes_externas_geojson/…`) | Vista de origen y filtro del job | Fecha de la capa | Sigla / unidad | Máx. atraso |
|---|---|---|---|---|---|
| Gescov | `Gescov/Gescov.geojson` | `pon-prod-raw-disec."usr_disec.v_evos_planes"`, `estado_cumplimiento = 'Cumple'` | `fecha_inicio_plan` | `sigla_unidad` / `id_unidad` | 3 d |
| MTRS | `Mtrs_riesgo_ubicacion/v_mtrs_riesgo_ubicacion.geojson` | `usr_disec.v_mtrs_riesgo_ubicacion` | `fecha_inicio_mesa_tecnica` | `sigla` / cuadrante | 30 d |
| SIEDCO I, II, III | `Disec/DisecDelitosTurno{1,2,3}.geojson` | `vw_delitos_corregido_schema_temporal` por hora (I 22–06:59, II 07–13:59, III 14–21:59), con subcategoría | `fecha_hecho` | `sigla_undepertenece` / `juris.cai` si es cuadrante | 3 d |
| SIMEP | `Simep/Simep.geojson` | `pon-prod-raw-disec.simep_datac`, `fecha_creacion <= hoy` | `fecha_creacion` | (por la unidad) / `id_unidad` | 3 d |
| SIRAP | `Sirap/raw_geopol_usr_pmu_servicios_extra.geojson` | `pon-prod-raw-geopol.raw_geopol_usr_pmu_actividades` | `fecha_inicio` | `unidad` (sigla) / **sin unidad** | 3 d |
| IRIS P1 | `General_iris/IrisP1.geojson` | `usr_disec.v_general_irisp`, `estado_existencia != 'No existe'` | `fecha_creacion_irisp1` | `sigla_unidad` / `nro_cuadrante` | 30 d |

SIRAP no trae unidad: se evalúa solo por sigla. El job semanal `internal_layer_irisp1` (domingos 00:00 UTC) también
reescribe `IrisP1.geojson` y `Simep.geojson`.

## Qué valida

1. **El archivo que lee el mapa**, recorrido en streaming (un feature por línea; unos 1 GB y unos minutos): registros por
   sigla, unidad y día de la ventana, fechas inválidas o futuras, coordenadas fuera de Colombia, registro más reciente.
2. **Frescura**: hora de modificación de cada archivo y corridas del job diario (y del semanal de IRIS P1).
3. **Contraste con la vista de origen** en Athena, día por día, con los mismos filtros del job y tolerancia de 3 % o 3
   registros. Detecta un archivo que no refleja su fuente.
4. **Vista materializada de origen** (Gescov e IRIS P1): `max(fecha_actualiza_vm)` — si Oracle no la refresca, el archivo
   se regenera a diario pero con datos viejos.

## Uso

```powershell
# 0. Una vez: registro en DynamoDB, archivos, vistas de origen y jobs
python descubrir_fuentes_internas.py --profile <perfil>

# 1. Validar (sin --catalogo lo lee de S3; el workgroup primary exige --output-s3)
$out = "s3://aws-athena-query-results-us-east-1-926162397524/"
python validar_fuentes_internas.py --profile <perfil> --output-s3 $out
python validar_fuentes_internas.py --capas gescov simep ...         # solo algunas capas (menos descarga)
python validar_fuentes_internas.py --sigla MEPAS MEBOG ...          # análisis por sigla/unidad de algunas siglas
python validar_fuentes_internas.py --sin-archivos ...               # sin descargar los GeoJSON (fuente, fechas y jobs)
python validar_fuentes_internas.py --hilos 8 ...                    # más archivos a la vez
python validar_fuentes_internas.py --rezago-siedco 0 ...            # evaluar también los últimos días de SIEDCO
python validar_fuentes_internas.py --solo-sql                       # ver el SQL sin tocar AWS
```

Requisitos, catálogo y credenciales: igual que en [`validacion-secad`](../validacion-secad/README.md). Además:
`s3:GetObject/HeadObject` sobre el bucket de los GeoJSON, `glue:GetJob/GetJobRuns/GetTriggers`, y
`dynamodb:Scan` sobre `pon_prod_external_layers` (solo el descubrimiento). Cada corrida escanea pocos MiB en Athena y
descarga unos 1.050 MiB de S3.

## Qué reporta

Las reglas de volumen por sigla y unidad son las de SECAD (`CAIDA_A_CERO`, `RACHA_SIN_DATOS`, `CAIDA_VOLUMEN`,
`DIAS_SIN_DATOS`, `SIN_DATOS_EN_TODAS_LAS_CAPAS`, `COORD_INVALIDA`, `DIA_ATIPICO`…), con ventana de **15 días**,
`--min-base 20` y `--min-esperado-falla 5`. No se emite `UNIDAD_NO_ASIGNADA`: aquí la unidad es una aproximación.
Dos ajustes propios de estas fuentes:

- **`DIA_ATIPICO` por día de la semana**: cada día nacional se compara con la mediana de los mismos días de la semana de
  los 30 días revisados (SIMEP baja en fines de semana: 1–6 registros contra 30–89 entre semana). Con menos de 3 días
  iguales usa la mediana diaria global; los días con mediana esperada menor a 30 no se evalúan.
- **Rezago de SIEDCO (`--rezago-siedco`, 2 días)**: SIEDCO registra los hechos con atraso, así que los 2 últimos días de la
  ventana siempre están incompletos aunque el archivo coincida con su vista de origen (p. ej. 21 registros el último día
  contra 550–900 en un día normal). Esos días no cuentan para la racha final, los días sin datos ni `DIA_ATIPICO`, pero sí
  están en los totales y en el contraste con la fuente. `--rezago-siedco 0` los vuelve a evaluar.

Nuevos:

| Código | Nivel | Cuándo | Estado |
|---|---|---|---|
| `ARCHIVO_DESACTUALIZADO` | CARGA | el archivo se modificó hace más de `--max-horas-archivo` (30 h) | FALLA |
| `ARCHIVO_VACIO` | NACIONAL | el archivo no tiene registros | FALLA |
| `CAPA_SIN_REGISTROS_EN_VENTANA` | NACIONAL | ningún registro en la ventana; da el último registro y, si aplica, la causa (vista sin refrescar) | FALLA |
| `ULTIMO_REGISTRO_VIEJO` | NACIONAL | hay registros en la ventana pero el más reciente supera el atraso máximo de la capa | FALLA |
| `FUENTE_VM_SIN_REFRESCAR` | CARGA | la vista materializada de origen lleva más de `--max-dias-vm` (3) sin refrescarse | FALLA |
| `ARCHIVO_DIFIERE_DE_LA_FUENTE` | CARGA | días donde el archivo no coincide con su vista de origen; FALLA si al archivo le falta más del 10 % | FALLA / AVISO |
| `FECHAS_INVALIDAS` | NACIONAL | registros sin fecha o con fecha anterior al 2000: con un filtro de fechas el mapa no los muestra | AVISO |
| `FECHAS_FUTURAS` | NACIONAL | registros con fecha posterior a mañana | AVISO |
| `REGISTROS_SIN_SIGLA` | SIGLA | registros sin sigla: el mapa los ubica por coordenada, pero no entran en la validación por sigla/unidad | AVISO |
| `JOB_FUENTES_INTERNAS_VENCIDO` | CARGA | la última corrida exitosa del job diario supera 30 h | FALLA |
| `JOB_FUENTES_INTERNAS_DIAS_SIN_CORRIDA` | CARGA | días de la ventana sin corrida exitosa (la siguiente regenera todo) | AVISO |
| `JOB_SEMANAL_IRIS_VENCIDO` | CARGA | el semanal de IRIS P1 lleva más de 9 días | AVISO |

## Salida

En `salida/` (ignorada por git), sufijo con fecha y hora de Colombia: `resumen_siglas`, `resumen_unidades`, `diario`,
`archivos` (una fila por capa: tamaño, modificación, registros, ventana, último registro, contraste con la fuente),
`consultas_fuentes_internas.sql`, `meta_fuentes_internas.json` y:

- **`novedades_*.csv`**: el CSV de novedades. Un registro por hallazgo, FALLA primero y por nivel (`CARGA`,
  `NACIONAL`, `SIGLA`, `UNIDAD`). Columnas: `nivel, estado, sigla, unidad_id, unidad, tipo, capa (o TODAS), codigo,
  mensaje, contexto, sigla_sin_datos`.

Exit: 0 sin FALLA, 1 con FALLA, 2 argumentos, 3 bloqueado/error AWS, 130 interrupción.

## Qué no hace (y por qué)

- **No emula el polígono del front.** El mapa muestra los puntos dentro del polígono de la unidad; aquí la unidad sale
  de las propiedades (`id_unidad`, cuadrantes), así que los resultados por unidad son **indicativos**. En SIEDCO,
  `juris.cai` suele traer el nombre del CAI y solo a veces un cuadrante: las unidades de SIEDCO se subestiman.
  Hacerlo exacto exigiría una unión espacial con los polígonos de `UnidadesBase`.
- El SQL de la fuente de SIEDCO copia la clasificación en subcategorías del job (`pon_prod_update_fuentes_internas.py`,
  2026-08-11). Si el job cambia esa lógica, hay que actualizar `SIEDCO_SUBCATEGORIA` aquí o el contraste dará diferencias.
- No mide el filtro por turno del front (`coincideTurnoSiedco`) ni los filtros por tipo de SIEDCO.

## Pruebas

```powershell
python -m unittest discover -s tests -v
```

| Archivo | Rol |
|---|---|
| `descubrir_fuentes_internas.py` | Paso 0: DynamoDB, archivos, vistas de origen, jobs |
| `validar_fuentes_internas.py` | Validador: recorrido de los GeoJSON, contraste con Athena, jobs, informe y CSV |
| `tests/test_fuentes_internas.py` | Pruebas sin AWS |
