# Calidad de los datos en la fuente: SECAD, RNMC, GESCOV/Antecedentes y Fuentes internas

**Fecha:** 2026-11-03 · **Ventana:** 2026-05-03 a 2026-11-02 (6 meses) · **Cuenta:** PON-PROD (926162397524)

Este informe mide los datos **tal como llegan del cliente**, antes de cualquier ETL nuestro. Para cada capa del
mapa se tomó la tabla cruda más cercana a la base de la Policía y se contaron los registros sin identificador,
sin fecha, sin coordenadas o con coordenadas inválidas, y sin unidad o sigla.

Archivos que acompañan este informe:

- `calidad_fuentes_detalle_2026-11-03.csv`: una fila por fuente y sigla con todos los contadores. Cada columna dice
  lo que cuenta (por ejemplo `coordenada_en_cero`, `sin_id_del_registro`, `registros_criticos_sin_id_fecha_o_coordenada_valida`).
- `calidad_fuentes_diccionario_columnas.csv`: qué cuenta cada columna del detalle y qué campo se revisó en cada
  fuente (por ejemplo, `sin_tipo_de_incidente` es `CODI_PEDIDO` en SECAD y `descripcion_tipo` en PremierOne).
- `calidad_secad_por_sigla_2026-11-03.csv`: coordenadas de SECAD por sigla, separando llamadas atendidas (con
  patrulla despachada) y no despachadas.

## 1. Respuesta corta

- **Unos 701 mil registros de los últimos 6 meses llegan de la fuente sin ubicación usable o sin identificador.**
  Esos registros no pueden pintarse en el mapa. Casi todos están en dos fuentes:
  - **SECAD:** 484.585 llamadas atendidas sin coordenadas, el 39 % de las atendidas.
  - **PremierOne de Bogotá (MEBOG):** 195.113 registros (18,5 %) sin número de incidente o con coordenadas en 0.
- **Además, 2,8 millones de consultas de IMEI (17,2 %) llegan sin la unidad que las hizo.** Se ven en el mapa,
  pero no salen al filtrar por unidad.
- **El resto de fuentes llega limpio:** RNMC, GESCOV, MTRS, IRIS P1, SIMEP, SIEDCO, SIRAP y las consultas de
  personas y vehículos tienen menos del 1 % de registros dañados.
- **Lo dañado viene así desde la base del cliente.** Nuestras copias no transforman los datos (ver la sección 2).
  Hay una excepción: unas 472 mil filas duplicadas en la tabla cruda de consultas de personas salen de nuestra copia,
  no de la fuente (ver la sección 5).

## 2. Cómo se verificó que el daño viene de la fuente

| Fuente | Cómo llega a AWS | ¿Modifica los datos? |
|---|---|---|
| SECAD (BD Oracle regionales) | AWS DMS → `pon-prod-raw-secad.cadpedidos` (CDC) | No. Las 96 tareas DMS solo tienen reglas de **selección**: ninguna regla de transformación, filtro ni cambio de columnas. |
| RNMC/CNP, SIVICC, GEOPOL, DISEC, SIMEP | DMS o el job Glue `jdbc_incremental.py` | No. El job copia por *bookmark*, con `ApplyMapping` 1 a 1 (mismos tipos) y `DropNullFields`, que solo quita columnas vacías y no toca filas. |
| PremierOne (MEBOG) | GUTIC sube un Excel por día a `s3://pon-prod-mebog-iapol/raw-data-premier-one/`; el job Glue `pon-prod-premier-one-raw-to-parquet` lo pasa a Parquet en `s3://pon-prod-llamadas/data/mdp/llamadas-mebog/` | No cambia los valores: renombra columnas y convierte tipos. Una salvedad: una latitud o longitud con texto no numérico queda vacía (`to_numeric(errors="coerce")`). No hay conexión a la base de PremierOne. |
| Hexagon (MEVAL) | Archivos que entrega el cliente en S3 | No. Se leen tal cual con una tabla externa. |
| SIEDCO | Extracción que entrega el cliente (`raw_data_siedco_full_delitos`) | No. Tabla externa sobre los archivos. |

No se consultó directamente Oracle ni Postgres: no tenemos usuario de lectura y no se tomaron credenciales de
Secrets Manager. Como la copia es fiel, lo que hay en las tablas crudas es lo que entrega la base del cliente. Si se
quiere la prueba en Oracle, basta repetir los conteos de la sección 7 con un usuario de solo lectura.

**Criterios.** Coordenada válida es la que está dentro de Colombia: latitud entre -4,3 y 13,6 y longitud entre -82 y
-66. Se separaron estos casos:

- nula o vacía;
- texto que no es número;
- igual a 0;
- invertida (latitud y longitud cambiadas);
- fuera del país.

Se cuentan como **registros críticos** los que no tienen ID, ni fecha, ni una coordenada válida, porque el mapa no
puede mostrarlos.

## 3. Resumen por fuente (últimos 6 meses)

| Validador | Fuente cruda | Registros | Críticos | % | Problema principal |
|---|---|---:|---:|---:|---|
| SECAD | `cadpedidos`, llamadas atendidas (con canal) | 1.242.705 | **484.585** | **39,0 %** | 452.142 con coordenada 0 y 32.443 vacías |
| SECAD | `cadpedidos`, registros sin canal (no atendidos) | 2.860.765 | 2.697.699 sin coordenadas | 94,3 % | no son incidentes: no van al mapa (ver la sección 4.1) |
| SECAD | PremierOne `raw_mebog.llamadas_mebog` (MEBOG) | 1.054.967 | **195.113** | **18,5 %** | 99.333 sin `no_incidente` y 106.286 con coordenada 0 |
| SECAD | Hexagon `pon-prod-hexagon.hexagon` (MEVAL) | 408.645 | 3.800 | 0,9 % | coordenada 0; además 35.412 filas repetidas entre archivos |
| RNMC | `raw_dboper_usr_cnp_cnp_hechos` | 1.134.232 | 4.599 | 0,4 % | coordenada vacía; 3.380 sin cuadrante, por lo que quedan sin sigla |
| RNMC | `raw_dboper_usr_cnp_cnp_solici_media` (mediaciones en sala) | 24.023 | 27 | 0,1 % | 26 fuera del país y 1 invertida |
| Antecedentes | `auditoria_cons_persona` | 84.879.542 | 596 | <0,01 % | coordenadas; 472.647 filas duplicadas por nuestra copia |
| Antecedentes | `auditoria_cons_vehiculo` | 71.094.388 | 5.075 | <0,01 % | 4.545 sin coordenada; 11.615 sin unidad |
| Antecedentes | `auditoria_cons_imei` | 16.368.780 | 6.504 | 0,04 % | **2.810.092 (17,2 %) sin unidad** (`undelaborando` vacío) |
| GESCOV / F. internas | `usr_disec.v_evos_planes` | 2.097.430 | 0 | 0 % | 12.049 planes (may.–oct.) sin `estado_cumplimiento` |
| F. internas | SIEDCO `raw_data_siedco_full_delitos` | 663.918 | 1.044 | 0,2 % | 1.036 con coordenada 0 |
| F. internas | SIMEP `usr_simep_v_simep_autos_arcgis` | 15.700 | 61 | 0,4 % | fuera del país; 234 fechas inválidas en toda la tabla |
| F. internas | IRIS P1 `usr_disec.v_general_irisp` | 1.594 | 6 | 0,4 % | coordenadas fuera, invertidas o no numéricas |
| F. internas | SIRAP `raw_geopol_usr_pmu_actividades` | 1.258 | 15 | 1,2 % | 12 fuera del país y 3 invertidas |
| F. internas | MTRS `usr_disec.v_mtrs_riesgo_ubicacion` | 988 | 0 | 0 % | 28 sin cuadrante; 24 fechas inválidas en toda la tabla (año 0004 o futuras) |
| **Total** | | | **≈ 701.425** | | |

Ninguna fuente trae fechas nulas dentro de la ventana. Fuera de la ventana, en toda la tabla, hay 127.524 filas de
PremierOne sin fecha (sección 4.2), 234 en SIMEP y 24 en MTRS.

## 4. Detalle de las fuentes con problemas

### 4.1 SECAD: llamadas sin coordenadas

`cadpedidos` trae 4,1 millones de registros en la ventana (una fila por llamada, en su última versión):

- **2,86 millones no tienen registro en `CADPEDIDOS_CANALES`.** Son llamadas que nunca se clasificaron ni se
  despacharon. La vista de capas las descarta porque exige ese registro, así que no son pérdida para el mapa: no son
  incidentes. El 94 % no trae coordenadas, lo que es normal para ese tipo de llamada.
- **1,24 millones sí son llamadas atendidas.** Solo el 61 % trae coordenadas; las demás llegan con `0` (452.142) o
  vacías (32.443).

Por eso solo una parte de las llamadas reales llega a las capas 11, 12 y 13. El problema se concentra en pocas
siglas (llamadas atendidas sin coordenadas):

| Sigla | Atendidas | Sin coordenadas | % |
|---|---:|---:|---:|
| MEBAR | 210.297 | 208.433 | **99,1 %** |
| MEPER | 66.267 | 50.811 | 76,7 % |
| MEMAZ | 48.068 | 34.609 | 72,0 % |
| DEATA | 33.621 | 32.272 | 96,0 % |
| DEANT | 47.258 | 28.307 | 59,9 % |
| MECUC | 37.402 | 22.598 | 60,4 % |
| MEBUC | 148.970 | 22.113 | 14,8 % |
| MECAL | 214.646 | 19.794 | 9,2 % |
| DEVAL | 16.727 | 12.481 | 74,6 % |
| MESAN | 19.155 | 11.298 | 59,0 % |
| MEPAS | 14.892 | 8.771 | 58,9 % |
| MEVIL, DEURA, DEGUN, DEVAU | 10.927 | 10.664 | 91–100 % |

En cambio, más de 25 siglas superan el 99 % de llamadas atendidas con coordenadas. Entre ellas están METIB, METUN,
MENEV, DECUN, DESUC, DERIS, DECES, DECAS, DECAQ y DEBOY. El problema está en cómo registra la ubicación cada
SECAD, no en el sistema.

Barranquilla (MEBAR) prácticamente no georreferencia sus llamadas. **Recomendación:** pedir a las SECAD de MEBAR,
DEATA, MEPER, MEMAZ y DEVAL que diligencien latitud y longitud, o al menos que entreguen la dirección para
geocodificarla.

Otros hallazgos de SECAD:

- 85 llamadas con código de caso que no existe en `CADCASOS`.
- Las fallas de DMS ya documentadas en el informe del 2026-10-29 (caídas OCI, DEATA, DECUN, DEURA, DEVAU y DECOR)
  son huecos de **volumen**, no de calidad de los campos.

### 4.2 PremierOne (MEBOG): sin número de incidente y coordenadas en 0

- **Coordenada 0:** 106.286 registros (10,1 %). Pasa todos los meses desde 2023 en una proporción parecida
  (10–14 %); es un comportamiento estable de PremierOne.
- **Sin `no_incidente`:** 99.333 registros (9,4 %). Antes de nov-2025 eran 1–2 % al mes; desde entonces subió con
  picos de 69.591 en dic-2025, 46.829 en mar-2026, 26.026 en may-2026 y 34.813 en jul-2026. Sin ese número la
  llamada no se puede deduplicar ni cargar.
- **127.524 filas con `fecha_inicial` vacía** en toda la tabla; de ellas, 126.604 tampoco tienen incidente. Son
  filas vacías dentro de los archivos entregados: 121.649 vienen de `10-Mebog-CIAPD2_Automatico_1_enero_14_sep_parte_2`.
  Se abrió el Excel original que entregó GUTIC y trae 634.481 filas totalmente vacías en la hoja
  `10-Mebog-CIAPD2_Automatico`, así que el daño viene del archivo y no de la conversión.
- **19.839 filas repetidas** con el mismo número de incidente. Nuestro loader las deduplica.
- **Archivos Parquet duplicados (nuestro lado, fuera de la ventana):** del 18 al 30 de noviembre de 2025 hay 11 días
  convertidos dos veces con nombres distintos (`...Automatico-GUTIC_21-11-2025` y `...Automatico_GUTIC_21-11-2025`).
  GUTIC entregó un solo Excel por día, así que la copia repetida la generó nuestra conversión. Conviene borrar una de
  cada par en `s3://pon-prod-llamadas/data/mdp/llamadas-mebog/`.
- El 59,5 % no trae `cai`. No afecta al mapa, porque la unidad se asigna por coordenadas.

### 4.3 Hexagon (MEVAL)

- 3.800 llamadas (0,9 %) con coordenada 0.
- 35.412 filas repetidas: 33.985 incidentes aparecen dos veces, casi siempre (33.108) en **archivos distintos**. El
  cliente entrega archivos que se solapan. Nuestro loader ya deduplica por `cod_incidente`, así que no afecta al mapa.
- Hexagon no entrega datos de marzo de 2026, y febrero de 2026 llegó casi vacío. Su último dato es de alrededor del
  2026-10-11 (ver el informe de MEVAL).

### 4.4 Antecedentes (SIVICC)

- **IMEI: 2.810.092 consultas (17,2 %) sin `undelaborando`.** Salen en el mapa nacional, pero no al filtrar por
  unidad. Las consultas de personas no tienen este problema y las de vehículos solo en 11.615 casos.
- Además, del informe del 2026-10-28: el 12,7 % de las consultas de personas trae un `codigo_unidad` que no existe
  en el catálogo nacional de unidades.
- Las coordenadas son prácticamente perfectas: menos de 7 mil inválidas sobre 172 millones de consultas.

### 4.5 RNMC

- La base CNP llega limpia:
  - 4.599 hechos sin coordenada (0,4 %); la vista `boletin_z1_capas_v2` los excluye;
  - 3.380 sin cuadrante, que quedan como `(SIN SIGLA)`;
  - 29 sin infractor (persona tipo 350); por el `INNER JOIN` de la vista, no llegan a la capa.
- **6.533 hechos aparecen dos veces** en la tabla cruda. Las copias difieren en algún campo (ubicación, cuadrante o
  estado), así que pueden ser modificaciones copiadas dos veces por la carga incremental. **El origen no está
  confirmado** (ver la sección 5).

### 4.6 GESCOV y fuentes internas

- **GESCOV:** sin problemas de ID ni de coordenadas.
  - Los 72.074 planes de noviembre sin `estado_cumplimiento` son normales: aún no se evalúan.
  - En mayo–octubre faltan 12.049 estados (0,6 %).
  - La vista materializada ya se refresca de nuevo (último refresco: 2026-11-01). El congelamiento en 2026-09-30
    del informe anterior quedó resuelto en la fuente.
- **SIEDCO:**
  - 1.036 hechos con coordenada 0.
  - Las 232.893 filas con `hechos_id` repetido **no son error**: un hecho tiene una fila por persona o bien.
- **SIMEP, IRIS P1, SIRAP y MTRS:** menos de 100 registros dañados cada una. En MTRS y SIMEP hay fechas imposibles
  en el histórico (año 0004 o fechas futuras).

## 5. Lo que NO es de la fuente

| Hallazgo | Responsable | Evidencia |
|---|---|---|
| 472.647 filas duplicadas en `auditoria_cons_persona` | **Nuestra copia** | Son copias idénticas y todas caen en un solo bloque de IDs (284.684.160 a 285.156.816): una carga se repitió. Conviene deduplicar en `V2_SIVICC-migration-to-RDS`, si no lo hace ya. |
| 6.533 hechos duplicados en `cnp_hechos` | **Por confirmar** | Copias con algún campo distinto. Puede ser la carga incremental o la base CNP; se resuelve comparando con Oracle. |
| 11 días de PremierOne (18–30 nov-2025) con dos Parquet idénticos | **Nuestra conversión** | En `raw-data-premier-one/` hay un solo Excel por día, pero en `llamadas-mebog/` hay dos Parquet del mismo tamaño con nombres distintos. |

## 6. Recomendaciones

1. **Coordenadas de SECAD.** Escalar a las SECAD de MEBAR, DEATA, MEPER, MEMAZ, DEVAL, DEANT y MECUC: entre todas
   suman unas 390 mil llamadas atendidas sin ubicación en 6 meses. Como alternativa nuestra, geocodificar la
   dirección (`dire_caso`) cuando falten las coordenadas.
2. **PremierOne (MEBOG).** Reportar al proveedor que llegan registros sin número de incidente (9,4 %, en aumento
   desde nov-2025) y filas vacías dentro de los archivos.
3. **Consultas de IMEI.** Pedir a SIVICC que diligencie la unidad que hace la consulta (17 % vacía).
4. **Hexagon.** Pedir que los archivos no se solapen, y las entregas faltantes de febrero y marzo de 2026.
5. **Nuestro lado.** Deduplicar `auditoria_cons_persona` en la carga a RDS, confirmar el origen de los duplicados de
   `cnp_hechos` y borrar los 11 Parquet repetidos de PremierOne.

## 7. Cómo repetir la medición

Los scripts están en `informes/calidad_fuentes/`: `cal_perfil.py` hace el perfil por fuente y `cal_check.py` los
cruces de control (se corren desde esa carpeta con credenciales del perfil PON-PROD; `cal_resumen.py` genera el CSV de
detalle con nombres cortos y `columnas.py` lo convierte a los nombres legibles y genera el diccionario). Cada fuente es una consulta Athena con la misma lógica de clasificación de coordenadas. La corrida
completa escanea unos 10 GB; los más grandes son `auditoria_cons_persona` y `cadpedidos`.
