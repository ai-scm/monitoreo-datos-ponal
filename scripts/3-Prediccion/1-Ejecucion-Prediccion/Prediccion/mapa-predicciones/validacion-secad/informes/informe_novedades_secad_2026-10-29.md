# Informe: ¿por qué hay novedades en las capas SECAD? (2026-10-29)

Corrida analizada: `salida/novedades_2026-10-29_14-48-36.csv` (ventana 22–28 oct. contra 15–21 oct.).
Diagnóstico fila por fila: [`diagnostico_novedades_secad_2026-10-29.csv`](diagnostico_novedades_secad_2026-10-29.csv)
(las 133 novedades de nivel sigla, con responsable, causa raíz, evidencia y acción sugerida).

Todo se verificó en solo lectura: Athena (tablas crudas, vistas y capas), historial y scripts de Glue, S3, catálogo de Glue,
CloudWatch (Lambda) y estado/eventos de DMS. No se leyó ningún secreto ni se tocó la base de la Policía.

## 1. Respuesta corta

Las novedades vienen de **tres lugares distintos**, y pesan distinto:

| Origen | Qué pasa | Peso |
|---|---|---|
| **Nuestro pipeline** | (a) el ETL no lee 10 siglas completas; (b) el crawler corre al mismo tiempo que los jobs y el último día queda a medias; (c) cada fila hasta el 25-oct está duplicada; (d) Bogotá (MEBOG) no tiene quién la cargue | Se pierde **16–21 % de las llamadas** y 14 de las 31 FALLAS de sigla |
| **La fuente (base/CAD de la Policía y su replicación)** | (a) **solo 15,8 % de las llamadas llegan con coordenadas**; (b) caídas de conexión Oracle→AWS (`OCI connection failure`), una masiva el 26-oct 23:00–01:00 y varias crónicas; (c) algunas salas casi no registran o bajaron su volumen | 17 de las 31 FALLAS de sigla |
| **Limpieza / asignación de unidad** | ETIALI no ubica en ningún polígono la mayoría de los puntos de varias siglas (DESUC 91 %, DEANT 98 %…) | 54 avisos de sigla y buena parte de las ~1.270 unidades "sin datos" |

La **limpieza como tal (filtros, deduplicado, categorías) está bien**: la conciliación llamada por llamada contra la fuente
da 93,3 % idénticas, 3,1 % reasignadas a propósito a otra sigla, 3,6 % por rezago de visibilidad y **solo 3 llamadas
realmente perdidas de 6.341** en riñas.

## 2. Cómo se verificó

1. Se leyeron los scripts reales: Lambda `pon-prod-scheduler-capas-secad`, job `pon-prod-capas-secad-etl`, y las vistas
   `v_pedido_atendido_partitioned` / `_separadas` / `_consolidada`.
2. **Conciliación llamada por llamada** (15–28 oct): cada llamada de la vista de origen se buscó en las capas y se clasificó:
   misma sigla, reasignada, en archivo S3 sin partición registrada, pendiente del próximo job, o perdida.
3. **Salud de la fuente cruda** (`cadpedidos`): filas y filas con coordenadas por SECAD y día, y eventos/estado de las 96 tareas DMS.
4. Cada novedad de sigla se clasificó con esa evidencia (CSV de diagnóstico).

## 3. Hallazgos en nuestro pipeline

### P1. El ETL no lee 10 siglas (la causa de 10 de las 17 FALLAS "sin datos en ninguna capa")
`pon-prod-capas-secad-etl` consulta `v_pedido_atendido_partitioned`, y esa vista **excluye a propósito** seis SECAD compartidos
(`raw_secad_deces, menev, mepoy, mesan, mebar, metun`). Las llamadas de esos SECAD las reparte por canal la vista
`v_pedido_atendido_separadas` en 12 siglas, pero **ningún job de capas lee esa vista**. La vista `_consolidada` (partitioned + separadas)
solo la usa el job de homogeneización.

- Siglas afectadas: DEBOY, DECAU, DECES, DEMAG, DEUIL, MENEV, METUN, MEPOY, MESAN, MEVAP.
- La fuente sí tiene llamadas con coordenadas (15–28 oct): **1.480 riñas, 113 hurto violento/homicidio y 326 hurto general** que no llegan a ninguna capa:
  16–21 % de todo lo procesable.
- Eran siglas con historia desde 2023-10; su última partición cargada es **2026-08-31** (DECAU, DECES, DEUIL, MENEV, MEPOY, MEVAP),
  2026-09-20 (METUN) y 2026-09-28 (DEBOY).

### P2. Carrera entre el crawler y los jobs: el último día queda a medias
`pon-prod-capas-crawler` corre a las `cron(00 09 * * ? *)` UTC, el **mismo minuto** que la regla que lanza los jobs (04:00 Colombia).
El crawler terminó a las 04:12; los jobs escriben a las 04:04, así que las particiones nuevas (sigla/unidad/día nunca vistos)
no quedan registradas y Athena (y el BFF) no las ve hasta el crawler del día siguiente.

- Hoy a las 15:00, de las llamadas del 28-oct que el job ya procesó había **133 riñas escondidas en 47 particiones S3 sin registrar** (36 del 28 y 11 del 27), más 2 de hurto general y 1 de hurto violento.
- Además el job toma la partición CDC del día anterior (UTC): las llamadas de la tarde/noche de un día (después de las 19:00) llegan en la partición siguiente y se procesan un día después (93 riñas esperan al job de mañana).
- Resultado: **el día D solo está completo después de la corrida de D+2**. El 28-oct se ve al 39 % de lo que ya existe en la fuente. Esto explica la mayoría de los `DIAS_SIN_DATOS` del 27 y 28 y algunas rachas del final de la ventana.

### P3. Filas duplicadas hasta el 25-oct
El Lambda `pon-prod-scheduler-capas-secad` se invocó **2 veces por día** hasta el 25-oct (una por día desde el 26-oct) y la regla `pon-prod-capas-scheduler`
tiene un solo destino, así que la segunda invocación vino de otro lado que no pude identificar. Cada noche corrieron dos jobs por capa (con 04:00 y 04:01) y escribieron la misma llamada dos veces
(archivos de dos corridas distintas). En las capas, **toda fila con fecha ≤ 25-oct está duplicada** (≈2,0 filas por llamada). Desde el 26-oct no hay duplicados.
El job no deduplica contra lo ya cargado en estas capas (solo lo hace con `--campo_dedup_historico`, como capa_10).

Filas por llamada distinta en las capas (excluye MEBOG):

| Mes 2026 | Riñas (capa_13) | Hurto violento/homicidio (capa_11) | Hurto general (capa_12) |
|---|---|---|---|
| Julio | 64.352 filas / 22.347 llamadas (2,9×) | 4.333 / 1.325 (3,3×) | 21.073 / 5.074 (4,2×) |
| Agosto | 67.962 / 21.906 (3,1×) | 3.729 / 1.194 (3,1×) | 19.495 / 4.877 (4,0×) |
| Septiembre | 43.870 / 16.473 (2,7×) | 2.306 / 830 (2,8×) | 11.988 / 3.662 (3,3×) |
| Octubre | 21.196 / 12.140 (1,75×) | 1.501 / 789 (1,9×) | 5.719 / 3.028 (1,9×) |

Hasta octubre la proporción es mayor que 2× (en julio–septiembre hay más multiplicación que la de las dos corridas por noche; no investigué esa causa, que
puede ser la multiplicación de filas de los JOIN de la vista antes de que el job deduplicara por `NUME_LLAMADA`). El 1,75–1,9× de octubre es el efecto de las dos corridas por noche
hasta el 25-oct.

El validador no se infla por esto (cuenta `puntos` distintos), pero el BFF devuelve el doble de filas.

### P4. MEBOG: nadie carga Bogotá
Bogotá no sale de SECAD sino de PremierOne (`raw_mebog.llamadas_mebog`). **La fuente está al día** (último registro 2026-10-28, archivos de hoy),
pero las capas de MEBOG quedaron en 2026-09-04 (riñas), 2026-09-16 (hurto general) y 2026-08-11 (hurto violento).
`premierone_to_capas` corrió por última vez el 2026-09-21 y ningún programador lo ejecuta. Con MEVAL pasa algo parecido: su loader (`hexagon_to_capas`)
corrió por última vez el 2026-01-21 (el validador no lo marca porque el volumen es bajo).

### P5. Asignación de unidad (ETIALI / UnidadesBase)
En riñas del 15 al 26 de oct., el `codigo_unidad` es 0 en: DEANT 98 % de las filas, DESUC 91 %, DECUN 81 %, METIB 55 %, MECUC 51 %, DESAN 47 %, DECAQ 46 %.
De los puntos sin unidad, solo caen dentro de algún polígono de su `…-UnidadesBase.geojson`: DESUC 1 de 403 (41 polígonos), DEANT 0 de 89, METIB 25 de 571, MECUC 14 de 167 y DECUN 3 de 142
(y el ETL tampoco los encuentra en las demás siglas). O faltan polígonos o las coordenadas del CAD son imprecisas. Esas filas se ven al elegir la sigla, no la unidad.
MESOA además recibe códigos (64684, 64690, 64689…) que existen en los polígonos pero no en el catálogo del mapa.

### Observaciones menores (no son fallas hoy)
- 6 llamadas perdidas de ~8.400 (0,07 %), todas de DEVIC (15–18 oct). No se explican con lo que se puede ver; el job lee cada partición CDC una sola vez, así que es un riesgo latente si una tabla hija llega tarde (el `INNER JOIN` de la vista).
- `DESFASE_PARTICION` (7 siglas, 30 unidades): la partición sale de `FECHA_CASO` y hay llamadas cerca de medianoche con `HORA_CASO` del día siguiente (p. ej. fecha 24-oct 00:00 y hora 25-oct 01:58). Es el dato de origen; efecto mínimo.
- La SECAD de Cali (MECAL) y la de Tunja (METUN) recargan en el crudo historia anterior a 2023-10 (17,8 M y 1,9 M filas en 11 particiones); la vista las filtra, solo cuesta rendimiento.

## 4. Hallazgos en la fuente (base de la Policía y su replicación)

### F1. Casi nada llega con coordenadas
En 22–28 oct, de **148.807 llamadas** de las 44 SECAD, solo **23.498 (15,8 %)** tienen latitud/longitud; el resto llega con `0` y la vista las descarta (con razón: no se pueden dibujar).

| SECAD | Llamadas | Con coordenadas |
|---|---|---|
| DENAR, DEURA | 599, 530 | **0 %** |
| MEBAR (Barranquilla, incluye DEATA) | 25.953 | **0,0 %** (3) |
| MEVIL | 6.117 | 0,4 % |
| MEMOT, MEPER | 8.822, 3.011 | 1,9 %, 2,1 % |
| DEANT, MESAN, MECUC | 8.582, 2.492, 8.704 | 3,1 %, 4,3 %, 5,8 % |
| MECAR, DEAMA, DECUN | 7.549, 806, 6.568 | 7,0 %, 8,7 %, 10,5 % |
| MEBUC, DEQUI | 14.317, 4.341 | 13,6 %, 17,2 % |
| (buenos) DECAS, DETOL, DEGUV, DESAP, DESAN | | 99–100 % |

Esto explica por qué DENAR, DEURA y DEATA no tienen nada y por qué hay tantas rachas en MEVIL, DEANT y similares.

### F2. Caídas de conexión Oracle → AWS (DMS)
En 14 días hubo **2.294 eventos "failed"** en las tareas DMS de SECAD: 1.758 `OCI connection failure` (DMS no logra conectarse a la base Oracle de origen),
318 que agotaron los reintentos (9–10) y 65 `Endpoint initialization failed`.

- **Caída masiva el 26-oct entre las 23:00 y la 01:00**: 62 tareas distintas fallaron en cada una de esas dos horas (casi todas las SECAD a la vez). Apunta a red/VPN/base de origen, no a una SECAD concreta. Las tareas se reintentan solas (inicios a las 00:35).
- **Crónicas**: DECUN 368 eventos (las dos tareas están en `failed` ahora mismo, hoy 14:35–14:58; el 28-oct llegaron solo 144 filas contra ~1.000 diarias), DEURA 249, DEVAU 182, MESAN 118, DEVIC 116, METIB 109, DEBOL 92, y DECOR (en `failed` ahora).
- **DEATA**: su tarea falla casi cada hora con `Endpoint initialization failed` desde el 27-oct (50 eventos; apunta a la configuración del endpoint, no a la red).
- **DEVAU**: la carga inicial de `CADPEDIDOS` trajo solo 480 filas y no hay un solo insert; la tarea no avanza desde que existe. No hay datos de DEVAU en la fuente.
- Desde AWS solo se ve que DMS no conecta; no se puede saber si es firewall, VPN, listener o la base caída: eso lo tiene que revisar el equipo de redes/DBA de la Policía.

### F3. Salas que casi no registran o bajaron su volumen
- **DECHO** (17 filas en 16 días) y **DEGUA** (9): la SECAD casi no se usa; DMS replica sin problema.
- **DEMAM**: el crudo cayó de 931 filas (15–21 oct) a 100 (22–28 oct), bajando desde el 21-oct sin fallas DMS. Es la causa de su `CAIDA_A_CERO` en hurto general.
- **DESAN**: la mitad de volumen desde el 25-oct (5–24 llamadas/día contra 40–80).
- **DEPUY**: **cero llamadas el 25-oct** (venía con ~110/día) y sin falla DMS ese día.
- **DETOL**: 0–1 llamada por día salvo dos picos (111 y 107 los días 22 y 23).

## 5. Las 31 FALLAS de sigla, una por una

| Sigla / capa | Código | Responsable | Causa |
|---|---|---|---|
| DEBOY, DECAU, DECES, DEMAG, DEUIL, MENEV, METUN, MEPOY, MESAN, MEVAP | Sin datos en todas las capas | **Pipeline** | P1: vista `partitioned` excluye la SECAD compartida; ETL no lee `separadas` |
| MEBOG | Sin datos en todas las capas | **Pipeline** | P4: `premierone_to_capas` sin programar desde 21-sep |
| DEATA | Sin datos en todas las capas | Fuente | F1+F2: 0 % con coordenadas en su SECAD y endpoint DMS fallando |
| DENAR, DEURA | Sin datos en todas las capas | Fuente | F1: 0 % de llamadas con coordenadas (DEURA además con 249 fallas DMS) |
| DEVAU | Sin datos en todas las capas | Fuente | F2: DMS nunca replicó llamadas |
| DECHO, DEGUA | Sin datos en todas las capas | Fuente | F3: casi sin actividad en SECAD |
| DECUN / hurto violento | Racha sin datos | Pipeline (por diseño) | Las 3 llamadas se reasignaron a MESOA; no hay pérdida |
| DEANT / hurto general | Racha sin datos | Pipeline (por diseño) | Las 2 llamadas se reasignaron a MEVAL |
| MECUC / hurto violento | Racha sin datos | Pipeline | P2: 2 llamadas del 28 llegan en la partición de mañana |
| DEMAM / hurto general | Caída a cero | Fuente | F3: el crudo cayó de 931 a 100 filas |
| DESAB / riñas | Racha sin datos | Fuente | No tiene SECAD propio: recibe llamadas de DECUN, cuya replicación falla (F2) y cuyas riñas bajaron a 12, 8 y 5 por día |
| MEVIL / hurto general | Racha sin datos | Fuente | F1: 24 de 6.117 llamadas con coordenadas |
| DENOR (hurto general y hurto violento), DEPUY (hurto violento), DESAN (hurto general), DESAP (hurto general), DETOL (riñas), MECAL (hurto violento), MEPAS (riñas) | Racha sin datos | Fuente | La vista tampoco tiene llamadas de esa categoría con coordenadas esos días; la sigla sí recibe llamadas. Son conteos pequeños (3–13 esperadas): probable variación real, salvo DESAN y DEPUY que además bajaron su volumen (F3) |

## 6. Avisos (102 de sigla)

- 57 `UNIDAD_NO_ASIGNADA` (54 + 3 de MESOA): P5.
- 10 por rezago de visibilidad (`DIAS_SIN_DATOS` del 27 y 28 de oct., con llamadas ya en la fuente): P2.
- 8 `CAIDA_VOLUMEN` por muestra pequeña (de 4–14 a 2–5 llamadas): variación normal.
- 3 `CAIDA_VOLUMEN` con caída real del crudo y 1 día sin llamadas (DEPUY el 25-oct): F3.
- 4 de DESAB (`CAIDA_VOLUMEN`/racha): dependen de la SECAD de DECUN, F2.
- 7 `DESFASE_PARTICION`: dato de origen cerca de medianoche.
- 12 avisos de categorías sin llamadas con coordenadas (F1/F3).
- Nivel unidad (2.205 filas): 2.054 son "sin datos en ninguna capa"; 781 son consecuencia de que toda su sigla está vacía (P1, P4, F1–F3) y **1.273 son unidades de siglas con datos**: el CAD no registra hechos en esas unidades o ETIALI no las ubica (P5). Individualmente no son accionables.

## 7. Lo que descarté

- **Deduplicado por `NUME_LLAMADA` solo**: no hay colisiones entre SECAD (llamadas distintas por número = llamadas distintas por origen+sitio+número); solo colapsa la multiplicación de filas de los JOIN.
- **Codificación de "RIÑA"**: el filtro llega bien (`RIÑA` en latin-1); lo que se veía como `�` era la consola.
- **Corridas fallidas del ETL**: de 10-27 a 10-29 las capas 11/12/13 terminaron en `SUCCEEDED`. El `SystemExit` y el `AMBIGUOUS_REFERENCE` de 16_1/16_2 son de otras capas.
- **Pérdida por reasignación de sigla**: el ETL reasigna ~3 % de las llamadas a la sigla que contiene el punto (DECUN → MESOA/DESAB, DEANT → MEVAL). Es correcto; el validador mide por sigla de origen y lo ve como racha.

## 8. Qué no pude comprobar

- La base Oracle ni la red de la Policía: no hay forma de ver desde AWS por qué falla `OCI connection failure`, ni si hubo llamadas en Oracle que DMS no replicó durante las caídas (DMS CDC retoma desde su punto, pero si los logs de Oracle ya rotaron se pierden).
- Quién invocó el Lambda por segunda vez cada noche hasta el 25-oct.
- Por qué se perdieron las 6 llamadas de DEVIC.
- El resultado final del 28-oct: se completa con los jobs de mañana y el crawler de mañana; conviene volver a correr el validador el 31-oct.

## 9. Acciones recomendadas (por impacto)

1. **Pipeline – leer `v_pedido_atendido_consolidada` (o `separadas`) en `pon-prod-capas-secad-etl`/Lambda** y recargar desde 2026-08-31. Recupera 10 siglas y 16–21 % de las llamadas.
2. **Pipeline – registrar particiones después de escribir**: mover el crawler a las 04:30 o añadir `enableUpdateCatalog`/`ALTER TABLE ADD PARTITION` al job. Hoy el último día está al 40–60 % durante 24 h.
3. **Pipeline – programar `premierone_to_capas` (MEBOG) y `hexagon_to_capas` (MEVAL)** con ventana diaria y cargar el hueco desde septiembre/enero.
4. **Pipeline – deduplicar el histórico ≤ 25-oct** en capa_11/12/13 y añadir deduplicado por (`sitio_graba`, `nume_llamada`) en el job; identificar la segunda invocación del Lambda.
5. **Policía/redes – caso por `OCI connection failure`**: caída del 26-oct 23:00–01:00 y tareas crónicas (DECUN, DEURA, DEVAU, DEVIC, METIB, DECOR); y el endpoint de DEATA. Conviene alarmar sobre tareas DMS en `failed`.
6. **Policía/CAD – georreferenciación**: 84 % de las llamadas sin coordenadas; prioridad MEBAR, DENAR, DEURA, MEVIL, MEMOT, MEPER, DEANT.
7. **Catálogo de unidades** – revisar polígonos de DESUC, DEANT, METIB, MECUC y DECUN, y los códigos 64684/64690/64689 de MESOA.
8. **Validador** – medir SECAD con `rezago=1`, contar la sigla de destino (post-reasignación) y avisar de particiones S3 sin registrar.
