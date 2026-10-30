# validacion-secad

Valida los datos de las **tres capas SECAD del mapa** de la última semana, por **sigla** y por
**unidad** (con sus unidades hijas), directamente en **Athena**. Solo lectura. Pensado para
correr en local por ahora.

| Capa del mapa | Tabla Athena (`pon-prod-capas-iapol`) |
|---|---|
| SECAD: Riñas (`secad-rinas`) | `capa_13` |
| SECAD: Hurto violento y homicidio (`secad-hurtos-violento-y-homicidio`) | `capa_11` |
| SECAD: Hurto general (`secad-hurtos-general`) | `capa_12` |

> **Estado:** 52 pruebas sin AWS y **ejecutado contra producción el 2026-10-28** (cuenta
> `926162397524`): esquema, catálogo y consultas funcionan; una corrida completa de las 54 siglas
> escanea ~2 MiB. Lo que esa corrida confirmó o corrigió está en "Supuestos" y "Cómo se alimentan
> las capas".

> `secad_comun.py` y `validar_secad.py` son también el motor de [`../validacion-rnmc`](../validacion-rnmc/README.md) y
> [`../validacion-gescov-antecedentes`](../validacion-gescov-antecedentes/README.md) y
> [`../validacion-fuentes-internas`](../validacion-fuentes-internas/README.md); cambios en sus funciones públicas (`analizar`, `imprimir`, `construir_sql`, `Arbol`) deben probarse en ambas carpetas.

## Qué valida y por qué así

Las capas del mapa salen de estas tablas vía el BFF (`pon-iapol-capas/capas_adicionales`), que
filtra por `sigla`, `codigo_unidad` y la **partición** `anio/mes/dia`. El mapa pide:

- **Departamento o metropolitana** (tipo `4.0`/`5.0`): solo la sigla, sin `id_unidad` → entran
  todas las filas de la sigla, incluidas las que quedaron sin unidad (`codigo_unidad` 0/vacío).
- **Cualquier otra unidad**: la sigla más `id_unidad` = la unidad **y todas sus descendientes**
  visibles en el árbol.

El script reproduce ambas consultas: una consulta por capa agrupa por sigla, `codigo_unidad` y
día; el rollup padre→hijas se hace en Python con el árbol del catálogo, aplicando las mismas
reglas del front/backend (tipos `1.0`, `308.0`, `164.0`, `187.0`, `6.0` omitidos con sus hijas
ascendidas; unidad `64780` sin polígono fuera).

El **día** es el de la partición (lo que filtra el BFF), no el de `hora_caso`; la diferencia
se reporta como `DESFASE_PARTICION`. La ventana son los **7 días completos hasta ayer**, y se
compara con los 7 días anteriores. Hoy (incompleto) solo se usa para la última fecha.

## Uso

```powershell
# 0. Una vez: ver el esquema real y los jobs que alimentan las capas
python descubrir_secad.py --profile <perfil>            # --muestra trae 3 filas por capa

# 1. Catálogo de unidades (el mismo que arma el árbol del mapa)
aws s3 cp s3://pon-prod-ml-iapol/EstructuraNacionaOperativalPONAL.csv . --profile <perfil>

# 2. Validar (sin --catalogo lo lee solo desde S3; el workgroup primary exige --output-s3)
$out = "s3://aws-athena-query-results-us-east-1-926162397524/"
python validar_secad.py --profile <perfil> --output-s3 $out --catalogo .\EstructuraNacionaOperativalPONAL.csv
python validar_secad.py --sigla MEPAS MEBOG ...                   # solo algunas siglas
python validar_secad.py --capas secad-rinas ...                   # solo una capa
python validar_secad.py --fecha-fin 2026-10-20 ...                # otra semana, reproducible
python validar_secad.py --solo-sql --sin-esquema --sin-catalogo   # ver el SQL sin tocar AWS
```

Dependencias: Python 3.9+ y `boto3`. Credenciales: las de la máquina (`aws sso login`, perfil, o
variables de entorno). El script verifica que la cuenta sea `926162397524`. Si boto3 dice que el
token SSO expiró aunque `aws sts get-caller-identity` funcione (el CLI guarda credenciales de rol
en caché), renueva con `aws sso login --profile <perfil>`.

**Permisos:** `sts:GetCallerIdentity`, `glue:GetTable` (opcional `glue:GetJobs`, `GetJobRuns`),
`athena:StartQueryExecution/GetQueryExecution/GetQueryResults/StopQueryExecution`, lectura de las
tablas y de S3 (catálogo si se baja con `--catalogo s3://...`, y resultados de Athena). Si el
workgroup no define destino de resultados (el `primary` de esta cuenta no lo tiene):
`--output-s3 s3://...` o la variable `ATHENA_OUTPUT_S3`. El destino usado en las pruebas es el
bucket de resultados que ya usan los jobs de Glue de la cuenta.

**Catálogo.** Es el `EstructuraNacionaOperativalPONAL.csv` del backend (columnas `NOMBRE,
TIUN_CODIGO, CONSECUTIVO, SIGLA_FISICA, UNDE_CONSECUTIVO`, entre otras). El `id_unidad` del mapa es
`CONSECUTIVO` (no `UNIDAD_ID`, que solo existe en las unidades operativas) y coincide con el
`codigo_unidad` de las capas. También se acepta un CSV sin encabezados con las posiciones del
backend, o uno con `UNIDAD_ID, DEPENDENCIA, SIGLA_FISICA` (sin jerarquía, salvo que traiga una
columna de padre). Con `--sin-catalogo` solo se ven siglas y unidades que aparezcan en Athena: **no** detecta
unidades que no tengan ningún dato.

## Qué reporta

Cada fila es una (sigla o unidad) × capa con estado `OK / AVISO / FALLA`. A nivel **sigla** lo
grave es FALLA; a nivel **unidad** es AVISO, porque una unidad pequeña puede legítimamente no
tener casos en una semana.

| Código | Cuándo | Sigla / Unidad |
|---|---|---|
| `CAIDA_A_CERO` | 0 filas en la ventana y ≥ `--min-base` (10) en la semana previa | FALLA / AVISO |
| `SIN_DATOS` | 0 filas y sin base suficiente (solo en los resúmenes: es una unidad sin actividad, no una novedad) | AVISO |
| `SIN_DATOS_EN_TODAS_LAS_CAPAS` | ninguna de las capas tiene filas (sigla: en 2 semanas) | FALLA / AVISO |
| `SIN_PUNTOS_VALIDOS` | hay filas pero todas con coordenadas inválidas: el mapa no dibuja y el BFF responde 500 | FALLA |
| `RACHA_SIN_DATOS` | ≥ `--racha-dias` (2) días sin filas al cierre y se esperaban ≥ 3 filas en ese lapso | FALLA / AVISO |
| `CAIDA_VOLUMEN` | puntos < (1 − `--caida-pct`/100) × semana previa | AVISO |
| `DIAS_SIN_DATOS` | días intermedios sin filas (solo sigla) | AVISO |
| `UNIDAD_NO_ASIGNADA` | > `--max-invalidos-pct` de filas sin una unidad visible en el mapa para esa sigla; el mensaje da la causa (sin código, no existe en el catálogo, unidad oculta en el árbol, de otra sigla). Salen al elegir la sigla, no al elegir una unidad | AVISO (sigla) |
| `SIGLA_FUERA_CATALOGO` | hay datos de una sigla que el catálogo no tiene como departamento/metropolitana | AVISO |
| `COORD_INVALIDA` | coordenadas nulas o fuera de lon −82…−66 / lat −5…15 (el BFF las descarta) | AVISO |
| `HORA_INVALIDA` / `HORA_FUTURA` | `hora_caso` nula/ilegible, o > 24 h en el futuro | AVISO |
| `DESFASE_PARTICION` | `hora_caso` cae en un día distinto al de su partición | AVISO |

Opciones del motor que usan las otras carpetas (SECAD no las activa): `dias_atipicos(…, por_dia_semana, rezago)` compara
cada día nacional con la mediana de los mismos días de la semana (mínimo 3) y puede dejar fuera los últimos días, y
`analizar(…, rezago={capa: días})` no cuenta esos últimos días en la racha final ni en los días sin datos. Sirven para
fuentes con ciclo semanal o que se registran con atraso (SIMEP, SIEDCO); sin ellas todo se comporta como se describe arriba.

`puntos` cuenta pares (`hora_caso`, coordenadas) distintos con coordenadas válidas, que es como
el front deja un punto por llamada, así que se acerca a lo que se dibuja. `filas` es lo que hay
en la tabla.

Salida en `salida/` (ignorada por git), con sufijo de fecha y hora de Colombia:

- `resumen_siglas_*.csv`: sigla × capa (lo que ve el mapa al elegir toda la sigla).
- `resumen_unidades_*.csv`: unidad × capa en orden de árbol, con `consulta_mapa` (sigla completa o
  unidad + N descendientes), `filas_propias` (solo ella) y `sigla_sin_datos`.
- `novedades_*.csv`: **el CSV de novedades**. Un registro por hallazgo, ordenado FALLA primero,
  luego siglas y luego unidades en orden de árbol. Columnas: `nivel` (SIGLA/UNIDAD), `estado`,
  `sigla`, `unidad_id`, `unidad`, `tipo`, `capa` (o `TODAS`), `codigo`, `mensaje`, `contexto`,
  `sigla_sin_datos`. `contexto` explica las unidades sin datos (p. ej. qué porcentaje de las filas
  de su sigla quedó sin unidad asignada); `sigla_sin_datos=SI` marca las que son consecuencia de
  que toda su sigla está vacía. No incluye `SIN_DATOS` (ver arriba).
- `diario_*.csv`: sigla × `codigo_unidad` × capa × día, incluidos códigos fuera del catálogo.
- `consultas_secad_*.sql` y `meta_secad_*.json`: SQL ejecutado y configuración.

En consola, las unidades de una sigla que no tiene datos en esa capa no se repiten una a una
(se cuentan aparte); están completas en `novedades_*.csv`. Exit: 0 sin FALLA, 1 con FALLA, 2
argumentos, 3 bloqueado/error AWS, 130 interrupción.

## Qué no hace

- No valida el **origen** (Oracle on-prem, DMS, tablas crudas `pon-prod-raw-secad`): solo las
  tablas que lee el mapa. Un hallazgo dice *dónde* falta el dato en la capa, no *por qué*.
- No llama al BFF ni al front: no detecta fallas propias del servicio (por ejemplo, que ante un
  error de Athena responda 204 igual que ante "sin datos").
- Un cero no prueba una falla de carga. Por eso es AVISO a nivel unidad y se compara con la
  semana previa.

## Cómo se alimentan las capas (visto con `descubrir_secad.py`, 2026-10-28)

`capa_11/12/13` viven en `s3://pon-prod-raw-llamadas-unificado/homogenized/capa_NN/` (parquet,
particionadas por `sigla, codigo_unidad, anio, mes, dia`; valores sin cero a la izquierda: `'7'`,
`'10'`). Las llenan **varios jobs de Glue**, cada uno de una fuente CAD distinta:

| Job | Fuente | Última ejecución vista |
|---|---|---|
| `pon-prod-capas-secad-etl` | `pon-prod-raw-secad.v_pedido_atendido_partitioned` (SECAD), el día anterior | diaria, 04:00 |
| `premierone_to_capas` | `raw_mebog.llamadas_mebog` (PremierOne, Bogotá) | 2026-09-21, rango hasta 2026-09-13 |
| `hexagon_to_capas` | `pon-prod-hexagon.hexagon` | 2026-01-21, rango hasta 2026-01-20 |

Las categorías salen del `--query` de cada job: **riñas** = `MOTIVO` o `COD_INICIAL_DESC` igual a
`RIÑA`; **hurto general** = contienen `hurto` (esto, en el job de SECAD). El `query` de SECAD para
`capa_11` (hurto violento y homicidio) no estaba entre las ejecuciones listadas; el de Hexagon filtra
`HURTO CALIFICADO` u `HOMICIDIO` y el de PremierOne usa `MUERTO`/`HERIDO`. Por eso una sigla puede estar
vacía en las capas porque su CAD no la alimenta un job diario, no solo por una falla de carga.

## Supuestos

Confirmados en la corrida real: columnas y particiones de las tres tablas (`hora_caso` es texto,
`latitud/longitud` son `double`), `sigla` en mayúsculas, y que `codigo_unidad` es el `CONSECUTIVO` del
catálogo (en la ventana, ~72 % de las filas casan con una unidad del catálogo; el resto llega con
`codigo_unidad = 0` o ajeno).

Sin confirmar:

1. `hora_caso` se asume en hora local de Colombia (los días se cuentan por partición, así que solo
   afecta a `HORA_FUTURA` y a la última hora reportada).
2. Una unidad sin datos puede ser real (sin llamadas) o un problema de asignación: mirar la columna
   `contexto` de `novedades_*.csv`.
3. La ventana de 7 días y los umbrales (`--min-base`, `--caida-pct`, `--racha-dias`) son valores
   iniciales, no compromisos operativos.

## Pruebas

```powershell
python -m unittest discover -s tests -v
```

## Archivos

| Archivo | Rol |
|---|---|
| `descubrir_secad.py` | Paso 0: esquema Glue de las capas y la tabla cruda, jobs relacionados y su historial |
| `validar_secad.py` | Validador: SQL por capa, rollup por árbol, reglas, informe y CSV |
| `secad_comun.py` | Sesión AWS, cliente Athena, catálogo y árbol de unidades |
| `tests/test_secad.py` | Pruebas sin AWS |
| `informes/` | Informe de causas de las novedades (2026-10-29) y su diagnóstico fila por fila |
