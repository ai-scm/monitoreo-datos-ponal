# Fuentes con novedades: bases, conexiones, tablas y consultas

Acompaña a `../informe_calidad_fuentes_2026-11-03.md`. Las columnas del CSV de detalle se explican en
`../calidad_fuentes_diccionario_columnas.csv`. Los datos de conexión salen de los endpoints de AWS DMS y
de las conexiones de AWS Glue de la cuenta PON-PROD (926162397524, us-east-1), consultados el 2026-11-03.

**Credenciales.** Aquí no hay claves. Las conexiones de Glue y los endpoints de DMS guardan la clave en Secrets
Manager, y esta guía no la lee. Para consultar, lo correcto es pedir al DBA de cada base un usuario de **solo
lectura**.

**Red.** Las bases Oracle están en la red de la Policía (direcciones `172.x` y nombres `*.prod.mdp.local`). AWS
llega a ellas por la VPC. Desde un portátil hace falta la VPN de la Policía, o un equipo dentro de la VPC de
PON-PROD (EC2/Cloud9).

**Sin acceso a Oracle.** Las mismas novedades se ven en Athena sobre la copia cruda. Esa copia es fiel a la base:
DMS solo selecciona tablas y el job JDBC copia las columnas tal cual.

| # | Fuente / validador | Base | Cadena de conexión | Usuario conocido | Tablas | Consultas |
|---|---|---|---|---|---|---|
| 1 | SECAD (capas 11/12/13) | Oracle, una por SECAD regional | `jdbc:oracle:thin:@//<SIGLA>.secad.prod.mdp.local:1521/secad123` (ver la tabla de hosts) | `USR_MOTPOL` (DMS) | `USR_CAD.CADPEDIDOS`, `USR_CAD.CADPEDIDOS_CANALES`, `USR_CAD.CADCASOS` | `consultas/01_secad_oracle.sql` |
| 1b | SECAD, réplica en AWS | Aurora PostgreSQL (primer salto de DMS) | `jdbc:postgresql://prod-mdp-ponal-reader.ciyl8uc12rup.us-east-1.rds.amazonaws.com:5432/raw_secad_<sigla>` | `read_only` (conexión Glue `prod-mecal-secad`) | `usr_cad.cadpedidos`, `usr_cad.cadpedidos_canales`, `usr_cad.cadcasos` | final de `01_secad_oracle.sql` |
| 2 | RNMC (riñas, comportamientos, incautaciones, mediaciones) | Oracle DBOPER | `jdbc:oracle:thin:@//172.28.18.185:1538/DBOPER` (Glue `pon-prod-raw-cnp-connection`). En DMS: `cnp.prod.mdp.local:1538/dboper` | `USR_CNP` (DMS) | `USR_CNP.CNP_HECHOS`, `USR_CNP.CNP_PERSONAS`, `USR_CNP.CNP_SOLICI_MEDIA` | `consultas/02_rnmc_cnp_oracle.sql` |
| 3 | Antecedentes (personas, vehículos, IMEI) | Oracle SIVICC | `jdbc:oracle:thin:@//172.28.18.185:1538/SIVICC` (Glue `pon-prod-raw-sivicc-connection`) | `USR_MNVCC` (Glue `glue_connection_sivicc`) | `USR_MNVCC.AUDITORIA_CONS_PERSONA`, `..._VEHICULO`, `..._IMEI` | `consultas/03_sivicc_antecedentes_oracle.sql` |
| 4 | GESCOV, MTRS, IRIS P1 | Oracle DISEC | `jdbc:oracle:thin:@//172.28.18.185:1538/DISEC` (Glue `pon-prod-raw-disec-connection`, `DISEC_CONNECTION-DIRECT`) | `USR_DISEC` | `USR_DISEC.V_EVOS_PLANES`, `USR_DISEC.V_MTRS_RIESGO_UBICACION` (base: `MTRS_RIESGO_UBICACION`), `USR_DISEC.V_GENERAL_IRISP` / `VM_GENERAL_IRISP` | `consultas/04_fuentes_internas_oracle.sql` |
| 5 | SIMEP | Oracle DBOPER | `jdbc:oracle:thin:@//172.28.18.185:1538/DBOPER` (Glue `pon-prod-raw-simep-connection`) | (en secreto) | `USR_SIMEP.V_SIMEP_AUTOS_ARCGIS` (+ `V_SIMEP_MEDIDAS_ARCGIS`, `V_SIMEP_INTERVINIENTES_ARCGIS`) | `04_fuentes_internas_oracle.sql` |
| 6 | SIRAP | Oracle GEOPOL | `jdbc:oracle:thin:@//172.28.18.192:1521/GEOPOL` (Glue `pon-prod-raw-geopol-connection`) | (en secreto; DMS usa `USR4DBLGEOPOL2AWS`) | `USR_PMU.ACTIVIDADES` | `04_fuentes_internas_oracle.sql` |
| 7 | SIEDCO | Oracle SIEDCO | `jdbc:oracle:thin:@//172.28.4.213:1521/SIEDCO` (Glue `pon-prod-raw-siedco-connection`) | (en secreto) | `USR_ESTA.MOTIVOSPOLICIA`, que el job `pon-prod-full-siedco-motivospolicia-temporal` copia a `raw_data_siedco_full_delitos` | `04_fuentes_internas_oracle.sql` |
| 8 | PremierOne (MEBOG) | **Sin conexión a la base.** GUTIC sube un Excel por día a `s3://pon-prod-mebog-iapol/raw-data-premier-one/`; el job `pon-prod-premier-one-raw-to-parquet` lo pasa a `s3://pon-prod-llamadas/data/mdp/llamadas-mebog/` | En Athena `raw_mebog.llamadas_mebog`. La base de PremierOne (Motorola, normalmente SQL Server) solo la tiene GUTIC | consola AWS / Athena | — | `consultas/05_premierone_hexagon_athena.sql`; para GUTIC: `consultas/06_premierone_plantilla_para_gutic.sql` |
| 9 | Hexagon (MEVAL) | **No es una base**: archivos Parquet en S3 | `s3://pon-prod-raw-data-unfiltered/data/hexagon/`, en Athena `"pon-prod-hexagon".hexagon` | consola AWS / Athena | — | `05_premierone_hexagon_athena.sql` |

En Oracle, el último segmento de la cadena (`secad123`, `DBOPER`, `SIVICC`, …) es el nombre registrado en DMS o en
Glue. Si la conexión rechaza el formato de servicio (`@//host:puerto/servicio`), pruebe el de SID:
`@host:puerto:SID`.

## Hosts de SECAD (endpoints DMS de origen, puerto 1521, servicio `secad123`)

Las siglas con más llamadas sin coordenadas van primero:

| Sigla | Host | Sin coordenadas (atendidas) |
|---|---|---:|
| MEBAR | `MEBAR.secad.prod.mdp.local` | 99,1 % |
| DEATA | `DEATA.secad.prod.mdp.local` (tarea DMS con fallas recurrentes) | 96,0 % |
| MEPER | `MEPER.secad.prod.mdp.local` | 76,7 % |
| DEVAL | `DEVAL.secad.prod.mdp.local` | 74,6 % |
| MEMAZ | `MEMAZ.secad.prod.mdp.local` | 72,0 % |
| MECUC | `MECUC.secad.prod.mdp.local` | 60,4 % |
| DEANT | `DEANT.secad.prod.mdp.local` | 59,9 % |
| MESAN | `MESAN.secad.prod.mdp.local` | 59,0 % |
| MEPAS | `172.22.66.170` | 58,9 % |
| MEVIL, DEURA, DEGUN, DEVAU | `<SIGLA>.secad.prod.mdp.local` | 91–100 % (pocas llamadas). Las tareas DMS de DEURA y DEGUN están en estado *failed*. |

Hosts por IP: DEBOL `172.26.227.4`, DECAL `172.26.42.253`, DESAP `172.27.25.10`, DESUC `172.26.226.2`, MEMOT
`172.26.105.220` y MEPAS `172.22.66.170`. DEAMA usa el servicio `secad`, no `secad123`. Todas las demás siglas
siguen el patrón `<SIGLA>.secad.prod.mdp.local:1521/secad123`. En la réplica Aurora, la base es
`raw_secad_<sigla en minúsculas>`.

## Qué prueba cada consulta

| Fuente | Novedad (del informe) | Consulta | Qué esperar si es de la fuente |
|---|---|---|---|
| SECAD | 484.585 atendidas sin coordenadas | 01 · N1 | Mismos conteos por sigla que en `calidad_secad_por_sigla_2026-11-03.csv`. La consulta por operador muestra quién registra sin ubicación. |
| SECAD | 2,86 M sin canal | 01 · N2 | Casi todo es `TIPO_PEDIDO = '01'` sin coordenadas: no son incidentes. |
| SECAD | 85 códigos sin catálogo | 01 · N3 | Códigos que no están en `CADCASOS`. |
| RNMC | 4.599 sin coordenadas, 3.380 sin cuadrante, 29 sin infractor | 02 · N1–N3 | Mismos órdenes de magnitud. |
| RNMC | 6.533 IDs duplicados en AWS | 02 · N4 | **Si Oracle devuelve 0 filas, el duplicado es de nuestra carga incremental.** |
| Antecedentes | IMEI 17,2 % sin unidad | 03 · N1 | La columna `FUENTE` indica qué canal o aplicación consulta sin unidad. |
| Antecedentes | 472.647 personas duplicadas | 03 · N4 | Debe dar filas = IDs: así se confirma que el duplicado es de nuestra copia. |
| GESCOV | estados de cumplimiento nulos | 04 · N1 | Noviembre al 100 % (normal); los meses anteriores alrededor del 0,6 %. |
| MTRS / SIMEP | fechas imposibles | 04 · N2, N5 | Año 0004 o fechas futuras. |
| IRIS, SIMEP, SIRAP, SIEDCO | coordenadas inválidas | 04 · N4–N7 | Pocas decenas por fuente; SIEDCO alrededor de 1.000 con coordenada 0. |
| PremierOne | 9,4 % sin incidente, 10 % coordenada 0, filas vacías | 05 · N1–N3 | Con `"$path"` se ve el archivo exacto. Las 121.649 filas vacías vienen de `10-Mebog-CIAPD2_Automatico_1_enero_14_sep_parte_2`; el Excel original de GUTIC ya las trae. Los 11 días de nov-2025 con dos Parquet (`...-GUTIC_21-11-2025` y `..._GUTIC_21-11-2025`) son de **nuestra** conversión: GUTIC entregó un solo Excel. |
| PremierOne | lo mismo, en la base de GUTIC | 06 · N1–N3 | Plantilla T-SQL: GUTIC debe poner la vista del reporte `10-Mebog-CIAPD2_Automatico`. |
| Hexagon | coordenada 0, entregas solapadas, meses faltantes | 05 · N4–N6 | Los mismos incidentes aparecen en reportes de días consecutivos. |

Las consultas Oracle usan `TO_NUMBER(... DEFAULT NULL ON CONVERSION ERROR)`, que requiere Oracle 12.2 o posterior.
Si la base es más antigua, quite los bloques de coordenadas *fuera de Colombia* y deje los de nulo/0.
