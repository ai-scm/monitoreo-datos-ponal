# validacion-rnmc

Valida los datos de las **cinco capas RNMC del mapa** de los últimos **15 días**, por **sigla** y por
**unidad** (con sus unidades hijas), con el mismo patrón de [`../validacion-secad`](../validacion-secad/README.md).
Solo lectura, para correr en local. Genera al final un **CSV de novedades**.

> **Depende de `../validacion-secad`:** reutiliza su motor (`secad_comun.py` y `validar_secad.py`:
> sesión AWS, cliente Athena, catálogo/árbol de unidades, reglas y CSV). No muevas esa carpeta sin esta.
> Estado: 28 pruebas sin AWS y ejecutado contra producción el 2026-10-28 (cuenta `926162397524`).

## De dónde sale cada capa

| Capa del mapa | Origen real (BFF `capas_adicionales`) | Qué valida este script |
|---|---|---|
| RNMC: Riñas (`rnmc-rinas`) | RDS `public.hechos_z1_v2`, `articulo=27` y `numeral=1` | la vista de Athena que la alimenta + la carga a RDS |
| RNMC: Incautaciones (`rnmc-incautaciones`) | RDS `public.hechos_z1_v2`, `incautacion=1` | idem |
| Comportamientos contrarios a la convivencia (`comportamientos-contrarios-convivencia-materializados`) | RDS `public.hechos_z1_v2`, el resto | idem |
| RNMC: Mediaciones in situ (`rnmc-mediaciones-situ`) | Athena `pon-prod-capas-iapol.capa_16_1` | la tabla + el ETL diario |
| RNMC: Mediaciones en sala (`rnmc-mediaciones-sala`) | Athena `pon-prod-capas-iapol.capa_16_2` | la tabla + el ETL diario |

Las tres primeras **no están en Athena**: las sirve una base Postgres (`pon-prod-db-postgres`) que llena
a diario (09:00) el job de Glue **`RNMC-to-RDS`**, leyendo la vista `pon-prod-raw-cnp.boletin_z1_capas_v2`
del día anterior (por `fecha_creacion`), asignándole `codigo_unidad` por polígono y **agregando** filas
(`append`). Este script **no consulta RDS**: valida la vista (lo que debe llegar) y el historial del job
(si cada día se cargó). Las mediaciones las llena el ETL `pon-prod-capas-secad-etl` (04:00), que sale con
`SystemExit` ("No hay datos para procesar") y queda FALLIDO cuando la fuente devuelve 0 filas ese día.
`capa_20` (comportamientos) existe en Athena pero es la versión anterior a RDS y el BFF ya no la usa.

## Uso

```powershell
# 0. Una vez: esquemas, jobs y su historial
python descubrir_rnmc.py --profile <perfil>               # --muestra trae 3 filas de cada fuente

# 1. Validar (sin --catalogo lo lee de S3; el workgroup primary exige --output-s3)
$out = "s3://aws-athena-query-results-us-east-1-926162397524/"
python validar_rnmc.py --profile <perfil> --output-s3 $out
python validar_rnmc.py --sigla MEPAS MEBOG ...                         # solo algunas siglas
python validar_rnmc.py --capas rnmc-rinas rnmc-mediaciones-sala ...    # solo algunas capas
python validar_rnmc.py --fecha-fin 2026-10-20 ...                      # otra ventana, reproducible
python validar_rnmc.py --sin-carga ...                                 # sin leer el historial de los jobs
python validar_rnmc.py --solo-sql --sin-esquema --sin-catalogo         # ver el SQL sin tocar AWS
```

Requisitos, permisos, catálogo y credenciales: igual que en [`validacion-secad`](../validacion-secad/README.md).
Además: `glue:GetJobRuns` (historial de `RNMC-to-RDS` y del ETL) y lectura de la vista
`pon-prod-raw-cnp.boletin_z1_capas_v2`. Sin esos permisos las cargas quedan "sin evaluar" y el resto corre.

## Qué reporta

Ventana de **15 días completos hasta ayer** contra los 15 anteriores (`--ventana-dias`). Valores por
defecto distintos a SECAD: `--min-base 20` y `--min-esperado-falla 5` (una racha de días sin datos solo es
FALLA a nivel sigla si se esperaban ≥ 5 filas; con menos es AVISO). Para las capas de la vista, el día es
el de la **fecha del hecho** (lo que filtra el BFF) y la unidad se atribuye por `cuadrante2` → catálogo
(`CUADRANTES_ID`, 99 % de coincidencia); el job usa el polígono, así que puede diferir en casos de borde.

Los códigos de SECAD (`CAIDA_A_CERO`, `SIN_DATOS_EN_TODAS_LAS_CAPAS`, `RACHA_SIN_DATOS`, `CAIDA_VOLUMEN`,
`DIAS_SIN_DATOS`, `UNIDAD_NO_ASIGNADA`, `COORD_INVALIDA`, …) significan lo mismo. Nuevos aquí:

| Código | Nivel | Cuándo | Estado |
|---|---|---|---|
| `CARGA_RDS_FALLIDA` | CARGA | un día de creación sin corrida exitosa de `RNMC-to-RDS` y con una fallida; el mensaje dice cuántos hechos faltan, por capa | FALLA |
| `CARGA_RDS_AUSENTE` | CARGA | ídem, sin ninguna corrida ese día | FALLA |
| `CARGA_RDS_DUPLICADA` | CARGA | un día cargado más de una vez (la carga es `append`: posibles filas repetidas) | AVISO |
| `CARGA_CAPA_FALLIDA` / `CARGA_CAPA_AUSENTE` | CARGA | un día que el ETL no cargó a `capa_16_1`/`capa_16_2` | FALLA |
| `DIA_ATIPICO` | NACIONAL | un día con < 40 % (`--dia-atipico-pct`) de la mediana diaria nacional de esa capa | AVISO |
| `HECHOS_SIN_SIGLA` | SIGLA | hechos sin sigla ni cuadrante (`(SIN_SIGLA)` en la vista, `NO HA` en las mediaciones): el mapa no puede mostrarlos | AVISO |

Para decidir si un día está cargado, se interpreta el `--QUERY` de cada corrida (`current_date`, un rango de
fechas o `YEAR/MONTH/DAY`) y la tabla de destino: una corrida manual hacia `hechos_z1_v2_prueba` **no** cuenta.
Un `--QUERY` manual que no se pueda interpretar con seguridad (p. ej. con filtros extra) tampoco cuenta y se
informa. El día de ayer espera hasta las 11:00 (RDS) o las 07:00 (ETL) antes de darse por no cargado.

## Salida

En `salida/` (ignorada por git), sufijo con fecha y hora de Colombia: `resumen_siglas`, `resumen_unidades`,
`diario`, `consultas_rnmc.sql` y `meta_rnmc.json`, más:

- **`novedades_*.csv`**: el CSV de novedades. Un registro por hallazgo, ordenado FALLA primero y por nivel
  (`CARGA`, `NACIONAL`, `SIGLA`, `UNIDAD`). Columnas: `nivel, estado, sigla, unidad_id, unidad, tipo, capa
  (o TODAS), codigo, mensaje, contexto, sigla_sin_datos`. Excluye `SIN_DATOS` (unidad sin actividad, que queda
  en `resumen_unidades_*.csv`).

Exit: 0 sin FALLA, 1 con FALLA (incluidas las de carga), 2 argumentos, 3 bloqueado/error AWS, 130 interrupción.

## Qué no hace (y por qué)

- **No lee `hechos_z1_v2`.** Hace falta una conexión Postgres con credenciales (el BFF usa el secreto
  *administrador* del clúster). Es una decisión de seguridad que no está tomada: lo razonable es un usuario de
  solo lectura. Con ese acceso se podría comparar la vista contra la tabla, que es la prueba directa de lo que
  ve el mapa (y detectaría duplicados reales y filas sin `codigo_unidad`).
- Para las capas de la vista no mide `hora_invalida`, `hora_futura` ni `desfase_particion` (no aplican: la
  vista no tiene particiones y las filas sin fecha no entran al filtro).
- `puntos` cuenta hechos distintos con coordenadas válidas; el front puede colapsar filas idénticas.
- Un cero no prueba una falla de carga (igual que en SECAD).

## Pruebas

```powershell
python -m unittest discover -s tests -v
```

| Archivo | Rol |
|---|---|
| `descubrir_rnmc.py` | Paso 0: esquemas de la vista y de `capa_16_1/2`, jobs de carga y su historial |
| `validar_rnmc.py` | Validador: SQL por capa, cargas de los jobs, días atípicos, informe y CSV |
| `tests/test_rnmc.py` | Pruebas sin AWS |
