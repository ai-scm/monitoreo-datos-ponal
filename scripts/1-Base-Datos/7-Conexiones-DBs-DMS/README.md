# A — Conexión y cambios de IP/credenciales
# 7 — Conexiones a bases (DBs / DMS)

Contenido de esta carpeta:

**Scripts (funcionan):**
- **`monitor_conexion.py`** — descubre endpoints, revisa antigüedad de credenciales
  y detecta cambios de IP/host/puerto (CloudShell normal). Ver abajo.
- **`test_endpoint_dms.py`** — prueba real de conexión de un endpoint vía
  `dms:TestConnection` (CloudShell normal). Ver abajo.

**`docs-investigacion/`** (documentación de referencia):
- **`HALLAZGOS.md`** — resumen de lo investigado (LogMiner/PDB en MEBUC, credenciales
  antiguas, endpoints sin configurar, datos de red verificados). Insumo para tickets.
- **`INVENTARIO-ENDPOINTS.md`** — inventario de los ~66 endpoints DMS Oracle
  (host:puerto por base/sigla).
- **`PRUEBA-VPC-ENVIRONMENT.md`** — cómo hacer el ping TCP real desde un CloudShell
  VPC environment, con los valores de VPC/subnet/SG verificados y **sin subir
  archivos** (en un entorno VPC la subida está deshabilitada).

---

`monitor_conexion.py` descubre los endpoints de las bases Oracle on-premise, revisa
la antigüedad de sus credenciales y **detecta cambios** de host/puerto o de
credenciales comparando contra la corrida anterior. **Solo lectura.**

- **Frecuencia**: cada 1–2 semanas.
- **Corre en CloudShell normal, sin aprovisionar nada.** El descubrimiento, la
  antigüedad de credenciales y la detección de cambios usan solo APIs de AWS.
- El **snapshot** para comparar entre corridas se guarda en el **`$HOME` de
  CloudShell** (no requiere S3 ni permisos extra).

## Qué hace

1. **Identidad** (`sts:GetCallerIdentity`).
2. **Descubre endpoints** sin necesitar contraseña:
   - 8 bases del pipeline Glue → host:puerto de las Glue Connections (`glue:GetConnection`).
   - Bases replicadas por DMS → ServerName/Port de los endpoints Oracle (`dms:DescribeEndpoints`).
3. **Antigüedad de credenciales**: los 8 secretos + los `pon-prod-secad-<SIGLA>-on-premise`
   (`secretsmanager:ListSecrets` + `DescribeSecret`). Avisa si superan 365 días.
4. **Detección de cambios** (lo principal): guarda un snapshot en `$HOME` y lo
   compara con el de la corrida anterior. Marca cambios de **host**, **puerto**,
   endpoints nuevos/desaparecidos y credenciales cuya edad "rejuveneció" (rotación).
5. **Ping TCP (opcional, auto-detectado)**: confirmar que la base responde por red
   requiere ruta on-premise, que solo existe en un CloudShell VPC environment. El
   script detecta si hay ruta:
   - **CloudShell normal**: omite el ping con un aviso (no da falsos errores).
   - **VPC environment** (o con `--ping`): hace el ping TCP real.

## Qué guarda el snapshot

Un JSON en `$HOME/.monitor_conexion_snapshot.json` con, por cada base: **host,
puerto, servicio** y la **edad en días** de cada secreto. No guarda contraseñas ni
usuarios. Sirve para comparar "hoy vs. la vez pasada" y así detectar cambios.

## Uso (CloudShell normal)

```bash
export AWS_DEFAULT_REGION=us-east-1
python3 monitor_conexion.py                 # descubre + credenciales + detección de cambios
python3 monitor_conexion.py --con-avisos    # muestra también los avisos (credenciales viejas)
python3 monitor_conexion.py --sin-snapshot  # solo estado actual, sin guardar/comparar
python3 monitor_conexion.py --ping          # fuerza el ping TCP (útil solo en VPC environment)
python3 monitor_conexion.py --no-export
```

## Salida

- Consola: progreso + **RESUMEN** con severidad. Por defecto solo lo **crítico**
  (cambios detectados, o endpoints no alcanzables si se hizo ping); los avisos
  (credenciales viejas, ping omitido) se muestran con `--con-avisos`.
- CSV en `7-Conexiones-DBs-DMS/salida/conexion_<fecha>.csv`.
- Snapshot en `$HOME/.monitor_conexion_snapshot.json` (se puede cambiar con la
  variable `SNAPSHOT_DIR`).

## Exit code

- `0` — sin cambios (y, si se hizo ping, todo alcanzable).
- `1` — se detectó un cambio (o algún endpoint no alcanzable con `--ping`).
- `2` — argumentos inválidos.

## Permisos IAM (solo lectura)

- `sts:GetCallerIdentity`
- `glue:GetConnection`
- `dms:DescribeEndpoints`
- `secretsmanager:ListSecrets`, `secretsmanager:DescribeSecret`

(No requiere permisos de S3: el snapshot es local.)

## Notas

- **Primera corrida**: no hay snapshot previo, así que no reporta cambios; guarda la
  línea base y desde la siguiente corrida ya compara.
- **Persistencia del snapshot**: el `$HOME` de CloudShell persiste entre sesiones.
  Si el entorno se reciclara (poco frecuente), simplemente se reinicia la línea base.
- **Ping real**: solo si necesitas confirmar que la base responde por red, hazlo en
  un CloudShell VPC environment. Ver **`docs-investigacion/PRUEBA-VPC-ENVIRONMENT.md`** (incluye los
  valores exactos de VPC/subnet/SG ya verificados y cómo probar **sin subir el
  script**, porque en un entorno VPC la subida de archivos está deshabilitada).
- **CETUS**: tiene secreto pero no connection ni endpoint DMS detectado, así que no
  hay host:puerto que descubrir; solo aparecería su antigüedad de credencial.

---

## `test_endpoint_dms.py` — prueba real de un endpoint vía DMS (sin IPs)

Script aparte para **probar de verdad la conexión a UNA base** usando el mecanismo
nativo `dms:TestConnection`. A diferencia del ping del `monitor_conexion.py`, este
**sí funciona desde CloudShell normal** porque **DMS ejecuta la prueba desde su
replication instance** (que está dentro de la VPC con ruta on-premise). Tú no
manejas IPs ni host:puerto: DMS ya conoce el endpoint.

### Qué hace

1. Descubre los endpoints DMS y las replication instances.
2. Elige **un** endpoint (el primero Oracle, o el que pases con `--endpoint`) y una
   replication instance (la primera `available`, o `--instance`).
3. Lanza `dms:TestConnection` (asíncrono) y **sondea** hasta que termina.
4. Muestra `EXITOSA` / `FALLIDA` con el motivo que reporta DMS.

Es un test **activo** (DMS realmente intenta conectarse a la base), acotado a **un**
endpoint.

### Uso (CloudShell normal)

```bash
export AWS_DEFAULT_REGION=us-east-1
python3 test_endpoint_dms.py                    # primer endpoint Oracle + primera instancia
python3 test_endpoint_dms.py --listar           # solo lista endpoints e instancias
python3 test_endpoint_dms.py --endpoint mebuc-oracle-src
python3 test_endpoint_dms.py --endpoint mebuc-oracle-src --instance arn:aws:dms:...:rep:XXXX
```

### Exit code

- `0` — conexión exitosa.
- `1` — conexión fallida, timeout, o error (falta endpoint/instancia/permiso).

### Permisos IAM

- `sts:GetCallerIdentity`
- `dms:DescribeEndpoints`, `dms:DescribeReplicationInstances`
- `dms:TestConnection`, `dms:DescribeConnections`

### Notas

- Necesita al menos **una replication instance** para ejecutar la prueba (es DMS
  quien conecta, no tú). Si no hay ninguna, el script lo indica.
- `TestConnection` puede tardar ~10–60 s; el script espera con un timeout
  (`TEST_POLL_MAX` × `TEST_POLL_SEG`).
- Empieza probando **un** endpoint para validar el enfoque; si funciona, se puede
  extender a varios.
