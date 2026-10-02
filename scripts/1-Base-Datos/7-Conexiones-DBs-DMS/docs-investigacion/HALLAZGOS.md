# Hallazgos de la investigación — conexiones a bases Oracle on-premise

Resumen de lo descubierto probando la conectividad y la configuración de las bases
Oracle (pipeline Glue + replicación DMS). Sirve como insumo para tickets y para el
equipo de datos/redes. Fecha base de la investigación: 2026-09-30.

---

## 1. La conectividad real solo se prueba desde dentro de la VPC

- **CloudShell normal NO tiene ruta** a los hosts on-premise (`172.28.x.x`,
  `*.secad.prod.mdp.local`). Cualquier ping TCP desde ahí da timeout aunque la base
  esté sana. No es un problema de permisos ni del script: es de red.
- **Dos formas válidas de probar la conexión real:**
  1. **`dms:TestConnection`** (`test_endpoint_dms.py`) — funciona desde CloudShell
     normal, porque DMS ejecuta la prueba desde su replication instance (dentro de
     la VPC). Prueba red + credenciales + inicialización CDC.
  2. **CloudShell VPC environment** + ping TCP (`/dev/tcp`) — prueba solo red.
     Ver `PRUEBA-VPC-ENVIRONMENT.md`.
- En un **CloudShell VPC environment, Upload/Download de archivos está
  deshabilitado** por AWS. Por eso el ping se hace con comandos nativos, no subiendo
  el script.

## 2. Datos de red verificados (para el VPC environment)

Tomados de la replication instance `pon-prod-raw-secad-replication-instance2`:

| Recurso | Valor |
|---|---|
| VPC | `vpc-045111e41496789d2` |
| Subnet (una privada) | `subnet-0bec547e6ef7879bd` (`10.41.6.0/25`, us-east-1c, sin IP pública) |
| Security group | `sg-0aff4af7416257ef4` (egress `-1` a `0.0.0.0/0`) |

Sirven para probar cualquier base `secad`, porque todas se alcanzan por esa misma
ruta. No hay que crear ni modificar security groups.

## 3. MEBUC: la conexión de RED funciona; el problema es de CONFIGURACIÓN CDC

Al probar `test_endpoint_dms.py --endpoint bucaramanga-secad-source`, DMS respondió:

> `Test Endpoint failed: Application-Status: 1020912, Application-Message: Log Miner
> is not supported in Oracle PDB environment. Endpoint initialization failed.`

Interpretación:
- **La red y la autenticación SÍ funcionaron** (DMS llegó a la base y se conectó).
- Lo que falla es la **captura de cambios (CDC)**: la base es un **Oracle PDB**
  (multitenant) y el endpoint DMS está configurado con **LogMiner**, que **no es
  compatible con PDB**. La solución típica es cambiar el endpoint a **Binary Reader**.
- **Es un tema de configuración de DMS, no de conectividad.** Un ping TCP a MEBUC
  probablemente salga ALCANZABLE.

> Acción sugerida: revisar con quien administra el endpoint DMS si debe migrarse a
> Binary Reader para PDB. No es un incidente de red.

## 4. Credenciales muy antiguas

`monitor_conexion.py --con-avisos` detectó credenciales que superan largamente el
año (umbral 365 días). Ejemplos:

- Muchos secretos `pon-prod-secad-<SIGLA>-on-premise` con **~897 días (≈ 2.5 años)**
  sin rotar (MEBUC, METUN, DETOL, DECAS, y decenas más).
- De las 8 bases del pipeline: SIVIC 401 d, SIEDCO 477 d, DISEC 376 d.

> Acción sugerida: plan de rotación de credenciales, priorizando las de mayor
> antigüedad. Ninguna tiene rotación automática configurada.

## 5. Inventario de endpoints DMS: cosas a limpiar/confirmar

Del `INVENTARIO-ENDPOINTS.md` (66 endpoints Oracle):

- **~50 endpoints `pon-prod-raw-secad-<SIGLA>-source-endpoint` están SIN configurar**
  (ServerName/Port = `None`). Creados pero sin destino. Revisar si deben completarse
  o si están obsoletos.
- **Endpoints duplicados / de prueba** mezclados con productivos:
  - MEBUC tiene dos: `bucaramanga-secad-source` y `mebuc-secad-prod-source` (mismo host).
  - MECUC: `mecuc-secad-prod-source` y `mecuc-secad-prod-source1`.
  - DISEC: varios (`disec-endpoint-source`, `pon-prod-disec-source`, `pon-test-disec`,
    `pon-test-disec-dms`, `pon-test-diseendpoint`).
  - SIEDCO: `siedco-prod-source` y `siedco-copy-prod-source`.
  - Varios `pon-test-*` (geopol, telepol, sivvic, rnmc-test).
- **Mezcla de host por DNS vs IP directa**: la mayoría usa `<SIGLA>.secad.prod.mdp.local`,
  pero DEBOL, DECAL, DESAP, DESUC, MEMOT, MEPAS usan IP directa.

> Acción sugerida: depurar endpoints obsoletos/duplicados y estandarizar el uso de
> hostname DNS sobre IP directa donde se pueda.

## 6. CETUS: sin endpoint ni connection

Existe el secreto `pon-prod-CETUS-on-premise`, pero **no se le detectó ni Glue
Connection ni endpoint DMS**. No hay host:puerto que probar. Confirmar con el equipo
de datos si CETUS sigue vigente o quedó fuera de alcance.

## 7. Permisos IAM (confirmado con `verificar_permisos.py`)

Se tienen **todos** los permisos que usan los scripts de monitoreo (identidad, Glue,
Logs Insights, DMS Describe/TestConnection, Secrets Manager). No hubo que pedir
ninguno adicional para las pruebas realizadas.

---

## Estado de las herramientas

| Herramienta | Estado | Dónde corre |
|---|---|---|
| `monitor_conexion.py` | Funciona (descubrimiento + credenciales + cambios) | CloudShell normal |
| `monitor_conexion.py --ping` | Funciona (ping real) | Solo CloudShell VPC environment |
| `test_endpoint_dms.py` | Funciona (prueba real vía DMS) | CloudShell normal |
| Ping `/dev/tcp` manual | Funciona | Solo CloudShell VPC environment |
