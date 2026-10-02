# Prueba de conexión real (ping TCP) en un CloudShell VPC environment

Cómo probar que una base Oracle on-premise **responde por red**, desde un
CloudShell VPC environment, **sin subir ningún script** (en un entorno VPC la
subida/bajada de archivos está deshabilitada por AWS).

## Por qué un VPC environment

CloudShell normal no tiene ruta a las IPs/hosts on-premise (`172.28.x.x`,
`*.secad.prod.mdp.local`). Un **CloudShell VPC environment** queda dentro de la
VPC y sí alcanza esos hosts a través del Transit Gateway. El ping real (confirmar
que el puerto responde) solo funciona ahí.

## Datos del VPC environment (verificados)

Al crear el entorno (**Actions → Create VPC environment**), usa:

| Campo | Valor |
|-------|-------|
| **Name** | `test-mebuc` (o el que quieras) |
| **VPC** | `vpc-045111e41496789d2` |
| **Subnet** | `subnet-0bec547e6ef7879bd` (privada, `10.41.6.0/25`, us-east-1c) |
| **Security group** | `sg-0aff4af7416257ef4` (egress abierto `-1` a `0.0.0.0/0`) |

Estos valores salen de la replication instance `pon-prod-raw-secad-replication-instance2`
(la que replica las bases `secad`). Se reutilizan a propósito: como DMS ya llega a
esas bases por esa red, un CloudShell en la misma red también llega. No hay que
crear ni modificar security groups.

> La misma VPC/subnet/SG sirve para **cualquier** base `secad` (MEBUC, DEARA, ...),
> porque todas se alcanzan por esa misma ruta. No necesitas un entorno por base.

## Importante: en un VPC environment NO se pueden subir archivos

En la pestaña del VPC environment, **Actions → Upload file / Download file
aparecen deshabilitados** (en gris). Es una restricción de AWS, no un error. Por
eso la prueba se hace con comandos nativos (abajo), sin subir `monitor_conexion.py`.

## Cómo probar (sin subir nada)

Dentro de la terminal del VPC environment:

```bash
# 1. Confirma que estás en la red de la VPC (la IP local debe ser 10.41.6.x)
python3 -c "import socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.connect(('10.255.255.255',1));print('IP local:',s.getsockname()[0]);s.close()"

# 2. Ping TCP al host:puerto de la base (bash, nativo, sin instalar nada)
timeout 5 bash -c "</dev/tcp/MEBUC.secad.prod.mdp.local/1521" \
  && echo "ALCANZABLE (MEBUC responde en 1521)" \
  || echo "cerrado o sin ruta"
```

Equivalente en Python (mismo resultado, un poco más de detalle):

```bash
python3 -c "
import socket, time
host, port = 'MEBUC.secad.prod.mdp.local', 1521
t0 = time.time()
try:
    socket.create_connection((host, port), timeout=5)
    print(f'ALCANZABLE: {host}:{port} respondio en {(time.time()-t0)*1000:.0f} ms')
except Exception as e:
    print(f'NO alcanzable: {host}:{port} -> {e}')
"
```

Para probar **otra sigla**, cambia el host por el de esa base (ver el
`INVENTARIO-ENDPOINTS.md`). Ejemplos:

```bash
timeout 5 bash -c "</dev/tcp/DEARA.secad.prod.mdp.local/1521" && echo OK || echo NO
timeout 5 bash -c "</dev/tcp/172.26.227.4/1521" && echo OK || echo NO   # DEBOL (por IP)
```

## Interpretación

- **ALCANZABLE / OK**: la base responde por red. El montaje del VPC environment es
  correcto y la ruta on-premise funciona.
- **cerrado o sin ruta / timeout**: no hay respuesta. Revisa (a) que la subnet
  tenga ruta al Transit Gateway, (b) el firewall on-premise, o (c) que la base/host
  esté arriba. Ojo: el ping TCP solo prueba **red**; que el puerto abra no valida
  usuario/clave (para eso está `test_endpoint_dms.py`).

## Notas

- El VPC environment se **borra tras 30 min de inactividad** (su home no persiste).
  No guardes nada importante ahí; es solo para la prueba de red.
- Si necesitas el script completo `monitor_conexion.py --ping` dentro del VPC
  environment (para varias bases con formato de reporte), como no puedes subirlo,
  tendrías que pegar su contenido con un heredoc o clonar desde un repo accesible.
  Para una validación puntual, los comandos de arriba bastan.
