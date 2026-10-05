# VPN on-premise `ofite-bogota-*` — hallazgo transversal (fuera de los scripts)

> **Esto no lo monitorea ningún script hoy.** Es un hallazgo de infraestructura que
> afecta a **las tres integraciones on-premise** (SIVICC, RNMC y SECAD) a la vez,
> porque todas comparten la misma red. Documentado aquí a nivel de categoría, no dentro
> de un script, porque es un dominio de red/infra, no de pipeline de datos.

## Estado observado (crítico)

**7 de 8 túneles VPN caídos.** Un solo túnel sostiene TODO el tráfico on-premise:

| Conexión VPN | Túnel 1 | Túnel 2 |
|--------------|---------|---------|
| `ofite-bogota-fortinet-accelerated` | **UP** (único activo) | DOWN |
| `ofite-bogota-cisco` | DOWN | DOWN |
| `ofite-bogota-fortinet` | DOWN | DOWN |
| *(4º par)* | DOWN | DOWN |

La ruta es **Transit Gateway → VPN → on-premise**. Con un único túnel UP, es un
**punto único de falla real**: si ese túnel cae, se corta el tráfico de SIVICC, RNMC
y las ~30 fuentes regionales de SECAD simultáneamente.

## Qué tráfico depende de este único túnel

| Integración | Endpoint on-premise | Puerto |
|-------------|---------------------|--------|
| SIVICC | `172.28.18.185` | 1538 |
| RNMC | `cnp.prod.mdp.local` | 1538 |
| SECAD | ~30 fuentes regionales (una por departamento) | varía |

## Acción recomendada (aparte de cualquier script)

Esto **debe escalarse ya** al equipo de red: no es un punto ciego de monitoreo, es una
degradación activa. Vale la pena restaurar los túneles caídos antes de que el único UP
también falle.

## Relación con los scripts de monitoreo

- **Punto 7 (`monitor_conexion.py`, Base-Datos)**: es el dueño natural de la
  verificación de conectividad. Se le agregaron los endpoints de SIVICC
  (`172.28.18.185:1538`) y RNMC (`cnp.prod.mdp.local:1538`) a su lista de ping TCP. El
  ping real solo funciona desde un **CloudShell VPC environment** (o con `--ping`); en
  CloudShell normal se auto-omite. Cuando corra con ruta on-premise, este chequeo TCP
  detectaría exactamente esta degradación.
- **SIVICC / RNMC (`monitor_migracion_sivicc.py`, `monitor_rnmc.py`)**: siguen enfocados
  en sus pipelines (jobs, triggers, DMS), NO replican el ping (lo hace el punto 7). Pero
  si uno de esos scripts muestra el pipeline "silenciosamente seco" (todo INFO/OK pero
  faltan datos), **el primer sospechoso es esta VPN**.

## Por qué no se monitorea con un chequeo dedicado de estado de túnel

Leer `TunnelState` requeriría permisos de EC2/VPC (`ec2:DescribeVpnConnections`) y es
un dominio de red compartido entre muchos pipelines, no exclusivo de estas
integraciones. Si se decide vigilarlo activamente, lo natural es un **monitor propio de
conectividad VPN**, no incrustarlo en los scripts de pipeline. Por ahora: el ping TCP
del punto 7 cubre la señal práctica ("¿la base responde por red?") sin permisos extra.
