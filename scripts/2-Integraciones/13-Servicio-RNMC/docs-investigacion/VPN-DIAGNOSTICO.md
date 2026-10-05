# VPN `ofite-bogota-*` — pista de diagnóstico (fuera del script)

Esto **no** lo monitorea `monitor_rnmc.py`. Es una nota de diagnóstico para cuando el
pipeline RNMC falle **sin causa aparente en los jobs** (jobs sin `FAILED` claro, pero
los datos no llegan o la ingesta se seca).

> **Hallazgo transversal**: esto afecta a las tres integraciones on-premise (SIVICC,
> RNMC, SECAD), no solo a RNMC. El estado completo y actualizado de los túneles está en
> `../../docs-investigacion/VPN-ONPREMISE-ESTADO.md`. Esta nota conserva el enfoque
> específico de cómo impacta a RNMC.

## Qué se observó (estado crítico actual)

Las conexiones VPN compartidas que llevan el tráfico **on-premise** de RNMC
(`cnp.prod.mdp.local:1538`) usan túneles `ofite-bogota-*`. Estado real:
**7 de 8 túneles caídos**, solo `ofite-bogota-fortinet-accelerated` (túnel 1) UP
sosteniendo todo. Punto único de falla (ver el doc transversal para la tabla completa).

## Por qué importa para RNMC

El pipeline RNMC depende de datos que vienen de on-premise (a través de esa VPN) antes
de que los jobs de Glue/DMS los procesen. Si los túneles están caídos:

- Puede **no llegar el archivo** a S3 → la Lambda `pon-prod-launch-homogenize-rnmc-job`
  no se invoca → el job `homogenize` no corre. El script lo verá como **INFO**
  ("sin invocaciones / sin corrida", normal si no llega archivo), **no** como fallo —
  porque desde la óptica de AWS no hay nada roto, simplemente no llegó la fuente.
- O la ingesta/replicación puede fallar por timeouts de red que **no** siempre quedan
  como un `FAILED` limpio en el job.

Por eso: **si el pipeline parece "silenciosamente seco"** (todo INFO/OK en el script
pero el negocio dice que faltan datos), **revisar primero el estado de los túneles
`ofite-bogota-*`**.

## Dónde mirar (manual)

- Consola: **VPC → Site-to-Site VPN Connections**, filtrar por `ofite-bogota`, revisar
  el estado de cada túnel (UP/DOWN) y la métrica `TunnelState` en CloudWatch.
- Es infraestructura **compartida** (no exclusiva de RNMC), así que un cambio ahí
  afecta a otros pipelines on-premise también.

## Por qué NO está en el script

- Es un **dominio distinto** (red/infra), no el pipeline de datos RNMC en sí.
- Es infraestructura compartida: monitorearla aquí mezclaría responsabilidades.
- Si se decide vigilarla, lo natural es un **monitor propio de conectividad VPN** (o
  sumarla al punto 7 `Conexiones-DBs-DMS` de Base-Datos, que ya vigila conectividad),
  no incrustarla en `monitor_rnmc.py`.

## Resumen

Pista de diagnóstico, no una verificación automatizada: cuando RNMC no muestre fallo
en los jobs pero falten datos, los túneles `ofite-bogota-*` DOWN son el primer
sospechoso.
