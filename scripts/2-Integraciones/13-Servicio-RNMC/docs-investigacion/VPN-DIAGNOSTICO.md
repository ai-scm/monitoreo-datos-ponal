# VPN `ofite-bogota-*` — pista de diagnóstico (fuera del script)

Esto **no** lo monitorea `monitor_rnmc.py`. Es una nota de diagnóstico para cuando el
pipeline RNMC falle **sin causa aparente en los jobs** (jobs sin `FAILED` claro, pero
los datos no llegan o la ingesta se seca).

## Qué se observó

Las conexiones VPN compartidas que llevan el tráfico **on-premise** de RNMC usan
túneles con nombre `ofite-bogota-*`. En la investigación mostraron **estado mixto**:
algunos túneles **DOWN**.

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
