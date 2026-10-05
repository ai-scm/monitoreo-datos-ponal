# VPN on-premise — pista de diagnóstico para SIVICC (fuera del script)

> **Esto no lo monitorea `monitor_migracion_sivicc.py`.** Es una pista de diagnóstico.

SIVICC depende de datos que vienen de on-premise (`172.28.18.185:1538`) por la VPN
compartida `ofite-bogota-*`, que está en **estado crítico**: 7 de 8 túneles caídos,
solo uno sostiene el tráfico. Es un punto único de falla que afecta a SIVICC, RNMC y
SECAD a la vez.

El estado completo y actualizado está en el hallazgo transversal:
`../../docs-investigacion/VPN-ONPREMISE-ESTADO.md`.

**Si el pipeline de SIVICC parece "silenciosamente seco"** (los backfills no traen
datos nuevos pese a estar OK), el primer sospechoso es esta VPN. El ping TCP real a
ese endpoint lo hace el **punto 7** (`monitor_conexion.py`), no este script.
