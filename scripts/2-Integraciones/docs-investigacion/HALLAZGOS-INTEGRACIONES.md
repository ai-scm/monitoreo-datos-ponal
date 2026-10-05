# Hallazgos de arquitectura — Integraciones (contexto, no monitoreado)

Hallazgos de la investigación de la cuenta que **no** se traducen en chequeos de los
scripts (son contexto de arquitectura o huecos a construir), pero conviene dejarlos
documentados para no re-investigarlos y para que las decisiones futuras tengan base.

Cuenta **926162397524**, región **us-east-1**.

## 1. `RNMC-to-RDS` es un job de Glue, NO una tarea DMS

Corrección a un enunciado común: el flujo que escribe en Aurora **no** es DMS.

- `RNMC-to-RDS` → **job de Glue**, trigger `daily_rds` (Glue Trigger, 14:00 UTC).
  Lee de Athena (`pon-prod-raw-cnp.boletin_z1_capas_v2`, último día) y escribe en
  Aurora `pon-prod-db-postgres`, tabla `public.hechos_z1_v2`. Confirmado que corre a
  diario (hoy 14:00-14:05 UTC OK).
- `monitor_rnmc.py` ya lo trata correctamente como job de Glue (no DMS).
- Existe un **endpoint DMS huérfano** `pon-prod-db-postgres-instance-1` que apunta a
  ese mismo RDS, pero **ninguna tarea DMS lo usa como destino** (confirmado). Es un
  endpoint definido y sin uso; no monitorear.

## 2. El paso `RDS → S3` NO existe (hueco de arquitectura)

Búsqueda exhaustiva (Lambdas, jobs de Glue, reglas EventBridge) de algo que lea de
`hechos_z1_v2` / Aurora y escriba a S3: **cero resultados**. Hoy `hechos_z1_v2` es un
**sumidero terminal unidireccional**.

- No es un punto ciego de monitoreo: **es un hueco a construir primero**. No se puede
  "verificar un paso que no existe". Si el negocio esperaba ese export, hay que
  implementarlo antes de monitorearlo.

## 3. SECAD — fan-out de ~60 tareas DMS (contexto "a futuro")

SECAD (no es un punto implementado todavía) es el **verdadero** pipeline DMS → Aurora:

- ~60 tareas DMS por departamento/región de Colombia (DEAMA, DEANT, MEBUC, ...),
  ~45 CDC puras + ~15 full-load-and-CDC. Destino: Aurora `writer.aurora.prod.mdp.local`
  (bases `raw_secad_<municipio>`). Cada tarea tiene su propio secreto on-premise.
- Confirmado activo hoy (ej. `deama-secad`, 100%, 27/27 tablas).
- **Problema concreto ya presente**: la tarea `mesan-secad-...` está en
  `FATAL_ERROR` tras varios intentos de recuperación. No es hipotético.
- Si se crea un punto de monitoreo para SECAD, el patrón natural es el de
  `monitor_dms.py` (Base-Datos) ampliado al fan-out, más la relación con la VPN
  (ver `VPN-ONPREMISE-ESTADO.md`).

## 4. Dependencia transversal: VPN on-premise

Las tres integraciones (SIVICC, RNMC, SECAD) dependen de la misma VPN
`ofite-bogota-*`, hoy en estado crítico (7/8 túneles caídos). Ver
`VPN-ONPREMISE-ESTADO.md`. Es la causa raíz más probable cuando un pipeline aparece
"silenciosamente seco" sin fallo en los jobs.

## Qué de esto SÍ está en los scripts y qué no

| Hallazgo | ¿En un script? |
|----------|----------------|
| `RNMC-to-RDS` como job Glue diario | ✅ `monitor_rnmc.py` |
| Endpoint DMS huérfano `pon-prod-db-postgres-instance-1` | ❌ no (sin uso) |
| Paso `RDS → S3` | ❌ no existe (a construir) |
| SECAD fan-out + `mesan-secad` FATAL | ❌ no (punto futuro) |
| VPN `ofite-bogota-*` | ⚠️ indirecto: ping TCP en punto 7 |
