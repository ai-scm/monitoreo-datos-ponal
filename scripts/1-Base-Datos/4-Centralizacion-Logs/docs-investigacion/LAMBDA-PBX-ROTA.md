# Lambda PBX rota — hallazgo (fuera del alcance de este script)

> **No se monitorea en `monitor_logs.py`.** Es un hallazgo real pero de **otro dominio**
> (PBX), no de la centralización/higiene de logs del pipeline de datos. Se documenta
> aquí para que no se pierda y para dejar explícita la decisión de no incluirlo.

## Qué se encontró

Al buscar infraestructura de **centralización de logs** (Lambdas, Kinesis, Firehose,
subscription filters) se confirmó que **no existe ninguna funcionando**:

- 0 Kinesis streams, 0 Firehose nuevos relacionados, 0 subscription filters.
- Dos Lambdas candidatas por nombre, con veredicto claro:

| Lambda | Estado | Veredicto |
|--------|--------|-----------|
| `pon-prod-pbx-auditoria-procesados-files` | **Rota al 100%** | De PBX, no de logs del pipeline |
| `pon-prod-Monitor-Glue-Jobs-DPU` | Sana (1 inv/día, 0 errores) | Monitorea DPU de Glue, no logs |

## Detalle de la Lambda rota

`pon-prod-pbx-auditoria-procesados-files` falla en **cada invocación**, mes tras mes,
con el mismo error:

```
NameError: name 'key_control' is not defined   (línea 57)
```

Invocaciones ≈ errores (1:1). No tiene event source mapping ni política de recursos:
algo la invoca directamente (otra Lambda/Step Function no identificada), pero **nunca
produce un resultado útil**.

## Por qué NO entra en `monitor_logs.py`

- Es del dominio **PBX** (auditoría de archivos de CDR telefónico), no de la
  centralización/higiene de los logs de Glue/DMS que cubre este script.
- Mezclarla aquí confundiría el alcance (igual criterio que mantener los crawlers de
  PBX fuera del monitor de Athena).

## Decisión pendiente (del usuario)

Si se quiere monitorear este componente roto, lo correcto es **un chequeo propio de
PBX**, no sumarlo a `monitor_logs.py`. El texto del error ya está confirmado
(`NameError: name 'key_control' is not defined`, línea 57), así que sería trivial
marcarlo como FALLA real en un futuro monitor de PBX.

## Conclusión para "centralización de logs"

**No hay infraestructura de centralización que monitorear.** `monitor_logs.py` hace lo
correcto: higiene de log groups + reportar la ausencia de pipeline de centralización
como informativo. Esta Lambda no cambia esa conclusión.
