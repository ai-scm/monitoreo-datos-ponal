# Validación de optimización

**Script:** `validador_optimizacion_cloudshell.py`.

Revisa los resultados de:

`s3://pon-prod-ml-iapol/PrediccionMdP/AÑO/SEMANA/CAD/UNIDAD/Optimizacion/`

Busca archivos defectuosos, parejas incompletas y diferencias de cantidad frente
al patrón observado de cada unidad. Por defecto revisa **la semana actual y la
siguiente**, según Bogotá.

## Qué revisa

- Nombres `Optimizacion_UNIDAD_YYYYMMDD_TURNO_F_Bnn.geojson` y su pareja `CPnn`.
- Unidad, fechas, turnos e índices; nombres incompletos y archivos de cero bytes.
- Parejas B/CP y las 21 combinaciones de fecha y turno de la semana.
- Cantidad esperada de referencia y cantidad encontrada, por CAD y por semana.
- JSON legible, contenido GeoJSON, identificadores, anillos y coordenadas básicos.
- Avisos por colecciones vacías, geometrías nulas o diferencias de contenido.

Cada unidad puede producir cantidades distintas. No exige índices consecutivos
ni aplica un rango 0–1 a la intensidad. El turno 1 usa fechas de domingo a sábado;
los turnos 2 y 3, de lunes a domingo.

## Qué cargar en CloudShell

- `validador_optimizacion_cloudshell.py`.
- `referencia_optimizacion.json`, junto al `.py`, para comparar patrones históricos.

La referencia es opcional: sin ella continúan los otros controles, pero no se
pueden calcular todas las cantidades esperadas. Describe lo observado; no es un
catálogo de entregas obligatorias. No necesitas subir `tests/` ni los análisis.

## Cómo ejecutarlo

Primera prueba, una unidad:

```bash
python3 "$HOME/validador_optimizacion_cloudshell.py" \
  --sigla MEPER --unidad 51716 \
  --salida "$HOME/salida"
```

Todas las unidades de un CAD:

```bash
python3 "$HOME/validador_optimizacion_cloudshell.py" \
  --sigla MEPER --salida "$HOME/salida"
```

Revisión nacional por bloques:

```bash
python3 "$HOME/validador_optimizacion_cloudshell.py" \
  --max-segundos 600 --max-lecturas 5000 --trabajadores 12 \
  --salida "$HOME/salida"
```

Primero lista archivos y después revisa contenido. Si quedan lecturas pendientes,
repite el mismo comando. Conserva `$HOME/salida/optimizacion/.estado/`: permite
reutilizar revisiones de archivos sin cambios. Cada ejecución vuelve a consultar
el inventario; la caché evita descargar de nuevo el contenido ya revisado.
Los presupuestos se comprueban entre llamadas; una llamada en curso puede durar más.

Para mirar solo nombres y cantidades añade `--solo-metadata`; el contenido queda
NO EVALUADO. Retira esa opción para continuar con la revisión de contenido.
`--desde YYYYMMDD --hasta YYYYMMDD` permite un rango manual de lunes, incluso
entre años. Sin esas opciones el rango se actualiza cada lunes y comprende dos semanas.

## Qué entrega y dónde

Genera dos CSV:

- **`general.csv`:** filtra `tipo_registro=RESUMEN_SIGLA` para comparar esperados y
  encontrados por CAD y semana. `RESUMEN` muestra cada unidad; `CONTROL` explica
  hallazgos y limitaciones.
- **`detalle.csv`:** una fila por archivo con ruta, estado del nombre, contenido
  y motivo. Indica si se leyó de nuevo o se reutilizó la caché.

Los esperados aparecen como mínimo y máximo cuando la referencia admite variantes.
Si falta referencia, no se inventa una cantidad. No sumes los distintos tipos de
registro: el resumen por CAD ya incluye los conteos de sus unidades.

Local: `$HOME/salida/optimizacion/FECHA/HORA/`.

S3: `s3://pon-prod-ml-iapol/ValidationReports/08-MonitorOptimizacion/FECHA/HORA/`.

FECHA/HORA corresponde a la ejecución en Bogotá, no a la semana inspeccionada.
La caché y `contexto.json` quedan locales. `--sin-subir` permite conservar solo
los informes locales; si la subida falla, también quedan disponibles allí.

## Cómo leer el resultado

- **FALLA:** defecto observado, como nombre inválido, archivo vacío o JSON corrupto.
- **AVISO:** cambio de patrón o condición que necesita interpretación.
- **PENDIENTE:** falta una pareja o turno y la entrega no está confirmada como cerrada.
- **INCOMPLETO / NO EVALUADO:** falta información o no terminó la lectura.
- **OK:** pasaron los controles aplicados.

Si confirmaste que la entrega terminó, añade `--ciclo-cerrado`. Las parejas o
turnos ausentes en unidades que sí tienen archivos pasan a FALLA. Una unidad
sin archivos conserva AVISO: no hay una obligación de entrega confirmada para ella.

La referencia histórica no detecta todas las unidades que deberían existir pero
nunca aparecieron. La geometría se revisa de forma básica; no se certifican
cobertura territorial, utilidad de las zonas ni calidad del algoritmo.

## Estado actual

Alcance técnico implementado y probado a escala nacional. La revisión del
30 de octubre de 2026 cubrió 54 CAD, 4.705 unidades-semana y 376.580 archivos:
376.134 OK, 446 con AVISO y ninguno con FALLA o lectura pendiente.
Las semanas analizadas fueron `20261026` y `20261102`.

Los avisos son hallazgos para interpretar, no lecturas pendientes.
La [interpretación nacional](analisis_avisos/2026-10-30-nacional/README.md) y el
[análisis histórico de MEPER](analisis_patrones/2026-10-30-MEPER/README.md)
conservan el detalle. El script no corrige archivos ni envía notificaciones.
