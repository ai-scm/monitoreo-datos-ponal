# Patrón de optimización de MEPER

Revisión realizada directamente en S3 el **30 de octubre de 2026**. Se compararon
seis semanas anteriores con la semana actual y la siguiente. La referencia de
43 unidades queda corroborada: las cinco carpetas adicionales no contienen
archivos de optimización en ninguna de las ocho semanas revisadas.

## Resultado

| Semana | Unidades observadas | Con optimización | Sin archivos de optimización | Archivos |
|---|---:|---:|---:|---:|
| 20260914 | 48 | 43 | 5 | 3570 |
| 20260921 | 48 | 43 | 5 | 3570 |
| 20260928 | 48 | 43 | 5 | 3570 |
| 20261005 | 48 | 43 | 5 | 3570 |
| 20261012 | 48 | 43 | 5 | 3570 |
| 20261019 | 48 | 43 | 5 | 3570 |
| 20261026 — actual | 48 | 43 | 5 | 3570 |
| 20261102 — siguiente | 48 | 43 | 5 | 3570 |

Se completaron **384 listados de unidad-semana**, correspondientes a
**28.560 archivos**. No se encontraron nombres inválidos, archivos de cero bytes,
duplicados semánticos, parejas B/CP incompletas ni fechas/turnos faltantes en las
unidades con archivos. El patrón relativo de cada una de las 43 unidades es el
mismo en las ocho semanas y coincide con su referencia actual.

Este análisis revisó nombres, tamaños y demás metadata. No volvió a leer los
GeoJSON; la prueba de contenido de las semanas actual y siguiente se conserva
en el informe de ejecución del script.

## Por qué hay 48 carpetas y solo 43 referencias

Las unidades **51699, 51723, 51740, 51747 y 59222** aparecen como carpetas bajo
MEPER, pero sus prefijos `Optimizacion/` tienen cero archivos en las ocho semanas.
No existe una cantidad positiva observada con la que construir su patrón.

La ausencia repetida permite distinguirlas de una pérdida puntual frente a una
entrega histórica, pero no confirma que estén exentas de generar optimización.
Por eso no se les asigna una obligación de cero archivos ni se declaran fallas
de entrega a partir de esta evidencia.

El cociente 43/48 describe la proporción de carpetas con un patrón positivo
observado. La referencia cubre **43/43 unidades que sí tienen archivos** en estas
semanas. No hay un 10,4 % de archivos sin validar por ese motivo.

## Las cantidades dependen de cada unidad

| Archivos por unidad y semana | Unidades |
|---|---:|
| 42 | 26 |
| 84 | 5 |
| 126 | 5 |
| 168 | 2 |
| 210 | 4 |
| 252 | 1 |
| Sin archivos observados | 5 |

La suma es **3.570 archivos por semana**, con 21 combinaciones de fecha/turno
por unidad productora y una pareja B/CP por índice. Cada unidad mantiene su
cantidad y sus índices; no se aplica una cantidad uniforme a toda MEPER.

Los nombres responden a estas dos formas:

```text
Optimizacion_<unidad>_<YYYYMMDD>_<turno>_F_B<indice>.geojson
Optimizacion_<unidad>_<YYYYMMDD>_<turno>_F_CP<indice>.geojson
```

El índice es positivo y tiene al menos dos dígitos. El turno 1 comprende el
domingo anterior al sábado; los turnos 2 y 3, del lunes al domingo.

## Qué queda establecido para el monitor

- Mantener 3.570 como cantidad semanal de referencia para MEPER, con el detalle
  propio de cada unidad.
- Continuar revisando todas las carpetas observadas, incluidas las cinco sin
  archivos históricos. Si empiezan a generar archivos, validar sus nombres y
  contenido y registrar que necesitan una referencia de cantidad.
- Conservar las diferencias frente a la referencia como AVISO; la historia no
  sustituye un plan de entregas obligatorias.
- Mantener la revisión operativa limitada a la semana actual y la siguiente.
  La consulta de semanas anteriores fue un análisis de patrones.

El JSON existente ya contiene los patrones positivos correctos de MEPER; no
necesita ampliarse con cantidades inventadas para las cinco unidades sin archivos.
No se modificaron los resultados ni se escribió en S3 durante este análisis.

## Evidencia

- [Comparación por semana](comparacion_semanal.csv): ocho filas con las cantidades
  y controles agregados.
- [Patrones por unidad](patrones_por_unidad.csv): las 48 unidades, con cantidad
  en cada semana y resultado de estabilidad.
- [Detalle de cada unidad-semana](detalle_unidad_semana.csv): 384 listados completos.
- [Perfiles observados](perfiles_observados.json): índices B/CP por fecha/turno
  relativa para cada unidad y semana con archivos.
- [Contexto del análisis](contexto_analisis.json): captura, alcance, cobertura,
  límites y huella del JSON de referencia usado.

Origen:
`s3://pon-prod-ml-iapol/PrediccionMdP/2026/<semana>/MEPER/<unidad>/Optimizacion/`.
La paginación de S3 se agotó en cada listado. Las firmas comparan los nombres
normalizados por fecha relativa, turno, clase e índice; no representan el
contenido de los GeoJSON. Los CSV de esta carpeta documentan un análisis puntual
y no cambian los dos CSV que genera el monitor operativo.
