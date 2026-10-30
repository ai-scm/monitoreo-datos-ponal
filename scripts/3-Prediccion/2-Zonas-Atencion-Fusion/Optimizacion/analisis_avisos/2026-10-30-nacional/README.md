# Interpretación de los avisos nacionales de optimización

Se interpretaron los **446 archivos con AVISO de contenido** del informe
`ValidationReports/08-MonitorOptimizacion/2026-10-30/12-13-33_849918/`.
Corresponden a las carpetas semanales **20261026 y 20261102**.

La prueba terminó con 376.580 archivos revisados: 376.134 OK, 446 AVISO,
cero FALLA y cero pendientes de contenido. Todos los avisos son archivos B.
Los 446 CP de las mismas unidades, fechas, turnos e índices tienen contenido
no vacío, estado OK y cantidad de elementos coincidente con el índice.

Este análisis usa exclusivamente los CSV ya generados. No volvió a leer
los GeoJSON ni modificó el script o los resultados de S3.

## 414 archivos B sin geometrías: prioridad de investigación alta

Los archivos existen, tienen bytes y contienen un JSON legible con
`FeatureCollection` y `features=[]`. No aportan polígonos para representar
esas zonas. Es un problema potencial de utilidad del resultado, aunque no
incumpla la estructura JSON que verifica el monitor.

| CAD | Unidad | Semana 20261026 | Semana 20261102 | Total B sin geometrías |
|---|---|---:|---:|---:|
| DEANT | 43162 | 21 | 21 | 42 |
| DEANT | 43196 | 20 | 20 | 40 |
| DECAL | 53436 | 21 | 21 | 42 |
| DECAL | 53437 | 21 | 21 | 42 |
| DENOR | 30872 | 21 | 21 | 42 |
| DESUC | 24897 | 21 | 21 | 42 |
| DESUC | 24899 | 21 | 21 | 42 |
| MEMAZ | 52440 | 21 | 21 | 42 |
| MEMAZ | 52443 | 20 | 20 | 40 |
| MEMAZ | 52445 | 20 | 20 | 40 |
| **Total** | **10 unidades** | **207** | **207** | **414** |

En siete unidades, los 21 B01 de cada semana están vacíos: DEANT/43162,
DECAL/53436, DECAL/53437, DENOR/30872, DESUC/24897, DESUC/24899 y MEMAZ/52440.
Es decir, todas sus combinaciones de fecha/turno de ambas semanas carecen
de geometrías en el producto B01. En DEANT/43196, MEMAZ/52443 y MEMAZ/52445
están vacíos 20 de 21 B01 por semana.

Las parejas CP sí existen y tienen geometría: el defecto observado se concentra
en el contenido B. Se repite en dos carpetas semanales; la evidencia no determina
si se originó en la geometría de referencia, un filtro o el paso que construye B.
Tampoco confirma si el consumidor acepta expresamente un resultado sin zonas.

**Acción recomendada:** revisar primero la geometría de esas unidades y la lógica
que produce B01. Confirmar con el consumidor si debe existir al menos una zona.
Si es obligatorio, estos casos deben elevarse a FALLA funcional. Mientras no
se confirme ese contrato, conservar AVISO y priorizar su investigación.

## 32 archivos B con un polígono menos: prioridad media

Tienen uno, dos o tres elementos geométricos revisados, siempre **uno menos**
que el número del nombre: B02 con 1, B03 con 2 o B04 con 3. No se observaron en
ellos errores de anillos, coordenadas, identificación de unidad/CAD ni geometrías
nulas. Sus CP asociados están OK y conservan tantos elementos como el índice.

| CAD | Unidad | Semana 20261026 | Semana 20261102 | Total |
|---|---|---:|---:|---:|
| DESAB | 64822 | 21 | 0 | 21 |
| DESUC | 24881 | 2 | 4 | 6 |
| MEBAR | 66877 | 2 | 0 | 2 |
| MEVIL | 49531 | 2 | 1 | 3 |
| **Total** | **4 unidades** | **27** | **5** | **32** |

Destaca DESAB/64822: 21 B02 de la semana 20261026 contienen un solo polígono.
Los demás casos son puntuales en DESUC/24881, MEBAR/66877 y MEVIL/49531.

El monitor usa la igualdad entre índice y cantidad de elementos como una regla
observada en las muestras iniciales. No está confirmado que el índice obligue
a conservar exactamente esa cantidad de polígonos en B. Por eso estos 32 avisos
son de consistencia; no se pueden declarar 32 resultados fallidos automáticamente.

**Acción recomendada:** confirmar el significado de Bnn en el generador y en
el consumidor. Si admite menos polígonos, ajustar esa regla de conteo sin
retirar el control de archivos completamente vacíos. No se cambia la regla
del script únicamente para ocultar estos avisos.

## Lectura del resultado

- La entrega por nombres, parejas y calendario se completó para los archivos
  observados. Los 446 avisos no son archivos faltantes.
- Los 414 vacíos merecen mayor atención que los 32 desajustes de cantidad.
  El porcentaje nacional reducido oculta que siete unidades tienen todos
  los B01 vacíos en ambas semanas.
- Los datos analizados describen productos de `Optimizacion/`. No identifican
  por sí solos el modelo seleccionado ni permiten atribuir los casos a Uniform.
- Este informe interpreta los 446 avisos de contenido; no mezcla los controles
  de carpetas sin archivos o cambios frente a la referencia del CSV general.
- No se confirma la causa raíz ni se certifica la calidad del algoritmo con
  estos conteos.

El [CSV con los 446 archivos clasificados](avisos_interpretados.csv) incluye
ruta S3, motivo, prioridad, conteo B y conteo de su pareja CP para investigarlos
sin repetir la validación nacional.
