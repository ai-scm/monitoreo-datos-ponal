# V2 con validación del contenido — copia independiente

**Archivo para CloudShell:** `validar_todas_siglas_v2_contenido.py`.

Esta copia añade la lectura de los GeoJSON al control de nombres y horas de V2.
El archivo original **`validar_todas_siglas v2.py` se conserva intacto** y sigue
disponible con su funcionamiento anterior. La copia no depende de ese archivo,
del validador de matrices ni de la versión Lambda.

## Qué revisa

Busca resultados en:

`s3://pon-prod-ml-iapol/PrediccionMdP/AÑO/SEMANA/CAD/UNIDAD/Prediccion/`

| Control | Resultado que comprueba |
| --- | --- |
| Nombre y horas | `Prediccion_UNIDAD_YYYYMMDD_HHMM.geojson`, con las 24 horas de los siete días de cada carpeta semanal. |
| Archivo y JSON | Contenido no vacío, legible, sin claves duplicadas, NaN ni infinitos. |
| Estructura | `FeatureCollection` no vacía, elementos `Feature`, propiedades y geometrías. |
| Geometrías | `Polygon` y `MultiPolygon` con anillos cerrados, al menos cuatro posiciones y tres vértices distintos; coordenadas numéricas en rango geográfico. |
| Identidad | `id_unidadbase` debe coincidir con la unidad de la ruta; `prediction_date`, con la fecha y hora del nombre. Si aparece `CAD`, también lo contrasta. |
| Intensidades | `intensity_val` presente, numérico, finito y no negativo. Avisa cuando todos los valores son cero o constantes. |

Las propiedades `id_unidadbase`, `prediction_date` e `intensity_val` se
confirmaron en el código del generador y en muestras reales de MEPER y METIB.
Las reglas de estructura poligonal se basan en [GeoJSON, RFC 7946](https://www.rfc-editor.org/rfc/rfc7946.html#section-3.1.6).

Se mantiene el alcance de V2: **solo archivos directos que comienzan por
`Prediccion_`**. No entra en `Turno/` ni revisa `PuntosApoyo`.

## Primera ejecución

Sube únicamente `validar_todas_siglas_v2_contenido.py` a CloudShell. No necesitas
subir el original, archivos JSON, los tests ni instalar librerías geoespaciales.

Prueba con una unidad:

```bash
python3 "$HOME/validar_todas_siglas_v2_contenido.py" \
  --desde 20261102 --hasta 20261102 \
  --sigla MEPER --unidad 51716 \
  --salida "$HOME/salida"
```

Para revisar MEPER completo, elimina `--unidad 51716`. Para todas las siglas,
elimina también `--sigla MEPER`.

Actualiza las fechas según la semana que quieras consultar. Como en el V2 original,
sin fechas toma el lunes actual en Bogotá y todas las semanas disponibles desde
allí dentro de ese año. Para limitarlo a una semana, usa el mismo valor en `--desde`
y `--hasta`. Un rango explícito también puede cruzar de año.

Los permisos necesarios son listar el bucket y leer sus objetos. Solo consulta S3;
no modifica las predicciones ni ejecuta procesos que las generen.

## Revisiones grandes y continuación

Por defecto realiza hasta **1.000 lecturas nuevas**, con un presupuesto de
**180 segundos entre lecturas** y un máximo de **16 MiB por archivo**. El tiempo
total puede ser mayor por los listados y la escritura de informes.

Si hay pendientes por presupuesto, repite el mismo comando. Reutiliza las
revisiones de archivos sin cambios, guardadas en:

`--salida/.estado_predicciones_v2_contenido/`

Si cambian el ETag, tamaño o fecha de modificación de un objeto, vuelve a leerlo.
Conserva esa carpeta para continuar. Los informes anteriores quedan en carpetas
separadas por fecha y hora de ejecución de Bogotá.

Opciones útiles:

```bash
python3 "$HOME/validar_todas_siglas_v2_contenido.py" \
  --desde 20261102 --hasta 20261102 --sigla MEPER \
  --max-lecturas 5000 --max-segundos 600 \
  --salida "$HOME/salida"
```

- `--solo-metadata`: revisa nombres y horas; el contenido queda **NO EVALUADO**.
- `--sin-cache`: relee los objetos, respetando el presupuesto configurado.
- `--max-mib 32`: permite archivos individuales de hasta 32 MiB.
- `--mostrar-todas`: muestra el progreso por unidad.

Un error de permisos o un formato no soportado necesita resolver su causa;
repetir el comando por sí solo no lo corrige.

## Qué entrega

Crea una carpeta local `ResultadoAnalisisContenido[_CAD]_FECHA_HORA/` con **dos CSV**:

- **`detalle_prediccion_rango_....csv`:** una fila por archivo revisado, incluyendo
  los correctos. Incluye estado del nombre, estado del contenido, motivo, origen
  de la revisión, número de features, tipos de geometría, errores de unidad/fecha
  y mínimo/máximo de intensidad. Los bloqueos de lectura y listado también quedan
  registrados; no se ocultan como un resultado correcto.
- **`horas_faltantes_rango_....csv`:** fechas y horas que no se encontraron.
  Solo se calculan faltantes cuando se completó el listado de la unidad.

No sobrescribe informes de V2 ni sube estos informes a S3. La consola muestra una
tabla por CAD con archivos presentes, contenido correcto, avisos, fallas,
pendientes y horas faltantes.

## Cómo interpretar el resultado

- **OK:** pasó los controles de contenido aplicados.
- **AVISO:** intensidades en cero o constantes, o por encima de la escala 0–255
  observada en el productor. Requiere interpretación.
- **FALLA:** defecto concreto: JSON corrupto, archivo vacío, estructura inválida,
  unidad/fecha diferente, intensidad inválida o geometría poligonal mal formada.
- **INCOMPLETO / NO EVALUADO:** falta lectura por presupuesto, acceso, tamaño,
  otro sistema de coordenadas o geometría fuera del perfil soportado.

Código de salida: `0` sin fallas ni pendientes, aunque puede haber avisos; `1`
si hay fallas o faltantes horarios; `3` si hay limitaciones sin fallas confirmadas;
`130` si se interrumpe. Una falla y una lectura pendiente pueden coexistir.

## Límites

Un archivo sin contraste **no demuestra que se haya elegido Uniform** ni que la
predicción sea incorrecta. Este control verifica integridad y coherencia del
resultado; no mide precisión estadística.

La geometría se comprueba por su estructura. No verifica cruces entre segmentos,
solapamientos, orientación de anillos ni coincidencia con el polígono oficial de
la unidad. Un CRS desconocido o un tipo distinto de Polygon/MultiPolygon queda
sin evaluar, en lugar de declararlo correcto.

El calendario conserva los siete días desde la fecha de la carpeta semanal,
sin convertir las horas del nombre a otra zona horaria. Tampoco detecta unidades
nunca observadas sin un catálogo esperado, ni confirma el cierre del generador.
Los campos adicionales como `irisP1` y rankings no se contrastan con sus fuentes;
sí se comprueba que formen parte de un JSON legible y con números finitos.

## Verificación local

Las pruebas cubren contenido, reanudación, errores de lectura, dos CSV y
equivalencia de nombres/cobertura con V2. No necesitan AWS.

```bash
python3 -B -m unittest discover \
  -s 'scripts/3-Prediccion/1-Ejecucion-Prediccion/Prediccion/script s3/tests' -v
```

La evidencia del formato y las muestras usadas se registra en
`evidencia_v2_contenido_2026-11-03.json`, sin copiar los GeoJSON de negocio al repositorio.
