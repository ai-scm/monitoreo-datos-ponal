# Validación de matrices de predicción

**Script:** `validador_matrices_cloudshell.py`. **Versión:** `2026-11-03.1`.

Revisa los nombres, las horas presentes y el contenido de las matrices guardadas en:

`s3://pon-prod-ml-iapol/PrediccionMdP/AÑO/SEMANA/CAD/UNIDAD/MatricesPrediccion/`

La lectura del contenido está habilitada por defecto. Solo necesitas cargar el
script en CloudShell; no requiere instalar NumPy ni `open_cp`.

## Qué revisa

| Control | Qué detecta |
| --- | --- |
| Nombres y cobertura | Formato `Prediccion_UNIDAD_YYYYMMDD_HHMM.pkl`, unidad, fecha, hora y faltantes de los siete días: 24 horas por día. |
| Integridad del archivo | Archivos de cero bytes, PKL truncados, contenido ilegible y bytes adicionales al final. |
| Estructura de la matriz | Grilla `GridPredictionArray`, matriz numérica de dos dimensiones no vacías y cantidad de bytes coherente con su forma y tipo. |
| Posición y celdas | Tamaños de celda positivos y posiciones numéricas finitas. No impone la misma dimensión a todas las unidades. |
| Valores | NaN, infinitos e intensidades negativas distintas del marcador `-1`. |
| Celdas utilizables | Separa celdas enmascaradas, celdas `-1` fuera de zona y valores utilizables. Detecta matrices sin celdas útiles. |
| Contraste | Avisa si todas las intensidades utilizables son cero o tienen el mismo valor. No identifica el modelo utilizado. |
| Consistencia por unidad | Avisa si cambian la forma o la ubicación de la grilla respecto a otra matriz revisada de esa unidad y semana. |

El significado de `-1` se confirmó en el código del generador: `maskPrediction`
lo asigna a las celdas fuera de la zona. Esas celdas no se cuentan como intensidades
negativas ni afectan el mínimo, máximo o suma. Una máscara explícita también se
respeta, aunque su valor de relleno sea NaN.

Se conservan los controles de nombres y calendario anteriores: solo archivos
directos que empiezan por `Prediccion_`; no entra en `Turno/` ni otras subcarpetas.
Un nombre terminado en `_var.pkl` sigue reportándose como inválido.

## Cómo ejecutarlo

Carga únicamente `validador_matrices_cloudshell.py` en tu carpeta de CloudShell.
No necesitas subir `tests/`, referencias JSON ni módulos adicionales.

Primera prueba, con una unidad:

```bash
python3 "$HOME/validador_matrices_cloudshell.py" \
  --desde 20261102 --hasta 20261102 \
  --sigla MEPER --unidad 51716 \
  --salida "$HOME/salida"
```

Un CAD completo:

```bash
python3 "$HOME/validador_matrices_cloudshell.py" \
  --desde 20261102 --sigla MEPER \
  --salida "$HOME/salida"
```

Todas las siglas de una semana:

```bash
python3 "$HOME/validador_matrices_cloudshell.py" \
  --desde 20261102 --salida "$HOME/salida"
```

Actualiza la fecha al lunes de la semana que quieras revisar. Sin fechas explícitas
revisa **la semana siguiente**. Si indicas solo `--desde`, `--hasta` toma el mismo
valor. Puedes indicar un rango que cruce de un año a otro.

La consulta requiere permisos para listar el bucket y leer sus objetos
(`s3:ListBucket` y `s3:GetObject`). Los archivos de S3 se conservan intactos.

## Continuar una revisión grande

Por defecto intenta hasta **1.000 lecturas nuevas**, con un presupuesto de
**180 segundos para las lecturas**, y un máximo de **16 MiB por archivo**.
Los límites se comprueban entre llamadas. El descubrimiento, los listados y la
generación del informe pueden hacer que el tiempo total sea mayor.

Si quedan archivos por presupuesto, repite el mismo comando. Las revisiones se
guardan comprimidas en `.estado_matrices/`, dentro de `--salida`. Los archivos
sin cambios reutilizan su revisión; si cambian ETag, tamaño o fecha de modificación,
se vuelven a leer. Conserva esa carpeta para continuar.

Para ampliar un bloque:

```bash
python3 "$HOME/validador_matrices_cloudshell.py" \
  --desde 20261102 --sigla MEPER \
  --max-lecturas 5000 --max-segundos 600 \
  --salida "$HOME/salida"
```

Otras opciones:

- `--solo-metadata`: revisa nombres y presencia; el contenido queda **NO EVALUADO**.
- `--sin-cache`: vuelve a leer los archivos, respetando los límites configurados.
- `--max-mib 32`: permite archivos individuales de hasta 32 MiB. La revisión mantiene
  además un límite de dos millones de celdas por matriz.
- `--mostrar-todas`: muestra progreso por cada unidad.

Los formatos no soportados y los problemas de permisos necesitan revisión de su
causa; repetir el comando no los resuelve por sí solo.

## Qué entrega

En `--salida` crea:

`ResultadoMatrices[_SIGLA]_FECHA_HORA.zip`

Contiene **dos CSV**:

- **`detalle_matrices_rango_....csv`:** una fila por archivo, incluyendo los correctos.
  Muestra estados del nombre y contenido, motivo, tamaño, dimensiones, tipo de dato,
  celdas útiles/sin dato/enmascaradas, valores no finitos o negativos y mínimo/máximo/suma.
  Indica si la revisión vino de S3 (`GET`), caché o quedó pendiente. Los errores de
  listado, lectura y descubrimiento también quedan registrados.
- **`horas_faltantes_matrices_rango_....csv`:** fechas, horas y nombres faltantes.
  Los faltantes se calculan únicamente cuando el listado de la unidad se completó.

El primer CSV sustituye al antiguo archivo de errores de nombres. El ZIP usa fecha
y hora de ejecución de Bogotá. Los informes siguen siendo locales; este script no
los sube a S3 ni envía alertas.

La consola muestra una tabla por CAD: unidades-semana, archivos presentes, contenido
OK, avisos, fallas, pendientes y horas faltantes. **Presente no significa contenido
correcto**: un PKL corrupto puede existir y tener su nombre bien formado.

## Cómo interpretar los estados

- **OK:** pasaron los controles aplicados al contenido.
- **AVISO:** valores constantes/cero o diferencias de grilla que necesitan interpretación.
- **FALLA:** defecto observado, como archivo vacío, PKL corrupto, dimensiones inválidas,
  NaN/infinito o intensidades negativas no correspondientes a `-1`.
- **INCOMPLETO / NO EVALUADO:** límite de revisión, error de lectura, formato no
  soportado o modo de metadata. No se declara el contenido saludable.

Código de salida: `0` sin fallas ni pendientes (puede haber avisos), `1` si hay fallas
o faltantes horarios, `3` si solo quedan limitaciones o bloqueos, y `130` si se
interrumpe con Ctrl+C. Si hay fallas y lecturas pendientes, se conserva salida `1`
y ambas condiciones aparecen en los informes.

## Límites y verificación

El lector interpreta únicamente estructuras numéricas permitidas mediante clases
inertes locales. No importa las clases del predictor ni usa una carga general de
pickle. Los formatos desconocidos quedan sin evaluar. El mecanismo restringe
constructores siguiendo el patrón de [lectura restringida de Python](https://docs.python.org/3/library/pickle.html#restricting-globals).

El formato se contrastó con matrices reales de MEPER y METIB y con el código de
serialización del predictor. La muestra de MEPER pasó los controles; la de METIB
tenía intensidades constantes y produjo AVISO. Eso no constituye una revisión
nacional del contenido.

El calendario sigue siendo el de los siete días de la carpeta semanal. No determina
por sí solo si las fechas del productor usan otra convención horaria. Tampoco
detecta unidades nunca observadas mediante un catálogo esperado, verifica la
posición contra el polígono real de la unidad ni certifica calidad estadística.
Los PKL observados no incluyen un CAD, unidad o fecha internos que permitan
contrastar esos datos con el nombre; esa identidad se verifica en la ruta y nombre.

Pruebas locales, sin AWS ni dependencias del predictor:

```bash
python3 -B -m unittest discover \
  -s 'scripts/3-Prediccion/3-Scripts-Adicionales/MatricesPrediccion/tests' -v
```
