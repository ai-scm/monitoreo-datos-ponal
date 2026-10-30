# Validación de predicciones — V2

**Script:** `validar_todas_siglas v2.py`.

Revisa los nombres y las horas presentes de los resultados de predicción en:

`s3://pon-prod-ml-iapol/PrediccionMdP/AÑO/SEMANA/CAD/UNIDAD/Prediccion/`

## Qué revisa

- Archivos directos con formato `Prediccion_UNIDAD_YYYYMMDD_HHMM.geojson`.
- Unidad del nombre, fecha válida y hora completa entre `0000` y `2300`.
- Las 24 horas de cada uno de los siete días de la carpeta semanal:
  **168 archivos horarios por unidad y semana** según este control.

Solo evalúa archivos que empiezan por `Prediccion_`. No entra en `Turno/` ni
otras subcarpetas. Los archivos `PuntosApoyo` quedan fuera de esta revisión.

## Cómo ejecutarlo en CloudShell

Carga el archivo V2 en `$HOME`. Mantén las comillas porque el nombre tiene espacios.

```bash
cd "$HOME"
python3 "$HOME/validar_todas_siglas v2.py"
```

Por defecto toma el lunes de la semana actual en Bogotá y todas las carpetas
semanales disponibles desde allí, dentro del año de la fecha inicial.
No se limita automáticamente a dos semanas.

Para una prueba de una sola semana y CAD:

```bash
python3 "$HOME/validar_todas_siglas v2.py" \
  --desde 20261102 --hasta 20261102 --sigla MEPER
```

Sustituye la fecha por el lunes que quieras revisar. `--mostrar-todas` permite
ver en consola también las unidades sin hallazgos. Para rangos que crucen de año,
ejecuta cada año por separado.

## Qué entrega y dónde

Crea en la carpeta desde la que ejecutaste el comando:

`ResultadoAnalisis[_SIGLA]_YYYYMMDD_HHMM/`

Contiene dos CSV, cuyo sufijo identifica el rango:

- **`errores_prediccion_rango_....csv`:** nombres que no cumplen el formato.
- **`horas_faltantes_rango_....csv`:** horas y nombres esperados que no aparecieron.

Los CSV pueden contener solo las cabeceras si no se registraron hallazgos.
El comando manual V2 no sube esos informes a S3. Los informes de la solución ya
implementada en AWS conservan su ubicación operativa en `ValidationReports/2026/`.

## Cómo leer el resultado

Empieza por las unidades con errores de formato o con horas faltantes.
Los CSV incluyen semana, CAD, unidad y ruta para localizar el caso.
Comprueba también la consola: los errores de lectura de una unidad se imprimen
allí y no quedan registrados como una fila de error en los CSV.

Este control verifica nombres y presencia, no abre el GeoJSON ni evalúa su
contenido. Una carpeta o unidad nunca observada no se contrasta con un catálogo
obligatorio. Los siete días del control parten de la fecha de la carpeta semanal.
El script consulta S3; no modifica resultados ni reprocesa predicciones.
