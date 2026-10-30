# Validación de matrices de predicción

**Script:** `validador_matrices_cloudshell.py`.

Revisa nombres y horas presentes de las matrices en:

`s3://pon-prod-ml-iapol/PrediccionMdP/AÑO/SEMANA/CAD/UNIDAD/MatricesPrediccion/`

**Estado de seguimiento:** 100% del alcance actual aceptado para esta etapa.
Las observaciones de la revisión quedan como mejoras posteriores; esa aceptación
no amplía los controles que realiza el archivo.

## Qué revisa

- Archivos directos con formato `Prediccion_UNIDAD_YYYYMMDD_HHMM.pkl`.
- Unidad del nombre, fecha válida y hora completa entre `0000` y `2300`.
- Las 24 horas de cada uno de los siete días de la carpeta semanal:
  **168 archivos horarios por unidad y semana** según este control.

Solo procesa nombres que empiezan por `Prediccion_`. No entra en subcarpetas
como `Turno/`. Un nombre `Prediccion_..._var.pkl` se reporta como inválido porque
no coincide con el formato horario; no se ignora automáticamente.

## Cómo ejecutarlo en CloudShell

Carga el `.py` en `$HOME` y ejecuta:

```bash
cd "$HOME"
python3 "$HOME/validador_matrices_cloudshell.py"
```

Por defecto revisa **solo la semana siguiente**, desde su lunes.
Para el 30 de octubre de 2026, corresponde a la carpeta `20261102`.

Para una semana y CAD concretos:

```bash
python3 "$HOME/validador_matrices_cloudshell.py" \
  --desde 20261102 --hasta 20261102 --sigla MEPER
```

Actualiza las fechas para el período que quieras revisar. Si cambias la semana
inicial, indica también `--hasta`. `--mostrar-todas` muestra en consola las unidades
sin hallazgos. El rango se busca dentro del año de la fecha inicial; revisa cada
año por separado si el intervalo cruza diciembre.

## Qué entrega y dónde

En la carpeta desde la que ejecutaste el comando crea:

`ResultadoMatrices[_SIGLA]_YYYYMMDD_HHMM.zip`

Contiene dos CSV cuyo sufijo identifica el rango:

- **`errores_matrices_rango_....csv`:** nombres que no cumplen el formato.
- **`horas_faltantes_matrices_rango_....csv`:** fechas, horas y nombres faltantes.

La fecha del ZIP corresponde a la ejecución en UTC-5. La versión actual guarda
el ZIP localmente; no publica informes en S3. Para descargarlo usa la ruta que
muestra la consola, normalmente dentro de `/home/cloudshell-user/`.

## Cómo leer el resultado

Busca semana, CAD y unidad en los CSV para localizar cada caso. Los archivos
pueden contener solo las cabeceras si no se registraron hallazgos.
Comprueba también la consola: un error de lectura de una unidad se imprime allí
y no se registra como fila de error en estos CSV.

Los siete días se cuentan desde la fecha de la carpeta semanal. Esta versión
no abre los PKL ni comprueba su tamaño como condición de validez. Un nombre
válido no demuestra que la matriz sea legible o tenga datos útiles.
No identifica unidades completamente ausentes mediante un catálogo esperado.
El comando puede terminar con salida 0 aunque haya hallazgos; revisa los informes.

El script consulta S3; no modifica matrices, reprocesa tareas ni envía alertas.
