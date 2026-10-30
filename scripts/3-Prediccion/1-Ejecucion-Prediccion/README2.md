# Resultados de predicción

Aquí está el validador de los archivos horarios de `Prediccion/` en S3.
La versión tomada como referencia para el monitoreo es **`validar_todas_siglas v2.py`**.

Revisa nombres de archivos y horas faltantes por semana, CAD y unidad.
La [guía de uso de V2](<Prediccion/script s3/README.md>) explica qué cargar,
el comando de ejecución y los dos CSV que genera la versión manual.

`Prediccion/files/` conserva documentación y código de automatización.
Su [README de implementación](Prediccion/files/README.md) describe ese material.
El validador ya implementado en AWS y su ubicación de informes se mantienen;
esta actualización de documentación no cambia código ni despliegues.

`validar_todas_siglas v1.py` es una versión anterior y no se cuenta como otro
validador dentro de los diez scripts del monitoreo.
