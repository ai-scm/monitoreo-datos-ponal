# Matrices y material adicional

El script de monitoreo de esta carpeta es
**`MatricesPrediccion/validador_matrices_cloudshell.py`**.

Revisa los nombres, las horas presentes y el contenido de las matrices PKL generadas
en S3: integridad, dimensiones, valores numéricos y grillas sin celdas útiles.
También avisa por matrices sin contraste y conserva caché para continuar revisiones.
La [guía de matrices](MatricesPrediccion/README.md) explica el comando, las semanas
revisadas y el ZIP con los dos CSV de resultados.

`Hechos/` conserva ejemplos de archivos y capturas de referencia. No contiene
otro validador dentro de los diez scripts acordados para este monitoreo.
