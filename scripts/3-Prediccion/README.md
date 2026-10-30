# Monitoreo integral de predicciones

Esta carpeta conserva **10 scripts**: **nueve de uso habitual** (seis monitores
del proceso y tres validadores de resultados) y **uno de diagnóstico opcional**.
Sirven para encontrar problemas en los datos, el lanzamiento, la ejecución
y los archivos generados.

Cada script se ejecuta por separado. Ejecutar un monitor no pone en marcha la
predicción ni ejecuta los demás monitores. Las guías describen el funcionamiento
actual de los archivos, sin incluir una futura implementación en Lambda.

## Estado del alcance — 30 de octubre de 2026

**100% del alcance funcional aceptado para esta etapa**, incluyendo los
validadores de predicciones V2, optimización y matrices. El validador de matrices
se acepta con sus controles actuales; las mejoras identificadas quedan para después.

`monitor_freshness_probabilidaddelito.py` queda fuera de la ejecución habitual:
la tabla se trata como referencia fija, sin exigir actualización semanal.
El archivo se conserva para diagnóstico. Se confirmó que la tarea de predicción
de DECAU consulta esa referencia; no se ha establecido una frecuencia de recálculo.

Este porcentaje expresa avance y aceptación del alcance acordado. No incluye
la implementación en Lambda, la programación ni el envío automático de alertas,
y no es una medida del porcentaje de fallas que el sistema puede detectar.

## Pipeline y arquitectura AWS

La [guía de diagramas](documentacion-predicciones/README.md) reúne la arquitectura
con logos AWS, el pipeline, la cobertura por script y el paquete actualizado
al 30 de octubre de 2026. Distingue la versión vigente de los documentos históricos.

## Qué revisar

| Script | Para qué sirve | Informes del comando recomendado |
|---|---|---|
| [monitor_datos_entrada.py](<5-Jobs and task/1. Datos antes de predecir/README.txt>) | Compara volumen, recencia y coordenadas antes de predecir. | 2 CSV: general y diario. |
| [monitor_disparo_semanal.py](<5-Jobs and task/2. Programación y lanzamiento/README.txt>) | Revisa los disparos programados, las Lambdas y la evidencia de lanzamiento. | 1 CSV. |
| [monitor_procesos_upstream.py](<5-Jobs and task/3. Preparación de datos/README.txt>) | Revisa las tareas de limpieza, el crawler y las tablas históricas. | 1 CSV. |
| [monitor_glue_jobs.py](<5-Jobs and task/4. Referencias, entrenamiento e informes/README.txt>) | Revisa los siete jobs de referencia, entrenamiento e informes. | 1 CSV. |
| [mapear_inicio_progreso.py](<5-Jobs and task/5. Ejecución de las predicciones/README.txt>) | Muestra estados, tiempos y errores de tareas visibles en ECS. | 1 CSV. |
| [mapear_modelos_dependencias.py](<5-Jobs and task/6. Modelos utilizados y componentes/README.txt>) | Resume modelos por CAD, porcentaje de Uniform y avisos de pocos datos. | 1 CSV en el modo habitual. |
| [monitor_freshness_probabilidaddelito.py](<5-Jobs and task/7. Actualidad de la tabla de probabilidades/README.txt>) | Diagnóstico opcional de la tabla de referencia; fuera de la ejecución habitual. | 1 CSV si se ejecuta. |
| [validar_todas_siglas v2.py](<1-Ejecucion-Prediccion/Prediccion/script s3/README.md>) | Revisa nombres y horas faltantes de las predicciones GeoJSON. | 2 CSV locales. |
| [validador_optimizacion_cloudshell.py](<2-Zonas-Atencion-Fusion/Optimizacion/README.md>) | Revisa nombres, parejas B/CP, cantidades y contenido de optimización. | 2 CSV. |
| [validador_matrices_cloudshell.py](<3-Scripts-Adicionales/MatricesPrediccion/README.md>) | Revisa nombres y horas faltantes de las matrices PKL. | 2 CSV dentro de un ZIP local. |

Las versiones V1 y el código de adaptación a Lambda de predicciones son material
separado. La versión de referencia para esta lista es **V2**.
`4-Anticipacion-Demanda/` queda fuera de estos diez scripts.

## Cómo empezar en CloudShell

1. Abre la región `us-east-1`, en la cuenta `926162397524`.
2. Carga el `.py` del monitor que vas a ejecutar en tu carpeta de usuario.
   Para optimización, carga también `referencia_optimizacion.json` junto al script
   si quieres comparar las cantidades con la referencia histórica.
3. Abre su README y copia el comando. Los ejemplos usan `$HOME`, que en CloudShell
   corresponde a `/home/cloudshell-user`.
4. Lee el resultado en consola y abre los CSV en la ruta indicada al terminar.

Se necesita Python 3, boto3 y una sesión AWS con permisos de consulta para los
servicios revisados. Los scripts que publican informes también necesitan permiso
para escribir en su carpeta de `ValidationReports/`. No se cargan credenciales
ni archivos de pruebas junto a los scripts.

## Dónde quedan los informes

Los seis monitores habituales, el diagnóstico opcional y el validador de
optimización publican sus CSV bajo:

`s3://pon-prod-ml-iapol/ValidationReports/`

Cada uno tiene su propia carpeta, de `01-MonitorDatosEntrada/` a
`08-MonitorOptimizacion/`. Dentro se utiliza `FECHA/HORA/`, con el día de ejecución
en Bogotá, aunque los datos revisados pertenezcan a otra semana.

La V2 manual de predicciones guarda dos CSV locales. El validador de matrices
crea un ZIP local. Sus README indican cómo encontrarlos. Los reportes del
validador de predicciones ya implementado en AWS conservan su ubicación operativa
en `ValidationReports/2026/`; este conjunto de guías no cambia ese despliegue.

## Cómo leer un resultado

- **OK / INFO:** pasó el control aplicado o hay evidencia disponible, según el script.
- **AVISO:** hay una condición que conviene revisar.
- **FALLA:** se encontró un error o incumplimiento del control.
- **INCOMPLETO / PARCIAL:** faltó información para terminar la lectura.
- **NO EVALUADO:** falta una definición o evidencia para aplicar el control.
- **PENDIENTE:** la entrega o la observación todavía no está cerrada.

Revisa siempre los mensajes y la cobertura. Que un comando termine no garantiza
que todas las predicciones estén completas. Los scripts manuales de esta lista
no configuran ni envían alertas automáticas.

**CAD o sigla** identifica el ámbito de la predicción, por ejemplo MEPER.
**Unidad** es una unidad base dentro de ese CAD. **Semana** es la carpeta cuya
fecha identifica el inicio del período; cada validador explica qué semanas toma.
