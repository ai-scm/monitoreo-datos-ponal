MONITORES DEL PROCESO DE PREDICCIÓN
==================================

Aquí se conservan siete scripts: seis monitores de uso habitual y un diagnóstico
opcional de la tabla de probabilidades. Los otros tres validadores del proyecto
revisan Prediccion/, Optimizacion/ y MatricesPrediccion/; sus enlaces están
en ../README.md.

El seguimiento global queda al 100% del alcance funcional aceptado al
30 de octubre de 2026, incluyendo los tres validadores de resultados.
La implementación en Lambda y las alertas automáticas no entran en ese porcentaje.

QUÉ HACE CADA UNO

1. monitor_datos_entrada.py
   Volumen, recencia, coordenadas y conteo diario por unidad y fuente.
   Entrega 2 CSV: general.csv y diario.csv.

2. monitor_disparo_semanal.py
   Disparos esperados de EventBridge, Lambdas y evidencia de lanzamiento.
   Entrega 1 CSV: informe_disparo_semanal.csv.

3. monitor_procesos_upstream.py
   Tareas de limpieza, última ejecución del crawler y métricas de DynamoDB.
   Entrega 1 CSV: informe_procesos_upstream.csv.

4. monitor_glue_jobs.py
   Ejecuciones de referencia, entrenamiento e informes; reúne los siete jobs.
   Entrega 1 CSV: informe_glue_jobs.csv.

5. mapear_inicio_progreso.py
   Estado, tiempos, salidas y errores de tareas aún visibles en ECS.
   Entrega 1 CSV: tareas.csv.

6. mapear_modelos_dependencias.py
   Modelos por CAD, porcentaje de Uniform, pocos datos y cobertura del ciclo.
   Entrega 1 CSV: historial_tareas.csv en el modo recomendado.
   Los diagnósticos adicionales pueden generar otros archivos.

7. monitor_freshness_probabilidaddelito.py
   Diagnóstico opcional; fuera de la ejecución habitual.
   Revisa existencia de la tabla probabilidaddelito y archivos de su fuente.
   Entrega 1 CSV si se ejecuta: informe_freshness_probabilidaddelito.csv.
   Su lectura por el predictor está confirmada. Se trata como referencia fija,
   sin exigir una actualización semanal ni alarmar por su antigüedad.

CÓMO USARLOS

Carga el .py actualizado en $HOME de CloudShell, en us-east-1 y la cuenta
926162397524. Abre el README de su carpeta y copia el comando recomendado.
Cada monitor funciona por separado; no necesita los otros seis .py.
La numeración organiza la lectura del proceso; no programa ni encadena tareas.

DÓNDE SE GUARDAN

Los seis monitores habituales generan siete CSV en total. El diagnóstico
opcional añade otro CSV cuando se ejecuta. Conservan copias locales en
$HOME/salida y estas carpetas de s3://pon-prod-ml-iapol/ValidationReports/:

  01-MonitorDatosEntrada/
  02-MonitorDisparoSemanal/
  03-MonitorProcesosUpstream/
  04-MonitorGlueJobs/
  05-MonitorInicioProgreso/
  06-MonitorModelosDependencias/
  07-MonitorFreshnessProbabilidaddelito/ (solo diagnóstico opcional)

Dentro de cada carpeta: FECHA/HORA/archivo.csv. La fecha es el día de ejecución
en Bogotá. Los JSON y SQL auxiliares quedan locales.
Si falla la publicación, revisa el mensaje y utiliza la copia local.

Si conservas una ruta antigua en ATHENA_OUTPUT_S3, puedes quitarla con:

  unset ATHENA_OUTPUT_S3

Los README de datos de entrada y probabilidades explican su uso de Athena.
No borres $HOME/salida si vas a reanudar una consulta de modelos pendiente.

CÓMO INTERPRETARLOS

Empieza por el resumen; después revisa FALLA, AVISO e INCOMPLETO.
SIN DATOS no equivale a cero problemas. Un estado SUCCEEDED o RUNNING tampoco
certifica la calidad de una predicción. La comprobación de archivos se hace
con los validadores de resultados.

Estos scripts consultan recursos y guardan sus informes. No arrancan procesos,
no corrigen datos y no envían notificaciones automáticas.
ALCANCE_Y_VALIDACION.txt conserva el detalle de criterios y validaciones previas.
