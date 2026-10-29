MONITOREO DE PREDICCIONES
========================

Esta carpeta reúne siete scripts para revisar el proceso de predicción:
los datos disponibles, el lanzamiento de las tareas, su ejecución y los
modelos que reportan. Cada script se puede ejecutar por separado en CloudShell.

Los monitores muestran una tabla y guardan informes CSV. Consultan los procesos
sin arrancarlos ni detenerlos; escriben sus propios informes en S3.
Actualmente no envían correos ni notificaciones automáticas.

POR DÓNDE EMPEZAR

1. Abre CloudShell en la región us-east-1 con acceso a la cuenta 926162397524.
2. Carga los siete archivos .py actualizados en /home/cloudshell-user/.
   Cada script funciona por sí solo; no necesitas subir los tests.
3. Abre el README de la carpeta que te interese y copia su comando.
4. Revisa la tabla de consola y después el CSV. La consola muestra dónde
   quedaron los archivos.

Si antes configuraste ATHENA_OUTPUT_S3 con una ruta antigua, ejecuta una vez:

  unset ATHENA_OUTPUT_S3

Así, cada monitor utilizará su propia carpeta predeterminada para Athena.

La numeración ayuda a recorrer las partes del monitoreo. Ejecutar un script
no ejecuta los demás ni pone en marcha la predicción.

QUÉ REVISA CADA CARPETA

1. Datos antes de predecir
   monitor_datos_entrada.py
   Compara volumen, fechas y coordenadas de los datos de entrada.
   Entrega 2 CSV: general y diario.

2. Programación y lanzamiento
   monitor_disparo_semanal.py
   Revisa los disparos esperados y las funciones que lanzan tareas o informes.
   Entrega 1 CSV.

3. Preparación de datos
   monitor_procesos_upstream.py
   Revisa limpieza, crawler y señales de limitación de solicitudes en DynamoDB.
   Entrega 1 CSV.

4. Referencias, entrenamiento e informes
   monitor_glue_jobs.py
   Revisa las ejecuciones de los siete jobs Glue configurados.
   Entrega 1 CSV.

5. Ejecución de las predicciones
   mapear_inicio_progreso.py
   Muestra estado, tiempos y errores de las tareas que ECS todavía conserva.
   Entrega 1 CSV.

6. Modelos utilizados y componentes
   mapear_modelos_dependencias.py
   Resume modelos reportados por sigla, uso de Uniform, pocos datos y errores.
   Contrasta los CAD/modalidades con el catálogo de inicio cuando se recupera
   y distingue las tareas creadas de las que reportaron selecciones de modelo.
   Entrega 1 CSV con el comando recomendado.

7. Actualidad de la tabla de probabilidades
   monitor_freshness_probabilidaddelito.py
   Revisa la tabla probabilidaddelito y los archivos de su fuente.
   La actualidad del contenido sigue pendiente de confirmación.
   Entrega 1 CSV.

Con los comandos recomendados, los siete monitores generan 8 informes CSV.
Los modos adicionales de diagnóstico pueden producir otros CSV.

DÓNDE SE GUARDAN LOS INFORMES

En S3, dentro de s3://pon-prod-ml-iapol/ValidationReports/:

  01-MonitorDatosEntrada/
  02-MonitorDisparoSemanal/
  03-MonitorProcesosUpstream/
  04-MonitorGlueJobs/
  05-MonitorInicioProgreso/
  06-MonitorModelosDependencias/
  07-MonitorFreshnessProbabilidaddelito/

Cada carpeta organiza sus informes por fecha y hora de Bogotá:
CARPETA/FECHA/HORA/archivo.csv. Aparece cuando se guarda el primer CSV.
FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.

Los comandos recomendados conservan también una copia en $HOME/salida.
Los archivos auxiliares JSON, SQL y los registros de consola quedan locales.
Si falla la subida a S3, el monitor informa el problema y conserva el CSV local.

ValidationReports/2026/ pertenece al validador de resultados en S3, que está
fuera de estos siete monitores y conserva su ubicación.

CÓMO LEER LOS ESTADOS

  OK / INFO: el control pasó o hay evidencia disponible, según el monitor.
  AVISO: hay una señal que conviene revisar; no prueba una falla por sí sola.
  FALLA: se detectó un error o un incumplimiento del control evaluado.
  INCOMPLETO / PARCIAL: faltó información para terminar la revisión.
  AUSENTE: no se encontró el recurso configurado.
  NO EVALUADO: faltan datos o una definición para aplicar ese control.
  PENDIENTE: la ventana de observación todavía está abierta.

La cobertura de lectura y el resultado son cosas distintas. Una lectura
completa puede encontrar fallas, y una lectura parcial puede mostrar errores
que sí quedaron confirmados. Revisa los estados del CSV; que el comando termine
no garantiza que toda la predicción haya salido bien.

TÉRMINOS QUE APARECEN EN LOS INFORMES

Sigla o CAD: código del ámbito de la predicción, por ejemplo METIB o MEBOG.
En la configuración de las tareas se utiliza la variable CAD.
Unidad: unidad base dentro de una sigla.
BD: fuente de datos; en el monitor de entradas se consideran RNMC, SECAD y SIEDCO.
Ciclo: período de revisión asociado al disparo semanal de predicciones.

PARA UNA REVISIÓN MÁS DETALLADA

ALCANCE_Y_VALIDACION.txt reúne los criterios y la evidencia de validación.
Cada README describe el uso actual de su script y sus límites.
