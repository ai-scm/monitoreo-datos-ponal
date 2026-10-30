PROGRAMACIÓN Y LANZAMIENTO
=========================

Script: monitor_disparo_semanal.py

PARA QUÉ SIRVE

Busca evidencia de los disparos programados para iniciar predicciones e informes,
y revisa las funciones Lambda encargadas de lanzarlos.

QUÉ REVISA

- Las reglas xcrime_prediccion_siglas_unidades y generacion_informes_anticipaciones.
- Programación, destinos y evidencia de cada disparo esperado en la ventana.
- Invocaciones, errores, solicitudes limitadas y duración de las Lambdas.
- Configuración para conservar fallos de entrega: DLQ y destinos de fallo.
- Evidencia de lanzamiento de tareas en los logs de la función lanzadora.

Compara cada disparo con su ventana; una invocación de otra semana no sustituye
la evidencia del disparo que se está revisando.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME y ejecuta:

  python3 "$HOME/monitor_disparo_semanal.py" \
    --region us-east-1 \
    --dias 8 \
    --max-eventos 50000 \
    --salida "$HOME/salida"

QUÉ ENTREGA Y DÓNDE

Genera un CSV: informe_disparo_semanal.csv.
Filtra tipo_registro=RESUMEN para la vista principal. CONTROL explica los
hallazgos; EVIDENCIA reúne referencias de lanzamiento; CONTEXTO describe la lectura.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/02-MonitorDisparoSemanal/FECHA/HORA/

Local:
  $HOME/salida/disparo_semanal/FECHA/HORA/informe_disparo_semanal.csv

FECHA/HORA es la ejecución en Bogotá. La copia local queda disponible si falla
la publicación en S3.

CÓMO LEER EL RESULTADO

INFO en un disparo indica evidencia de actividad de la regla. PENDIENTE significa
que sigue abierta la espera de evidencia. SIN EVIDENCIA genera AVISO al terminar
esa espera. FALLA indica invocaciones fallidas observadas. INCOMPLETO significa
que no se pudo recuperar toda la información.

Un aviso por falta de DLQ describe una configuración de manejo de errores;
no demuestra que el disparo haya fallado. SIN DATOS tampoco equivale a cero.
Un ARN mencionado en logs no confirma por sí solo la creación de la tarea.

Usa la programación actual, interpretada en UTC; muestra también horas de Bogotá.
No certifica que todas las siglas arrancaran ni que sus predicciones terminaran.
Consulta y guarda su informe; no dispara reglas, invoca funciones ni crea alarmas.
