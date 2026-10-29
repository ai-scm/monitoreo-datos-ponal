PROGRAMACIÓN Y LANZAMIENTO
=========================

Archivo: monitor_disparo_semanal.py

PARA QUÉ SIRVE

Revisa si hay evidencia de los disparos programados para iniciar las
predicciones y los informes. También comprueba errores de las funciones
encargadas de esos lanzamientos.

QUÉ REVISA

- Las reglas xcrime_prediccion_siglas_unidades y generacion_informes_anticipaciones.
- Sus horarios y las funciones Lambda que tienen como destino.
- Cada disparo semanal esperado dentro de los últimos ocho días.
- Invocaciones, errores, solicitudes limitadas y duración de las Lambdas.
- Si hay configurado un mecanismo para conservar los fallos de entrega.
- Evidencia de lanzamiento de tareas en los registros de la función lanzadora.

Compara cada disparo con su propia ventana de observación, para que la actividad
de otra semana no oculte un disparo sin evidencia.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/monitor_disparo_semanal.py" \
    --dias 8 \
    --max-eventos 50000 \
    --salida "$HOME/salida"

QUÉ INFORME ENTREGA

Genera informe_disparo_semanal.csv.

En Excel, filtra la columna tipo_registro:

  RESUMEN: vista principal de reglas, disparos esperados y funciones.
  CONTROL: explicación de los avisos o problemas.
  EVIDENCIA: referencias de lanzamiento encontradas en los registros.
  CONTEXTO: período y configuración utilizados en la revisión.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/02-MonitorDisparoSemanal/FECHA/HORA/

Copia local:
  $HOME/salida/disparo_semanal/FECHA/HORA/informe_disparo_semanal.csv

CÓMO INTERPRETARLO

Un disparo con INFO tiene evidencia de actividad de la regla.
SIN EVIDENCIA genera AVISO cuando termina la ventana de observación y espera.
PENDIENTE significa que todavía se está esperando evidencia.
FALLA indica que se observaron invocaciones fallidas en la ventana del disparo.
INCOMPLETO significa que no se pudo recuperar toda la información.

Un aviso por falta de DLQ o destino de fallo señala que no está configurado un
mecanismo para conservar errores de entrega; no significa que el disparo haya fallado.

LÍMITES DEL RESULTADO

- SIN DATOS no equivale a cero invocaciones ni demuestra una falla.
- La actividad de Lambda puede incluir llamadas distintas del disparo semanal.
- Una tarea mencionada en un registro no prueba que se haya creado o ejecutado.
- No certifica que hayan arrancado todas las siglas esperadas ni que terminaran bien.
- Usa la programación actual. Si cambió, no reconstruye el calendario anterior.
- Los horarios se interpretan en UTC y los disparos esperados también se muestran
  en hora de Bogotá. Programaciones no admitidas quedan NO EVALUADAS.

El script no dispara reglas ni invoca funciones. Solo publica su informe.
Si la subida a S3 falla, conserva el CSV local.
