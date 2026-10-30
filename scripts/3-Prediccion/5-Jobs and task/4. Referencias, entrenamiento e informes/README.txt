REFERENCIAS, ENTRENAMIENTO E INFORMES
====================================

Script: monitor_glue_jobs.py

PARA QUÉ SIRVE

Reúne en un solo informe las ejecuciones de los siete jobs Glue configurados.
Muestra si arrancaron, siguen abiertos, terminaron o fallaron.

QUÉ REVISA

Referencia:
  pon-prod-xcrime-update-fuentes-internas
  pon-prod-xcrime-update-estructura-nal
  pon-prod-xcrime-update-geometrias-nal

Entrenamiento:
  pon_prod_xcrime_train_models_job
  pon_prod_xcrime_homologacion_job
  pon_prod_xcrime_consume_models_job

Informes:
  pon-prod-xcrime-informes-anticipaciones

Para cada job muestra último estado, inicio, fin, duración y errores.
Cuenta los estados de la ventana; un fallo permanece visible aunque después
haya una ejecución exitosa.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME y ejecuta:

  python3 "$HOME/monitor_glue_jobs.py" \
    --region us-east-1 \
    --dias 8 \
    --historial-completo \
    --salida "$HOME/salida"

Recorre el historial disponible y evalúa ocho días. Puede tardar si hay muchos
registros. Para limitarlo, sustituye --historial-completo por
--max-ejecuciones 5000; si faltan páginas, el informe lo indica.
Para revisar solo un grupo, añade --grupo referencia, --grupo entrenamiento
o --grupo informes. Sin esa opción revisa los tres.

QUÉ ENTREGA Y DÓNDE

Genera un CSV: informe_glue_jobs.csv.
Filtra tipo_registro=RESUMEN. DETALLE contiene las ejecuciones seleccionadas;
CONTROL y CONTEXTO explican hallazgos y cobertura.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/04-MonitorGlueJobs/FECHA/HORA/

Local:
  $HOME/salida/glue_jobs/FECHA/HORA/informe_glue_jobs.csv

FECHA/HORA es la ejecución en Bogotá. Si falla la subida, conserva el CSV local.

CÓMO LEER EL RESULTADO

Revisa Control y Cobertura por separado: una lectura parcial puede contener
fallos confirmados. El último estado puede ser anterior a la ventana; consulta
también los conteos del período.

SUCCEEDED significa éxito reportado por Glue; no certifica calidad ni uso del
modelo o del informe producido. Sin una frecuencia confirmada, no haber
ejecuciones recientes no demuestra un disparo perdido. Las duraciones son
informativas, sin un máximo estándar.

Este script no lanza ni corrige jobs, y no envía notificaciones.
