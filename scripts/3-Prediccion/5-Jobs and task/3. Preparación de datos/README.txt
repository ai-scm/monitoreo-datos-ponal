PREPARACIÓN DE DATOS
====================

Script: monitor_procesos_upstream.py

PARA QUÉ SIRVE

Revisa los servicios que limpian y preparan la información antes de la predicción.
Permite encontrar errores de ejecución o señales de solicitudes limitadas.

QUÉ REVISA

- Tareas visibles de pon-prod-xcrime-datacleaning-cluster.
- Estado y última ejecución de pon-prod-xcrime-datacleaning-glue-crawler.
- Estado y métricas de solicitudes limitadas de las tablas DynamoDB:
  pon_prod_llamada_siglas_historico,
  pon_prod_comparendo_siglas_historico y
  pon_prod_delito_siglas_historico.

Los jobs que actualizan fuentes, estructura y geometrías se revisan con
monitor_glue_jobs.py, en la carpeta 4.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME y ejecuta:

  python3 "$HOME/monitor_procesos_upstream.py" \
    --region us-east-1 \
    --dias 8 \
    --salida "$HOME/salida"

Los ocho días corresponden a las métricas. ECS muestra tareas que aún conserva;
del crawler se revisa solo su última ejecución disponible.

QUÉ ENTREGA Y DÓNDE

Genera un CSV: informe_procesos_upstream.csv.
Filtra tipo_registro=RESUMEN para ver una fila por recurso. CONTROL explica los
hallazgos; DETALLE y CONTEXTO conservan la evidencia y la cobertura de lectura.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/03-MonitorProcesosUpstream/FECHA/HORA/

Local:
  $HOME/salida/procesos_upstream/FECHA/HORA/informe_procesos_upstream.csv

FECHA/HORA es la ejecución en Bogotá. Si falla la subida, conserva la copia local.

CÓMO LEER EL RESULTADO

FALLA puede indicar un error de la limpieza o de la última ejecución del crawler.
Las solicitudes limitadas en DynamoDB indican una condición para revisar.
SIN DATOS no significa cero limitaciones ni ausencia de problemas.

ACTIVE o SUCCEEDED no garantizan que los datos estén actualizados. No lee los
registros de negocio; el volumen y la calidad se revisan con datos de entrada.
El comando puede devolver salida 0 aunque haya una FALLA en el informe:
revisa siempre la columna Control.

No arranca tareas ni crawlers, no habilita métricas y no crea alarmas.
