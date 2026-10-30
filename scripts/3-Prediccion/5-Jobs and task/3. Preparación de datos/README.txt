PREPARACIÓN DE DATOS
====================

Archivo: monitor_procesos_upstream.py

PARA QUÉ SIRVE

Revisa los servicios que preparan información antes de la predicción.
Ayuda a encontrar fallos en la limpieza o señales de que las fuentes están
limitando solicitudes.

QUÉ REVISA

- Las tareas visibles del cluster de limpieza:
  pon-prod-xcrime-datacleaning-cluster.
- El estado y la última ejecución del crawler que actualiza el catálogo:
  pon-prod-xcrime-datacleaning-glue-crawler.
- El estado y las señales de solicitudes limitadas en las tres tablas
  históricas DynamoDB de llamadas, comparendos y delitos.

Las ejecuciones de los jobs que actualizan fuentes, estructura y geometrías
se revisan con monitor_glue_jobs.py, en la carpeta 4.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/monitor_procesos_upstream.py" \
    --dias 8 \
    --salida "$HOME/salida"

Los ocho días corresponden a las métricas. Las tareas ECS y la última
ejecución del crawler tienen la disponibilidad descrita más abajo.

QUÉ INFORME ENTREGA

Genera informe_procesos_upstream.csv.

En Excel, filtra tipo_registro=RESUMEN para ver una fila por recurso.
CONTROL explica los hallazgos; DETALLE conserva la información observada.
CONTEXTO registra el período y las limitaciones de la lectura.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/03-MonitorProcesosUpstream/FECHA/HORA/

Copia local:
  $HOME/salida/procesos_upstream/FECHA/HORA/informe_procesos_upstream.csv

CÓMO INTERPRETARLO

Revisa la columna Control y los mensajes de cada recurso.
Una FALLA puede señalar un error de la tarea de limpieza o de la última
ejecución del crawler. Las solicitudes limitadas en DynamoDB son una señal
para revisar la capacidad de atención de la fuente.

SIN DATOS en una métrica no demuestra ausencia de problemas ni significa
que hubo cero solicitudes limitadas.

LÍMITES DEL RESULTADO

- ECS solo muestra las tareas que todavía conserva; no es un historial completo.
- Del crawler se consulta la última ejecución, no todas las ejecuciones de la semana.
- Un estado ACTIVE o un último SUCCEEDED no garantizan que los datos estén actualizados.
- No lee los registros de negocio ni mide la calidad de la tabla de entrada;
  esa revisión corresponde a monitor_datos_entrada.py.

Que el comando termine sin error no garantiza que no haya hallazgos:
este monitor puede devolver salida 0 y conservar una FALLA observada en el CSV.
Revisa siempre la columna Control.

El script no arranca tareas ni crawlers. Guarda el informe en S3 y conserva
la copia local si no logra subirlo.
