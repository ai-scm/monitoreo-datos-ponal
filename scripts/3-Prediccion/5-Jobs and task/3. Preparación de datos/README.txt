PROCESOS UPSTREAM - ECS LIMPIEZA, CRAWLER Y DYNAMODB
==================================================
Subir la version actual de monitor_procesos_upstream.py a CloudShell:
  python3 ~/monitor_procesos_upstream.py

Consulta cinco recursos iniciales:
  Un cluster ECS de limpieza: tareas aun visibles, salidas y errores ECS.
  Un crawler Glue: estado actual y ultima ejecucion disponible.
  Tres tablas DynamoDB historicas: estado y throttling observado en CloudWatch.

Los tres jobs de referencia se trasladaron a Glue-Jobs/monitor_glue_jobs.py,
que tambien cubre entrenamiento e informes. Para consultarlos:
  python3 ~/monitor_glue_jobs.py --grupo referencia --historial-completo
No hay dependencia entre ambos .py; cada uno puede ejecutarse por separado.
Las opciones --job y --max-ejecuciones pertenecen ahora al monitor Glue.

Upstream conserva los criterios de ECS/crawler/DynamoDB. Solo lectura, sin
alarmas, modificaciones, logs de aplicacion ni registros de negocio. Ventana
de metricas 8 dias por defecto (--dias). ECS no es un historial de ocho dias;
crawler solo muestra LastCrawl. SIN DATOS no significa cero ni datos correctos.

Opciones: --region, --cuenta, --dias 1..31, --cluster, --crawler, --tabla NOMBRE
(repetible), --max-tareas 1..1000 y --salida. Una tabla y un unico
informe_procesos_upstream.csv local; filtrar tipo_registro=RESUMEN en Excel.

Exit codes conservados: 0 lectura completa (incluso con fallos observados),
1 recurso ausente, 2 argumentos, 3 lectura incompleta/bloqueo, 130 interrupcion.
No automatizar alertas basandose solo en exit 0; revisar los controles del CSV.
Esta semantica es distinta a la del nuevo monitor_glue_jobs.py.
Ver ALCANCE-Y-PRUEBA.txt para recursos, criterios y permisos.
