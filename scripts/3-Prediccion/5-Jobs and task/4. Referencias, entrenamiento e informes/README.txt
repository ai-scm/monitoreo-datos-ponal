JOBS GLUE: REFERENCIA, ENTRENAMIENTO E INFORMES
==============================================
Un solo monitor de ejecuciones Glue, con tres grupos. Solo lectura.
Subir unicamente monitor_glue_jobs.py a CloudShell; no requiere otros .py.

Todos los jobs con lectura completa del historial disponible:
  python3 ~/monitor_glue_jobs.py --historial-completo
Consulta limitada (por defecto hasta 1000 runs por job):
  python3 ~/monitor_glue_jobs.py
Filtros:
  python3 ~/monitor_glue_jobs.py --grupo referencia --historial-completo
  python3 ~/monitor_glue_jobs.py --grupo entrenamiento --historial-completo
  python3 ~/monitor_glue_jobs.py --grupo informes --historial-completo
  python3 ~/monitor_glue_jobs.py --grupo referencia --grupo informes
  python3 ~/monitor_glue_jobs.py --job NOMBRE_COMPLETO --dias 15

Muestra ultimo estado, inicio/fin en Bogota, duracion, conteos de estados en
ventana, fallos, clase de error y cobertura por job. Un FAILED en ventana se
muestra aunque el ultimo run sea SUCCEEDED. No ejecuta jobs ni envia alertas.

Una tabla y un CSV por consulta:
  salida/glue_jobs/YYYY-MM-DD/HH-MM-SS_microsegundos/informe_glue_jobs.csv
Filtrar tipo_registro=RESUMEN para la vista principal en Excel.
DETALLE contiene runs de ventana, ultimo observado y metadata incompleta;
no es una exportacion de todo el historial. CONTROL explica los hallazgos;
CONTEXTO conserva configuracion, ventana y cobertura, sin mensajes crudos.

Columnas independientes:
  Control: INFO / AVISO / FALLA / AUSENTE / NO EVALUADO.
  Cobertura: COMPLETA / PARCIAL / ERROR / AUSENTE.
COMPLETA significa paginas agotadas y tiempos recibidos interpretables;
no certifica datos/modelos/informes correctos ni su frecuencia esperada.
INFO puede mostrar un ultimo FAILED fuera de ventana; ver en_ventana.

Exit: 0 lectura completa sin FALLA/AUSENTE, 1 FALLA/AUSENTE, 2 argumentos,
3 cobertura PARCIAL/ERROR o bloqueo, 130 interrupcion. Si coexisten FALLA y
PARCIAL, devuelve 3 y conserva FALLA en la columna Control.

Cuenta: 926162397524. Region: us-east-1. Ventana: 8 dias por defecto.
Permisos: sts:GetCallerIdentity y glue:GetJobRuns. Credenciales de la sesion;
no almacena claves. Reintentos y timeouts por llamada configurados.

Reemplaza monitor_entrenamiento.py, monitor_glue_job_informes.py y la parte
de jobs de monitor_procesos_upstream.py. No necesita ejecutar esos scripts.
Ver ALCANCE-Y-PRUEBA.txt para los criterios exactos y las pruebas locales.
