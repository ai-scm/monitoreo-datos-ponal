MONITORES DE PREDICCION - ORGANIZACION ACTUAL
============================================
Reorganizacion: 28 de octubre de 2026. Consultas manuales de solo lectura.

Se concentro GetJobRuns en Glue-Jobs/monitor_glue_jobs.py. Sustituye:
  - Entrenamiento/monitor_entrenamiento.py (tres jobs).
  - Informes/monitor_glue_job_informes.py (un job).
  - Los tres jobs de referencia que consultaba upstream.
Procesos-Upstream mantiene ECS de limpieza, crawler y DynamoDB. Cada monitor
sigue siendo independiente en CloudShell; no importa los otros .py.

SIETE SCRIPTS OPERATIVOS (la cantidad de controles no equivale a archivos)
Carpeta                 Archivo                                     Funcion actual
-------------------- ------------------------------------------- ------------------------------
Datos-Entrada          monitor_datos_entrada.py                     Volumen/recencia/calidad de entradas
Disparo-Semanal        monitor_disparo_semanal.py                   EventBridge, Lambda y evidencia de lanzamiento
Procesos-Upstream      monitor_procesos_upstream.py                 ECS limpieza, crawler, throttling DynamoDB
Glue-Jobs              monitor_glue_jobs.py                         Runs de referencia, entrenamiento e informes
Inicio-Progreso        mapear_inicio_progreso.py                    Estado ECS, tiempos, logs y metricas disponibles
Modelos-Dependencias   mapear_modelos_dependencias.py               Modelos reportados y dependencias del predictor
Freshness              monitor_freshness_probabilidaddelito.py      Esquema/fuente y evaluacion disponible de actualidad

El validador S3 existente vive fuera de este directorio y no se ha movido ni
integrado. Los otros monitores tampoco se han convertido a Lambda. Cada
subcarpeta documenta sus limites actuales.

USO TRAS ACTUALIZAR LOS ARCHIVOS EN CLOUDSHELL
Subir Glue-Jobs/monitor_glue_jobs.py y la version actual de
Procesos-Upstream/monitor_procesos_upstream.py. No subir tests ni docs.

Todos los jobs, agotando el historial disponible:
  python3 ~/monitor_glue_jobs.py --historial-completo
Solo un grupo:
  python3 ~/monitor_glue_jobs.py --grupo referencia --historial-completo
  python3 ~/monitor_glue_jobs.py --grupo entrenamiento --historial-completo
  python3 ~/monitor_glue_jobs.py --grupo informes --historial-completo
ECS limpieza, crawler y DynamoDB:
  python3 ~/monitor_procesos_upstream.py

Las dos CLI anteriores se retiraron de este directorio para evitar consultas
duplicadas. Sus versiones previas estan en Git. Su documentacion se preservo
en Glue-Jobs/docs/historico/: sus comandos NO son la guia actual.

CAMBIOS EXPLICITOS DEL MONITOR GLUE
- Una tabla y un unico informe_glue_jobs.csv para los grupos seleccionados.
- Control y cobertura de lectura son columnas separadas: una lectura parcial
  no oculta una FALLA conocida. Errores entre paginas conservan datos previos.
- Ultimo estado puede ser antiguo. FALLA del resumen se refiere a runs de la
  ventana (inicios/fines dentro del periodo o ejecuciones aun abiertas).
  Un FAILED antiguo queda visible, sin contarlo como falla de la ventana.
- Exit 1 si hay FALLA/AUSENTE; 3 si la lectura es parcial/error. Los monitores
  anteriores podian devolver 0 aun con fallas. Revisar esto al automatizar.
- Las opciones upstream --job/--max-ejecuciones pasan al monitor Glue.

PRUEBAS LOCALES (desde la raiz del repositorio)
  python3 -B -m unittest discover -s 'scripts/3-Prediccion/5-Jobs and task/tests' -v
Clientes simulados: sin boto3, credenciales ni AWS. Verifican la unificacion
y que upstream no vuelva a consultar GetJobRuns.

PENDIENTE PARA DESPLIEGUE
Esta reorganizacion no implementa handlers, estado persistente, presupuesto
de tiempo Lambda, calendario esperado ni notificaciones. La auditoria bajo
docs/auditorias/ es una fotografia anterior; sus rutas y duplicaciones deben
leerse con esa fecha de corte.

_______________________________________________________________
_______________________________________________________________
_______________________________________________________________

1. monitor_datos_entrada.py — Datos antes de predecir
Consulta pon-prod-mdp.mdp_limpio. Compara los últimos 15 días completos contra los 15 anteriores, por sigla, unidad y BD. Revisa volumen, recencia, días sin registros y coordenadas. En este modo, las fechas inválidas fuera del filtro temporal quedan sin evaluar.

Este comando analiza las unidades observadas, sin limitarse a MEPAS, y guarda los reportes en la ruta S3 acordada:

export ATHENA_OUTPUT_S3='s3://pon-prod-ml-iapol/ValidationReports/MonitorDatosEntrada/'
python3 ~/monitor_datos_entrada.py \
  --database pon-prod-mdp \
  --fecha fecha_hora \
  --unidad unidad_base \
  --sigla sigla \
  --fuente bd \
  --latitud latitud \
  --longitud longitud \
  --comparar-periodos --ventana-dias 15 \
  --max-grupos 10000 \
  --ejecutar --timeout 600

2. monitor_disparo_semanal.py — Programación y lanzamiento
Revisa las reglas EventBridge de predicciones e informes, sus destinos Lambda, errores, throttling, duración y configuración de manejo de fallos. Consulta evidencia de lanzamiento en logs durante los últimos 8 días. No certifica por sí solo que todas las tareas esperadas hayan arrancado.

python3 ~/monitor_disparo_semanal.py --max-eventos 50000

3. monitor_procesos_upstream.py — Preparación de datos
Revisa ECS de limpieza, la última ejecución del crawler Glue y el throttling observado en las tres tablas DynamoDB históricas. Los jobs de referencia ahora pertenecen al monitor Glue unificado. ECS muestra tareas aún visibles; el crawler muestra únicamente su última ejecución.

python3 ~/monitor_procesos_upstream.py

4. monitor_glue_jobs.py — Referencias, entrenamiento e informes
Consulta los 7 jobs Glue de actualización de referencias, entrenamiento, homologación, consumo y generación de informes. Muestra estados, inicio, fin, duración, fallos y clases de error. Separa el resultado del job de la cobertura de lectura y genera un único CSV.
Este comando agota el historial disponible y evalúa una ventana de 8 días:

python3 ~/monitor_glue_jobs.py --historial-completo

5. mapear_inicio_progreso.py — Ejecución de las predicciones
Revisa tareas del cluster de predicciones: sigla identificada, estado ECS, tiempos de arranque y ejecución, códigos de salida, errores y telemetría disponible. Incluye tareas detenidas que ECS todavía conserve. No impone una duración máxima estándar ni interpreta RUNNING como avance confirmado.

python3 ~/mapear_inicio_progreso.py \
  --ventana-minutos 60 \
  --max-eventos 10000

6. mapear_modelos_dependencias.py — Modelos utilizados y componentes
Recoge selecciones de modelos reportadas en logs por tarea y sigla, conteos de Uniform y avisos de datos insuficientes. También identifica imagen y configuración; dispone de modos adicionales para inspeccionar dependencias y auxiliares. Los conteos representan declaraciones observadas, no predicciones únicas ni calidad certificada.

Este comando consulta modelos de tareas activas y detenidas aún visibles, con logs de las últimas 24 horas:

python3 ~/mapear_modelos_dependencias.py \
  --incluir-finalizadas \
  --correlacionar-modelos \
  --ventana-minutos 1440 \
  --max-eventos 10000

7. monitor_freshness_probabilidaddelito.py — Actualidad de la tabla de probabilidades
Consulta el esquema de pon-prod-mdp.probabilidaddelito y la metadata de su fuente S3: objetos, tamaños y última modificación. La actualidad del contenido sigue sin evaluarse, porque no hay una columna temporal ni una frecuencia esperada confirmadas.

python3 ~/monitor_freshness_probabilidaddelito.py \
  --revisar-fuente-s3 \
  --listado-completo


ACTUALIZACION - HISTORIAL POR STREAMS DE PREDICCION
Los siete monitores se conservan. La recuperacion historica se implementa en:
  6. Modelos utilizados y componentes/mapear_modelos_dependencias.py
  python3 ~/mapear_modelos_dependencias.py --historial-logs --dias-logs 8 --max-eventos 100000
Guia: 6. Modelos utilizados y componentes/HISTORIAL-LOGS.txt.
Busca streams incluso sin tareas visibles en ECS, agrega modelos/Uniform,
pocos datos, errores y cierres de subunidad, y recupera metadata ECS disponible.
Sigla y HORA/TURNO requieren evidencia explicita; no estan en el nombre del stream.
End Script no equivale a exito global. Se exporta historial_tareas.csv y contexto.
El monitor 5 de ejecucion y el modo normal del monitor 6 tambien leen la vida de
las tareas STOPPED visibles, conservando la ventana reciente para las activas.
Los otros cinco monitores conservan su alcance; este patron es de los logs ECS
Del predictor, no de los logs de Lambda, Glue o los datos S3.

RESUMEN POR SIGLAS OBSERVADAS - COMPORTAMIENTO ACTUAL DEL MONITOR 6
python3 ~/mapear_modelos_dependencias.py muestra por defecto una fila por sigla
identificada desde los streams conservados y la metadata ECS de sus tareas.
Incluye activas, detenidas y recuperadas de logs, sin exigir ninguna RUNNING.
No consulta estructura nacional ni catalogos S3. Las siglas nunca observadas
no se pueden identificar ni declarar ausentes. Ventana default: 30 dias,
sujeta a retencion; --dias-logs 8 sirve para acotar una revision semanal.
El CSV contiene RESUMEN_SIGLA y DETALLE_TAREA. --por-tarea conserva el mapeo
anterior de ECS/ECR. Ver HISTORIAL-LOGS.txt.

MAPA EXPLICITO DE SIGLAS DE LA ESTRUCTURA Y STREAMS
python3 ~/mapear_modelos_dependencias.py --mapear-streams
Muestra Sigla/CAD | Stream para todas las SIGLA_PAPA de la estructura S3,
sin filtro DESAB/RUNNING. Varias filas por CAD si tiene varios streams.
SIN STREAM ASOCIADO no implica ejecucion faltante. Guarda mapa_siglas_streams.csv.
El resumen por defecto mantiene la fuente de logs; este modo explicito consulta
la estructura para contrastar todas sus siglas, como se solicito para el mapeo.
El mapa es una consulta rapida: CAD ECS/cache o dos muestras pequenas por stream,
sin analizar modelos. Guarda CSV/cache y tiene presupuesto tecnico de 180 s.
Puede repetirse para continuar y ejecutarse con nohup; ver HISTORIAL-LOGS.txt.
