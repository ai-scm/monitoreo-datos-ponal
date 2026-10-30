EJECUCIÓN DE LAS PREDICCIONES
============================

Script: mapear_inicio_progreso.py

PARA QUÉ SIRVE

Muestra qué ocurre con las tareas que ECS todavía conserva en el cluster
pon-prod-xcrime-predicciones-cluster, de la familia
pon-prod-xcrime-prediccion-definition.

QUÉ REVISA

- CAD o sigla de cada tarea, obtenido de su configuración.
- Estado, creación, inicio, parada y duración.
- Motivo de parada, códigos de salida y salud de contenedores esenciales.
- Indicios de errores, reintentos y avance en los logs disponibles.
- CPU y memoria por tarea, cuando existen métricas disponibles.

Incluye activas y detenidas visibles en ECS. No impone un máximo de duración
común para todas las siglas.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME y ejecuta:

  python3 "$HOME/mapear_inicio_progreso.py" \
    --region us-east-1 \
    --ventana-minutos 60 \
    --max-eventos 10000 \
    --salida "$HOME/salida"

Los 60 minutos son la ventana de logs y métricas de tareas activas, no un plazo
para terminar. Para tareas detenidas consulta sus logs de vida disponibles.

QUÉ ENTREGA Y DÓNDE

Genera un CSV: tareas.csv, con una fila por tarea.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/05-MonitorInicioProgreso/FECHA/HORA/

Local:
  $HOME/salida/inicio_progreso/FECHA/HORA/tareas.csv

FECHA/HORA es la ejecución en Bogotá. También conserva contexto.json local.
--no-export desactiva los archivos locales y la publicación del CSV.

CÓMO LEER EL RESULTADO

Empieza por CAD, estado ECS, tiempos, control y motivo.
RUNNING indica ejecución; STOPPED indica parada, cuyo motivo y salida se revisan.
FALLA señala un fallo observado de arranque, salud o salida esencial.
AVISO puede indicar interrupción, datos faltantes o indicios en logs.

RUNNING u OK no prueban avance ni una predicción correcta. Una salida cero
confirma cierre sin error ECS en lo observado; los resultados se validan en S3.
SIN DATOS de CPU o memoria no equivale a consumo cero.

Para tareas que ECS ya no conserva, el monitor de modelos puede recuperar
los logs conservados, pero no inventa una hora de fin ni un código de salida.
Conservar capturas durante el ciclo permite disponer después de esos datos.
El script no arranca ni detiene tareas y no envía notificaciones.
