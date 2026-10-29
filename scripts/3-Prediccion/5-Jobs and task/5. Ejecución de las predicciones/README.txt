EJECUCIÓN DE LAS PREDICCIONES
============================

Archivo: mapear_inicio_progreso.py

PARA QUÉ SIRVE

Muestra qué está pasando con las tareas de predicción que todavía son visibles
en ECS: su estado, sus tiempos y los errores que se puedan observar.

Consulta el cluster pon-prod-xcrime-predicciones-cluster y la familia
pon-prod-xcrime-prediccion-definition.

QUÉ REVISA

- Creación, inicio, fin y duración de cada tarea.
- Sigla identificada en la configuración; CAD es la variable utilizada.
- Estado de las tareas y códigos de salida de sus contenedores.
- Errores de arranque, falta de memoria y controles de salud.
- Indicios de errores o reintentos en los registros de ejecución.
- CPU, memoria y avance, cuando hay información disponible.

Incluye tareas activas y detenidas que ECS aún conserva. No aplica un tiempo
máximo común a todas las siglas.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/mapear_inicio_progreso.py" \
    --ventana-minutos 60 \
    --max-eventos 10000 \
    --salida "$HOME/salida"

La ventana de 60 minutos corresponde a registros y métricas recientes de
tareas activas; no es un plazo máximo para terminar una predicción.
Para tareas detenidas consulta sus registros de vida mientras estén disponibles.

QUÉ INFORME ENTREGA

Genera tareas.csv, con una fila por tarea.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/05-MonitorInicioProgreso/FECHA/HORA/

Copia local:
  $HOME/salida/inicio_progreso/FECHA/HORA/tareas.csv

También conserva contexto.json local para explicar la revisión.

CÓMO INTERPRETARLO

Empieza por sigla, estado ECS, tiempos, estado del control y motivo.

  RUNNING: ECS muestra la tarea en ejecución.
  STOPPED: ECS muestra la tarea detenida; hay que revisar el motivo y la salida.
  OK: no se detectó un error ECS en los controles aplicados.
  AVISO: hay una señal para revisar o información que falta.
  FALLA: se observó un fallo de arranque, salud o salida esencial.

STOPPED puede corresponder a un cierre normal, un error o una interrupción.
El código de salida y el motivo ayudan a distinguirlos.

LÍMITES DEL RESULTADO

- RUNNING u OK no demuestran avance real ni una predicción correcta.
- Los tiempos no permiten declarar demora sin un criterio apropiado para la sigla.
- SIN DATOS de CPU o memoria no significa consumo cero.
- Los indicios de error en logs no confirman por sí solos el fallo de toda la tarea.
- "End Script!" indica un cierre reportado de subunidad, no el éxito global.
- Para recuperar evidencia de tareas que ECS ya no conserva, utiliza el monitor
  de modelos de la carpeta 6.

El script no arranca ni detiene tareas. Guarda su CSV en S3 y localmente.
--no-export desactiva ambas copias.
