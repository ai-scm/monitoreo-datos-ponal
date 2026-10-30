REFERENCIAS, ENTRENAMIENTO E INFORMES
=====================================

Archivo: monitor_glue_jobs.py

PARA QUÉ SIRVE

Revisa si los procesos Glue configurados arrancaron, siguen ejecutándose,
terminaron o fallaron. Reúne en un solo informe los procesos de actualización,
entrenamiento e informes.

QUÉ REVISA

Los siete jobs están organizados en tres grupos:

  Referencia
    Actualización de fuentes internas, estructura nacional y geometrías.

  Entrenamiento
    Entrenamiento, homologación y consumo de modelos.

  Informes
    Generación de informes de anticipaciones.

Para cada job muestra el último estado, inicio, fin, duración y errores.
También cuenta los estados observados dentro del período de revisión.
Un fallo del período sigue visible aunque una ejecución posterior haya sido exitosa.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/monitor_glue_jobs.py" \
    --dias 8 \
    --historial-completo \
    --salida "$HOME/salida"

El comando recorre el historial disponible y evalúa una ventana de ocho días.
Puede tardar si hay muchas ejecuciones.

Para revisar solo un grupo, añade una de estas opciones al mismo comando:
  --grupo referencia
  --grupo entrenamiento
  --grupo informes

QUÉ INFORME ENTREGA

Genera informe_glue_jobs.csv.

En Excel, filtra tipo_registro=RESUMEN para la vista principal.
DETALLE contiene las ejecuciones seleccionadas; CONTROL explica los hallazgos.
CONTEXTO registra el período y la cobertura de lectura.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/04-MonitorGlueJobs/FECHA/HORA/

Copia local:
  $HOME/salida/glue_jobs/FECHA/HORA/informe_glue_jobs.csv

CÓMO INTERPRETARLO

Revisa dos columnas:

  Control: si se encontraron fallos u otras situaciones que requieren revisión.
  Cobertura: si se pudo leer toda la información o quedó una parte pendiente.

FALLA se refiere a ejecuciones del período evaluado. El último estado mostrado
puede ser anterior a ese período; consulta también los conteos de la ventana.
Una lectura parcial conserva los fallos que sí fueron observados.

LÍMITES DEL RESULTADO

- SUCCEEDED significa que Glue reportó éxito; no prueba calidad, actualidad,
  entrega ni uso de los datos o modelos producidos.
- Sin una frecuencia esperada confirmada, no haber ejecuciones recientes no
  demuestra que se haya perdido un disparo.
- La duración se muestra como información, sin un límite máximo estándar.
- No confirma el orden ni las dependencias reales entre jobs y predicciones.
- El CSV contiene el detalle seleccionado, no una copia de todo el historial.

Este script ya reúne referencias, entrenamiento e informes; se ejecuta de
forma independiente. No lanza ni corrige jobs. Guarda el CSV en S3 y conserva
su copia local si falla la subida.
