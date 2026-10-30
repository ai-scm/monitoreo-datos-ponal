MODELOS UTILIZADOS Y COMPONENTES
================================

Archivo: mapear_modelos_dependencias.py

PARA QUÉ SIRVE

Ayuda a ver qué modelos reporta cada sigla y cuánto se está utilizando Uniform.
También reúne avisos de pocos datos y errores de la aplicación.

Lee los registros de ejecución conservados en CloudWatch. Por eso puede
recuperar evidencia de tareas que ya no aparecen en ECS, además de las activas.

QUÉ REVISA CON EL COMANDO RECOMENDADO

- Selecciones reportadas por modelo y sigla.
- Total de selecciones observadas y porcentaje de Uniform.
- Cantidad de tareas cuyas selecciones observadas son todas Uniform.
- Avisos de datos insuficientes y clases de error reportadas.
- Creaciones de tareas confirmadas por CAD y modalidad HORA/TURNO, comparadas
  con el catálogo que estaba vigente al inicio del ciclo cuando se puede recuperar.
- Combinaciones con tareas creadas que todavía no tienen selecciones observadas.

Recupera la versión inicial del catálogo y cruza el historial de lanzamiento
con respuestas completas de la función Lambda. La relación se confirma por el
identificador de la misma tarea en ambas fuentes; no se deduce por el orden de
los mensajes. No requiere subir un Excel.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/mapear_modelos_dependencias.py" \
    --verificar-ciclo \
    --salida "$HOME/salida"

Para consultar solamente las siglas observadas en los últimos siete días,
sin contrastarlas con el catálogo de lanzamiento:

  python3 "$HOME/mapear_modelos_dependencias.py" \
    --dias-logs 7 \
    --salida "$HOME/salida"

Sin opciones, el resumen revisa los últimos 30 días, sujeto a la conservación
de los registros.

QUÉ INFORME ENTREGA

Genera un único historial_tareas.csv. En Excel, filtra tipo_registro:

  RESUMEN_SIGLA: una fila por sigla con tareas, modelos, Uniform y avisos.
  DETALLE_TAREA: evidencia por tarea y stream, que es su canal de registros.
  COBERTURA_CICLO: comparación CAD/modalidad; aparece con --verificar-ciclo.
  LANZAMIENTO_CICLO: evidencia de creación de cada tarea confirmada.
  CONTROL_CICLO: problemas de lectura o errores del lanzamiento, si se encuentran.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/06-MonitorModelosDependencias/FECHA/HORA/

Copia local:
  $HOME/salida/modelos_dependencias/FECHA/HORA/historial_tareas.csv

CÓMO LEER UNIFORM Y LOS AVISOS

El porcentaje se calcula sobre selecciones observadas en los registros.
Por ejemplo, 65 selecciones Uniform de 100 selecciones totales representan 65%.
Los nombres completos de los modelos se mantienen separados.

"Tareas 100% Uniform" cuenta tareas distintas con al menos una selección y
únicamente Uniform en lo observado. Una tarea sin selecciones no entra en ese
conteo. Una sigla puede tener una tarea solo Uniform aunque su porcentaje global
sea menor a 100%.

Los motivos del aviso distinguen uso exclusivo de Uniform, datos insuficientes,
errores de aplicación, lectura incompleta y falta de información histórica de ECS.
Los errores agrupados, como NameError, cuentan eventos; no predicciones fallidas únicas.
Un aviso de pocos datos para un modelo tampoco mide toda la calidad de los datos.

CÓMO LEER LA COBERTURA DEL CICLO

Consulta por separado las tareas creadas confirmadas, las que tienen selecciones
de modelo y los arranques que ECS permite verificar. Una tarea puede estar creada
y tener cero selecciones observadas: genera una señal para revisar, no una falla
confirmada de lanzamiento. Varias tareas para el mismo CAD/modalidad también se avisan.

Las columnas tareas_iniciales_con_selecciones y tareas_iniciales_sin_selecciones
siguen los identificadores del lanzamiento inicial. Así, otra tarea del mismo CAD
ejecutada después no oculta la falta de selecciones de una tarea inicial.
La columna tareas_con_selecciones incluye toda la actividad observada del período.

Se comprueban creaciones de la primera hora del ciclo; esa ventana de búsqueda no
es un tiempo máximo para terminar la predicción. El análisis de modelos cubre el
período completo desde el disparo hasta la captura.

Para conocer la lista inicial se leen las versiones conservadas del catálogo S3.
Una nueva versión con el mismo contenido no se considera un cambio de CAD.
Si no se puede recuperar la referencia inicial, queda explícita la limitación.
La configuración de la lanzadora puede haber cambiado después: la cobertura
de tareas creadas no reconstruye toda la historia de configuración.

Esta comprobación necesita acceso al historial CloudTrail, los registros de la
Lambda y las versiones del catálogo. Si faltan permisos o evidencia, se informa
INCOMPLETO. Si cambia la lógica de selección y no puede comprobarse, queda NO EVALUADO.

LÍMITES DEL RESULTADO

- Las selecciones son declaraciones en logs, no solicitudes ni predicciones únicas.
- Uniform no demuestra por sí solo un modelo de respaldo ni un fallo final.
- Con una lectura parcial, 100% Uniform describe solo lo que se logró observar.
- Sin información histórica de ECS no se confirma inicio, fin ni salida de la tarea.
- Actividad de una tarea antigua no prueba que haya ocurrido un nuevo lanzamiento.
- Los registros conservados no equivalen a un historial ilimitado.
- No valida los archivos finales de predicción.

SI LA CONSULTA SE INTERRUMPE

Conserva $HOME/salida y repite el mismo comando. El monitor guarda información
para recuperar consultas pendientes. Si falla la subida a S3, también conserva
la consulta para reintentar sin repetir el escaneo de logs.

OTRAS REVISIONES DISPONIBLES

El mismo archivo tiene opciones para revisar imagen, versiones de dependencias
y archivos auxiliares. Se ejecutan por separado, pueden tardar y generar más CSV.
Consulta --help para verlas; no son necesarias para el resumen habitual.
--desde-reportes conserva su análisis local sin consultas ni subidas a AWS.
