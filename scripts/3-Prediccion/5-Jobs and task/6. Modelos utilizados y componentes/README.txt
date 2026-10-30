MODELOS UTILIZADOS Y COMPONENTES
================================

Script: mapear_modelos_dependencias.py

PARA QUÉ SIRVE

Muestra qué modelos reporta cada CAD, cuánto utiliza Uniform y qué avisos de
pocos datos o errores aparecen. Lee los logs conservados en CloudWatch, por lo
que incluye evidencia de tareas activas y de otras que ya desaparecieron de ECS.

QUÉ REVISA EN EL USO HABITUAL

- Selecciones reportadas por modelo y CAD, y total de selecciones observadas.
- Porcentaje de Uniform y tareas con selecciones observadas solo de Uniform.
- Avisos de datos insuficientes y clases de error reportadas.
- Con --verificar-ciclo: tareas creadas por CAD/modalidad HORA o TURNO, frente al
  catálogo de inicio del ciclo que pueda recuperarse.
- Tareas iniciales creadas que aún no reportan selecciones y combinaciones repetidas.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME. Para revisar el ciclo semanal:

  python3 "$HOME/mapear_modelos_dependencias.py" \
    --region us-east-1 \
    --verificar-ciclo \
    --salida "$HOME/salida"

Este modo cruza catálogo S3, historial CloudTrail y respuestas de la Lambda por
el identificador de la misma tarea. No requiere subir un Excel. Si falta acceso
o evidencia, indica la limitación.

Para consultar solo los últimos siete días de logs, sin comparar el catálogo:

  python3 "$HOME/mapear_modelos_dependencias.py" \
    --region us-east-1 \
    --dias-logs 7 \
    --salida "$HOME/salida"

Sin opciones revisa los últimos 30 días, sujeto a retención. No se limita a
las tareas RUNNING. Los CAD nunca observados no se identifican en el modo de logs.

QUÉ ENTREGA Y DÓNDE

Genera un CSV en estos modos: historial_tareas.csv.
Filtra tipo_registro:

  RESUMEN_SIGLA: vista principal por CAD, modelos, total y porcentaje de Uniform.
  DETALLE_TAREA: evidencia por tarea y stream, su canal de logs.
  COBERTURA_CICLO: comparación CAD/modalidad con --verificar-ciclo.
  LANZAMIENTO_CICLO: evidencia de creación de las tareas confirmadas.
  CONTROL_CICLO: incidencias encontradas al comprobar el ciclo, si las hay.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/06-MonitorModelosDependencias/FECHA/HORA/

Local:
  $HOME/salida/modelos_dependencias/FECHA/HORA/historial_tareas.csv

FECHA/HORA es la ejecución en Bogotá. Conserva $HOME/salida para recuperar
consultas pendientes y reintentar publicaciones sin repetir el escaneo.
Si no termina la consulta, repite el mismo comando con la misma carpeta de salida.

CÓMO LEER EL RESULTADO

65 selecciones Uniform de 100 selecciones observadas significan 65%.
Estos conteos son declaraciones en logs, no solicitudes ni predicciones únicas.
"Tareas 100% Uniform" exige al menos una selección y que todas las observadas
sean Uniform. Una tarea sin selecciones no entra en ese conteo.

Un aviso distingue uso exclusivo de Uniform, pocos datos, errores o lectura
incompleta. Uniform no confirma por sí solo respaldo ni fallo de la predicción.
El porcentaje describe la evidencia leída: revisa también su cobertura.

La creación, el inicio y la finalización son evidencias distintas. Si ECS ya
no conserva una tarea, sus logs no confirman por sí solos su salida final.
"End Script!" puede corresponder a una subunidad, no al cierre de toda la tarea.

OTROS MODOS DEL MISMO ARCHIVO

Tiene diagnósticos de imagen, versiones de dependencias, auxiliares y selector
de unidades. Consulta --help; pueden generar más CSV y no forman parte del
comando habitual. --desde-reportes analiza archivos locales sin consultas AWS.
No ejecuta estos diagnósticos de imagen al pedir el resumen por CAD.

El script no valida archivos finales de predicción ni envía notificaciones.
