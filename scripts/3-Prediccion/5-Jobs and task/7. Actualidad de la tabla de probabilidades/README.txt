DIAGNÓSTICO DE LA TABLA DE PROBABILIDADES
=======================================

Script: monitor_freshness_probabilidaddelito.py

ESTADO DE USO - 30 DE OCTUBRE DE 2026

Fuera de la ejecución habitual del monitoreo. Se conserva como herramienta
opcional para revisar si existe la tabla y qué archivos hay en su fuente.

Se confirmó en Athena, CloudTrail y ECS que la tarea de predicción de DECAU
consulta esta tabla. Contiene proporciones por cuadrante y categoría.
Se acuerda tratarla como referencia fija, sin exigir actualización semanal.
La antigüedad de los archivos no es una alarma ni un bloqueo del alcance.
Su política de recálculo sigue sin establecerse.

DÓNDE ESTÁ LA TABLA

Región: us-east-1.
Catálogo: AWS Glue Data Catalog, consultable desde Athena.
Base de datos: pon-prod-mdp.
Tabla: probabilidaddelito.
Fuente observada: s3://pon-prod-ml-iapol/probabilidadDelito/.

En consola: Glue > Data Catalog > Databases > pon-prod-mdp > Tables.
El script obtiene la ubicación desde Glue al ejecutarse.

QUÉ REVISA EL DIAGNÓSTICO

- Existencia, esquema y ubicación de la tabla en Glue.
- Listado de los archivos de su fuente S3.
- Cantidad, tamaño, archivos vacíos y fechas de modificación.

Este comando no lee las filas ni ejecuta una consulta Athena.

CÓMO EJECUTARLO SI NECESITAS EL DIAGNÓSTICO

Carga el .py en $HOME y ejecuta:

  python3 "$HOME/monitor_freshness_probabilidaddelito.py" \
    --region us-east-1 \
    --revisar-fuente-s3 \
    --listado-completo \
    --salida "$HOME/salida"

QUÉ ENTREGA Y DÓNDE

Genera un CSV: informe_freshness_probabilidaddelito.csv.
Filtra tipo_registro=RESUMEN para existencia y observaciones principales.
CONTROL explica qué se pudo evaluar; OBJETO_S3 describe los archivos;
CONTEXTO conserva la configuración y el alcance de la revisión.

S3 del informe:
  s3://pon-prod-ml-iapol/ValidationReports/07-MonitorFreshnessProbabilidaddelito/FECHA/HORA/

Local:
  $HOME/salida/freshness_probabilidaddelito/FECHA/HORA/informe_freshness_probabilidaddelito.csv

FECHA/HORA es la ejecución en Bogotá. La fuente y la carpeta de informes son
ubicaciones distintas. Si falla la publicación, conserva el CSV local.

CÓMO LEER EL RESULTADO

INFO indica evidencia disponible. AUSENTE significa que no se encontró la tabla.
INCOMPLETO señala que no terminó una lectura. NO EVALUADO en los controles
temporales significa que no se está aplicando una obligación de actualización
a esta referencia.

LastModified indica cuándo cambió el objeto; no confirma el período de sus filas.
No se aplica una obligación semanal sin confirmarla. La tabla observada no tiene
una columna temporal ni particiones de fecha para comprobar ese período.

El archivo admite una consulta por período cuando se confirmen columna de fecha,
su significado y fechas esperadas. Esa opción no se activa en el comando anterior.
No reemplaza al validador de predicciones y no envía alertas automáticas.
