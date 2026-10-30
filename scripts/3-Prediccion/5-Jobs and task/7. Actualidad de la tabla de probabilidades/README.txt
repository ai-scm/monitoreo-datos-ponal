ACTUALIDAD DE LA TABLA DE PROBABILIDADES
=======================================

Archivo: monitor_freshness_probabilidaddelito.py

PARA QUÉ SIRVE

Revisa si existe la tabla pon-prod-mdp.probabilidaddelito y qué información
hay sobre los archivos de su fuente.

Su uso dentro de la predicción actual y su frecuencia de actualización siguen
sin confirmar. Por eso la actualidad del contenido permanece NO EVALUADA.
Este estado no debe interpretarse como datos vigentes ni como una falla de carga.

QUÉ REVISA CON EL COMANDO RECOMENDADO

- Existencia y ubicación de la tabla en el catálogo Glue.
- Archivos disponibles en la ubicación S3 de la tabla.
- Cantidad, tamaño y fecha de modificación de los archivos observados.
- Archivos vacíos y candidatos con contenido.

No lee las filas de esos archivos ni ejecuta una consulta Athena con este comando.

CÓMO EJECUTARLO EN CLOUDSHELL

  python3 "$HOME/monitor_freshness_probabilidaddelito.py" \
    --revisar-fuente-s3 \
    --listado-completo \
    --salida "$HOME/salida"

QUÉ INFORME ENTREGA

Genera informe_freshness_probabilidaddelito.csv.

En Excel, filtra tipo_registro:
  RESUMEN: existencia, ubicación y observaciones principales.
  CONTROL: qué pudo evaluarse y qué queda pendiente.
  OBJETO_S3: información de los archivos observados.
  CONTEXTO: configuración y alcance de la revisión.

Destino S3 del informe:
  s3://pon-prod-ml-iapol/ValidationReports/07-MonitorFreshnessProbabilidaddelito/FECHA/HORA/

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.

Copia local:
  $HOME/salida/freshness_probabilidaddelito/FECHA/HORA/informe_freshness_probabilidaddelito.csv

La fuente que se inspecciona es distinta de la carpeta del informe:
  s3://pon-prod-ml-iapol/probabilidadDelito/

CÓMO INTERPRETARLO

INFO indica evidencia disponible, como la existencia de la tabla o sus archivos.
INCOMPLETO señala que no se pudo terminar una lectura.
AUSENTE indica que no se encontró la tabla configurada.

La última modificación de un archivo solo dice cuándo cambió ese objeto.
No demuestra a qué período pertenecen sus datos ni si están completos.
Una fecha de modificación antigua necesita contexto de uso y frecuencia antes
de decidir si representa un problema.

LÍMITES DEL RESULTADO

- No se aplica una obligación de actualización semanal sin confirmarla.
- La tabla revisada no tiene una columna temporal ni particiones de fecha
  confirmadas para evaluar el período del contenido.
- La presencia de archivos no demuestra cobertura de todas las siglas o unidades.
- Este monitor no reemplaza al validador de resultados de predicción en S3.

El script dispone de una revisión por período cuando se confirmen una columna
de fecha, su significado y las fechas esperadas. Esa definición todavía falta;
el comando recomendado mantiene explícitamente el control NO EVALUADO.

El informe se guarda en S3 y localmente, incluso si documenta controles pendientes.
Si la subida falla, la copia local queda disponible.
