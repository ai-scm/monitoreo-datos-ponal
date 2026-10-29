DATOS ANTES DE PREDECIR
======================

Archivo: monitor_datos_entrada.py

PARA QUÉ SIRVE

Ayuda a detectar cambios o problemas en los datos antes de generar predicciones.
Consulta la tabla pon-prod-mdp.mdp_limpio y presenta los resultados por sigla,
unidad y fuente de datos (RNMC, SECAD y SIEDCO).

QUÉ REVISA

- Cuántos registros hay en los últimos 15 días completos y en los 15 anteriores.
- Si el volumen bajó respecto al período anterior.
- La última fecha encontrada dentro del período y los días sin registros.
- Coordenadas vacías o fuera de los rangos válidos de latitud y longitud.

El día de ejecución queda fuera de las dos ventanas, para comparar días completos.

CÓMO EJECUTARLO EN CLOUDSHELL

  unset ATHENA_OUTPUT_S3

  python3 "$HOME/monitor_datos_entrada.py" \
    --region us-east-1 \
    --database pon-prod-mdp \
    --fecha fecha_hora \
    --unidad unidad_base \
    --sigla sigla \
    --fuente bd \
    --latitud latitud \
    --longitud longitud \
    --comparar-periodos --ventana-dias 15 \
    --max-grupos 10000 \
    --ejecutar --timeout 600 \
    --salida "$HOME/salida"

Este comando revisa las unidades observadas de todas las siglas.
Para limitar una prueba a una sigla, añade --filtrar-sigla MEPAS.
Al ejecutar el script sin opciones solo se consulta el esquema; no se evalúan datos.

QUÉ INFORMES ENTREGA

Genera dos CSV:

  general.csv
    Resumen de volumen, recencia y calidad, junto con todos los controles.
    En Excel, filtra tipo_registro:
      RESUMEN: indicadores por sigla, unidad y BD.
      CONTROL: estados y motivos, incluidos los controles generales de alcance.

  diario.csv
    Conteo por día, sigla, unidad y BD. Sirve para ubicar los días sin datos
    o ver cuándo empezó una caída de volumen.

Destino S3:

FECHA corresponde al día de ejecución en hora de Bogotá, aunque se revisen datos de un período anterior.
  s3://pon-prod-ml-iapol/ValidationReports/01-MonitorDatosEntrada/FECHA/HORA/

Copias locales:
  $HOME/salida/general_datos_entrada_FECHA_HORA.csv
  $HOME/salida/diario_datos_entrada_FECHA_HORA.csv

CÓMO INTERPRETARLO

Empieza por los controles FALLA o AVISO de general.csv. Usa diario.csv para
revisar la distribución de registros. "Base insuficiente" significa que hay
pocos registros anteriores para comparar el volumen con confianza.

Un cero o una caída no demuestran por sí solos un problema de carga: una unidad
puede tener pocos incidentes o recibir registros con retraso.

LÍMITES DEL RESULTADO

- La fecha de un hecho no demuestra cuándo se cargó al sistema.
- En este modo, el filtro temporal excluye fechas nulas, no interpretables y
  futuras. Su calidad global queda indicada como NO EVALUADA.
- Sin una lista de unidades esperadas, no identifica todas las unidades
  completamente ausentes de la tabla.
- Revisar rangos de coordenadas no confirma que un punto pertenezca a su unidad.
- Los límites de aviso son orientativos y se pueden configurar.

Los datos de origen no se modifican. Se guardan los informes y los archivos
internos de consulta de Athena. Si la subida falla, las copias locales quedan
disponibles. --no-export desactiva la exportación de informes.
