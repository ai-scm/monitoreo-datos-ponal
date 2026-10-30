DATOS ANTES DE PREDECIR
======================

Script: monitor_datos_entrada.py

PARA QUÉ SIRVE

Detecta cambios en la información disponible antes de generar predicciones.
Consulta pon-prod-mdp.mdp_limpio por CAD, unidad y fuente: RNMC, SECAD y SIEDCO.

QUÉ REVISA

- Registros de los últimos 15 días completos frente a los 15 anteriores.
- Caídas de volumen, última fecha del período y días sin registros.
- Coordenadas vacías o fuera de los rangos de latitud y longitud.
- Distribución diaria de registros por unidad y fuente.

El día de ejecución no entra en las dos ventanas comparadas.

CÓMO EJECUTARLO EN CLOUDSHELL

Carga el .py en $HOME y ejecuta:

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

Revisa las unidades observadas de todas las siglas. Para una prueba añade
--filtrar-sigla MEPAS. Sin argumentos, solo descubre el esquema de la tabla;
no evalúa datos. La consulta Athena puede generar consumo por datos leídos.

QUÉ ENTREGA Y DÓNDE

Genera dos CSV:

  general.csv: indicadores y controles. Filtra tipo_registro=RESUMEN para
  las cifras por CAD/unidad/BD y CONTROL para los estados y sus motivos.

  diario.csv: conteos por día, útiles para ubicar caídas y días sin registros.

S3:
  s3://pon-prod-ml-iapol/ValidationReports/01-MonitorDatosEntrada/FECHA/HORA/

Copias locales:
  $HOME/salida/general_datos_entrada_FECHA_HORA.csv
  $HOME/salida/diario_datos_entrada_FECHA_HORA.csv

FECHA/HORA corresponde a la ejecución en Bogotá. Los archivos internos de
Athena quedan en _athena/ dentro del destino de consulta de la ejecución.
Si falla la subida de informes, conserva los CSV locales.

CÓMO LEER EL RESULTADO

Empieza por FALLA, AVISO e INCOMPLETO en general.csv. "Base insuficiente"
significa que hay pocos registros anteriores para comparar con confianza.
Una caída o un cero necesitan contexto: puede haber poca frecuencia o carga tardía.

La fecha del hecho no confirma cuándo se cargó el registro. El filtro temporal
excluye fechas nulas, no interpretables y futuras; su calidad global queda
NO EVALUADA en este modo. Sin catálogo esperado no detecta todas las unidades
totalmente ausentes. Los rangos de coordenadas no prueban pertenencia territorial.

El script guarda informes y consulta datos; no modifica la tabla ni envía alertas.
