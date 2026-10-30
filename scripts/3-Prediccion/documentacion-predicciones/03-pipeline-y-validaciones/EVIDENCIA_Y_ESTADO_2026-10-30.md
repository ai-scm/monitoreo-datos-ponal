# Evidencia y decisiones de alcance — 30 de octubre de 2026

Esta nota respalda la versión vigente de los diagramas. Reúne comprobaciones
realizadas durante la revisión y decisiones aceptadas por el usuario.
No se modificaron recursos del pipeline para obtener esta evidencia.

## Alcance aceptado

Nueve scripts de uso habitual: M01–M06 y los validadores R01 V2, R02 y R03.
M07 queda como diagnóstico opcional y se conserva su archivo. El seguimiento
se cierra al 100% del alcance funcional aceptado; no es una certificación de
que se detecte cualquier falla. Lambda, programación, estado continuo y alertas
pertenecen a la implementación operativa posterior.

Matrices se acepta con los controles actuales y con mejoras posteriores.
La aceptación no añade validación del contenido PKL ni corrige su calendario
fijo o el registro de errores de lectura. Los nombres `Prediccion_..._var.pkl`
se reportan como inválidos, mientras que las subcarpetas quedan excluidas.

## Prueba nacional de optimización

Fuente de informes:

`s3://pon-prod-ml-iapol/ValidationReports/08-MonitorOptimizacion/2026-10-30/12-13-33_849918/`

- Semanas `20261026` y `20261102`.
- 54 CAD, 2.359 unidades distintas y 4.705 listados de unidad-semana completos.
- 376.580 archivos: 376.134 OK, 446 AVISO, cero FALLA y cero lecturas pendientes.
- Los 446 avisos de contenido corresponden a 414 archivos B con colección vacía
  y 32 con diferencias entre el índice y el número de elementos. Son hallazgos,
  no lecturas inconclusas.

Interpretación completa:
[análisis nacional de avisos](../../2-Zonas-Atencion-Fusion/Optimizacion/analisis_avisos/2026-10-30-nacional/README.md).

La comparación histórica de MEPER cubrió ocho semanas y confirmó el patrón de
3.570 archivos semanales de las 43 unidades con entregas observadas. Las otras
cinco unidades no tenían archivos de optimización en las semanas revisadas;
esa ausencia no demuestra una obligación de entrega incumplida.

## Contenido de probabilidaddelito

Se leyeron los 16 Parquet de `s3://pon-prod-ml-iapol/probabilidadDelito/`:
199.506 filas, 4.684 cuadrantes y 1.318 descripciones no vacías distintas.
Las cinco columnas son `cuadrante`, `descripcion`, `delito_count`, `total_count`
y `probability`. La proporción coincide con `delito_count / total_count` en todas
las filas y las probabilidades suman aproximadamente uno en cada cuadrante.
No hay duplicados de cuadrante/descripción. Se encontraron 1.707 descripciones
vacías que representan 91.075 registros contabilizados.

Los objetos observados se modificaron el 15 de septiembre de 2024 y no tienen
columnas de fecha de los registros. No se deriva de esa antigüedad una falla.
Se acuerda tratar la tabla como referencia fija sin obligación semanal.
El modo de metadata de M07 no realiza estas comprobaciones de contenido.

## Lectura confirmada por el predictor

Se cruzaron identificadores de consultas exitosas de Athena con eventos
`StartQueryExecution` de CloudTrail y la identidad de la tarea ECS:

- Cuenta `926162397524`, región `us-east-1`.
- Tarea `810efd04b6cf4ac7bdd85c4eea2496e5`, CAD `DECAU`.
- Familia `pon-prod-xcrime-prediccion-definition:4`.
- Rol `pon-prod-iapol-anticipaciones-task-role`.
- Consultas `fc6ddfeb-2f12-461c-967a-024353455d77`,
  `d5ea58ec-cdaa-4e22-b4ad-2604bfcbbfd8` y
  `1acdab62-d002-4c2b-9def-c7b98ea70af8`, del 30 de octubre de 2026.
- El SQL selecciona `pon-prod-mdp.probabilidaddelito` para el cuadrante
  `DECAUMNVCCD01E01000000001`.

Esto acredita una lectura actual del predictor. No reconstruye la fórmula
posterior, no demuestra la misma lectura para todas las tareas y no establece
la frecuencia con que deba recalcularse la referencia.
