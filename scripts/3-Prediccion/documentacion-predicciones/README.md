# Documentación del pipeline de predicciones

La versión vigente está en
[03-pipeline-y-validaciones](03-pipeline-y-validaciones/README.md), actualizada
al **30 de octubre de 2026**.

- [Arquitectura con logos AWS](03-pipeline-y-validaciones/pipeline_aws.png).
- [Arquitectura en PDF](03-pipeline-y-validaciones/pipeline_aws.pdf).
- [Pipeline y puntos de validación](03-pipeline-y-validaciones/pipeline_validaciones.png).
- [Guía visual de los scripts](03-pipeline-y-validaciones/guia_cobertura.png).
- [Paquete completo vigente](03-pipeline-y-validaciones/pipeline_y_validaciones_2026-10-30.zip).

El estado es **100% del alcance funcional aceptado**: nueve scripts habituales
y un diagnóstico opcional. La automatización, Lambda y las alertas pertenecen
a la etapa posterior. Las guías explican los límites de los controles actuales.

`probabilidaddelito` aparece como referencia fija consultada por el predictor,
con lectura comprobada en DECAU. El diagnóstico M07 queda fuera de la ejecución
habitual. Optimización refleja su prueba nacional completada y matrices su
aceptación con el alcance actual.

Los PDF originales, inventarios, capturas `alrm*`, mapas `01` y `02`, y
`LECTURA-Y-HALLAZGOS.txt` se conservan como material del levantamiento inicial.
No representan el estado de avance vigente. `generar_mapa.py` corresponde a
ese material documental; no ejecuta controles de monitoreo.

Los ZIP fechados el 29 de octubre conservan la versión anterior. Para compartir
el estado actual utiliza los enlaces de esta guía o el paquete del 30 de octubre.
