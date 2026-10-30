# Arquitectura AWS y puntos de validación

Versión vigente: **30 de octubre de 2026**. Representa el flujo semanal y los
controles del alcance funcional aceptado, con logos oficiales de AWS.

- [Abrir PNG](pipeline_aws.png) · [SVG ampliable](pipeline_aws.svg) · [PDF](pipeline_aws.pdf).
- [Editar en draw.io / diagrams.net](pipeline_aws.drawio).
- [Descargar el paquete completo vigente](pipeline_y_validaciones_2026-10-30.zip).
- [Guía de cobertura](README.md) · [Matriz CSV](cobertura_scripts.csv) · [Evidencia](EVIDENCIA_Y_ESTADO_2026-10-30.md).

![Pipeline AWS](pipeline_aws.png)

## Alcance y estado

**100% del alcance funcional aceptado:** nueve scripts habituales y un diagnóstico
opcional. M01–M06 son los seis monitores del proceso; R01 es predicciones V2,
R02 optimización y R03 matrices. M07 se conserva fuera de la ejecución habitual.
Incluye las carpetas 1, 2, 3 y 5; excluye `4-Anticipacion-Demanda`.

Este estado no representa una implementación nueva en Lambda ni un canal de
alertas desplegado. Los scripts se ejecutan por separado y guardan informes.
La automatización y la operación del piloto pertenecen a la etapa posterior.

## Cambios que refleja esta versión

- **Probabilidades:** `probabilidaddelito` aparece como referencia consultada
  mediante Athena por el predictor. La lectura se confirmó en la tarea DECAU.
  Se trata como referencia fija; no se exige actualización semanal. M07 queda
  marcado como diagnóstico opcional de esquema y objetos fuente.
- **Optimización:** semana actual y siguiente, inventario antes de leer contenido,
  caché reanudable y prueba nacional completa: 376.580 archivos, 446 AVISO,
  cero FALLA y cero lecturas pendientes. Esto no certifica la calidad del algoritmo.
- **Matrices:** alcance actual aceptado, con controles de nombres y presencia.
  Sus mejoras quedan para después. No se atribuye lectura del contenido PKL.
- **Predicciones:** se considera exclusivamente V2; su código y despliegue se conservan.

## Cómo leer las conexiones

Las flechas continuas representan flujos o lecturas documentadas. La conexión de
la tabla de probabilidades con el generador acredita una lectura; no identifica
la fórmula posterior que utiliza sus proporciones.

Las líneas ámbar discontinuas mantienen las relaciones cuyo encadenamiento
exacto no se reconstruyó. Los jobs de referencia y entrenamiento no se muestran
como una secuencia obligatoria. Agrupar productos en S3 tampoco confirma el
productor exacto de cada carpeta.

Los enlaces verdes discontinuos relacionan cada recurso con su validador.
Verde indica lógica implementada o alcance actual aceptado; azul, la V2 existente;
gris, diagnóstico opcional o producto sin validador dentro del alcance, según el
texto. Los puntos de validación no son alarmas CloudWatch automáticas.

## Informes y límites

M01–M06 publican sus CSV en las carpetas 01–06 de `ValidationReports/`; R02,
en la 08. M07 usa la 07 únicamente si se ejecuta el diagnóstico. Las carpetas
FECHA/HORA corresponden a la ejecución en Bogotá. R01 V2 guarda dos CSV locales
y R03 un ZIP con dos CSV locales. Las guías de cada script describen sus límites.

Los iconos se incluyen en el SVG y en el draw.io y provienen del paquete oficial
AWS Architecture Icons del 31 de julio de 2026; su procedencia está en
`iconos_aws/procedencia.csv`. No hacen falta imágenes remotas para abrir el mapa.

Los ZIP fechados el 29 de octubre se conservan como documentos históricos.
La versión vigente y sus fuentes editables están enlazadas al inicio.
