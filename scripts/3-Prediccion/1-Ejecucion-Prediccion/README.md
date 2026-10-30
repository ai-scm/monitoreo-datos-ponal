# 5-Ejecucion-Prediccion

Mantenido por el **equipo de predicción**. Aquí **no hay un script de monitoreo** del
patrón común: es **material/solución de automatización** (documentación + código de
referencia) para automatizar el validador de predicciones en AWS.

## Contenido

```
Prediccion/
├── Diagrama_Arquitectura_Final.png
├── Resumen_Implementacion (1).docx
├── files/
│   ├── 01_ARQUITECTURA_SOLUCION.md         arquitectura completa
│   ├── 02_validar_predicciones_lambda.py   código Lambda adaptado
│   ├── 03_cloudformation_template.yaml     IaC (CloudFormation)
│   ├── 04_terraform_main.tf                IaC (Terraform)
│   ├── 05_terraform_prod.tfvars            variables Terraform
│   ├── 06_GUIA_DEPLOYMENT.md               guía de deployment (3 opciones)
│   ├── 07_GUIA_OPERACIONES.md              operación y mantenimiento
│   ├── 08_deploy.sh                        script de deployment
│   └── README.md                           visión general de la solución
├── script s3/
│   ├── validar_todas_siglas v1.py
│   └── validar_todas_siglas v2.py
└── mapa-predicciones/
    ├── validacion-secad/                   validación de las capas SECAD del mapa (ver su README)
    ├── validacion-rnmc/                    validación de las capas RNMC del mapa (ver su README)
    ├── validacion-gescov-antecedentes/     validación de Planes GESCOV y Antecedentes (ver su README)
    └── validacion-fuentes-internas/        validación de las fuentes internas: Gescov, MTRS, SIEDCO, SIMEP, SIRAP, IRIS P1
```

> `mapa-predicciones/` es la excepción a "aquí no hay monitores": scripts de **solo lectura sobre
> Athena y el historial de Glue**, por ahora para correr en local, que validan por sigla y unidad los
> datos de las capas del mapa. `validacion-secad`: Riñas, Hurto violento y homicidio, Hurto general.
> `validacion-rnmc`: Riñas, Incautaciones, Mediaciones in situ y en sala, Comportamientos contrarios a
> la convivencia. `validacion-gescov-antecedentes`: Planes Gescov Programados y Antecedentes (Personas,
> Vehículos, IMEI, y sus versiones "solo positivos"). `validacion-fuentes-internas`: los GeoJSON de Fuentes Internas
> (Gescov, MTRS, SIEDCO I/II/III, SIMEP, SIRAP, IRIS P1). Las tres últimas reutilizan el motor de `validacion-secad`.

## Qué propone

Convertir el validador de predicciones (hoy manual en CloudShell) en una ejecución
automática:

- **EventBridge** dispara la validación cada **lunes 02:00 AM UTC-5** (`cron(0 7 ? * MON *)`).
- **Lambda** (Python adaptado) lee S3 (`pon-prod-ml-iapol`), valida y escribe reportes
  CSV de vuelta en S3.
- **SNS** envía alertas (email/Slack) según umbrales de errores/horas faltantes.
- **CloudWatch** centraliza logs, métricas y alarmas.
- **CloudFormation / Terraform** para desplegar la infraestructura de forma reproducible.

El detalle completo (arquitectura, deployment paso a paso, operación y troubleshooting)
está en `Prediccion/files/README.md` y los documentos `01_`…`08_`.

> Nota: es material de referencia del equipo de predicción. No se corre como los
> monitores de este repo; su deployment vive en AWS (Lambda/EventBridge), no en CloudShell.
