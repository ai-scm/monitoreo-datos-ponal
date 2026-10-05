# monitoreo-datos-ponal

Scripts de monitoreo **independientes y autocontenidos** para CloudShell. Cada script
se sube suelto a CloudShell y se corre con `python3`. Todos de **solo lectura** salvo
donde se indique.

El detalle completo (catálogo de scripts, convenciones de severidad, cómo correrlos y
permisos IAM mínimos) está en [`scripts/README.md`](scripts/README.md).

## Estructura

```text
scripts/
├── 1-Base-Datos/        Glue jobs, DMS, Athena, logs, conexiones
├── 2-Integraciones/     Migración SIVICC, servicio RNMC
├── 3-Prediccion/        (desarrollo en el otro equipo)
├── 4-Planta-Nacional/   (desarrollo en el otro equipo)
└── 5-Chatbot/           Carga de base de conocimiento, auditoría de conversaciones
```

Cada carpeta tiene su propio `README.md` con flags, permisos IAM, salida y ejemplos.

Cuenta de referencia: **926162397524**, región **us-east-1**.
