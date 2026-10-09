# monitoreo-datos-ponal

Scripts de monitoreo **independientes y autocontenidos** para CloudShell: cada script se
sube suelto y se corre con `python3`, y son de **solo lectura** salvo donde se indique.
Las categorías 3 y 4 son una excepción (ver nota abajo): validadores del equipo de
predicción y una función Lambda, con otro estilo.

El detalle completo (catálogo de scripts, convenciones de severidad, cómo correrlos y
permisos IAM mínimos) está en [`scripts/README.md`](scripts/README.md).

## Estructura

```text
scripts/
├── 1-Base-Datos/        Glue jobs, DMS, Athena, logs, conexiones (5 monitores + 1 bloqueado)
├── 2-Integraciones/     Migración SIVICC, servicio RNMC
├── 3-Prediccion/        Validadores S3 de predicción (equipo de predicción, otro estilo) + material IaC
├── 4-Planta-Nacional/   alerta48horas.py (Lambda: alerta reporte Movistar Webex > 48 h)
└── 5-Chatbot/           Carga de base de conocimiento, auditoría de conversaciones
```

> **Nota**: las categorías 3 y 4 no siguen el patrón común de los monitores
> (`OK/AVISO/FALLA`, `--con-avisos`, etc.). La 3 son validadores de artefactos en S3
> del equipo de predicción (más material de una solución IaC) y la 4 es una función
> Lambda. El detalle está en [`scripts/README.md`](scripts/README.md).

Cada carpeta tiene su propio `README.md` con flags, permisos IAM, salida y ejemplos.

Cuenta de referencia: **926162397524**, región **us-east-1**.
