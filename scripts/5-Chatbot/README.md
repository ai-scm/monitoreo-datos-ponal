# 5 — Chatbot

Scripts de monitoreo del dominio **chatbot** (Bot Nadia y el bot personalizado del
chat). La investigación de la cuenta dejó un hallazgo importante: **el punto 11 no era
lo que su nombre sugería** (ver abajo).

**Estado: los 2 puntos implementados.**

| # | Carpeta | Script | Qué monitorea | Frecuencia |
|---|---------|--------|---------------|------------|
| 10 | `10-Carga-Base-Conocimiento/` | `monitor_carga_kb.py` | Ingesta de las Knowledge Bases (Bedrock) | diario |
| 11 | `11-Auditoria-Conversaciones-Glue/` | `monitor_auditoria_conversaciones_glue.py` | Job Glue de auditoría de conversaciones | diario |

Ambos son **solo lectura** y siguen el patrón común (ver el `README.md` raíz). El
detalle de cada uno está en el `README.md` de su carpeta.

## 10 — Carga Base de Conocimiento

La KB del chatbot es **Amazon Bedrock Knowledge Base** (confirmado; no Kendra ni
OpenSearch directo). La "carga" es la **ingesta** que Bedrock corre por data source.

- Mapa fijo de las **2 KBs relevantes** (de 4 en la cuenta; las 2 de prueba quedan
  fuera). Los data sources dentro de cada KB se descubren dinámicamente.
- **Punto clave de diseño**: la carga es **manual/humana** (nadie sube documentos a
  diario). Por eso **no se evalúa frescura** — una ingesta vieja no es un problema. Lo
  que importa es que, cuando se cargue, **no falle**: por eso los documentos fallidos
  (típicamente `400 Too many input tokens`) sí son AVISO.
- La KB inactiva (`NPYXAA8O6H`) se reporta como INFO ("dormida"), no como fallo.

## 11 — Auditoría de Conversaciones (Glue)

**Choque de nombres**: el punto se llamaba originalmente "Sincronización Glue", pero
**no existe ningún Glue que sincronice la KB** — esa sincronización la hace Bedrock
mismo (Step Function → Lambda → `StartIngestionJob`), y la cubre el punto 10. El único
Glue real del chatbot es el job de **auditoría de conversaciones** (DynamoDB → Parquet).
Por eso la carpeta se renombró de `11-Sincronizacion-Glue` a
`11-Auditoria-Conversaciones-Glue`.

Verifica el **Glue Trigger** (`conversation-audit-glue-full-refresh`, ACTIVATED, cron
02:00 UTC — confirmado con `glue.get_trigger`, no asumido por el nombre) y el último run
del job, con clasificación genérica de errores de Glue.

## Diferencia de permisos

El punto 10 es el primer script que usa **Amazon Bedrock** (`bedrock-agent:*`, plano de
control de Knowledge Bases). Ver su README para la política IAM completa.
