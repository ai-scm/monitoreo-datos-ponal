# 10 — Carga Base de Conocimiento (`monitor_carga_kb.py`)

Corre **a diario**. La "base de conocimiento" del chatbot es **Amazon Bedrock
Knowledge Base** (confirmado; **no** es Kendra ni OpenSearch directo). La "carga" real
es la **ingesta**: Bedrock corre *ingestion jobs* que indexan los documentos de cada
data source al vector store.

**Solo lectura.** No dispara ingestas.

## Qué revisa

De las **4 KBs** que hay en la cuenta, se monitorean las **2 relevantes del chatbot**
(mapa fijo); las otras 2 (`ponal-test-delitos-kb`, `knowledge-base-quick-start-8bznm`)
quedan **fuera a propósito** (prueba / otra tecnología Redshift SQL).

| KB | ID | Naturaleza | Criterio |
|----|----|-----------|----------|
| `Data-no-estructurada-Expertopol` | `1ECION66RT` | **PRINCIPAL** (alimenta al Bot Nadia) | por data source, último ingestion job |
| `KBBrChatKbStaENFMEE14EKB2A93F4E2` | `NPYXAA8O6H` | **DORMIDA** (inactiva desde 2025-02-17) | INFO, no escala salvo regresión |

### KB principal (`1ECION66RT`)

Descubre sus **data sources dinámicamente** (`ListDataSources` → Procedimientos,
Normativas, Generalidades) y evalúa el **último ingestion job** de cada uno:

- **OK** — `COMPLETE`, **sin importar la antigüedad** (la fecha se muestra como dato).
- **AVISO** — `COMPLETE` con `numberOfDocumentsFailed > 0` (indexación parcial, p. ej.
  documentos que superan el límite de tokens) · `IN_PROGRESS`/`STARTING`.
- **FALLA** — `FAILED` (muestra `failureReasons`).

> **La carga es manual/humana**: nadie sube documentos a diario, se suben cuando toca.
> Por eso **no se evalúa "frescura"**: que la última ingesta sea de hace semanas **no**
> es un problema (eliminada la regla de ventana que generaba falsos positivos). Lo que
> importa es que, **cuando se cargue, no falle** — de ahí que los documentos fallidos
> sí sean AVISO. La causa típica de un doc fallido es
> `400 Too many input tokens` (el documento supera el máximo de 8192 tokens).

> Los data sources se descubren dinámicamente (crecimiento esperado), pero las **KBs**
> se cablean fijas: así no se cuela ninguna de las de prueba.

### KB dormida (`NPYXAA8O6H`)

Tratamiento especial: se reporta como **INFO "inactiva"** (sin ingesta reciente), y
**nunca** escala a AVISO/FALLA — un recurso dormido no es un recurso roto (mismo
principio que `historico_personas` en SIVICC y la DMS detenida en RNMC). **Única
excepción**: si vuelve a tener actividad de ingesta reciente (dentro de
`KB_INACTIVO_DIAS`, 180 por defecto) **y** esa actividad **falla**, se marca como
**FALLA (regresión)**.

#### Nota histórica (no es alerta activa)

Este data source tuvo un error IAM de propagación de permisos
(`s3:ListBucket` sobre `pon-prod-ml-iapol`) **resuelto en minutos hace ~20 meses**;
desde entonces no corrió ningún ingestion job. **No** se usa como patrón de
clasificación en producción — solo queda documentado como contexto. Si algún día el
data source vuelve a correr y falla con el mismo error, eso sí sería una regresión real
(y lo cubre la lógica de arriba).

## Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

python3 monitor_carga_kb.py
python3 monitor_carga_kb.py --horas 48
python3 monitor_carga_kb.py --con-avisos    # muestra avisos e info
python3 monitor_carga_kb.py --no-export
```

### Flags y env vars

| Flag / Env | Default | Qué hace |
|------------|---------|----------|
| `--con-avisos` | — | Muestra AVISOS e INFO (docs fallidos, KB dormida). |
| `--detallado` | — | Salida larga sin spinner. |
| `--no-export` | — | No escribe el CSV. |
| `KB_INACTIVO_DIAS` | 180 | Días sin ingesta para considerar "dormida"; dentro de ese rango, actividad fallida = regresión. |

> Ya no hay flag `--horas` ni ventana de frescura: la carga manual lo hace irrelevante.

## Permisos IAM mínimos

> **Diferencia respecto a los demás scripts**: es el primero que usa **Amazon Bedrock**
> (plano de control de Knowledge Bases, `bedrock-agent`).

```json
{
  "Effect": "Allow",
  "Action": [
    "bedrock-agent:ListKnowledgeBases",
    "bedrock-agent:GetKnowledgeBase",
    "bedrock-agent:ListDataSources",
    "bedrock-agent:ListIngestionJobs",
    "bedrock-agent:GetIngestionJob",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

## Salida

- **Consola**: resumen `TOTAL: N | x OK | y aviso/info | z falla` + detalle con la
  naturaleza entre corchetes (`[PRINCIPAL]`, `[DORMIDA]`).
- **CSV**: `salida/carga_kb_<YYYYmmdd_HHMMSS>.csv` con columnas `recurso, naturaleza,
  ubicacion, estado, mensaje, accion_sugerida`.
- **Exit code**: `0` todo OK · `1` si algo falló · `2` args inválidos.

## Relación con el punto 11

**No hay cadena** entre esta carga y el punto 11. Los documentos llegan al bucket
`pon-prod-data-no-estructurada` por un proceso manual/externo, y la "sincronización" de
la KB la hace **Bedrock mismo** (una Step Function → Lambda → `StartIngestionJob`), no
Glue. El punto 11 (`Auditoría-Conversaciones-Glue`) monitorea un job de Glue **distinto
y sin relación** con esta KB.
