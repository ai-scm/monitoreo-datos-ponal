# Solución de Automatización AWS para Validación de Predicciones
## Arquitectura Completa con EventBridge, Lambda y Alertas

---

## 📋 Índice
1. [Visión General](#visión-general)
2. [Componentes de la Solución](#componentes)
3. [Flujo de Datos](#flujo-de-datos)
4. [Adaptación del Script](#adaptación-del-script)
5. [Configuración de Lambda](#configuración-lambda)
6. [Sistema de Alertas](#sistema-de-alertas)
7. [Infraestructura como Código](#infraestructura-como-código)
8. [Consideraciones Operativas](#consideraciones-operativas)

---

## Visión General

### Estado Actual
```
Manual: python3 validar_predicciones.py --desde 20260901 --hasta 20260907
├── Ejecución en CloudShell (manual)
├── Rango de fechas parametrizado
└── Resultados locales (CSV)
```

### Estado Objetivo
```
Automatizado:
┌─────────────────────────────────────────────────────────────┐
│  EventBridge (Cron rule) - Lunes 02:00 AM UTC-5           │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│  AWS Lambda (función Python adaptada)                       │
│  ├─ Calcula dinámicamente fechas (semana anterior)         │
│  ├─ Se conecta a S3 (bucket: pon-prod-ml-iapol)          │
│  ├─ Ejecuta validación completa                            │
│  ├─ Genera CSV en S3 (backup)                              │
│  ├─ Estructura logs en CloudWatch                          │
│  └─ Captura errores críticos                               │
└─────────────────┬───────────────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
    ✅ OK              ❌ ERROR
        │                   │
        ▼                   ▼
    Log éxito         SNS Topic (email/Slack)
                        │
                    ┌───┴────┬──────────┐
                    │        │          │
                    ▼        ▼          ▼
                  Email    Slack    CloudWatch Alarm
```

---

## Componentes de la Solución

### 1. **AWS Lambda Function**
- **Runtime**: Python 3.11+
- **Memory**: 512 MB (recomendado, escalable a 1024 MB si S3 tiene mucho volumen)
- **Timeout**: 900 segundos (15 minutos)
- **VPC**: Ninguna (S3 y CloudWatch son servicios públicos)
- **Dependencias**: boto3 (incluido), requests (para Slack), json, datetime

### 2. **EventBridge Rule**
- **Trigger**: CRON expression: `0 2 ? * MON *` (Lunes 02:00 AM UTC)
- **Target**: Lambda function
- **Payload**: JSON dinámico con fechas calculadas

### 3. **IAM Role**
- **Permissions**:
  - `s3:ListBucket`, `s3:GetObject` en bucket `pon-prod-ml-iapol`
  - `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
  - `sns:Publish` (para alertas)
  - `s3:PutObject` (para guardar resultados)

### 4. **SNS Topic**
- **Subscribers**:
  - Email (auto-confirmación)
  - Lambda para integración Slack (opcional)
  - HTTP endpoint (opcional)

### 5. **CloudWatch Logs**
- **Log Group**: `/aws/lambda/validar-predicciones-prod`
- **Retention**: 30 días (configurable)

### 6. **S3 (para resultados)**
- **Bucket**: `pon-prod-ml-iapol` (existente)
- **Prefix**: `ValidationReports/YYYY/YYYY-MM-DD/`
- **Archivos**: 
  - `errores_prediccion_rango.csv`
  - `horas_faltantes_rango.csv`
  - `execution_summary.json` (metadatos)

---

## Flujo de Datos

### Secuencia Diaria (Lunes 02:00 AM UTC-5)

```
1. EventBridge trigger
   ├─ Calcula: Lunes pasado en UTC-5
   └─ Invoca Lambda con: {"desde": "20260908", "hasta": "20260914"}

2. Lambda cold start (~2-3s con importes boto3)
   
3. Ejecutar validación:
   ├─ Conectar S3 (test head_bucket)
   ├─ Listar semanas entre fechas
   ├─ Por cada semana/SIGLA/unidad:
   │  ├─ Validar nombre archivos
   │  ├─ Validar cobertura 24 horas
   │  └─ Registrar errores
   ├─ Generar CSV en memoria
   └─ Subir resultados a S3

4. Análisis de resultados:
   ├─ Si errores_count > 0:
   │  ├─ Generar resumen ejecutivo
   │  └─ Enviar SNS alert
   ├─ Si error de conexión:
   │  ├─ Registrar exception
   │  └─ Enviar SNS CRITICAL alert
   └─ CloudWatch logs (siempre)

5. Retornar response Lambda:
   ├─ statusCode: 200 / 500
   ├─ body: JSON con métricas
   └─ CloudWatch Log Stream
```

### Manejo de Excepciones

```python
try:
    s3 = conectar_s3()
except ClientError as e:
    # CRITICAL: No puede conectar S3
    # → SNS alert INMEDIATO
    # → Severity: CRITICAL
    # → Action: Verificar credenciales IAM / S3 bucket
    raise

try:
    validar_unidad(...)
except Exception as e:
    # WARNING: Una unidad falló pero continúa
    # → Log en CloudWatch
    # → Incluir en reporte
    continue

if len(todos_invalidos) > THRESHOLD:  # e.g., 100+
    # WARNING: Muchos archivos inválidos
    # → SNS alert
    # → Severity: WARNING
    pass
```

---

## Adaptación del Script

### Cambios Principales

#### 1. Estructura de Entrada (Lambda Event)
```python
# ANTES (CLI arguments)
python3 validar_predicciones.py --desde 20260901 --hasta 20260907

# DESPUÉS (Lambda event dict)
{
    "desde": "20260901",           # Opcional, default semana pasada
    "hasta": "20260907",           # Opcional
    "sigla": None,                 # Opcional, procesar todas
    "env": "prod",                 # Para logging
    "alert_threshold": 50          # Alertar si > 50 errores
}
```

#### 2. Manejo de Almacenamiento (Filesystem → S3)
```python
# ANTES: Guardar en disco local
carpeta_resultados = crear_carpeta_resultados(args.sigla)
guardar_csv(detalle_csv, columnas_detalle, todos_invalidos)

# DESPUÉS: Guardar en S3
s3_prefix = f"ValidationReports/{datetime.now(UTC_MENOS_5):%Y/%Y-%m-%d}/"
s3.put_object(
    Bucket=BUCKET,
    Key=f"{s3_prefix}errores_prediccion_rango.csv",
    Body=csv_buffer.getvalue(),
    ContentType="text/csv"
)
```

#### 3. Logging Estructurado
```python
# ANTES: print() con emojis
print(f"❌ ERROR conectando a S3: {exc}")

# DESPUÉS: CloudWatch Logs con JSON estructurado
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("validacion_iniciada", extra={
    "semana": semana,
    "siglas": len(siglas),
    "timestamp": datetime.now(UTC_MENOS_5).isoformat()
})

logger.error("s3_connection_failed", extra={
    "error": str(exc),
    "bucket": BUCKET,
    "region": REGION
})
```

#### 4. Cálculo Dinámico de Fechas
```python
def calcular_semana_pasada():
    """Retorna lunes de hace 7 días en UTC-5."""
    hoy = datetime.now(UTC_MENOS_5).date()
    hace_semana = hoy - timedelta(days=7)
    lunes_pasado = hace_semana - timedelta(days=hace_semana.weekday())
    return lunes_pasado.strftime("%Y%m%d")

def calcular_hoy():
    """Retorna hoy en formato YYYYMMDD."""
    hoy = datetime.now(UTC_MENOS_5).date()
    return hoy.strftime("%Y%m%d")
```

#### 5. Retorno Lambda
```python
def lambda_handler(event, context):
    try:
        # ... ejecutar validación ...
        
        resultado = {
            "statusCode": 200,
            "body": {
                "mensaje": "Validación completada exitosamente",
                "semanas_procesadas": len(semanas),
                "siglas_procesadas": total_siglas,
                "errores_detectados": len(todos_invalidos),
                "horas_faltantes": sum(f['cantidad_faltantes'] for f in todos_faltantes),
                "s3_key_errores": f"{s3_prefix}errores_prediccion_rango.csv",
                "s3_key_horas": f"{s3_prefix}horas_faltantes_rango.csv"
            }
        }
        
        # Si hay errores críticos, enviar alerta
        if len(todos_invalidos) > ALERT_THRESHOLD:
            enviar_alerta_sns(resultado, "WARNING")
        
        return resultado
        
    except Exception as exc:
        logger.exception("lambda_execution_failed", extra={"event": event})
        enviar_alerta_sns(str(exc), "CRITICAL")
        raise
```

---

## Configuración Lambda

### 1. Crear la Función

```bash
# 1. Preparar código con dependencias
mkdir lambda-deployment
cd lambda-deployment
pip install boto3 -t .  # boto3 ya está en Lambda runtime
cp /path/to/validar_predicciones_lambda.py .

# 2. Empaquetar
zip -r deployment-package.zip .

# 3. Crear rol IAM (ver sección IaC)

# 4. Crear Lambda
aws lambda create-function \
    --function-name validar-predicciones-prod \
    --runtime python3.11 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-validar-predicciones-role \
    --handler validar_predicciones_lambda.lambda_handler \
    --zip-file fileb://deployment-package.zip \
    --timeout 900 \
    --memory-size 512 \
    --environment Variables="{S3_BUCKET=pon-prod-ml-iapol,AWS_REGION=us-east-1,ALERT_THRESHOLD=50}" \
    --region us-east-1
```

### 2. Variables de Entorno

```
BUCKET                      = pon-prod-ml-iapol
REGION                      = us-east-1
ROOT_PREFIX                 = PrediccionMdP
UTC_OFFSET                  = -5
ALERT_THRESHOLD             = 50        # Enviar alerta si > X errores
SNS_TOPIC_ARN              = arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts
SLACK_WEBHOOK_URL          = (opcional, desde Secrets Manager)
LOG_RETENTION_DAYS         = 30
```

### 3. Permisos IAM Mínimos

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Read",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::pon-prod-ml-iapol",
        "arn:aws:s3:::pon-prod-ml-iapol/*"
      ]
    },
    {
      "Sid": "S3WriteResults",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::pon-prod-ml-iapol/ValidationReports/*"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:ACCOUNT_ID:log-group:/aws/lambda/validar-predicciones-prod:*"
    },
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts"
    },
    {
      "Sid": "SecretsManager",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:us-east-1:ACCOUNT_ID:secret:slack-webhook*"
    }
  ]
}
```

### 4. Layers (Opcional, para dependencias adicionales)

Si necesitas `requests` para Slack:

```bash
# Crear estructura de layer
mkdir -p python/lib/python3.11/site-packages
pip install requests -t python/lib/python3.11/site-packages

# Empaquetar
zip -r lambda-layer.zip python/

# Publicar
aws lambda publish-layer-version \
    --layer-name validar-predicciones-dependencies \
    --zip-file fileb://lambda-layer.zip \
    --compatible-runtimes python3.11
```

---

## Sistema de Alertas

### 1. SNS Topic y Subscripciones

```bash
# Crear topic
aws sns create-topic --name validation-alerts --region us-east-1

# Suscribir email
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts \
    --protocol email \
    --notification-endpoint tu-email@empresa.com
```

### 2. Tipos de Alertas

#### Alerta CRITICAL (Conexión S3 falló)
```json
{
  "alert_type": "CRITICAL",
  "severity": "CRITICAL",
  "timestamp": "2026-09-14T02:15:30Z",
  "service": "Lambda validar-predicciones",
  "error": "ClientError: 403 Forbidden - Access Denied to bucket pon-prod-ml-iapol",
  "action_required": "Verificar credenciales IAM y permisos S3",
  "context": {
    "function": "validar-predicciones-prod",
    "execution_id": "abc123xyz"
  }
}
```

#### Alerta WARNING (Muchos errores detectados)
```json
{
  "alert_type": "WARNING",
  "severity": "WARNING",
  "timestamp": "2026-09-14T02:30:00Z",
  "service": "Lambda validar-predicciones",
  "summary": "Validación completada con observaciones",
  "metrics": {
    "semanas_procesadas": 1,
    "siglas_procesadas": 45,
    "unidades_procesadas": 2150,
    "errores_formato": 156,
    "horas_faltantes": 823
  },
  "top_errors": [
    {"unidad": "UNI001", "sigla": "SIGLA_A", "errores": 15},
    {"unidad": "UNI002", "sigla": "SIGLA_B", "errores": 12}
  ],
  "reports_s3": {
    "errores": "s3://pon-prod-ml-iapol/ValidationReports/2026/2026-09-14/errores_prediccion_rango.csv",
    "horas_faltantes": "s3://pon-prod-ml-iapol/ValidationReports/2026/2026-09-14/horas_faltantes_rango.csv"
  },
  "action_required": "Revisar reportes en S3 y coordinar con equipos de operaciones"
}
```

#### Alerta INFO (Éxito sin problemas)
```
✅ Validación exitosa - Semana 20260908
- 1 semana procesada
- 45 SIGLAS validadas
- 2150 unidades procesadas
- 0 errores detectados
- Próxima ejecución: 2026-09-22 02:00 UTC-5
```

### 3. Integración con Slack (Opcional)

```python
def enviar_alerta_slack(resultado, severity):
    """Envía alerta formateada a Slack."""
    import json
    import requests
    from botocore.exceptions import ClientError
    
    # Obtener webhook URL de Secrets Manager
    try:
        webhook_url = get_secret("slack-webhook-validacion")
    except ClientError:
        logger.warning("Slack webhook no configurado, usando SNS solamente")
        return
    
    if severity == "CRITICAL":
        color = "danger"
        title = "🚨 VALIDACIÓN CRÍTICA"
    elif severity == "WARNING":
        color = "warning"
        title = "⚠️ VALIDACIÓN CON OBSERVACIONES"
    else:
        color = "good"
        title = "✅ VALIDACIÓN EXITOSA"
    
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Métricas*\n"
                            f"• Semanas: {resultado['semanas']}\n"
                            f"• SIGLAS: {resultado['siglas']}\n"
                            f"• Errores: {resultado['errores']}\n"
                            f"• Timestamp: {resultado['timestamp']}"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Ver Reportes"
                        },
                        "url": f"https://s3.console.aws.amazon.com/s3/buckets/pon-prod-ml-iapol?prefix=ValidationReports/"
                    }
                ]
            }
        ]
    }
    
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        logger.error(f"Slack notification failed: {response.text}")
```

---

## Infraestructura como Código

### Opción 1: CloudFormation (RECOMENDADO para AWS)
Ver archivo: `02_CLOUDFORMATION_TEMPLATE.yaml`

### Opción 2: Terraform (RECOMENDADO para multi-cloud)
Ver archivo: `03_TERRAFORM_MAIN.tf`

---

## Consideraciones Operativas

### Timeout y Memoria

| Métrica | Valor | Razón |
|---------|-------|-------|
| **Timeout** | 900s (15 min) | Validar 45 SIGLAS × 50 unidades × 7 días = ~2150 iteraciones |
| **Memory** | 512 MB | boto3 + pandas lite ≈ 200 MB; JSON parsing ≈ 150 MB |
| **Cold Start** | 2-3s | Python 3.11 + boto3 estándar |
| **Warm Start** | 50-100ms | Reutilizar conexión S3 |

### Estimación de Tiempo de Ejecución

```
Casos de Uso:
1. Validación semanal normal (45 SIGLAS, 2150 unidades)
   ├─ S3 ListBucket: ~2-3s (1000 objetos)
   ├─ Validación unidades: ~10-15s
   └─ Total: ~20-30s ✅ Muy por debajo del timeout

2. Validación histórica (4 semanas, todas las SIGLAS)
   ├─ S3 ListBucket: ~5-8s
   ├─ Validación: ~60-90s
   └─ Total: ~100-120s ✅ Aceptable

3. Validación con error de S3 (retry + timeout)
   ├─ Intentos: 3
   ├─ Backoff exponencial: 1s, 2s, 4s
   └─ Total: ~30s ✅ Antes de timeout Lambda
```

### Almacenamiento de Logs

#### CloudWatch Logs
```
Log Group: /aws/lambda/validar-predicciones-prod
├─ Log Stream: 2026/09/14/[$LATEST]abc123...
│  ├─ Líneas: ~1000-5000 (según nivel de verbosidad)
│  ├─ Tamaño: ~500 KB - 2 MB
│  └─ Retention: 30 días (configurable)
├─ Costo: ~$0.50 por mes por función
└─ Acceso: AWS Console o AWS CLI
```

#### S3 (Reportes)
```
Bucket: pon-prod-ml-iapol
└─ ValidationReports/
   ├─ 2026/09/14/
   │  ├─ errores_prediccion_rango.csv (~50-500 KB)
   │  ├─ horas_faltantes_rango.csv (~100-1000 KB)
   │  └─ execution_summary.json (~5 KB)
   └─ Retención: 90 días (con S3 Lifecycle Policy)
```

### Monitoreo

#### CloudWatch Metrics

```python
# Publicar métricas custom
cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_data(
    Namespace='ValidacionPredicciones',
    MetricData=[
        {
            'MetricName': 'ErroresDetectados',
            'Value': len(todos_invalidos),
            'Unit': 'Count',
            'Timestamp': datetime.now(UTC_MENOS_5)
        },
        {
            'MetricName': 'TiempoEjecucion',
            'Value': execution_time_seconds,
            'Unit': 'Seconds'
        },
        {
            'MetricName': 'SiglasProcesadas',
            'Value': total_siglas,
            'Unit': 'Count'
        }
    ]
)
```

#### CloudWatch Alarms

```bash
# Alarma 1: Función falló (Non-zero exit code)
aws cloudwatch put-metric-alarm \
    --alarm-name validar-predicciones-failures \
    --alarm-description "Lambda execution failed" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --dimensions Name=FunctionName,Value=validar-predicciones-prod \
    --alarm-actions arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts

# Alarma 2: Timeout
aws cloudwatch put-metric-alarm \
    --alarm-name validar-predicciones-timeout \
    --metric-name Duration \
    --namespace AWS/Lambda \
    --statistic Maximum \
    --period 300 \
    --threshold 850000 \  # 850 segundos en ms
    --comparison-operator GreaterThanThreshold \
    --dimensions Name=FunctionName,Value=validar-predicciones-prod

# Alarma 3: Muchos errores detectados (custom metric)
aws cloudwatch put-metric-alarm \
    --alarm-name validar-predicciones-high-errors \
    --metric-name ErroresDetectados \
    --namespace ValidacionPredicciones \
    --statistic Average \
    --period 300 \
    --threshold 100 \
    --comparison-operator GreaterThanThreshold \
    --alarm-actions arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts
```

### Dashboard de Monitoreo

```json
{
  "DashboardName": "ValidacionPredicciones",
  "DashboardBody": {
    "widgets": [
      {
        "type": "metric",
        "properties": {
          "metrics": [
            ["AWS/Lambda", "Invocations", {"stat": "Sum"}],
            ["AWS/Lambda", "Duration", {"stat": "Average"}],
            ["AWS/Lambda", "Errors", {"stat": "Sum"}],
            ["ValidacionPredicciones", "ErroresDetectados", {"stat": "Average"}],
            ["ValidacionPredicciones", "SiglasProcesadas", {"stat": "Average"}]
          ],
          "period": 300,
          "stat": "Average",
          "region": "us-east-1",
          "title": "Lambda Validaciones - Últimas 24h"
        }
      }
    ]
  }
}
```

### Acceso a Logs

```bash
# Ver últimas 100 líneas de logs
aws logs tail /aws/lambda/validar-predicciones-prod --follow

# Filtrar por ERROR
aws logs filter-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --filter-pattern "ERROR"

# Descargar logs completos del último día
aws logs get-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --log-stream-name '2026/09/14/[$LATEST]abc123...'
```

### Verificación de Automatización

#### 1. EventBridge Rule Status
```bash
aws events describe-rule \
    --name validar-predicciones-weekly \
    --region us-east-1

# Debe retornar:
# "State": "ENABLED"
# "ScheduleExpression": "cron(0 2 ? * MON *)"
```

#### 2. Últimas Ejecuciones Lambda
```bash
aws logs tail /aws/lambda/validar-predicciones-prod \
    --since 1h \
    --format "[TIMESTAMP] [LEVEL] [MESSAGE]"
```

#### 3. Verificar SNS Topics
```bash
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts
```

#### 4. Test Manual
```bash
# Invocar Lambda directamente
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --payload '{"desde": "20260901", "hasta": "20260907"}' \
    --region us-east-1 \
    response.json

cat response.json | jq .
```

---

## Roadmap Adicional

### Fase 2: Mejoras
- [ ] Caché de S3 listings en DynamoDB (optimización)
- [ ] Reintentos automáticos con exponential backoff
- [ ] Integración con DataDog / New Relic
- [ ] Reportes HTML en S3 (CloudFront distribution)
- [ ] Webhook callbacks a sistemas externos

### Fase 3: Machine Learning
- [ ] Detectar patrones anómalos de errores
- [ ] Predicción de caídas futuras
- [ ] Alertas inteligentes (no all-boy-cried-wolf)

---

## Resumen Checklist

- [ ] Script adaptado a Lambda (validar_predicciones_lambda.py)
- [ ] Rol IAM creado con permisos mínimos
- [ ] CloudFormation template deployado (o Terraform)
- [ ] EventBridge rule activo (Lunes 02:00 AM)
- [ ] SNS topic con suscriptores configurados
- [ ] Test manual de Lambda exitoso
- [ ] CloudWatch Log Group creado
- [ ] CloudWatch Alarms configuradas
- [ ] Dashboard creado
- [ ] Documentación de runbooks
- [ ] Notificación a equipo: nueva automatización activa

---

**Versión**: 1.0  
**Última actualización**: 2026-09-14  
**Responsable**: Arquitectura Cloud
