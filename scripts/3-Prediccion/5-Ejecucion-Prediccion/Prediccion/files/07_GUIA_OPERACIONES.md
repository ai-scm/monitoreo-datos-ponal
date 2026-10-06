# Guía de Operaciones y Mantenimiento
## Validación Automatizada de Predicciones

---

## Monitoreo Diario

### 1. Verificar Última Ejecución

```bash
# Ver últimas invocaciones Lambda
aws lambda get-function-concurrency \
    --function-name validar-predicciones-prod

# Ver logs de hoy
aws logs tail /aws/lambda/validar-predicciones-prod \
    --since 24h \
    --format short

# Contar logs por nivel
aws logs filter-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --filter-pattern '{ $.level = "ERROR" }' \
    --since 24h
```

### 2. Revisar Reportes en S3

```bash
# Listar reportes de hoy
aws s3 ls s3://pon-prod-ml-iapol/ValidationReports/$(date +%Y/%Y-%m-%d)/

# Descargar reportes
aws s3 cp s3://pon-prod-ml-iapol/ValidationReports/2026/2026-09-14/ ./ --recursive

# Analizar errores
wc -l errores_prediccion_rango_*.csv
wc -l horas_faltantes_rango_*.csv
```

### 3. Dashboard CloudWatch

Acceder a dashboard (URL en outputs):
```
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=ValidacionPredicciones-prod
```

Verificar:
- [ ] Invocations: 1 (semanal)
- [ ] Errors: 0 (sin fallos)
- [ ] Duration: < 120 segundos (normal)
- [ ] Throttles: 0 (sin limitaciones)

### 4. Métricas CloudWatch

```bash
# Ver última métrica de invocaciones
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=validar-predicciones-prod \
    --statistics Sum \
    --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 86400 \  # 1 día
    --region us-east-1

# Ver duración promedio
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value=validar-predicciones-prod \
    --statistics Average,Maximum \
    --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 86400 \
    --region us-east-1
```

---

## Manejo de Alertas

### Alerta Recibida: Errores Detectados

Si recibiste email: "WARNING: Observaciones detectadas (50+ errores)"

**Pasos:**

1. **Descargar reportes**
```bash
FECHA=$(date +%Y/%Y-%m-%d)
aws s3 cp s3://pon-prod-ml-iapol/ValidationReports/$FECHA/ ./ --recursive
```

2. **Analizar CSV**
```bash
# Ver primeros errores
head -20 errores_prediccion_rango_*.csv

# Agrupar por SIGLA
cut -d',' -f2 errores_prediccion_rango_*.csv | sort | uniq -c | sort -rn

# Agrupar por razón
cut -d',' -f9 errores_prediccion_rango_*.csv | sort | uniq -c | sort -rn
```

3. **Investigar**
- Contactar equipo de operaciones responsable de SIGLA
- Revisar si es error recurrente o pico anómalo
- Verificar si los archivos faltantes están en S3 con ruta incorrecta

4. **Documentar incidente**
```
INCIDENTE #001 - 2026-09-14
- Tipo: Archivos con formato inválido
- SIGLAS afectadas: SIGLA_A (15 errores), SIGLA_B (12 errores)
- Causa raíz: Archivos con nombre mal formado
- Acción: Coordinar con equipo para renombrar
- Resolución: [pendiente]
```

### Alerta Recibida: Conexión S3 Fallida

Si recibiste email: "CRITICAL: S3 Connection Failed"

**Pasos URGENTES:**

1. **Verificar acceso S3**
```bash
aws s3 ls s3://pon-prod-ml-iapol/PrediccionMdP/ --region us-east-1
```

2. **Si no funciona: Revisar credenciales Lambda**
```bash
# Verificar IAM role policy
aws iam get-role-policy \
    --role-name lambda-validar-predicciones-prod \
    --policy-name S3Policy

# Verificar bucket policy
aws s3api get-bucket-policy --bucket pon-prod-ml-iapol
```

3. **Causas posibles y soluciones:**
   - **Credenciales expiradas**: Regenrar API keys
   - **Bucket eliminado**: Restaurar desde backup
   - **Policy mal configurada**: Revisar con equipo de seguridad
   - **Throttling S3**: Esperar unos minutos, reintentar

4. **Notificar escalado**
```
INCIDENTE CRÍTICO - CONTACTAR DEVOPS-LEAD
- Lambda no puede conectar a S3
- Validación semanal no ejecutó
- Impacto: Pérdida de visibilidad de predicciones
```

### Alerta Recibida: Timeout de Lambda

Si recibiste email: "Lambda execution timeout"

**Pasos:**

1. **Aumentar timeout temporalmente**
```bash
aws lambda update-function-configuration \
    --function-name validar-predicciones-prod \
    --timeout 1200  # 20 minutos
```

2. **Investigar**
```bash
# Ver último log completo
aws logs tail /aws/lambda/validar-predicciones-prod \
    --since 1h \
    --follow
```

3. **Posibles causas:**
   - S3 tiene mucho más data que antes
   - Problema de red/latencia
   - Algoritmo tiene que optimizarse

4. **Acciones:**
   - Aumentar memoria (más CPU)
   - Optimizar código de validación
   - Usar paralelización (procesamiento en chunks)

---

## Tareas de Mantenimiento

### Semanal (Cada Lunes)

- [ ] Revisar ejecución semanal (después de 02:15 AM UTC-5)
- [ ] Verificar reportes en S3
- [ ] Revisar emails de alerta
- [ ] Documentar problemas encontrados

### Mensual (Primer día del mes)

- [ ] Análisis de tendencias de errores
```bash
# Errores por semana (últimos 30 días)
for dir in s3://pon-prod-ml-iapol/ValidationReports/2026/2026-08-*/ s3://pon-prod-ml-iapol/ValidationReports/2026/2026-09-0{1..7}/; do
    echo "=== $(basename $dir) ==="
    aws s3 cp "$dir" /tmp/ --recursive 2>/dev/null
    wc -l /tmp/errores_prediccion_rango_*.csv 2>/dev/null | tail -1
done
```

- [ ] Revisar métricas CloudWatch
- [ ] Actualizar documentación si hay cambios
- [ ] Revisar costos AWS (Lambda, CloudWatch)

### Trimestral (Cada 3 meses)

- [ ] Reporte ejecutivo de validaciones
- [ ] Identificar patrones de fallos
- [ ] Evaluar si umbral de alertas es apropiado
- [ ] Revisar permisos IAM (least privilege)
- [ ] Backup de reportes históricos

### Anual (Cada 12 meses)

- [ ] Auditoría completa de infraestructura
- [ ] Revisión de disaster recovery
- [ ] Planificación de mejoras
- [ ] Renovación de documentación

---

## Cambios Comunes

### Cambiar Schedule de Ejecución

```bash
# Opción A: Via CLI
aws events put-rule \
    --name validar-predicciones-weekly-prod \
    --schedule-expression "cron(0 3 ? * MON *)" \
    --state ENABLED

# Opción B: Via Terraform
# Editar terraform_prod.tfvars o main.tf
# Cambiar: schedule_expression = "cron(0 3 ? * MON *)"
# Ejecutar: terraform apply

# Nota: Cron expressions en EventBridge están en UTC
# Para cambiar a 03:00 AM UTC (22:00 UTC-5 domingo):
# Usar: cron(0 3 ? * MON *)
```

### Cambiar Umbral de Alertas

```bash
# Opción A: Via Lambda Environment
aws lambda update-function-configuration \
    --function-name validar-predicciones-prod \
    --environment Variables="{ALERT_THRESHOLD=100}"

# Opción B: Via CloudFormation
aws cloudformation update-stack \
    --stack-name validacion-predicciones-prod \
    --use-previous-template \
    --parameters ParameterKey=AlertThreshold,ParameterValue=100 \
    --capabilities CAPABILITY_NAMED_IAM

# Opción C: Via Terraform
# Editar terraform_prod.tfvars
# alert_threshold = 100
# terraform apply
```

### Agregar Nueva Suscripción SNS

```bash
# Email adicional
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod \
    --protocol email \
    --notification-endpoint otro-email@empresa.com

# Slack (requiere webhook)
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod \
    --protocol https \
    --notification-endpoint https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Aumentar Memoria/Timeout Lambda

```bash
# Via CLI
aws lambda update-function-configuration \
    --function-name validar-predicciones-prod \
    --memory-size 1024 \
    --timeout 1200

# Via Terraform
# Editar variables:
# lambda_memory = 1024
# lambda_timeout = 1200
# terraform apply
```

### Cambiar Email de Alertas

```bash
# CloudFormation: No es posible actualizar sin recrear
# Terraform:
aws sns unsubscribe --subscription-arn arn:aws:sns:...:validation-alerts-prod:abc123

aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod \
    --protocol email \
    --notification-endpoint nuevo-email@empresa.com
```

---

## Debugging

### Ejecutar Validación Manual con Parámetros Específicos

```bash
# Validar una semana específica
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --payload '{
        "desde": "20260901",
        "hasta": "20260907",
        "sigla": "SIGLA_A",
        "alert_threshold": 10
    }' \
    /tmp/manual_test.json \
    --region us-east-1

jq '.body | fromjson' /tmp/manual_test.json
```

### Ver CloudWatch Logs en Tiempo Real

```bash
# Stream live
aws logs tail /aws/lambda/validar-predicciones-prod --follow

# Filtrar por ERROR
aws logs tail /aws/lambda/validar-predicciones-prod \
    --filter-pattern '{ $.level = "ERROR" }' \
    --follow

# Ver logs de fecha específica
aws logs filter-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --start-time $(date -d "2026-09-14" +%s)000 \
    --end-time $(date -d "2026-09-15" +%s)000
```

### Analizar Performance

```bash
# Obtener estadísticas de duración (últimas 7 días)
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value=validar-predicciones-prod \
    --statistics Average,Maximum,Minimum \
    --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 604800 \  # 1 semana
    --region us-east-1 | jq '.Datapoints | sort_by(.Timestamp)'
```

### Verificar que S3 Access está OK

```bash
# Test read
aws s3 ls s3://pon-prod-ml-iapol/PrediccionMdP/ --recursive | head -10

# Test write
echo "test content" | aws s3 cp - s3://pon-prod-ml-iapol/ValidationReports/test.txt

# Cleanup
aws s3 rm s3://pon-prod-ml-iapol/ValidationReports/test.txt
```

---

## Escalada y Contactos

### Matriz de Escalada

| Tipo | Severidad | Tiempo | Contacto | Acción |
|------|-----------|--------|----------|--------|
| Lambda error | CRITICAL | Inmediato | DevOps Lead | Investigar logs, revisar IAM |
| S3 Connection | CRITICAL | Inmediato | Cloud Architect | Verificar bucket, permisos |
| Muy muchos errores | WARNING | 2 horas | Data Team Lead | Análisis de datos |
| Timeout | WARNING | 4 horas | Performance Engineer | Optimizar o escalar |
| Cambios solicitados | INFO | 1 día | Tech Lead | Planificar deployment |

### Contactos

```
DevOps Lead: devops-lead@empresa.com
Cloud Architect: cloud-arch@empresa.com
Data Team Lead: data-lead@empresa.com
Performance Engineer: perf-eng@empresa.com
Tech Lead: tech-lead@empresa.com
```

---

## Runbooks (Procedimientos Estandarizados)

### Runbook 1: Lambda falla constantemente

```
1. Ver últimos 10 logs
   aws logs tail /aws/lambda/validar-predicciones-prod --since 1h | tail -10

2. Buscar ERROR o EXCEPTION
   aws logs filter-log-events \
       --log-group-name /aws/lambda/validar-predicciones-prod \
       --filter-pattern 'ERROR'

3. Posibles causas:
   - Permiso S3: Revisar IAM policy
   - Código: Revisar código Lambda
   - Timeout: Aumentar timeout
   - Network: Check VPC (if applicable)

4. Si es code issue:
   - Revertir a versión anterior
   - Fix código en local
   - Deploy nueva versión
   
5. Si es permiso:
   - Revisar S3 bucket policy
   - Revisar IAM role policy
   - Test con aws s3 ls manualmente
```

### Runbook 2: SNS no envía emails

```
1. Verificar topic existe
   aws sns get-topic-attributes \
       --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod

2. Verificar suscripción
   aws sns list-subscriptions-by-topic \
       --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod

3. Buscar "PendingConfirmation"
   - Si sí: Revisar email de confirmación
   - Si no: Crear nueva suscripción

4. Test publicación
   aws sns publish \
       --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod \
       --subject "Test" \
       --message "Test message"

5. Si recibiste: SNS está OK, revisar filtros de email
   Si no: Contactar AWS Support
```

### Runbook 3: EventBridge no ejecuta Lambda

```
1. Verificar rule existe y está ENABLED
   aws events describe-rule --name validar-predicciones-weekly-prod

2. Verificar target
   aws events list-targets-by-rule --rule validar-predicciones-weekly-prod

3. Verificar Lambda permission
   aws lambda get-policy --function-name validar-predicciones-prod

4. Ver CloudWatch Logs para errors
   aws logs tail /aws/lambda/validar-predicciones-prod --since 24h

5. Force execution (testing)
   aws lambda invoke \
       --function-name validar-predicciones-prod \
       response.json
```

---

## Costos y Optimización

### Estimación de Costos Mensuales

```
Asumiendo 1 ejecución semanal (4/mes):

Lambda:
- Invocaciones: 4 × $0.20 = $0.80
- Compute (30s × 512MB): 4 × 30s × 512MB/1024 × $0.0000166667 = $0.10
- Subtotal: ~$1.00/mes

CloudWatch Logs:
- Ingestion: ~1000 logs × 4 × $0.50/GB = $0.002/mes
- Storage: 30 días × 30 KB = 900 KB = $0.005/mes
- Subtotal: ~$0.01/mes

SNS:
- Publicaciones: 4 × 1 × $0.50 per million = $0.000002/mes
- Subtotal: ~$0.00/mes

S3:
- Uploads: 4 × 2 files × 0.4 KB = negligible
- Subtotal: ~$0.01/mes

EventBridge:
- Rules: 1 × $1.00 = $1.00/mes
- Invocations: 4 × $0.10 = $0.40/mes
- Subtotal: $1.40/mes

TOTAL ESTIMADO: ~$2.50/mes
```

### Optimizaciones Posibles

1. **Reducir Memory Lambda**
   - Cambiar de 512 MB a 256 MB
   - Ahorro: ~50% en compute
   - Riesgo: Puede timeout si data crece

2. **Reducir Log Retention**
   - Cambiar de 30 a 7 días
   - Ahorro: ~80% en storage
   - Riesgo: No poder hacer debugging después

3. **Usar S3 Lifecycle Policy**
   ```json
   {
     "Rules": [
       {
         "Id": "DeleteOldReports",
         "Prefix": "ValidationReports/",
         "Status": "Enabled",
         "Expiration": { "Days": 90 }
       }
     ]
   }
   ```

---

## Disaster Recovery

### Backup de Configuración

```bash
# Exportar CloudFormation
aws cloudformation get-template \
    --stack-name validacion-predicciones-prod \
    --query-string TemplateBody > backup_cfn.yaml

# Exportar Terraform
cp terraform.tfstate terraform.tfstate.backup
cp terraform.tfvars terraform.tfvars.backup

# Exportar IAM policies
aws iam get-role-policy \
    --role-name lambda-validar-predicciones-prod \
    --policy-name S3ReadWrite > backup_s3_policy.json
```

### Restore desde Backup

```bash
# Opción 1: CloudFormation
aws cloudformation create-stack \
    --stack-name validacion-predicciones-prod-restore \
    --template-body file://backup_cfn.yaml

# Opción 2: Terraform
cp terraform.tfstate.backup terraform.tfstate
terraform apply
```

---

**Versión**: 1.0  
**Última actualización**: 2026-09-14  
**Próxima revisión**: 2026-12-14
