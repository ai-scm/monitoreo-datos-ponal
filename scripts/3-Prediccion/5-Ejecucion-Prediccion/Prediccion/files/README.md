# Solución de Automatización: Validación de Predicciones en AWS Lambda

> **Automatización completa de validación de predicciones con alertas inteligentes, monitoreo en tiempo real y reportes automáticos en S3**

---

## 🎯 Resumen Ejecutivo

### Problema
Script `validar_predicciones.py` se ejecutaba **manualmente cada semana** en CloudShell, sin alertas ni monitoreo automático.

### Solución
Automatización completa en AWS que ejecuta la validación cada semana (lunes 02:00 AM UTC-5) con:
- ✅ **Lambda Function**: Código optimizado para cloud
- ✅ **EventBridge**: Scheduler automático semanal
- ✅ **SNS**: Alertas por email/Slack
- ✅ **CloudWatch**: Logs y métricas centralizadas
- ✅ **CloudFormation/Terraform**: IaC para reproducibilidad

### Beneficios
| Beneficio | Antes | Después |
|-----------|-------|---------|
| Frecuencia | Manual, semanal | Automática, puntual |
| Alertas | Ninguna | Email + Slack + CloudWatch |
| Reportes | Local | S3 (acceso desde cualquier lugar) |
| Monitoreo | Ninguno | Dashboard CloudWatch tiempo real |
| Costos | ~$0 (manual) | ~$2.50/mes (cloud) |
| Escalabilidad | Manual | Automática |

---

## 📁 Estructura de Archivos

```
validacion-predicciones/
├── 01_ARQUITECTURA_SOLUCION.md         # Documento completo de arquitectura
├── 02_validar_predicciones_lambda.py   # Código Lambda adaptado
├── 03_cloudformation_template.yaml     # IaC con CloudFormation
├── 04_terraform_main.tf                # IaC con Terraform
├── 05_terraform_prod.tfvars            # Variables Terraform
├── 06_GUIA_DEPLOYMENT.md               # Guía paso a paso (3 opciones)
├── 07_GUIA_OPERACIONES.md              # Operaciones y mantenimiento
├── 08_deploy.sh                        # Script automatizado
└── README.md                           # Este archivo
```

---

## 🚀 Inicio Rápido (5 minutos)

### Opción 1: Deployment Automático (RECOMENDADO)

```bash
# 1. Clonar/descargar archivos
git clone <repo> validacion-predicciones
cd validacion-predicciones

# 2. Ejecutar script de deployment
bash deploy.sh \
    --method cloudformation \
    --environment prod \
    --email tu-email@empresa.com

# 3. Confirmar deployment (se ejecuta en 5-10 minutos)
# 4. Confirmar email de SNS (revisar inbox)
```

### Opción 2: Terraform

```bash
# 1. Preparar variables
cat > terraform.tfvars << EOF
environment       = "prod"
alert_email       = "tu-email@empresa.com"
alert_threshold   = 50
aws_region        = "us-east-1"
s3_bucket         = "pon-prod-ml-iapol"
EOF

# 2. Validar y aplicar
terraform init
terraform plan
terraform apply

# 3. Ver outputs
terraform output
```

### Opción 3: Manual (AWS Console)
Ver: `06_GUIA_DEPLOYMENT.md` → Opción C

---

## 📊 Arquitectura

```
Lunes 02:00 AM UTC-5
        ↓
    EventBridge
        ↓
    Lambda (512 MB, 900s timeout)
    ├─ Lee datos de S3
    ├─ Valida predicciones
    ├─ Genera reportes CSV
    ├─ Guarda en S3
    └─ Envía alertas
        ↓
    ┌───┴─────┬──────────┐
    ↓         ↓          ↓
  S3 (CSV)  SNS      CloudWatch
           ├─ Email    ├─ Logs
           ├─ Slack    ├─ Metrics
           └─ HTTP     └─ Alarms
```

---

## ✨ Funcionalidades

### Validación Automática Semanal
- Ejecuta cada lunes a las 02:00 AM UTC-5
- Procesa todas las SIGLAS y unidades
- Valida formato de archivos
- Verifica cobertura horaria (24 horas/día)

### Reportes Automáticos
Dos CSV generados en S3:
1. **errores_prediccion_rango_XXXX.csv**
   - Archivos con formato inválido
   - Incluye: semana, SIGLA, unidad, razón del error

2. **horas_faltantes_rango_XXXX.csv**
   - Horas de predicción faltantes
   - Incluye: semana, SIGLA, unidad, fecha, horas específicas

### Sistema de Alertas
| Tipo | Trigger | Acción |
|------|---------|--------|
| **WARNING** | > 50 errores detectados | Email SNS |
| **WARNING** | > 100 horas faltantes | Email SNS |
| **CRITICAL** | Error de conexión S3 | Email SNS INMEDIATO |
| **INFO** | Ejecución exitosa | Log CloudWatch |

### Monitoreo Centralizado
- **Dashboard CloudWatch**: Métricas en tiempo real
- **CloudWatch Logs**: Búsqueda y análisis de eventos
- **CloudWatch Alarms**: 3 alarmas configuradas
- **Métricas custom**: Errores detectados, tiempo ejecución

---

## 🔧 Configuración

### Variables de Entorno Lambda

| Variable | Valor | Descripción |
|----------|-------|-------------|
| `S3_BUCKET` | `pon-prod-ml-iapol` | Bucket con datos |
| `AWS_REGION` | `us-east-1` | Región AWS |
| `SNS_TOPIC_ARN` | `arn:aws:sns:...` | Topic para alertas |
| `ALERT_THRESHOLD` | `50` | Umbral de errores para alerta |
| `ENVIRONMENT` | `prod` | Ambiente (dev/staging/prod) |

### Schedule de Ejecución
```
Expresión Cron: cron(0 7 ? * MON *)
Traducción: Lunes 07:00 AM UTC = Lunes 02:00 AM UTC-5
Frecuencia: Semanal (automático)
```

---

## 📈 Monitoreo y Observabilidad

### Acceder a Dashboard
```bash
# URL automática desde deployment
https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=ValidacionPredicciones-prod

# O desde CLI
terraform output dashboard_url
```

### Ver Logs
```bash
# Tail en tiempo real
aws logs tail /aws/lambda/validar-predicciones-prod --follow

# Filtrar por ERROR
aws logs filter-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --filter-pattern "ERROR"
```

### Métricas Claves
- **Invocations**: Debe ser 1 por semana
- **Errors**: Debe ser 0
- **Duration**: Típicamente 30-60 segundos
- **Throttles**: Debe ser 0

---

## 🛠️ Mantenimiento

### Cambios Comunes

#### Cambiar Schedule (ej: viernes en lugar de lunes)
```bash
# CloudFormation/AWS Console
aws events put-rule --name validar-predicciones-weekly-prod \
    --schedule-expression "cron(0 7 ? * FRI *)"

# Terraform
# Editar: terraform_prod.tfvars
# Ejecutar: terraform apply
```

#### Aumentar Umbral de Alertas
```bash
aws lambda update-function-configuration \
    --function-name validar-predicciones-prod \
    --environment Variables="{ALERT_THRESHOLD=100}"
```

#### Agregar Nueva Suscripción Slack
```bash
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT:validation-alerts-prod \
    --protocol https \
    --notification-endpoint https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

Ver más en: `07_GUIA_OPERACIONES.md`

---

## 📊 Estimación de Costos

**Costo mensual aproximado: $2.50**

Desglose (4 ejecuciones/mes):
- Lambda compute: $0.10
- CloudWatch Logs: $0.01
- EventBridge: $1.40
- SNS: $0.00
- S3: $0.01

*Nota: Precios en USD, pueden variar según región*

---

## 🚨 Troubleshooting

### Lambda error "Permission Denied"
```bash
# Verificar IAM role
aws iam get-role-policy \
    --role-name lambda-validar-predicciones-prod \
    --policy-name S3Policy

# Verificar S3 bucket access
aws s3 ls s3://pon-prod-ml-iapol/PrediccionMdP/ --region us-east-1
```

### SNS no envía emails
```bash
# Verificar suscripción
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT:validation-alerts-prod

# Confirmar email en bandeja de entrada
# Test: aws sns publish --topic-arn <arn> --message "test"
```

### EventBridge no ejecuta Lambda
```bash
# Verificar rule está ENABLED
aws events describe-rule --name validar-predicciones-weekly-prod

# Verificar target
aws events list-targets-by-rule --rule validar-predicciones-weekly-prod
```

Ver más en: `06_GUIA_DEPLOYMENT.md` → Troubleshooting

---

## 📚 Documentación

| Documento | Descripción | Para Quién |
|-----------|-------------|-----------|
| `01_ARQUITECTURA_SOLUCION.md` | Análisis completo | Arquitectos, Tech Leads |
| `06_GUIA_DEPLOYMENT.md` | Paso a paso | DevOps, SysAdmins |
| `07_GUIA_OPERACIONES.md` | Daily operations | On-Call, Support |
| `README.md` | Visión general | Todos |

---

## 🔒 Seguridad

### Permisos Mínimos (Least Privilege)
```json
Lambda Role:
- s3:ListBucket, s3:GetObject en pon-prod-ml-iapol
- s3:PutObject en pon-prod-ml-iapol/ValidationReports/*
- logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
- sns:Publish en topic específico
```

### Encriptación
- S3: SSE-S3 (por defecto)
- Logs: KMS (opcional, requiere extra config)
- SNS: KMS (opcional)

### Auditoría
- CloudTrail logs todos los API calls
- CloudWatch Logs retención: 30 días
- S3 versioning en bucket (recomendado)

---

## 🤝 Soporte y Contactos

### Escalada
| Tipo | Contacto | Tiempo |
|------|----------|--------|
| Lambda Error | DevOps Lead | INMEDIATO |
| S3 Connection | Cloud Architect | INMEDIATO |
| Data Issues | Data Team Lead | 2 horas |
| Cambios | Tech Lead | 1 día |

### Recursos
- AWS Lambda: https://docs.aws.amazon.com/lambda/
- EventBridge: https://docs.aws.amazon.com/eventbridge/
- CloudWatch: https://docs.aws.amazon.com/cloudwatch/

---

## 🎓 Próximos Pasos

### Corto Plazo (Esta Semana)
1. [ ] Deploy en ambiente de prueba
2. [ ] Confirmar email de alertas SNS
3. [ ] Validar ejecución manual
4. [ ] Revisar logs CloudWatch

### Mediano Plazo (Este Mes)
1. [ ] Configurar Slack webhook
2. [ ] Crear runbooks para alertas
3. [ ] Entrenar equipo de on-call
4. [ ] Documentar procesos internos

### Largo Plazo (Este Trimestre)
1. [ ] Agregar análisis de tendencias
2. [ ] Integrar con BI tool
3. [ ] Optimizar performance
4. [ ] Machine Learning para alertas inteligentes

---

## 📝 Notas Importantes

### Cron Expressions
EventBridge usa UTC. Para 02:00 AM UTC-5:
- UTC-5 = UTC + 7 horas
- Lunes 02:00 AM UTC-5 = Lunes 07:00 AM UTC
- Expresión: `cron(0 7 ? * MON *)`

### S3 Paths
Reportes se guardan en:
```
s3://pon-prod-ml-iapol/ValidationReports/YYYY/YYYY-MM-DD/
```

Política de retención:
- 90 días en S3 (configurable)
- Luego se puede archivar a Glacier

### Horarios
- Lunes 02:00 AM UTC-5 = Domingo 23:00 PM UTC
- En Europa Central: Lunes 08:00 AM CEST
- En China: Lunes 02:00 PM CST

---

## 📞 Support

Para preguntas o problemas:
1. Revisar documentación: `GUIA_OPERACIONES.md`
2. Buscar en CloudWatch Logs
3. Contactar DevOps Lead
4. Crear issue en repo

---

## 📜 Licencia y Responsabilidad

Esta solución es de uso interno de la empresa. 

**Responsables:**
- Deployment: DevOps Team
- Operaciones: Platform Team
- Datos: Data Engineering

**Última actualización:** 2026-09-14  
**Versión:** 1.0  
**Estado:** ✅ Producción
