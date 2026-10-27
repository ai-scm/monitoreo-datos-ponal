# Guía de Deployment - Automatización de Validación de Predicciones
## Paso a Paso Completo

---

## Índice
1. [Prerequisitos](#prerequisitos)
2. [Opción A: CloudFormation](#opción-a-cloudformation)
3. [Opción B: Terraform](#opción-b-terraform)
4. [Opción C: Deployment Manual](#opción-c-deployment-manual)
5. [Verificación Post-Deploy](#verificación-post-deploy)
6. [Testing](#testing)
7. [Rollback](#rollback)

---

## Prerequisitos

### 1. Acceso AWS
- [ ] Credenciales AWS configuradas: `aws configure`
- [ ] Permisos para: Lambda, IAM, EventBridge, SNS, CloudWatch, S3
- [ ] Verificar: `aws sts get-caller-identity`

### 2. Herramientas Necesarias
```bash
# Verificar versiones
aws --version          # >= 2.13.0
terraform version      # >= 1.0.0 (si usar Terraform)
python3 --version      # >= 3.9
```

### 3. Preparar Variables
```bash
# Identificar valores necesarios
AWS_REGION="us-east-1"
ENVIRONMENT="prod"
S3_BUCKET="pon-prod-ml-iapol"  # Ya debe existir
ALERT_EMAIL="tu-email@empresa.com"
ALERT_THRESHOLD="50"
```

### 4. Verificar S3 Bucket
```bash
aws s3 ls s3://pon-prod-ml-iapol --recursive | head -10
# Output debe mostrar archivos en PrediccionMdP/
```

---

## Opción A: CloudFormation

### Paso 1: Preparar Código Lambda

```bash
# 1. Crear directorio de trabajo
mkdir -p validacion-predicciones/lambda
cd validacion-predicciones

# 2. Copiar el código Lambda
cp /ruta/validar_predicciones_lambda.py lambda/

# 3. Copiar el template CloudFormation
cp /ruta/cloudformation_template.yaml ./

# 4. Crear zip con código Lambda (para S3)
cd lambda
zip -r ../lambda_function.zip validar_predicciones_lambda.py
cd ..
```

### Paso 2: Validar Template

```bash
# Validar sintaxis del template
aws cloudformation validate-template \
    --template-body file://cloudformation_template.yaml \
    --region us-east-1

# Output esperado: ParameterValidation: PASSED
```

### Paso 3: Crear el Stack

#### Opción 3a: Desde CLI

```bash
aws cloudformation create-stack \
    --stack-name validacion-predicciones-prod \
    --template-body file://cloudformation_template.yaml \
    --parameters \
        ParameterKey=Environment,ParameterValue=prod \
        ParameterKey=S3Bucket,ParameterValue=pon-prod-ml-iapol \
        ParameterKey=AlertEmail,ParameterValue=tu-email@empresa.com \
        ParameterKey=AlertThreshold,ParameterValue=50 \
        ParameterKey=LogRetentionDays,ParameterValue=30 \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1 \
    --tags Key=Environment,Value=prod Key=Application,Value=ValidacionPredicciones

# Output: StackId: arn:aws:cloudformation:...
STACK_ID="arn:aws:cloudformation:us-east-1:ACCOUNT:stack/validacion-predicciones-prod/..."
```

#### Opción 3b: Desde AWS Console
1. Ir a CloudFormation
2. Click "Create Stack"
3. Upload template file: `cloudformation_template.yaml`
4. Llenar parámetros
5. Click "Create"

### Paso 4: Monitorear Creación

```bash
# Ver eventos del stack
aws cloudformation describe-stack-events \
    --stack-name validacion-predicciones-prod \
    --region us-east-1

# Esperar a que finalice (5-10 minutos)
aws cloudformation wait stack-create-complete \
    --stack-name validacion-predicciones-prod \
    --region us-east-1

echo $?  # 0 = éxito, 254 = falló
```

### Paso 5: Actualizar Código Lambda

El CloudFormation crea Lambda con código placeholder. Actualizar con el código real:

```bash
# Opción A: Via CLI
aws lambda update-function-code \
    --function-name validar-predicciones-prod \
    --zip-file fileb://lambda_function.zip \
    --region us-east-1

# Opción B: Via S3 (para archivos > 50MB)
# 1. Subir ZIP a S3
aws s3 cp lambda_function.zip s3://pon-prod-ml-iapol/lambda-code/

# 2. Actualizar Lambda
aws lambda update-function-code \
    --function-name validar-predicciones-prod \
    --s3-bucket pon-prod-ml-iapol \
    --s3-key lambda-code/lambda_function.zip \
    --region us-east-1
```

### Paso 6: Obtener Outputs

```bash
aws cloudformation describe-stacks \
    --stack-name validacion-predicciones-prod \
    --query "Stacks[0].Outputs" \
    --region us-east-1 \
    --output table
```

Expected output:
```
LambdaFunctionArn    | arn:aws:lambda:us-east-1:ACCOUNT:function:validar-predicciones-prod
LambdaFunctionName   | validar-predicciones-prod
SNSTopicArn          | arn:aws:sns:us-east-1:ACCOUNT:validation-alerts-prod
EventBridgeRuleArn   | arn:aws:events:us-east-1:ACCOUNT:rule/validar-predicciones-weekly-prod
LogGroupName         | /aws/lambda/validar-predicciones-prod
DashboardURL         | https://console.aws.amazon.com/cloudwatch/...
```

---

## Opción B: Terraform

### Paso 1: Preparar Directorio

```bash
# Crear estructura
mkdir -p validacion-predicciones/terraform
cd validacion-predicciones/terraform

# Copiar archivos
cp /ruta/terraform_main.tf ./
cp /ruta/terraform_prod.tfvars ./
cp /ruta/validar_predicciones_lambda.py ./
```

### Paso 2: Inicializar Terraform

```bash
# Descargar providers y módulos
terraform init

# Expected output:
# Terraform has been successfully configured!
# 
# Initializing the backend...
# Initializing provider plugins...
```

### Paso 3: Planificar Deployment

```bash
# Ver cambios que se van a hacer
terraform plan -var-file=terraform_prod.tfvars -out=tfplan

# Revisar output cuidadosamente
# Expected resources:
#   + aws_lambda_function.validacion_predicciones
#   + aws_iam_role.lambda_role
#   + aws_iam_role.eventbridge_role
#   + aws_sns_topic.validation_alerts
#   + aws_cloudwatch_event_rule.validacion_schedule
#   + aws_cloudwatch_log_group.lambda_logs
#   + aws_cloudwatch_metric_alarm.lambda_errors (3x)
#   + aws_cloudwatch_dashboard.validacion
```

### Paso 4: Aplicar Configuración

```bash
# Crear recurso (debe confirmar)
terraform apply tfplan

# Output esperado:
# Apply complete! Resources: 15 added, 0 changed, 0 destroyed.
#
# Outputs:
#
# lambda_function_arn = "arn:aws:lambda:..."
# lambda_function_name = "validar-predicciones-prod"
# ...
```

### Paso 5: Guardar Estado

```bash
# Respaldar estado local
cp terraform.tfstate terraform.tfstate.backup

# Opcional: Mover a S3 backend (ver comentarios en terraform_main.tf)
# Esto permite colaboración en equipo
```

### Paso 6: Verificar Recursos

```bash
# Ver todos los recursos creados
terraform show

# Ver outputs
terraform output

# Específico
terraform output lambda_function_name
terraform output sns_topic_arn
```

---

## Opción C: Deployment Manual

Para ambientes con restricciones de herramientas, hacer deployment manual desde AWS Console:

### Paso 1: Crear IAM Role

1. Ir a IAM → Roles → Create role
2. Service: Lambda
3. Policies:
   - `AWSLambdaBasicExecutionRole` (adjunta)
   - Crear inline policy "S3ReadWrite":
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "S3ListAndRead",
         "Effect": "Allow",
         "Action": ["s3:ListBucket", "s3:GetObject"],
         "Resource": [
           "arn:aws:s3:::pon-prod-ml-iapol",
           "arn:aws:s3:::pon-prod-ml-iapol/*"
         ]
       },
       {
         "Sid": "S3WriteResults",
         "Effect": "Allow",
         "Action": ["s3:PutObject"],
         "Resource": "arn:aws:s3:::pon-prod-ml-iapol/ValidationReports/*"
       }
     ]
   }
   ```
   - Crear inline policy "SNSPublish":
   ```json
   {
     "Effect": "Allow",
     "Action": "sns:Publish",
     "Resource": "arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod"
   }
   ```
4. Name: `lambda-validar-predicciones-prod`
5. Create

### Paso 2: Crear SNS Topic

1. SNS → Topics → Create topic
2. Name: `validation-alerts-prod`
3. Create
4. Create subscription:
   - Protocol: Email
   - Endpoint: tu-email@empresa.com
   - Confirm subscription (revisar email)

### Paso 3: Crear CloudWatch Log Group

1. CloudWatch → Logs → Log groups → Create log group
2. Name: `/aws/lambda/validar-predicciones-prod`
3. Set retention: 30 days
4. Create

### Paso 4: Crear Lambda Function

1. Lambda → Functions → Create function
2. Author from scratch
3. Function name: `validar-predicciones-prod`
4. Runtime: Python 3.11
5. Execution role: `lambda-validar-predicciones-prod` (creado arriba)
6. Code source:
   - Copy-paste código de `validar_predicciones_lambda.py`
7. Configuration:
   - General → Memory: 512 MB
   - General → Timeout: 15 min
   - Environment variables:
     ```
     S3_BUCKET = pon-prod-ml-iapol
     AWS_REGION = us-east-1
     SNS_TOPIC_ARN = arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod
     ALERT_THRESHOLD = 50
     ENVIRONMENT = prod
     ```
8. Deploy

### Paso 5: Crear EventBridge Rule

1. EventBridge → Rules → Create rule
2. Name: `validar-predicciones-weekly-prod`
3. Event bus: default
4. Rule type: Schedule
5. Schedule: Cron expression
   - Expression: `cron(0 7 ? * MON *)`  # Lunes 07:00 UTC
6. Target:
   - Target type: AWS service
   - Service: Lambda function
   - Function: `validar-predicciones-prod`
   - Create new role: `eventbridge-lambda-validar-predicciones-prod`
   - Input: JSON
   ```json
   {
     "desde": "AUTO",
     "hasta": null,
     "sigla": null,
     "alert_threshold": 50
   }
   ```
7. Create

### Paso 6: Crear CloudWatch Alarms

1. CloudWatch → Alarms → Create alarm

**Alarm 1: Errores**
- Select metric: Lambda → Errors
- Dimensions: FunctionName = validar-predicciones-prod
- Statistic: Sum
- Period: 5 min
- Threshold: >= 1
- Alarm actions: validation-alerts-prod topic
- Create

**Alarm 2: Timeout**
- Select metric: Lambda → Duration
- Statistic: Maximum
- Threshold: 850000 (ms)
- Create

**Alarm 3: Duración alta**
- Select metric: Lambda → Duration
- Statistic: Average
- Threshold: 120000 (ms)
- Create

### Paso 7: Crear Dashboard

1. CloudWatch → Dashboards → Create dashboard
2. Name: `ValidacionPredicciones-prod`
3. Add widget:
   - Type: Line
   - Metrics: Lambda Invocations, Errors, Duration (Average)
   - Function: validar-predicciones-prod
4. Save

---

## Verificación Post-Deploy

### 1. Verificar Componentes Creados

```bash
# Lambda
aws lambda get-function --function-name validar-predicciones-prod

# EventBridge
aws events describe-rule --name validar-predicciones-weekly-prod

# SNS
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod

# CloudWatch Logs
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/validar-predicciones

# CloudWatch Alarms
aws cloudwatch describe-alarms --alarm-names validar-predicciones-errors-prod
```

### 2. Verificar Permisos

```bash
# Verificar que Lambda puede leer S3
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --payload '{"test": "true"}' \
    response.json
cat response.json

# Verificar que EventBridge puede invocar Lambda
aws lambda list-event-source-mappings \
    --function-name validar-predicciones-prod

# Verificar IAM Role
aws iam get-role --role-name lambda-validar-predicciones-prod
```

### 3. Verificar S3 Access

```bash
# Test que Lambda puede escribir en S3
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --payload '{"desde": "20260901", "hasta": "20260907"}' \
    /tmp/response.json

# Verificar si se crearon archivos de prueba
aws s3 ls s3://pon-prod-ml-iapol/ValidationReports/
```

---

## Testing

### Test 1: Invocación Manual

```bash
# Básico
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --region us-east-1 \
    /tmp/test_output.json

cat /tmp/test_output.json | jq .

# Con parámetros específicos
aws lambda invoke \
    --function-name validar-predicciones-prod \
    --payload '{
        "desde": "20260901",
        "hasta": "20260907",
        "sigla": null,
        "alert_threshold": 10
    }' \
    --region us-east-1 \
    /tmp/test_output.json

jq '.body | fromjson' /tmp/test_output.json
```

### Test 2: Verificar Logs

```bash
# Ver últimos logs
aws logs tail /aws/lambda/validar-predicciones-prod --follow

# Filtrar por ERROR
aws logs filter-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --filter-pattern "ERROR"

# Descargar logs del último día
aws logs get-log-events \
    --log-group-name /aws/lambda/validar-predicciones-prod \
    --log-stream-name '$(aws logs describe-log-streams --log-group-name /aws/lambda/validar-predicciones-prod --query "logStreams[0].logStreamName" --output text)'
```

### Test 3: Verificar SNS

```bash
# Publicar mensaje de prueba
aws sns publish \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod \
    --subject "Test SNS" \
    --message "Este es un mensaje de prueba"

# Verificar suscripciones
aws sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod
```

### Test 4: Simular Ejecución EventBridge

```bash
# EventBridge no tiene "test" directo, pero podemos:
# 1. Esperar al siguiente lunes a las 02:00 AM
# 2. O invocar manualmente
# 3. O modificar rule temporalmente

# Modificar rule a cada 5 minutos (testing)
aws events put-rule \
    --name validar-predicciones-weekly-prod \
    --schedule-expression "rate(5 minutes)" \
    --state ENABLED

# Esperar 5 minutos, verificar logs
aws logs tail /aws/lambda/validar-predicciones-prod --follow --since 5m

# Revertir a schedule original
aws events put-rule \
    --name validar-predicciones-weekly-prod \
    --schedule-expression "cron(0 7 ? * MON *)" \
    --state ENABLED
```

---

## Rollback

### Opción A: CloudFormation Rollback

```bash
# Revertir stack a versión anterior
aws cloudformation cancel-update-stack \
    --stack-name validacion-predicciones-prod

# O eliminar completamente
aws cloudformation delete-stack \
    --stack-name validacion-predicciones-prod

# Esperar a que se elimine
aws cloudformation wait stack-delete-complete \
    --stack-name validacion-predicciones-prod
```

### Opción B: Terraform Rollback

```bash
# Ver historial de cambios
terraform show -json | jq '.values.outputs'

# Revertir a snapshot anterior
terraform apply -target=aws_lambda_function.validacion_predicciones \
    -var-file=terraform_prod.tfvars \
    -refresh=true

# Destruir completamente
terraform destroy -var-file=terraform_prod.tfvars -auto-approve
```

### Opción C: Manual Rollback

1. Deshabilitar EventBridge rule
2. Eliminar recursos en orden inverso:
   - CloudWatch Alarms
   - EventBridge Rule
   - Lambda Function
   - SNS Topic
   - IAM Role
   - CloudWatch Log Group

```bash
# Script de cleanup
aws events delete-rule --name validar-predicciones-weekly-prod
aws lambda delete-function --function-name validar-predicciones-prod
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:validation-alerts-prod
aws iam delete-role --role-name lambda-validar-predicciones-prod
aws logs delete-log-group --log-group-name /aws/lambda/validar-predicciones-prod
```

---

## Checklist de Post-Deployment

- [ ] Lambda function creada y testeable
- [ ] IAM role con permisos correctos
- [ ] EventBridge rule habilitada y programada
- [ ] SNS topic creado con suscriptor de email confirmado
- [ ] CloudWatch Logs group creado con retención 30 días
- [ ] CloudWatch Alarms configuradas (3 total)
- [ ] Dashboard visible
- [ ] Test manual exitoso
- [ ] Logs verificados
- [ ] Documentación actualizada
- [ ] Equipo notificado de nueva automatización

---

## Troubleshooting Común

### Problema: Lambda timeout
**Solución:**
- Aumentar timeout: `aws lambda update-function-configuration --function-name validar-predicciones-prod --timeout 1200`
- Revisar logs: `aws logs tail /aws/lambda/validar-predicciones-prod --follow`

### Problema: Permission Denied en S3
**Solución:**
- Verificar IAM policy: `aws iam get-role-policy --role-name lambda-validar-predicciones-prod --policy-name S3Policy`
- Verificar bucket exists: `aws s3 ls s3://pon-prod-ml-iapol`

### Problema: SNS no envía emails
**Solución:**
- Verificar suscripción: `aws sns list-subscriptions-by-topic --topic-arn <arn>`
- Confirmar email subscription (revisar spam)
- Probar publicación: `aws sns publish --topic-arn <arn> --message "test"`

### Problema: EventBridge no invoca Lambda
**Solución:**
- Verificar rule está ENABLED: `aws events describe-rule --name validar-predicciones-weekly-prod`
- Verificar target: `aws events list-targets-by-rule --rule validar-predicciones-weekly-prod`
- Verificar Lambda permission: `aws lambda get-policy --function-name validar-predicciones-prod`

---

**Versión**: 1.0  
**Última actualización**: 2026-09-14
