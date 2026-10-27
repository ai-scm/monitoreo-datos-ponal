#!/bin/bash
# ============================================================================
# Script de Deployment Automatizado
# Solución de Validación Automática de Predicciones en AWS Lambda
# ============================================================================
# 
# Uso:
#   bash deploy.sh --method cloudformation --environment prod --email tu@empresa.com
#   bash deploy.sh --method terraform --environment prod
#   bash deploy.sh --help

set -euo pipefail

# ============================================================================
# Configuración por defecto
# ============================================================================

METHOD="cloudformation"  # cloudformation | terraform | manual
ENVIRONMENT="prod"
AWS_REGION="us-east-1"
S3_BUCKET="pon-prod-ml-iapol"
ALERT_EMAIL=""
ALERT_THRESHOLD="50"
VERBOSE=false
DRY_RUN=false

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

# ============================================================================
# Funciones Utilitarias
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

print_help() {
    cat << EOF
Deployment Script para Validación de Predicciones

Uso: bash deploy.sh [opciones]

Opciones:
    --method METHOD              cloudformation | terraform | manual [default: cloudformation]
    --environment ENV            dev | staging | prod [default: prod]
    --email EMAIL                Email para alertas (REQUERIDO si --method != terraform)
    --region REGION              AWS region [default: us-east-1]
    --bucket BUCKET              S3 bucket [default: pon-prod-ml-iapol]
    --threshold THRESHOLD        Umbral de alertas [default: 50]
    --verbose                    Output detallado
    --dry-run                    No aplicar cambios, solo mostrar
    --help                       Mostrar esta ayuda

Ejemplos:
    # CloudFormation deployment
    bash deploy.sh --method cloudformation --email ops@empresa.com

    # Terraform deployment (lee variables de terraform.tfvars)
    bash deploy.sh --method terraform

    # Test sin aplicar cambios
    bash deploy.sh --method cloudformation --email test@empresa.com --dry-run

EOF
    exit 0
}

# ============================================================================
# Parsear argumentos
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --method)
            METHOD="$2"
            shift 2
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --email)
            ALERT_EMAIL="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        --threshold)
            ALERT_THRESHOLD="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            print_help
            ;;
        *)
            log_error "Opción desconocida: $1"
            print_help
            ;;
    esac
done

# ============================================================================
# Validaciones Previas
# ============================================================================

log_info "Iniciando deployment con method=$METHOD, environment=$ENVIRONMENT"

# Validar que método es válido
if ! [[ "$METHOD" =~ ^(cloudformation|terraform|manual)$ ]]; then
    log_error "Método inválido: $METHOD"
    exit 1
fi

# Validar que environment es válido
if ! [[ "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Environment inválido: $ENVIRONMENT"
    exit 1
fi

# Validar AWS CLI
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI no encontrado. Instalar: pip install awscli"
    exit 1
fi

# Validar credenciales AWS
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "No hay credenciales AWS configuradas. Ejecutar: aws configure"
    exit 1
fi

# Validar S3 bucket existe
log_info "Validando acceso a S3 bucket..."
if ! aws s3 ls "s3://$S3_BUCKET" --region "$AWS_REGION" &> /dev/null; then
    log_error "No se puede acceder a bucket s3://$S3_BUCKET"
    exit 1
fi

# Validar email (si se requiere)
if [[ -z "$ALERT_EMAIL" && "$METHOD" != "terraform" ]]; then
    log_error "Email es requerido (--email). Ejecutar: bash deploy.sh --help"
    exit 1
fi

# Validar email formato
if [[ -n "$ALERT_EMAIL" ]] && ! [[ "$ALERT_EMAIL" =~ ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$ ]]; then
    log_error "Formato de email inválido: $ALERT_EMAIL"
    exit 1
fi

log_success "Todas las validaciones previas pasaron"

# ============================================================================
# Resumen de Configuración
# ============================================================================

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        DEPLOYMENT CONFIGURATION                               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Method:             $METHOD"
echo "  Environment:        $ENVIRONMENT"
echo "  AWS Region:         $AWS_REGION"
echo "  S3 Bucket:          $S3_BUCKET"
if [[ -n "$ALERT_EMAIL" ]]; then
    echo "  Alert Email:        $ALERT_EMAIL"
fi
echo "  Alert Threshold:    $ALERT_THRESHOLD"
echo "  Dry Run:            $DRY_RUN"
echo ""

if [[ "$DRY_RUN" == true ]]; then
    echo -e "${YELLOW}DRY RUN MODE - No se aplicarán cambios${NC}"
    echo ""
fi

# Pedir confirmación
read -p "¿Continuar? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "Deployment cancelado por usuario"
    exit 0
fi

# ============================================================================
# Deployment Según Método
# ============================================================================

case $METHOD in
    cloudformation)
        log_info "Iniciando deployment con CloudFormation..."
        deploy_cloudformation
        ;;
    terraform)
        log_info "Iniciando deployment con Terraform..."
        deploy_terraform
        ;;
    manual)
        log_info "Mostrando pasos para deployment manual..."
        deploy_manual
        ;;
esac

# ============================================================================
# Funciones de Deployment
# ============================================================================

deploy_cloudformation() {
    STACK_NAME="validacion-predicciones-$ENVIRONMENT"
    
    log_info "Validando template CloudFormation..."
    if ! aws cloudformation validate-template \
        --template-body file://cloudformation_template.yaml \
        --region "$AWS_REGION" > /dev/null; then
        log_error "Template CloudFormation inválido"
        exit 1
    fi
    
    log_info "Preparando parámetros..."
    PARAMS=(
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
        "ParameterKey=S3Bucket,ParameterValue=$S3_BUCKET"
        "ParameterKey=AlertEmail,ParameterValue=$ALERT_EMAIL"
        "ParameterKey=AlertThreshold,ParameterValue=$ALERT_THRESHOLD"
    )
    
    log_info "Verificando si stack existe..."
    if aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        &> /dev/null; then
        
        log_info "Stack existe, actualizando..."
        if [[ "$DRY_RUN" == true ]]; then
            log_warning "DRY RUN: No se ejecutará update-stack"
        else
            aws cloudformation update-stack \
                --stack-name "$STACK_NAME" \
                --template-body file://cloudformation_template.yaml \
                --parameters "${PARAMS[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --region "$AWS_REGION"
            
            log_success "Stack update iniciado: $STACK_NAME"
            log_info "Esperando que se complete..."
            
            aws cloudformation wait stack-update-complete \
                --stack-name "$STACK_NAME" \
                --region "$AWS_REGION"
        fi
    else
        log_info "Stack no existe, creando..."
        if [[ "$DRY_RUN" == true ]]; then
            log_warning "DRY RUN: No se ejecutará create-stack"
        else
            aws cloudformation create-stack \
                --stack-name "$STACK_NAME" \
                --template-body file://cloudformation_template.yaml \
                --parameters "${PARAMS[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --region "$AWS_REGION" \
                --tags Key=Environment,Value="$ENVIRONMENT" Key=Application,Value=ValidacionPredicciones
            
            log_success "Stack creation iniciado: $STACK_NAME"
            log_info "Esperando que se complete (5-10 minutos)..."
            
            aws cloudformation wait stack-create-complete \
                --stack-name "$STACK_NAME" \
                --region "$AWS_REGION"
        fi
    fi
    
    log_success "Obteniendo outputs..."
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query "Stacks[0].Outputs" \
        --region "$AWS_REGION" \
        --output table
}

deploy_terraform() {
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform no encontrado. Instalar: https://www.terraform.io/downloads.html"
        exit 1
    fi
    
    log_info "Inicializando Terraform..."
    if [[ "$DRY_RUN" == true ]]; then
        log_warning "DRY RUN: No se ejecutará terraform init"
    else
        terraform init
    fi
    
    log_info "Planificando cambios..."
    if [[ "$DRY_RUN" == true ]]; then
        log_warning "DRY RUN: No se ejecutará terraform plan"
    else
        TFVARS_FILE="terraform_${ENVIRONMENT}.tfvars"
        
        if [[ ! -f "$TFVARS_FILE" ]]; then
            log_error "File $TFVARS_FILE no encontrado"
            exit 1
        fi
        
        terraform plan -var-file="$TFVARS_FILE" -out=tfplan
        
        log_info "Aplicando cambios..."
        terraform apply tfplan
        
        log_success "Terraform deployment completado"
        terraform output
    fi
}

deploy_manual() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║             DEPLOYMENT MANUAL - PASOS A SEGUIR                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "1. CREAR IAM ROLE"
    echo "   - Ir a AWS Console > IAM > Roles > Create role"
    echo "   - Service: Lambda"
    echo "   - Adjuntar policies:"
    echo "     * AWSLambdaBasicExecutionRole"
    echo "     * Crear inline policies: S3ReadWrite, SNSPublish"
    echo ""
    echo "2. CREAR SNS TOPIC"
    echo "   - SNS > Topics > Create topic"
    echo "   - Name: validation-alerts-$ENVIRONMENT"
    echo "   - Subscribe con email: $ALERT_EMAIL"
    echo ""
    echo "3. CREAR LAMBDA FUNCTION"
    echo "   - Lambda > Functions > Create function"
    echo "   - Runtime: Python 3.11"
    echo "   - Role: lambda-validar-predicciones (creado en paso 1)"
    echo "   - Upload code: validar_predicciones_lambda.py"
    echo "   - Environment variables:"
    echo "     * S3_BUCKET=$S3_BUCKET"
    echo "     * AWS_REGION=$AWS_REGION"
    echo "     * ALERT_THRESHOLD=$ALERT_THRESHOLD"
    echo ""
    echo "4. CREAR EVENTBRIDGE RULE"
    echo "   - EventBridge > Rules > Create rule"
    echo "   - Schedule: cron(0 7 ? * MON *)"
    echo "   - Target: Lambda function"
    echo ""
    echo "5. CREAR CLOUDWATCH ALARMS"
    echo "   - 3 alarms: Errors, Timeout, Duration"
    echo ""
    echo "Para más detalles, revisar: GUIA_DEPLOYMENT.md"
    echo ""
}

# ============================================================================
# Post-Deployment Validation
# ============================================================================

post_deployment_validation() {
    log_info "Ejecutando validaciones post-deployment..."
    
    FUNCTION_NAME="validar-predicciones-$ENVIRONMENT"
    
    # Test 1: Verificar Lambda existe
    if aws lambda get-function \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        &> /dev/null; then
        log_success "✓ Lambda function encontrada"
    else
        log_error "✗ Lambda function no encontrada"
        return 1
    fi
    
    # Test 2: Verificar EventBridge rule
    RULE_NAME="validar-predicciones-weekly-$ENVIRONMENT"
    if aws events describe-rule \
        --name "$RULE_NAME" \
        --region "$AWS_REGION" \
        &> /dev/null; then
        log_success "✓ EventBridge rule encontrada"
    else
        log_error "✗ EventBridge rule no encontrada"
        return 1
    fi
    
    # Test 3: Invocar Lambda
    log_info "Ejecutando test de Lambda..."
    if aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --payload '{"test": true}' \
        /tmp/lambda_test.json \
        --region "$AWS_REGION" \
        2> /dev/null; then
        
        RESULT=$(jq -r '.statusCode' /tmp/lambda_test.json)
        if [[ "$RESULT" == "200" ]] || [[ "$RESULT" == "0" ]]; then
            log_success "✓ Lambda test exitoso"
        else
            log_warning "⚠ Lambda returned statusCode: $RESULT"
        fi
    else
        log_error "✗ Lambda test falló"
        return 1
    fi
    
    log_success "Post-deployment validation completada"
}

# ============================================================================
# Fin del Script
# ============================================================================

if [[ "$DRY_RUN" == false ]]; then
    echo ""
    log_info "Iniciando validaciones..."
    if post_deployment_validation; then
        echo ""
        log_success "╔════════════════════════════════════════════════════════════════╗"
        log_success "║         DEPLOYMENT COMPLETADO EXITOSAMENTE!                   ║"
        log_success "╚════════════════════════════════════════════════════════════════╝"
        echo ""
        log_info "Próximos pasos:"
        log_info "1. Confirmar email de SNS subscription"
        log_info "2. Revisar Dashboard: https://console.aws.amazon.com/cloudwatch/home"
        log_info "3. Ver documentación: GUIA_OPERACIONES.md"
        echo ""
    else
        log_error "Validaciones post-deployment fallaron"
        exit 1
    fi
else
    echo ""
    log_warning "DRY RUN COMPLETADO - No se aplicaron cambios"
    echo ""
fi
