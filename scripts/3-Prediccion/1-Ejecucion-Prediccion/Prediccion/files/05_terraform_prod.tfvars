# ============================================================================
# Terraform Variables - Ambiente PROD
# ============================================================================
# 
# Uso: terraform apply -var-file=terraform_prod.tfvars
#

aws_region        = "us-east-1"
environment       = "prod"
s3_bucket         = "pon-prod-ml-iapol"
alert_email       = "devops-team@empresa.com"
alert_threshold   = 50
log_retention_days = 30
lambda_memory     = 512
lambda_timeout    = 900
