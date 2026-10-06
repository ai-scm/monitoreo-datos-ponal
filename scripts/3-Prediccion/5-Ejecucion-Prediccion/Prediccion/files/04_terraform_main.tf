# ============================================================================
# Terraform Configuration para Automatización de Validación de Predicciones
# ============================================================================
#
# Uso:
#   terraform init
#   terraform plan -var-file=terraform.tfvars
#   terraform apply -var-file=terraform.tfvars

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Descomentar para usar remote state
  # backend "s3" {
  #   bucket         = "terraform-state-bucket"
  #   key            = "validacion-predicciones/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-lock"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = var.environment
      Application = "ValidacionPredicciones"
      ManagedBy   = "Terraform"
      CreatedAt   = timestamp()
    }
  }
}

# ============================================================================
# Variables
# ============================================================================

variable "aws_region" {
  description = "Región AWS"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment debe ser dev, staging o prod"
  }
}

variable "s3_bucket" {
  description = "Bucket S3 para datos de predicción"
  type        = string
  default     = "pon-prod-ml-iapol"
}

variable "alert_email" {
  description = "Email para recibir alertas"
  type        = string
  validation {
    condition     = can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.alert_email))
    error_message = "Email debe ser válido"
  }
}

variable "alert_threshold" {
  description = "Umbral de errores para alertar"
  type        = number
  default     = 50
  validation {
    condition     = var.alert_threshold > 0
    error_message = "Alert threshold debe ser > 0"
  }
}

variable "log_retention_days" {
  description = "Días de retención de CloudWatch Logs"
  type        = number
  default     = 30
  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.log_retention_days)
    error_message = "Log retention debe ser un valor permitido por AWS"
  }
}

variable "lambda_memory" {
  description = "Memoria de Lambda en MB"
  type        = number
  default     = 512
  validation {
    condition     = var.lambda_memory >= 128 && var.lambda_memory <= 10240
    error_message = "Memory debe estar entre 128 y 10240 MB"
  }
}

variable "lambda_timeout" {
  description = "Timeout de Lambda en segundos"
  type        = number
  default     = 900
  validation {
    condition     = var.lambda_timeout >= 1 && var.lambda_timeout <= 900
    error_message = "Timeout debe estar entre 1 y 900 segundos"
  }
}

# ============================================================================
# Data Sources
# ============================================================================

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# ============================================================================
# SNS Topic y Subscripciones
# ============================================================================

resource "aws_sns_topic" "validation_alerts" {
  name              = "validation-alerts-${var.environment}"
  display_name      = "Alertas de Validación de Predicciones"
  kms_master_key_id = "alias/aws/sns"

  tags = {
    Name = "validation-alerts"
  }
}

resource "aws_sns_topic_subscription" "validation_alerts_email" {
  topic_arn = aws_sns_topic.validation_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ============================================================================
# IAM Role para Lambda
# ============================================================================

resource "aws_iam_role" "lambda_role" {
  name               = "lambda-validar-predicciones-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "lambda-validar-predicciones"
  }
}

# Política para CloudWatch Logs (gestionada por AWS)
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Política para S3
resource "aws_iam_role_policy" "lambda_s3" {
  name = "lambda-s3-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ListAndRead"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*"
        ]
      },
      {
        Sid    = "S3WriteResults"
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = "arn:aws:s3:::${var.s3_bucket}/ValidationReports/*"
      }
    ]
  })
}

# Política para SNS
resource "aws_iam_role_policy" "lambda_sns" {
  name = "lambda-sns-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.validation_alerts.arn
      }
    ]
  })
}

# ============================================================================
# CloudWatch Log Group
# ============================================================================

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/validar-predicciones-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "validar-predicciones-logs"
  }
}

# ============================================================================
# Lambda Function
# ============================================================================

resource "aws_lambda_function" "validacion_predicciones" {
  filename      = data.archive_file.lambda_code.output_path
  function_name = "validar-predicciones-${var.environment}"
  role          = aws_iam_role.lambda_role.arn
  handler       = "validar_predicciones_lambda.lambda_handler"
  source_code_hash = data.archive_file.lambda_code.output_base64sha256
  runtime       = "python3.11"
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory

  environment {
    variables = {
      S3_BUCKET       = var.s3_bucket
      AWS_REGION      = var.aws_region
      SNS_TOPIC_ARN   = aws_sns_topic.validation_alerts.arn
      ALERT_THRESHOLD = var.alert_threshold
      ENVIRONMENT     = var.environment
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda_s3,
    aws_iam_role_policy.lambda_sns,
    aws_cloudwatch_log_group.lambda_logs
  ]

  tags = {
    Name = "validar-predicciones"
  }
}

# Archivar código Lambda
data "archive_file" "lambda_code" {
  type        = "zip"
  source_file = "${path.module}/validar_predicciones_lambda.py"
  output_path = "${path.module}/lambda_function.zip"
}

# ============================================================================
# IAM Role para EventBridge
# ============================================================================

resource "aws_iam_role" "eventbridge_role" {
  name               = "eventbridge-lambda-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = "eventbridge-lambda"
  }
}

resource "aws_iam_role_policy" "eventbridge_invoke_lambda" {
  name = "eventbridge-invoke-lambda-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.validacion_predicciones.arn
      }
    ]
  })
}

# ============================================================================
# EventBridge Rule
# ============================================================================

resource "aws_cloudwatch_event_rule" "validacion_schedule" {
  name                = "validar-predicciones-weekly-${var.environment}"
  description         = "Ejecuta validación cada lunes a las 02:00 AM UTC"
  schedule_expression = "cron(0 7 ? * MON *)"  # Lunes 07:00 UTC = 02:00 UTC-5
  is_enabled          = true

  tags = {
    Name = "validar-predicciones-schedule"
  }
}

resource "aws_cloudwatch_event_target" "validacion_lambda" {
  rule      = aws_cloudwatch_event_rule.validacion_schedule.name
  target_id = "ValidacionPredictionesTarget"
  arn       = aws_lambda_function.validacion_predicciones.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    desde             = "AUTO"
    hasta             = null
    sigla             = null
    alert_threshold   = var.alert_threshold
  })
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.validacion_predicciones.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.validacion_schedule.arn
}

# ============================================================================
# CloudWatch Alarms
# ============================================================================

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "validar-predicciones-errors-${var.environment}"
  alarm_description   = "Lambda reportó errores"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.validation_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.validacion_predicciones.function_name
  }

  tags = {
    Name = "lambda-errors"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_timeout" {
  alarm_name          = "validar-predicciones-timeout-${var.environment}"
  alarm_description   = "Lambda se acerca al timeout"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Maximum"
  threshold           = (var.lambda_timeout * 1000) * 0.95  # 95% del timeout en ms
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.validation_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.validacion_predicciones.function_name
  }

  tags = {
    Name = "lambda-timeout"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "validar-predicciones-duration-${var.environment}"
  alarm_description   = "Lambda demora más de 120 segundos"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = "300"
  statistic           = "Average"
  threshold           = "120000"  # 120 segundos en ms
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.validation_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.validacion_predicciones.function_name
  }

  tags = {
    Name = "lambda-duration"
  }
}

# ============================================================================
# CloudWatch Dashboard
# ============================================================================

resource "aws_cloudwatch_dashboard" "validacion" {
  dashboard_name = "ValidacionPredicciones-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", { stat = "Sum", dimensions = { FunctionName = aws_lambda_function.validacion_predicciones.function_name } }],
            [".", "Errors", { stat = "Sum", dimensions = { FunctionName = aws_lambda_function.validacion_predicciones.function_name } }],
            [".", "Duration", { stat = "Average", dimensions = { FunctionName = aws_lambda_function.validacion_predicciones.function_name } }],
            [".", "Throttles", { stat = "Sum", dimensions = { FunctionName = aws_lambda_function.validacion_predicciones.function_name } }]
          ]
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "Lambda Metrics - Últimas 24 horas"
          yAxis = {
            left = {
              showUnits = false
            }
          }
        }
      },
      {
        type = "log"
        properties = {
          query = "fields @timestamp, @message, @level | stats count() by @level"
          region = var.aws_region
          title  = "Log Summary (últimas 24h)"
        }
      }
    ]
  })
}

# ============================================================================
# Outputs
# ============================================================================

output "lambda_function_arn" {
  description = "ARN de la función Lambda"
  value       = aws_lambda_function.validacion_predicciones.arn
}

output "lambda_function_name" {
  description = "Nombre de la función Lambda"
  value       = aws_lambda_function.validacion_predicciones.function_name
}

output "sns_topic_arn" {
  description = "ARN del topic SNS para alertas"
  value       = aws_sns_topic.validation_alerts.arn
}

output "eventbridge_rule_name" {
  description = "Nombre de la regla de EventBridge"
  value       = aws_cloudwatch_event_rule.validacion_schedule.name
}

output "eventbridge_rule_arn" {
  description = "ARN de la regla de EventBridge"
  value       = aws_cloudwatch_event_rule.validacion_schedule.arn
}

output "log_group_name" {
  description = "Nombre del CloudWatch Log Group"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "dashboard_url" {
  description = "URL del CloudWatch Dashboard"
  value       = "https://console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.validacion.dashboard_name}"
}

output "log_group_url" {
  description = "URL del CloudWatch Logs"
  value       = "https://console.aws.amazon.com/logs/home?region=${var.aws_region}#logStream:group=${aws_cloudwatch_log_group.lambda_logs.name}"
}

output "lambda_invoke_command" {
  description = "Comando para invocar Lambda manualmente"
  value       = "aws lambda invoke --function-name ${aws_lambda_function.validacion_predicciones.function_name} --payload '{\"desde\": \"20260901\", \"hasta\": \"20260907\"}' response.json"
}
