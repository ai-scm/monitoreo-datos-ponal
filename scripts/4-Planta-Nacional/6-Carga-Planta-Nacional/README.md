# 6-Carga-Planta-Nacional

## `alerta48horas.py`

Función **Lambda** (`lambda_handler`) que vigila la llegada del reporte de llamadas
**Movistar Webex**. No es un script CloudShell del patrón común: se despliega como
Lambda (event-driven / schedule en AWS).

### Qué hace

1. Lista los `.zip` en `s3://pbx-cdr-view/input_emails/` y toma el más reciente
   (por `LastModified`).
2. Calcula cuántas horas pasaron desde ese último reporte.
3. Si **no hay reporte** o el último tiene **≥ 48 h**, publica una alerta por **SNS**
   (`arn:aws:sns:us-east-1:926162397524:alerta-reporte-webex-cdR`) indicando que el
   reporte no se ha recibido y que hay que verificar con Movistar.
4. Si el reporte llegó dentro de las 48 h, no envía nada.

### Configuración (constantes en el script)

| Constante | Valor |
|-----------|-------|
| `BUCKET`  | `pbx-cdr-view` |
| `PREFIX`  | `input_emails/` |
| `REGION`  | `us-east-1` |
| `SNS_ARN` | `arn:aws:sns:us-east-1:926162397524:alerta-reporte-webex-cdR` |
| `HORAS`   | `48` |

### Permisos

- `s3:ListBucket` / `s3:GetObject` (o `s3:ListObjects`) sobre `pbx-cdr-view`.
- `sns:Publish` sobre el topic de alerta.
