# 8-Zonas-Atencion-Fusion

Mantenido por el **equipo de predicción**. No sigue el patrón común de los monitores
(`OK/AVISO/FALLA`, `check_identity`, flags `--con-avisos`, etc.).

## Contenido

```
Optimizacion/
├── validador_optimizacion_cloudshell.py   validador S3 (CloudShell)
└── EstructuraArchivosOptimizacion.txt      ejemplo del naming esperado
```

## `validador_optimizacion_cloudshell.py`

Valida en S3 (bucket `pon-prod-ml-iapol`, prefijo
`PrediccionMdP/<año>/<semana>/<SIGLA>/<unidad>/Optimizacion/`) que cada día de la semana
tenga los **6 archivos de optimización** esperados:

```
1_F_B01   1_F_CP01
2_F_B01   2_F_CP01
3_F_B01   3_F_CP01   (archivos .geojson)
```

Recorre todas las SIGLAS y unidades del rango de semanas, comprueba el formato del
nombre y que no falte ninguno de los 6; reporta errores y archivos faltantes en CSV
(empaquetado en ZIP).

### Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

# por defecto valida la semana siguiente (lunes en UTC-5)
python3 validador_optimizacion_cloudshell.py

# rango de semanas y una SIGLA concreta
python3 validador_optimizacion_cloudshell.py --desde 20260101 --hasta 20260107 --sigla <SIGLA>

# mostrar también las unidades sin errores
python3 validador_optimizacion_cloudshell.py --mostrar-todas
```

Permisos: lectura de S3 (`s3:ListBucket`, `s3:GetObject`) sobre `pon-prod-ml-iapol`.
