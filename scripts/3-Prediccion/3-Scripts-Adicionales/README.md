# 9-Scripts-Adicionales

Mantenido por el **equipo de predicción**. No sigue el patrón común de los monitores
(`OK/AVISO/FALLA`, `check_identity`, flags `--con-avisos`, etc.).

## Contenido

```
MatricesPrediccion/
├── validador_matrices_cloudshell.py   validador S3 (CloudShell)
├── ejecutar en cloudshell.png          captura de uso
└── salida por consola.png              captura de salida
Hechos/
├── Hechos_*.geojson                    archivos de ejemplo
└── novedadDirectorioHechos.png         captura
```

## `validador_matrices_cloudshell.py`

Valida en S3 (bucket `pon-prod-ml-iapol`, prefijo
`PrediccionMdP/<año>/<semana>/<SIGLA>/<unidad>/MatricesPrediccion/`) la cobertura de las
**24 horas** de los archivos de predicción, con el formato:

```
Prediccion_<unidad>_<fecha YYYYMMDD>_<hora HHMM>.pkl
```

Para cada SIGLA/unidad del rango de semanas comprueba que estén las 24 horas
(`0000`…`2300`) por día; ignora otros tipos de archivo. Reporta errores de formato y
horas faltantes en CSV (empaquetado en ZIP).

### Uso (CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1

# por defecto valida la semana siguiente (lunes en UTC-5)
python3 validador_matrices_cloudshell.py

# rango de semanas y una SIGLA concreta
python3 validador_matrices_cloudshell.py --desde 20260101 --hasta 20260107 --sigla <SIGLA>

# mostrar también las unidades sin errores
python3 validador_matrices_cloudshell.py --mostrar-todas
```

Permisos: lectura de S3 (`s3:ListBucket`, `s3:GetObject`) sobre `pon-prod-ml-iapol`.

La carpeta `Hechos/` guarda archivos `.geojson` de ejemplo y capturas de referencia
del directorio de hechos; no son parte del validador.
