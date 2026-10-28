Este es un descubridor de modelos y dependencias de ML — auditoría estática de contenedores Docker y sus dependencias de Python. Solo lectura, no ejecuta ni modifica nada.

Propósito principal
Mapear modelos de predicción, dependencias Python y operaciones de archivo en la imagen Docker del predictor:

Cluster: pon-prod-xcrime-predicciones-cluster
Familia: pon-prod-xcrime-prediccion-definition
Funcionalidades clave
1. Inspección de imagen Docker (ECR)

Descarga capas sin extraerlas (presupuesto de 8 GB descomprimido)
Inventaría archivos sin ejecutar nada
Detecta:
Metadata de paquetes Python (METADATA, PKG-INFO)
Manifiestos (requirements.txt, pyproject.toml, setup.cfg)
Código Python de la aplicación
Archivos de datos (CSV, GeoJSON)
2. Validación de dependencias

Compara requirements.txt extraído vs. metadata en el entorno
Verifica versiones fijadas
Detecta inconsistencias estáticas (no ejecuta imports)
Controles por digest y entorno objetivo
3. Análisis estático de código

Parse AST sin ejecutar código
Detecta operaciones de archivo:
Lecturas: pandas.read_csv(), geopandas.read_file(), numpy.load()
Escrituras: to_csv(), to_file(), S3 get_object()/put_object()
Archivos: open(), download_file(), upload_file()
Rastreo de referencias a S3, rutas estáticas vs. dinámicas
Identificación de clases de modelo declaradas (Counter, KDE, SEEP, Uniform)
4. Rastreo de columnas CSV

Propaga origen de CSV en AST
Detecta qué columnas se leen/usan
No ejecuta, solo analiza estructura de código
5. Reportes locales

mapa.csv — inventario estático
evidencias.csv — referencias encontradas
contexto.json — metadatos y limitaciones
Limitaciones importantes
NO certifica ejecución — solo mapeo estático
NO valida modelos actualizados o en uso
NO ejecuta código — análisis AST puro, sin eval
NO resuelve dinámicas — exec(), importlib.import_module() dinámico, despacho por nombre
Referencias ≠ disponibilidad — archivo mencionado puede no existir
Ramas no resueltas — if/else ambiguas
Imports en paquetes vendorizados pueden no detectarse
Límites de cobertura: 100k nodos, 2k referencias, 4 niveles de recursión
Opciones de validación
--validar-entradas — lee cabeceras de CSV
--validar-filas — valida conteos de dos CSVs en S3
--inspeccionar-imagen — descarga capas ECR (más lento)
--probar-imports — ejecuta imports en Docker local (sin red)
Exit codes: 0 (sin diferencias), 1 (diferencia estática encontrada), 2 (args inválidos), 3 (lectura incompleta), 130 (interrupción)

Caso de uso: auditoría pre-predicción de dependencias Python y operaciones de archivo, sin ejecutar código.
