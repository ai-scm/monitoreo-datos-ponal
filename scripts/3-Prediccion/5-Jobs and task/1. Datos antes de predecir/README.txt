Este script es una herramienta de monitoreo y auditoría de datos de entrada para predicciones en AWS. Específicamente:

Propósito principal
Valida la calidad, recencia y completitud de datos en tablas Glue/Athena antes de usarlos en predicciones, sin modificar datos.

Funcionalidades clave

1. Descubrimiento
Busca la tabla configurada en los catálogos Glue disponibles
Muestra columnas y tipos de datos

2. Análisis de datos (ejecuta consultas Athena sobre):
Recencia: Edad de los registros más recientes (vs. umbral configurable)
Calidad de fechas: Detecta nulos, inválidos, fechas futuras
Volumen: Compara períodos anteriores vs. recientes, identifica caídas
Dimensiones: Validación por unidad, sigla, delito
Coordenadas: Si aplica, verifica latitud/longitud válidas

3. Modos de operación

--ejecutar	Corre las consultas en Athena y genera reportes
--solo-sql	Solo genera SQL, no ejecuta
--comparar-periodos	Compara 15 días anteriores vs. 15 recientes (UN SELECT)

4. Reportes
Genera 3 CSV en S3 (y localmente si --no-export no está activo):
general.csv — resumen por sigla/unidad/BD
diario.csv — registros por día
control.csv — controles y validaciones (OK/AVISO/FALLA)

Caso de uso
Auditoría diaria de mdp_limpio u otra tabla de entrada de datos antes de hacer predicciones. Detecta problemas como retraso de carga, datos ausentes, o cambios anómalos en volumen.

Exit codes: 0 (sin falla), 1 (con falla), 2 (argumentos inválidos), 3 (bloqueado/error AWS)
