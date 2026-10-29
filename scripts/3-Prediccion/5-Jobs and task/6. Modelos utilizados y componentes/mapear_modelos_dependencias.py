#!/usr/bin/env python3
"""Punto 4: descubrimiento de modelos y dependencias, no monitor de salud.

CloudShell: python3 mapear_modelos_dependencias.py
Por defecto resumen por sigla identificada desde streams de logs y metadata ECS vinculada.
--por-tarea conserva el mapeo ECS/ECR anterior. Sin ECS Exec, RunTask ni escrituras AWS.
--validar-entradas lee cabeceras; --validar-filas agrega conteos de dos entradas S3.
--historial-logs descubre streams por tarea aunque ECS ya no la conserve.
--mapear-streams cruza todas las siglas de la estructura S3 con sus streams identificados.
Se excluyen resultados de prediccion.
No consulta Secrets Manager. Los mapeos no ejecutan codigo del contenedor. --inspeccionar-imagen
descarga capas temporales con presupuesto e inventaria archivos sin extraerlos.
--probar-imports ejecuta solo imports en una copia Docker local, sin red ni ECS Exec.
Referencias en configuracion/logs no prueban disponibilidad ni modelo elegido.
Salida por sigla: historial_tareas.csv (RESUMEN_SIGLA/DETALLE_TAREA) y contexto_historial.json.
Modo --por-tarea: mapa.csv, evidencias.csv y contexto.json en salida/modelos_dependencias/FECHA/HORA.
Exit: 0 sin diferencias confirmadas (puede haber pendientes); 1 diferencia estatica o falla ECS;
2 argumentos; 3 lectura/comparacion incompleta; 130 Ctrl+C.
"""
import argparse
import ast
from collections import Counter
import csv
import codecs
import copy
import io
from datetime import datetime, timedelta, timezone
import json
import hashlib
import posixpath
from email.parser import Parser
from pathlib import Path
from pathlib import PurePosixPath
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
import uuid
from urllib.parse import urlsplit
from urllib.error import URLError, HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener


CLUSTER = 'pon-prod-xcrime-predicciones-cluster'
FAMILY = 'pon-prod-xcrime-prediccion-definition'
IMPORT_MODULES = ('numpy', 'pandas', 'scipy', 'geopandas', 'shapely', 'fiona',
    'pyproj', 'rtree', 'statsmodels.api', 'joblib', 'boto3', 'awswrangler',
    'open_cp', 'open_cp.naive', 'open_cp.sepp', 'open_cp.sources.sepp')
# Compatible con Python 3.6 del predictor; no importa la aplicacion.
IMPORT_PROBE = '''import importlib,json,sys
try:
    module=importlib.import_module(sys.argv[1])
    print("PONAL_IMPORT_RESULT="+json.dumps(dict(estado="OK",python=sys.version.split()[0],interprete=sys.executable,entorno_real=sys.prefix,version=str(getattr(module,"__version__",""))[:80])))
except Exception as exc:
    print("PONAL_IMPORT_RESULT="+json.dumps(dict(estado="FALLA",python=sys.version.split()[0],interprete=sys.executable,entorno_real=sys.prefix,error=type(exc).__name__,detalle=str(exc)[:1000])))
    sys.exit(1)
'''
BOGOTA = timezone(timedelta(hours=-5))
# Nombres observados; Conteo y Counter se conservan separados, sin inferir alias.
MODELS = ('Counter', 'Conteo', 'KDE', 'KDEPerDayKernelSize', 'SEEP', 'Uniform', 'Auto Exitatory')
EXCLUDED_IMAGE_PREFIXES = ('opt/conda/pkgs/', 'opt/conda/share/doc/')
DEFAULT_ENVIRONMENT = 'opt/conda/envs/predenv'
DEFAULT_REQUIREMENTS = 'code/requirements.txt'
APPLICATION_ROOT = 'code/Sci-XCrimeGeneradorCuadrantes/'
ERRORS = ('ModuleNotFoundError', 'ImportError', 'FileNotFoundError',
          'NoSuchKey', 'AccessDenied', 'AccessDeniedException', 'NoCredentialsError',
          'EndpointConnectionError', 'ConnectTimeoutError', 'ReadTimeoutError')
# Filtro de terminos opcionales OR. Los resultados siguen siendo referencias,
# nunca una prueba de seleccion de modelo o de fallo de toda la tarea.
# Consultar tambien las declaraciones: un nombre nuevo no debe desaparecer
# antes de llegar al parser. Las frases son terminos opcionales entre comillas.
MODEL_REPORT_TERMS = ('The best model to run the prediction is', 'selected model', 'chosen model',
    'modelo seleccionado', 'modelo elegido', 'selected_model', 'chosen_model',
    'modelo_seleccionado', 'modelo_elegido', 'Not enough records in the train data for')
LOG_FILTER = ' '.join('?' + (term if term.isalnum() else json.dumps(term))
    for term in dict.fromkeys(MODELS + tuple(m.lower() for m in MODELS) + ERRORS +
        MODEL_REPORT_TERMS + tuple(term.lower() for term in MODEL_REPORT_TERMS) +
        ('fallback_used', 'uso_respaldo', 'fallback_model')))


def load_report_csv(path, columns):
    with Path(path).open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if not set(columns) <= set(reader.fieldnames or []):
            raise ValueError('Columnas requeridas ausentes en ' + str(path))
        return list(reader)


def normalized_package(name):
    return re.sub(r'[-_.]+', '-', name).lower()


def pinned_version_match(expected, observed):
    if expected == observed:
        return True
    if re.fullmatch(r'\d+(?:\.\d+)*', expected) and re.fullmatch(r'\d+(?:\.\d+)*', observed):
        def release(value):
            parts = [int(p) for p in value.split('.')]
            while len(parts) > 1 and parts[-1] == 0:
                parts.pop()
            return parts
        return release(expected) == release(observed)
    # No inventar semantica PEP 440 para versiones complejas que difieran.
    return None


def dependency_controls(packages, files, environment, requirements):
    """Controles de consistencia de metadata por digest y entorno; no imports."""
    environment = environment.strip('/')
    digests = sorted({r['digest'] for r in packages} | {r['digest'] for r in files})
    if not digests:
        raise ValueError('Reportes vacios; no se pueden evaluar requisitos')
    controls, summaries = [], []
    for digest in digests:
        if not re.fullmatch(r'sha256:[0-9a-fA-F]{64}', digest):
            raise ValueError('Digest ausente o no interpretable en los reportes')
        pk = [r for r in packages if r['digest'] == digest]
        refs = [r for r in files if r['digest'] == digest]
        complete = bool(pk and refs) and all(r.get('cobertura') == 'LEIDA' for r in pk + refs)
        counts = Counter(fijadas_ok=0, distintas=0, sin_metadata=0, sin_fijar=0, no_evaluadas=0)
        def add(control, status, resource, expected='', observed='', evidence=''):
            controls.append(dict(digest=digest, entorno=environment, control=control,
                estado=status, recurso=resource, esperado=expected, observado=observed, evidencia=evidence))
        add('Cobertura del inventario', 'OK' if complete else 'NO EVALUADO', 'CSV por digest',
            'LEIDA en ambos reportes', 'LEIDA' if complete else 'INCOMPLETA',
            'Cobertura de extraccion declarada; no comprueba imports ni requisitos no extraidos')
        installed = {}
        for row in pk:
            match = re.fullmatch(r'(.+)/lib/python\d+\.\d+/site-packages/[^/]+/(?:METADATA|PKG-INFO)', row['archivo'])
            if match and match.group(1) == environment:
                installed.setdefault(normalized_package(row['paquete']), []).append(row)
        add('Metadata del entorno objetivo', 'OK' if installed else 'NO EVALUADO', environment,
            'Metadata directa del entorno', str(len(installed)) + ' nombres de distribucion',
            'Excluye otros entornos y distribuciones anidadas/vendorizadas')
        declarations = {r['referencia'] for r in refs if r['tipo'] == 'Manifiesto presente' and r['archivo'] == requirements}
        if len(declarations) != 1 or not next(iter(declarations), ''):
            add('Requisitos extraidos', 'NO EVALUADO', requirements,
                'Una lista de declaraciones', 'Ausente, vacia o ambigua')
            counts['no_evaluadas'] += 1
        else:
            for token in next(iter(declarations)).split('; '):
                match = re.fullmatch(r'([A-Za-z0-9_.-]+)(?:==([A-Za-z0-9_.+!-]+))?', token)
                if not match:
                    counts['no_evaluadas'] += 1
                    add('Requisito extraido', 'NO EVALUADO', sanitize_message(token),
                        evidence='Comparador admite nombre o nombre==version exacta, sin comodines')
                    continue
                name, version = match.groups()
                evidence = installed.get(normalized_package(name), [])
                versions = sorted({r['version'] for r in evidence})
                paths = '; '.join(sorted({r['archivo'] for r in evidence}))
                if not versions:
                    counts['sin_metadata'] += 1
                    add('Requisito extraido', 'AVISO', name, version or 'Sin version fija',
                        'SIN METADATA', 'Ausencia de metadata no prueba ausencia del paquete')
                elif len(versions) != 1:
                    counts['no_evaluadas'] += 1
                    add('Requisito extraido', 'NO EVALUADO', name, version or 'Sin version fija',
                        ', '.join(versions), 'Multiples versiones en el entorno; ' + paths)
                elif not complete:
                    counts['no_evaluadas'] += 1
                    add('Requisito extraido', 'NO EVALUADO', name, version or 'Sin version fija',
                        versions[0], 'Inventario parcial: capas omitidas pueden reemplazar metadata')
                elif version is None:
                    counts['sin_fijar'] += 1
                    add('Requisito extraido', 'AVISO', name, 'Sin version fija', versions[0], paths)
                elif pinned_version_match(version, versions[0]) is True:
                    counts['fijadas_ok'] += 1
                    add('Requisito extraido', 'OK', name, version, versions[0], paths)
                elif pinned_version_match(version, versions[0]) is False:
                    counts['distintas'] += 1
                    add('Requisito extraido', 'FALLA', name, version, versions[0],
                        'Diferencia estatica con version fijada; no prueba fallo de tarea. ' + paths)
                else:
                    counts['no_evaluadas'] += 1
                    add('Requisito extraido', 'NO EVALUADO', name, version, versions[0],
                        'Versiones complejas distintas; este comparador no resuelve equivalencias PEP 440')
        for model in MODELS:
            paths = sorted({r['archivo'] for r in refs if r['archivo'].startswith(APPLICATION_ROOT)
                            and r['tipo'] == 'Clase de modelo' and r['referencia'] == model})
            add('Clase de modelo declarada', 'OK' if paths and complete else 'NO EVALUADO', model,
                'Declaracion en codigo del generador', str(len(paths)) + ' archivos', '; '.join(paths))
        for control, resource in (
                ('Entorno efectivo del proceso', environment),
                ('Imports y compatibilidad transitiva/ABI', 'Librerias del predictor'),
                ('Contenido de auxiliares', 'Validacion separada con --validar-filas'),
                ('Cobertura completa de requirements', requirements)):
            add(control, 'NO EVALUADO', resource, evidence='No verificable solo con estos CSV de inspeccion estatica')
        summaries.append(dict(digest=digest, entorno=environment, cobertura='LEIDA' if complete else 'PARCIAL', **counts))
    return controls, summaries


def write_dependency_controls(folder, packages, files, environment, requirements):
    controls, summary = dependency_controls(packages, files, environment, requirements)
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    with (folder/'control_dependencias.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(controls[0]))
        writer.writeheader()
        writer.writerows(controls)
    context = dict(resumen=summary, controles=controls, alcance='Consistencia estatica por digest/entorno de requisitos extraidos; no salud del proceso',
                   requirements=requirements, entorno=environment)
    (folder/'contexto_controles.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\nCONTROLES ESTATICOS | Requisitos extraidos, por digest y entorno objetivo')
    print('Digest       | Fijadas OK | Distintas | Sin metadata | Sin fijar | No eval. | Cobertura')
    print('-------------+------------+-----------+--------------+-----------+----------+----------')
    for row in summary:
        print(f'{row["digest"][7:19]} | {row["fijadas_ok"]:10} | {row["distintas"]:9} | {row["sin_metadata"]:12} | {row["sin_fijar"]:9} | {row["no_evaluadas"]:8} | {row["cobertura"]}')
    print(f'Entorno objetivo: {environment} | Requirements: {requirements}')
    pending = sum(r['estado'] == 'NO EVALUADO' for r in controls)
    print(f'Controles NO EVALUADOS: {pending}. Coincidencia de metadata no certifica imports, compatibilidad ni salud de la tarea.')
    print(f'Controles locales: {folder}/control_dependencias.csv | contexto_controles.json')
    incomplete = any(r['cobertura'] != 'LEIDA' or r['no_evaluadas'] for r in summary)
    return 3 if incomplete else 1 if any(r['estado'] == 'FALLA' for r in controls) else 0


def controls_from_folder(source, output, environment, requirements):
    packages = load_report_csv(Path(source)/'paquetes.csv', ['digest', 'cobertura', 'archivo', 'paquete', 'version'])
    files = load_report_csv(Path(source)/'archivos_imagen.csv', ['digest', 'cobertura', 'archivo', 'tipo', 'referencia'])
    return write_dependency_controls(output, packages, files, environment, requirements)


def ecr_reference(image, observed_digest=''):
    """Resolver repositorio y digest exactos; un tag actual no prueba la version ejecutada."""
    match = re.fullmatch(r'(\d{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?/(.+)', image)
    if not match:
        return None
    account, region, path = match.groups()
    repo_and_tag, separator, configured_digest = path.partition('@')
    repository, colon, tag = repo_and_tag.partition(':')
    digest = observed_digest or (configured_digest if separator else '')
    if digest and not re.fullmatch(r'sha256:[0-9a-fA-F]{64}', digest):
        raise ValueError('Digest ECS/ECR no interpretable')
    return dict(cuenta=account, region=region, repositorio=repository,
                identificador={'imageDigest': digest} if digest else {'imageTag': tag if colon else 'latest'},
                origen='DIGEST OBSERVADO' if observed_digest else 'DIGEST CONFIGURADO' if digest else 'TAG ACTUAL; EJECUCION NO VERIFICADA')


def describe_ecr_image(ecr, reference):
    response = ecr.describe_images(registryId=reference['cuenta'],
        repositoryName=reference['repositorio'], imageIds=[reference['identificador']])
    details = response.get('imageDetails', [])
    if len(details) != 1:
        raise ValueError('ECR no devolvio una unica imagen')
    image = details[0]
    expected = reference['identificador'].get('imageDigest')
    if expected and image.get('imageDigest') != expected:
        raise ValueError('Digest devuelto por ECR distinto del solicitado')
    pushed = image.get('imagePushedAt')
    return dict(**reference, estado='ENCONTRADA', digest=image.get('imageDigest', ''),
                tags=image.get('imageTags', []), bytes=image.get('imageSizeInBytes'),
                publicacion=pushed.isoformat() if isinstance(pushed, datetime) else '',
                formato=image.get('imageManifestMediaType', ''))


def image_manifest(ecr, reference, digest, architecture):
    """Manifiesto por digest; resolver un indice sin elegir otra plataforma."""
    original = digest
    for _ in range(3):
        response = ecr.batch_get_image(registryId=reference['cuenta'],
            repositoryName=reference['repositorio'], imageIds=[{'imageDigest': digest}])
        images = response.get('images', [])
        if response.get('failures') or len(images) != 1:
            codes = ','.join(f.get('failureCode', 'desconocido') for f in response.get('failures', []))
            raise ValueError('Manifiesto no disponible: ' + (codes or 'respuesta incompleta'))
        raw = images[0]['imageManifest']
        if 'sha256:' + hashlib.sha256(raw.encode()).hexdigest() != digest:
            raise ValueError('Integridad del manifiesto no verificada')
        manifest = json.loads(raw)
        if 'manifests' not in manifest:
            if not isinstance(manifest.get('layers'), list):
                raise ValueError('Formato de imagen no soportado; se requiere OCI/Docker schema 2')
            return original, digest, manifest
        wanted = 'arm64' if architecture == 'ARM64' else 'amd64'
        candidates = [m for m in manifest['manifests'] if m.get('platform', {}).get('architecture') == wanted
                      and m.get('platform', {}).get('os') == 'linux']
        if len(candidates) != 1:
            raise ValueError('Indice sin una plataforma Linux unica compatible con la tarea')
        digest = candidates[0]['digest']
    raise ValueError('Demasiados indices anidados')


def archive_kind(path):
    # Cache/documentacion observadas en la imagen; no son el entorno activo.
    # Los enlaces fuera de estas rutas siguen declarandose como limitacion.
    if path.startswith(EXCLUDED_IMAGE_PREFIXES):
        return ''
    base = PurePosixPath(path).name
    if path.startswith(('code/', 'opt/config/')) and base.lower().endswith(('.csv', '.geojson')):
        return 'archivo_datos'
    if base == 'METADATA' and '.dist-info/' in path or base == 'PKG-INFO' and '.egg-info/' in path:
        return 'paquete'
    if re.fullmatch(r'requirements[^/]*\.txt', base, re.I) or base in ('pyproject.toml', 'setup.cfg', 'Pipfile', 'poetry.lock'):
        return 'manifiesto'
    if base.endswith('.py') and not any(p in path for p in ('site-packages/', 'dist-packages/', '/lib/python', 'usr/lib/', 'usr/local/lib/')):
        return 'codigo'
    return ''


def scan_layer(handle, previous, max_files=5000, max_expanded=8*1024**3):
    """Leer TAR sin extraer. Aplicar whiteouts antes de los cambios de la capa.

    Nunca seguir enlaces ni ejecutar setup.py, imports o pickle. Los resultados
    de capas omitidas/incompletas se rotulan PARCIAL, no inventario final.
    """
    updates, removed, opaque, invalidated, issues = {}, [], [], [], []
    expanded = selected = entries = 0
    retained = sum(len(item[1]) for item in previous.values() if item is not None)
    with tarfile.open(fileobj=handle, mode='r|*') as archive:
        for member in archive:
            entries += 1
            if entries > 100000:
                raise ValueError('Capa supera el limite de entradas')
            path = member.name
            while path.startswith('./'):
                path = path[2:]
            if member.isdir() and path in ('', '.', '/'):
                continue
            parts = PurePosixPath(path).parts
            if not parts or path.startswith('/') or '..' in parts:
                issues.append('Ruta de archivo no normalizada; omitida')
                continue
            path = str(PurePosixPath(path))
            expanded += max(member.size, 0)
            if expanded > max_expanded:
                raise ValueError('Capa supera el limite de lectura descomprimida')
            base, parent = parts[-1], str(PurePosixPath(path).parent)
            prefix = '' if parent == '.' else parent + '/'
            if base == '.wh..wh..opq':
                opaque.append(prefix)
                continue
            if base.startswith('.wh.'):
                removed.append(prefix + base[4:])
                continue
            if not member.isdir():
                invalidated.append(path)
            kind = archive_kind(path)
            if not kind:
                continue
            selected += 1
            updates[path] = None
            if selected > max_files:
                raise ValueError('Capa supera el limite de archivos candidatos')
            if kind == 'archivo_datos':
                # Solo cabecera TAR: no leer incidentes, catalogos o geometria.
                updates[path] = (kind, json.dumps(dict(bytes=member.size, regular=member.isfile(),
                    enlace=member.issym() or member.islnk())))
                continue
            if not member.isfile() or member.size > 512*1024:
                issues.append('Archivo candidato enlazado/no regular o mayor de 512 KiB: ' + path)
                continue
            stream = archive.extractfile(member)
            content = stream.read(512*1024+1) if stream else b''
            retained += len(content)
            if retained > 32*1024**2:
                raise ValueError('Inventario supera el limite de contenido retenido')
            updates[path] = (kind, content.decode('utf-8', errors='replace'))
    deleted = set(removed) | set(invalidated)
    for path in list(previous):
        ancestors = {str(parent) for parent in PurePosixPath(path).parents}
        if path in deleted or bool(ancestors & deleted) or any(path.startswith(prefix) for prefix in opaque):
            del previous[path]
    previous.update(updates)
    return issues


def symbolic_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = symbolic_name(node.value)
        return parent + '.' + node.attr if parent else ''
    return ''


def static_path(node, bindings):
    """Sustitucion limitada de cadenas AST, sin eval, imports ni llamadas reales."""
    def bounded(value):
        return value if len(value) <= 300 else '{limite_de_ruta}'
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, int)) and not isinstance(node.value, bool):
        value = str(node.value)
        return value if re.fullmatch(r'[A-Za-z0-9_./{}:%+ -]{0,180}', value) else '{no_proyectado}'
    name = symbolic_name(node)
    if name:
        return bindings.get(name, '{' + name + '}')
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return bounded(static_path(node.left, bindings) + static_path(node.right, bindings))
    if isinstance(node, ast.JoinedStr):
        return bounded(''.join(static_path(v.value, bindings) if isinstance(v, ast.FormattedValue)
                               and v.conversion == -1 and v.format_spec is None else
                               '{formato}' if isinstance(v, ast.FormattedValue) else static_path(v, bindings)
                               for v in node.values))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        template = static_path(node.left, bindings)
        values = node.right.elts if isinstance(node.right, ast.Tuple) else [node.right]
        # Only positional %s. Never invoke object conversion or arbitrary formatting.
        if template.count('%s') == len(values) and '%' not in template.replace('%s', ''):
            for value in values:
                template = template.replace('%s', static_path(value, bindings), 1)
            return bounded(template)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if symbolic_name(node.func) == 'os.path.join':
            return bounded(posixpath.join(*(static_path(v, bindings) for v in node.args))) if node.args else '{dinamico}'
        if node.func.attr == 'format' and isinstance(node.func.value, ast.Constant):
            template = static_path(node.func.value, bindings)
            values = {str(i): static_path(v, bindings) for i, v in enumerate(node.args)}
            values.update({k.arg: static_path(k.value, bindings) for k in node.keywords if k.arg})
            automatic = iter(range(len(node.args)))
            def replace(match):
                key = match.group(1) or str(next(automatic, -1))
                return values.get(key, '{' + (key or 'parametro') + '}')
            # Attributes, indexing and format specifications remain unresolved.
            return bounded(re.sub(r'(?<!{){([A-Za-z_][A-Za-z0-9_]*|[0-9]*)}(?!})', replace, template))
    return '{dinamico}'


def file_operations(tree, path):
    """Analisis acotado por modulo: asignaciones y llamadas a funciones locales.

    Cada expansion es una referencia posible, no prueba de ejecucion. No une
    modulos ni resuelve herencia, closures o despachos dinamicos.
    """
    if not path.startswith(APPLICATION_ROOT):
        return []
    aliases, s3_clients, functions = {}, set(), {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update({n.asname or n.name.split('.')[0]: n.name for n in node.names})
        elif isinstance(node, ast.ImportFrom) and node.module:
            aliases.update({n.asname or n.name: node.module + '.' + n.name for n in node.names})
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            value = node.value
            if isinstance(value.func, ast.Attribute) and value.func.attr == 'client' and value.args and isinstance(value.args[0], ast.Constant) and value.args[0].value == 's3':
                s3_clients.update(t.id for t in node.targets if isinstance(t, ast.Name))
    def register(body, owner=()):
        for item in body:
            if isinstance(item, ast.ClassDef):
                register(item.body, owner + (item.name,))
            elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions['.'.join(owner + (item.name,))] = item
    register(tree.body)
    operations, expansions, limits = [], set(), set()
    def qualified(node):
        if isinstance(node, ast.Name):
            return aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            return qualified(node.value) + '.' + node.attr
        return '?'
    def assigned(body):
        return {symbolic_name(n) for item in body for n in ast.walk(item)
                if isinstance(n, (ast.Name, ast.Attribute)) and isinstance(n.ctx, ast.Store)}
    def bind(target, value, bindings):
        name = symbolic_name(target)
        if name:
            # Do not propagate credential-like symbols into projected file paths.
            bindings[name] = '{oculto}' if re.search(r'password|secret|token|credential|access_key', name, re.I) else value
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                bind(element, '{desempaquetado}', bindings)
    def expand(node, bindings, scope, conditional, context, chain):
        name = symbolic_name(node.func)
        if name.startswith(('self.', 'cls.')) and len(scope) == 2:
            name = scope[0] + '.' + name.split('.', 1)[1]
        if name not in functions:
            return
        if name in chain or len(chain) >= 4:
            limits.add('Recursion o limite de 4 funciones')
            return
        function = functions[name]
        # Starred/expanded arguments cannot be bound reliably.
        if any(isinstance(v, ast.Starred) for v in node.args) or any(k.arg is None for k in node.keywords):
            return
        params = list(function.args.posonlyargs) + list(function.args.args)
        method_call = isinstance(node.func, ast.Attribute) and symbolic_name(node.func.value) in ('self', 'cls')
        if method_call and params and params[0].arg in ('self', 'cls'):
            params = params[1:]
        if len(node.args) > len(params):
            return
        local = {p.arg: '{' + p.arg + '}' for p in params + list(function.args.kwonlyargs)}
        all_params = list(function.args.posonlyargs) + list(function.args.args)
        for param, default in zip(all_params[len(all_params)-len(function.args.defaults):], function.args.defaults):
            local[param.arg] = static_path(default, {})
        for param, default in zip(function.args.kwonlyargs, function.args.kw_defaults):
            if default is not None:
                local[param.arg] = static_path(default, {})
        for param, arg in zip(params, node.args):
            local[param.arg] = static_path(arg, bindings)
        for keyword in node.keywords:
            if keyword.arg in local:
                local[keyword.arg] = static_path(keyword.value, bindings)
        location = (context + ' -> ' if context else '') + '.'.join(scope or ('<modulo>',)) + ':' + str(node.lineno)
        key = (name, tuple(sorted(local.items())), location)
        if key in expansions:
            return
        if len(expansions) >= 200:
            limits.add('Limite de 200 expansiones por modulo')
            return
        expansions.add(key)
        block(function.body, local, tuple(name.split('.')), conditional, location, chain + (name,))
    def expression(node, bindings, scope, conditional, context, chain):
        if len(operations) >= 2000:
            limits.add('Limite de 2000 operaciones por modulo')
            return
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            local = dict(bindings)
            for generator in node.generators:
                for name in assigned([generator]):
                    local[name] = '{iteracion:' + name + '}'
            for child in ast.iter_child_nodes(node):
                expression(child, local, scope, True, context, chain)
            return
        if isinstance(node, ast.NamedExpr):
            bind(node.target, '{asignacion_en_expresion}', bindings)
        if isinstance(node, ast.Call):
            method = qualified(node.func)
            suffix = method.rsplit('.', 1)[-1]
            kwargs = {k.arg: k.value for k in node.keywords if k.arg}
            first = node.args[0] if node.args else next((kwargs[k] for k in ('filepath_or_buffer', 'filename', 'path', 'path_or_buf', 'file') if k in kwargs), None)
            kind, target, origin = '', '', 'LOCAL'
            if method in ('pandas.read_csv', 'geopandas.read_file', 'numpy.load', 'numpy.loadtxt'):
                kind, target = 'LECTURA', static_path(first, bindings)
            elif suffix in ('to_csv', 'to_file', 'to_pickle'):
                kind = 'SIN ARCHIVO EXPLICITO' if first is None or isinstance(first, ast.Constant) and first.value is None else 'ESCRITURA'
                target = static_path(first, bindings)
            elif method == 'open':
                mode_node = node.args[1] if len(node.args) > 1 else kwargs.get('mode')
                mode = mode_node.value if isinstance(mode_node, ast.Constant) else 'r' if mode_node is None else None
                kind = ('MIXTO' if '+' in mode else 'ESCRITURA' if any(v in mode for v in 'wax') else 'LECTURA') if isinstance(mode, str) else 'MODO NO RESUELTO'
                target = static_path(first, bindings)
            elif suffix in ('get_object', 'put_object') and 'Bucket' in kwargs and 'Key' in kwargs:
                kind = 'LECTURA' if suffix == 'get_object' else 'ESCRITURA'
                origin = 'S3'
                target = 's3://' + static_path(kwargs['Bucket'], bindings) + '/' + static_path(kwargs['Key'], bindings)
            elif suffix in ('download_file', 'upload_file') and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id in s3_clients:
                offset = 0 if suffix == 'download_file' else 1
                if len(node.args) >= offset+2:
                    origin = 'S3'
                    kind = 'LECTURA' if suffix == 'download_file' else 'ESCRITURA'
                    target = 's3://' + static_path(node.args[offset], bindings) + '/' + static_path(node.args[offset+1], bindings)
            if kind:
                if target.startswith('s3://'):
                    origin = 'S3'
                elif origin != 'S3' and '{' in target and method != 'open':
                    origin = 'NO RESUELTO'
                operations.append(dict(archivo_codigo=path, linea=node.lineno, funcion='.'.join(scope) or '<modulo>',
                    condicional='SI' if conditional else 'NO DETECTADA', llamada=method,
                    uso=kind, origen=origin, referencia=target,
                    ruta_estatica='NO' if '{' in target or not target else 'SI',
                    ejecucion='NO CONFIRMADA', llamada_desde=context or 'DEFINICION; USO NO CONFIRMADO'))
            expand(node, bindings, scope, conditional, context, chain)
        for child in ast.iter_child_nodes(node):
            if not isinstance(child, (ast.Lambda, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                expression(child, bindings, scope, conditional, context, chain)
    def block(body, bindings, scope=(), conditional=False, context='', chain=()):
        for item in body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(item, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.With, ast.AsyncWith) + ((ast.Match,) if hasattr(ast, 'Match') else ())):
                # Each branch sees its own bindings. Afterwards invalidate all writes,
                # even if both branches happen to have the same literal.
                groups = [getattr(item, key, []) for key in ('body', 'orelse', 'finalbody')]
                groups += [h.body for h in getattr(item, 'handlers', [])]
                groups += [case.body for case in getattr(item, 'cases', [])]
                for child in ast.iter_child_nodes(item):
                    if isinstance(child, ast.expr):
                        expression(child, bindings, scope, True, context, chain)
                for manager in getattr(item, 'items', []):
                    expression(manager.context_expr, bindings, scope, True, context, chain)
                inner = dict(bindings)
                for name in assigned([item]):
                    if name not in bindings or isinstance(item, (ast.For, ast.AsyncFor, ast.While)):
                        inner[name] = '{rama:' + name + '}'
                for group in groups:
                    block(group, dict(inner), scope, True, context, chain)
                for name in assigned([item]):
                    bindings[name] = '{rama:' + name + '}'
                continue
            expression(item, bindings, scope, conditional, context, chain)
            if isinstance(item, (ast.Assign, ast.AnnAssign)):
                value = static_path(item.value, bindings)
                targets = item.targets if isinstance(item, ast.Assign) else [item.target]
                for target in targets:
                    bind(target, value, bindings)
            elif isinstance(item, ast.AugAssign):
                bind(item.target, '{reasignado}', bindings)
            elif isinstance(item, ast.Delete):
                for target in item.targets:
                    bind(target, '{eliminado}', bindings)
    block(tree.body, {})
    for name, function in functions.items():
        params = list(function.args.posonlyargs) + list(function.args.args) + list(function.args.kwonlyargs)
        bindings = {p.arg: '{' + p.arg + '}' for p in params}
        # Local assignments shadow parameters/globals until statically encountered.
        for local in assigned(function.body):
            bindings.setdefault(local, '{' + local + '}')
        block(function.body, bindings, tuple(name.split('.')), chain=(name,))
    if limits:
        operations.append(dict(archivo_codigo=path, linea='', funcion='<analisis>', condicional='NO EVALUADA',
            llamada='', uso='NO ANALIZADO', origen='NO RESUELTO', referencia='; '.join(sorted(limits)),
            ruta_estatica='NO', ejecucion='NO CONFIRMADA', llamada_desde='LIMITE DE COBERTURA'))
    return operations


def column_consumption(tree, path):
    """Propagar origen CSV en formas AST reconocidas; nunca ejecutar Python.

    Las referencias no son un contrato obligatorio: ramas, parametros e imports
    no resueltos conservan limitaciones. Solo se exportan nombres y ubicaciones.
    """
    if not path.startswith(APPLICATION_ROOT):
        return dict(referencias=[], limitaciones=[])
    functions, aliases = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update({n.asname or n.name.split('.')[0]: n.name for n in node.names})
        elif isinstance(node, ast.ImportFrom) and node.module:
            aliases.update({n.asname or n.name: node.module + '.' + n.name for n in node.names})
    def register(body, owner=()):
        for node in body:
            if isinstance(node, ast.ClassDef):
                register(node.body, owner + (node.name,))
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions['.'.join(owner + (node.name,))] = node
    register(tree.body)
    refs, limits, seen = [], set(), set()
    budget = [0, 0]
    known = {'s3://' + INPUT_BUCKET + '/' + key for key in INPUT_KEYS}
    def value(kind='unknown', paths=(), text='', created=()):
        return dict(kind=kind, paths=frozenset(paths), text=text, created=frozenset(created))
    def strings(env):
        return {name: v['text'] for name, v in env.items() if v['kind'] == 'string'}
    def qualified(node):
        if isinstance(node, ast.Name):
            return aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            return qualified(node.value) + '.' + node.attr
        return '?'
    def literal_columns(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node.value]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [v.value for v in node.elts if isinstance(v, ast.Constant) and isinstance(v.value, str)]
        return []
    def record(base, columns, node, scope, conditional, trace, access):
        for source in sorted(base['paths'] & known):
            for column in columns:
                if column in base['created']:
                    continue
                if len(column) > 160 or any(ord(c) < 32 for c in column):
                    limits.add('Nombre de columna no proyectable')
                    continue
                key = (source, column, node.lineno, scope, conditional, trace, access)
                if key not in seen:
                    seen.add(key)
                    refs.append(dict(recurso=source, columna=column, archivo_codigo=path, linea=node.lineno,
                        funcion=scope or '<modulo>', condicional='SI' if conditional else 'NO DETECTADA',
                        llamada_desde=trace or 'DEFINICION; USO NO CONFIRMADO', acceso=access,
                        asociacion='TRAZADA EN AST' if len(base['paths']) == 1 else 'ORIGEN POSIBLE',
                        obligatoria='NO CONFIRMADA', ejecucion='NO CONFIRMADA'))
    def merge(values):
        if not values:
            return value()
        if all(v == values[0] for v in values):
            return values[0]
        # No fijar valores de ramas. Solo conservar procedencia como posibilidad.
        if all(v['kind'] in ('df', 'series') for v in values):
            return value('df', set().union(*(v['paths'] for v in values)),
                         created=set.intersection(*(set(v['created']) for v in values)))
        return value()
    def evaluate(node, env, scope, conditional, trace, chain):
        budget[0] += 1
        if budget[0] > 100000 or len(refs) >= 2000:
            limits.add('Limite de 100000 nodos/2000 referencias por modulo')
            return value()
        name = symbolic_name(node)
        if name in env:
            return env[name]
        if isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr)):
            # Evaluar hijos solo para registrar accesos, nunca eval()/exec().
            for child in ast.iter_child_nodes(node):
                evaluate(child, env, scope, conditional, trace, chain)
            return value('string', text=static_path(node, strings(env)))
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Attribute) and node.value.attr in ('loc', 'iloc'):
                base = evaluate(node.value.value, env, scope, conditional, trace, chain)
                if base['kind'] == 'df':
                    if node.value.attr == 'loc' and isinstance(node.slice, ast.Tuple) and len(node.slice.elts) == 2:
                        record(base, literal_columns(node.slice.elts[1]), node, scope, conditional, trace, 'SELECCION LOC')
                    else:
                        limits.add('Seleccion por posicion o indice no resuelta')
                    evaluate(node.slice, env, scope, conditional, trace, chain)
                    return base
            base = evaluate(node.value, env, scope, conditional, trace, chain)
            columns = literal_columns(node.slice)
            if base['kind'] == 's3obj' and columns == ['Body']:
                return value('buffer', base['paths'])
            if base['kind'] == 'df':
                record(base, columns, node, scope, conditional, trace, 'LECTURA POR NOMBRE')
                evaluate(node.slice, env, scope, conditional, trace, chain)
                return value('series' if isinstance(node.slice, ast.Constant) else 'df', base['paths'], created=base['created'])
            return value()
        if isinstance(node, ast.Attribute):
            base = evaluate(node.value, env, scope, conditional, trace, chain)
            if base['kind'] in ('df', 'series'):
                if node.attr not in ('columns', 'index', 'shape', 'size', 'values', 'empty', 'dtype', 'dtypes', 'iloc', 'loc'):
                    record(base, [node.attr], node, scope, conditional, trace, 'ATRIBUTO; CONFIRMAR COLUMNA')
                return value()  # Un atributo no prueba que se conserve un DataFrame.
            return value()
        if isinstance(node, ast.Call):
            method = qualified(node.func)
            suffix = method.rsplit('.', 1)[-1]
            kwargs = {k.arg: k.value for k in node.keywords if k.arg}
            arguments = [evaluate(v, env, scope, conditional, trace, chain) for v in node.args]
            keywords = {k: evaluate(v, env, scope, conditional, trace, chain) for k, v in kwargs.items()}
            receiver = evaluate(node.func.value, env, scope, conditional, trace, chain) if isinstance(node.func, ast.Attribute) else value()
            if method == 'os.path.join' or suffix == 'format' and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Constant):
                return value('string', text=static_path(node, strings(env)))
            if suffix == 'get_object' and 'Bucket' in kwargs and 'Key' in kwargs:
                source = 's3://' + static_path(kwargs['Bucket'], strings(env)) + '/' + static_path(kwargs['Key'], strings(env))
                return value('s3obj', [source])
            if method == 'pandas.read_csv':
                source = arguments[0] if arguments else keywords.get('filepath_or_buffer', value())
                paths = source['paths'] if source['kind'] == 'buffer' else [source['text']] if source['kind'] == 'string' else []
                if paths:
                    # Explicit usecols is itself a named input selection.
                    if 'names' in kwargs or isinstance(kwargs.get('header'), ast.Constant) and kwargs['header'].value is None:
                        limits.add('read_csv redefine los nombres o no usa cabecera')
                        return value()
                    result = value('df', paths)
                    record(result, literal_columns(kwargs.get('index_col')), node, scope, conditional, trace, 'READ_CSV INDEX_COL')
                    record(result, literal_columns(kwargs.get('usecols')), node, scope, conditional, trace, 'READ_CSV USECOLS')
                    return result
                limits.add('Origen de read_csv no resuelto')
                return value('df')
            if suffix in ('BytesIO', 'StringIO') and arguments and arguments[0]['kind'] == 'buffer':
                return arguments[0]
            if suffix == 'get' and receiver['kind'] == 's3obj' and node.args and literal_columns(node.args[0]) == ['Body']:
                return value('buffer', receiver['paths'])
            if suffix in ('read', 'decode') and receiver['kind'] == 'buffer':
                return receiver
            if receiver['kind'] in ('df', 'series'):
                if suffix == 'get':
                    record(receiver, literal_columns(node.args[0]) if node.args else [], node, scope, True, trace, 'GET CON DEFAULT; REVISAR')
                    return value('series', receiver['paths'], created=receiver['created'])
                if suffix in ('copy', 'astype', 'fillna', 'dropna', 'head', 'tail', 'sort_values', 'query'):
                    if suffix in ('sort_values', 'query'):
                        limits.add('Columnas de ' + suffix + ' no resueltas completamente')
                    return receiver
                if suffix in ('rename', 'drop', 'set_axis', 'assign', 'merge', 'join', 'groupby', 'pivot', 'set_index', 'reset_index'):
                    if 'inplace' in kwargs and not (isinstance(kwargs['inplace'], ast.Constant) and kwargs['inplace'].value is False):
                        env[symbolic_name(node.func.value)] = value()
                    limits.add('Transformacion de DataFrame no resuelta: ' + suffix)
                    return value()
                return value()
            local_name = symbolic_name(node.func)
            if local_name.startswith(('self.', 'cls.')) and scope.count('.') == 1:
                local_name = scope.split('.')[0] + '.' + local_name.split('.', 1)[1]
            if local_name in functions:
                if local_name in chain or len(chain) >= 4 or budget[1] >= 200:
                    limits.add('Recursion/limite de 4 funciones o 200 llamadas por modulo')
                    return value()
                function = functions[local_name]
                if any(isinstance(v, ast.Starred) for v in node.args) or any(k.arg is None for k in node.keywords):
                    limits.add('Argumentos expandidos no resueltos')
                    return value()
                params = list(function.args.posonlyargs) + list(function.args.args)
                local = {p.arg: value() for p in params + list(function.args.kwonlyargs)}
                for param, default in zip(params[len(params)-len(function.args.defaults):], function.args.defaults):
                    local[param.arg] = value('string', text=static_path(default, {}))
                for param, default in zip(function.args.kwonlyargs, function.args.kw_defaults):
                    if default is not None:
                        local[param.arg] = value('string', text=static_path(default, {}))
                if isinstance(node.func, ast.Attribute) and symbolic_name(node.func.value) in ('self', 'cls') and params and params[0].arg in ('self', 'cls'):
                    params = params[1:]
                if len(arguments) > len(params):
                    return value()
                for param, argument in zip(params, arguments):
                    local[param.arg] = argument
                local.update({k: v for k, v in keywords.items() if k in local})
                budget[1] += 1
                location = (trace + ' -> ' if trace else '') + (scope or '<modulo>') + ':' + str(node.lineno)
                return merge(block(function.body, local, local_name, conditional, location, chain + (local_name,)))
            if any(v['paths'] for v in arguments + list(keywords.values())):
                limits.add('Funcion externa/despacho no resuelto')
            return value()
        for child in ast.iter_child_nodes(node) if node is not None else ():
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                evaluate(child, env, scope, conditional, trace, chain)
        return value()
    def assign(target, result, env, scope, conditional, trace, chain):
        name = symbolic_name(target)
        if name:
            if name.endswith('.columns'):
                env[name.rsplit('.', 1)[0]] = value()
                limits.add('Reasignacion de nombres de columnas')
            else:
                env[name] = result
        elif isinstance(target, ast.Subscript):
            base_name = symbolic_name(target.value)
            base = env.get(base_name, value())
            if base['kind'] == 'df':
                # Un campo creado no se convierte en requisito del CSV original.
                env[base_name] = value('df', base['paths'], created=base['created'] | set(literal_columns(target.slice)))
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                assign(element, value(), env, scope, conditional, trace, chain)
    def block(body, env, scope='', conditional=False, trace='', chain=()):
        returns = []
        for node in body:
            if budget[0] > 100000 or len(refs) >= 2000:
                limits.add('Limite de 100000 nodos/2000 referencias por modulo')
                break
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                result = evaluate(node.value, env, scope, conditional, trace, chain)
                for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                    assign(target, result, env, scope, conditional, trace, chain)
            elif isinstance(node, ast.Return):
                returns.append(evaluate(node.value, env, scope, conditional, trace, chain))
                break
            elif isinstance(node, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.With, ast.AsyncWith)):
                groups = [getattr(node, key, []) for key in ('body', 'orelse', 'finalbody')]
                groups += [h.body for h in getattr(node, 'handlers', [])]
                for child in ast.iter_child_nodes(node):
                    if isinstance(child, ast.expr):
                        evaluate(child, env, scope, True, trace, chain)
                for item in getattr(node, 'items', []):
                    evaluate(item.context_expr, env, scope, True, trace, chain)
                branches = [dict(env)]
                for group in groups:
                    branch = dict(env)
                    target = getattr(node, 'target', None)
                    if target is not None:
                        assign(target, value(), branch, scope, True, trace, chain)
                    returns.extend(block(group, branch, scope, True, trace, chain))
                    branches.append(branch)
                for name in set().union(*(set(b) for b in branches)):
                    env[name] = merge([b.get(name, value()) for b in branches])
            elif isinstance(node, (ast.AugAssign, ast.Delete)):
                targets = node.targets if isinstance(node, ast.Delete) else [node.target]
                for target in targets:
                    assign(target, value(), env, scope, conditional, trace, chain)
            else:
                evaluate(node, env, scope, conditional, trace, chain)
        return returns
    block(tree.body, {})
    for name, function in functions.items():
        params = list(function.args.posonlyargs) + list(function.args.args) + list(function.args.kwonlyargs)
        block(function.body, {p.arg: value() for p in params}, name, chain=(name,))
    return dict(referencias=refs, limitaciones=sorted(limits))


def write_column_consumption(folder, inspections):
    rows, limitations = [], []
    for inspection in inspections:
        for result in inspection.get('consumo_columnas', []):
            rows.extend(dict(digest=inspection['digest'], cobertura_archivos=inventory_coverage(inspection), **r) for r in result['referencias'])
            limitations.extend(dict(archivo_codigo=result['archivo_codigo'], motivo=reason) for reason in result['limitaciones'])
        limitations.extend(dict(archivo_codigo=f['archivo'], motivo=f['referencia']) for f in inspection.get('codigo', [])
                           if f['archivo'].startswith(APPLICATION_ROOT) and f['tipo'] == 'NO ANALIZADO')
    fields = ['digest', 'cobertura_archivos', 'recurso', 'columna', 'archivo_codigo', 'linea', 'funcion', 'condicional', 'llamada_desde', 'acceso', 'asociacion', 'obligatoria', 'ejecucion']
    folder = Path(folder)
    with (folder/'consumo_entradas.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    context = dict(referencias=rows, limitaciones=limitations,
        alcance='Accesos a columnas con origen CSV trazado en AST del mismo modulo. No ejecuta codigo, no lee S3 de negocio ni configura contratos automaticamente.',
        pendientes=['Imports entre modulos', 'Transformaciones no reconocidas y efectos laterales/alias', 'Necesidad por modalidad', 'Ejecucion de las ramas del programa'])
    (folder/'contexto_consumo_entradas.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\nCOLUMNAS CONSULTADAS | Evidencia AST; no contrato obligatorio')
    for key in INPUT_KEYS:
        source = 's3://' + INPUT_BUCKET + '/' + key
        names = sorted({r['columna'] for r in rows if r['recurso'] == source})
        print(f'{key}: {len(names)} nombres; detalle por linea en CSV')
    print(f'Limitaciones: {len(limitations)}; sin referencias no se demuestra ausencia de uso.')
    print(f'Consumo local: {folder}/consumo_entradas.csv | contexto_consumo_entradas.json')


def consumer_code_evidence(tree, path):
    """Proyectar filtros y formatos de logs, sin ejecutar ni guardar codigo completo."""
    if not path.startswith(APPLICATION_ROOT):
        return dict(reglas_unidad=[], formatos_modelos=[], limitaciones=[])
    columns = {'UNIDAD_ID', 'TIUN_CODIGO', 'SIGLA_FISICA', 'SIGLA_PAPA', 'CONSECUTIVO', 'UNDE_CONSECUTIVO'}
    operations = {'dropna', 'notna', 'notnull', 'isna', 'isnull', 'isin', 'drop', 'query', 'fillna', 'astype', 'to_list', 'tolist', 'unique'}
    rule_rows, model_rows, limits = [], [], set()
    class ProjectLiterals(ast.NodeTransformer):
        def visit_Constant(self, node):
            v = node.value
            if isinstance(v, str):
                keep = v in columns or v in MODELS or not v.strip(' -:;.,{}') or bool(re.fullmatch(r'[0-9.]{1,24}', v))
                if not keep and len(v) <= 250 and not re.search(r'password|secret|token|credential|access.?key', v, re.I):
                    keep = bool(re.search(r'The best model to run the prediction|Not enough records in the train data|fallback|respaldo', v, re.I))
                    keep = keep or ('TIUN_CODIGO' in v and bool(re.fullmatch(r'[A-Za-z_0-9 .,<>=!()\[\]&|+\-]{1,200}', v)))
                if not keep:
                    return ast.copy_location(ast.Constant(value='[LITERAL OMITIDO]'), node)
                cleaned = v if not v.strip() else v[:len(v)-len(v.lstrip())] + sanitize_message(v) + v[len(v.rstrip()):]
                return ast.copy_location(ast.Constant(value=cleaned), node)
            if isinstance(v, (bytes, complex)) or isinstance(v, (int, float)) and (not -1000000 <= v <= 1000000):
                return ast.copy_location(ast.Constant(value='[LITERAL OMITIDO]'), node)
            return node
    def expression(node):
        projected = ProjectLiterals().visit(copy.deepcopy(node))
        rendered = ast.unparse(projected) if hasattr(ast, 'unparse') else ast.dump(projected, include_attributes=False)
        if len(rendered) > 900:
            limits.add('Expresiones truncadas a 900 caracteres; no interpretar como regla completa')
        return rendered[:900]
    def fields(node):
        return {col for child in ast.walk(node) for col in columns
                if isinstance(child, ast.Constant) and isinstance(child.value, str) and col in child.value
                or isinstance(child, ast.Attribute) and child.attr == col}
    def methods(node):
        return sorted({child.func.attr for child in ast.walk(node)
                       if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute) and child.func.attr in operations})
    def walk(node, scope=(), guards=(), function_columns=frozenset()):
        if len(rule_rows) + len(model_rows) >= 1500:
            limits.add('Limite de 1500 evidencias por archivo')
            return
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                walk(child, scope + (node.name,), (), frozenset())
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in node.body:
                walk(child, scope + (node.name,), (), frozenset(fields(node)))
            return
        selected = []
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)) and node.value is not None:
            selected = [('ASIGNACION', node.value)]
        elif isinstance(node, ast.Return) and node.value is not None:
            selected = [('RETORNO', node.value)]
        elif isinstance(node, ast.Expr):
            selected = [('EXPRESION', node.value)]
        elif isinstance(node, (ast.If, ast.While)):
            selected = [('CONDICION', node.test)]
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            selected = [('ITERACION', node.iter)]
        for kind, expr in selected:
            used, ops = fields(expr), methods(expr)
            common = dict(archivo_codigo=path, linea=node.lineno, funcion='.'.join(scope) or '<modulo>',
                expresion=expression(expr), condiciones=json.dumps(list(guards[-8:]), ensure_ascii=False),
                ejecucion='NO CONFIRMADA')
            if used & {'UNIDAD_ID', 'TIUN_CODIGO'} or (used or ops) and function_columns & {'UNIDAD_ID', 'TIUN_CODIGO'}:
                rule_rows.append(dict(**common, categoria=kind, columnas='; '.join(sorted(used)), operaciones='; '.join(ops),
                    asociacion='REFERENCIA EN EXPRESION' if used & {'UNIDAD_ID', 'TIUN_CODIGO'} else 'CONTEXTO DE FUNCION; ORIGEN NO CONFIRMADO',
                    regla_activada='NO'))
            for call in (child for child in ast.walk(expr) if isinstance(child, ast.Call)):
                name = symbolic_name(call.func).rsplit('.', 1)[-1]
                if name not in ('print', 'info', 'warning', 'warn', 'debug', 'error', 'exception', 'log'):
                    continue
                literals = [child.value for child in ast.walk(call) if isinstance(child, ast.Constant) and isinstance(child.value, str)]
                if not any(re.search(r'The best model to run the prediction|Not enough records in the train data|fallback|respaldo', v, re.I)
                           or any(re.search(r'(?<!\w)' + re.escape(model) + r'(?!\w)', v) for model in MODELS) for v in literals):
                    continue
                projected = dict(common, linea=call.lineno, expresion=expression(call))
                variable_names = sorted({child.id for child in ast.walk(call) if isinstance(child, ast.Name)})
                model_rows.append(dict(**projected, emisor=name, variables='; '.join(variable_names[:60]),
                    correlacion='NO CONFIRMADA; formato de codigo, no evento observado'))
        if isinstance(node, ast.If):
            test = expression(node.test)
            for child in node.body:
                walk(child, scope, guards + ('if ' + test,), function_columns)
            for child in node.orelse:
                walk(child, scope, guards + ('else de if ' + test,), function_columns)
            return
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            guard = 'bucle: ' + expression(node.iter if hasattr(node, 'iter') else node.test)
            for child in node.body + node.orelse:
                walk(child, scope, guards + (guard,), function_columns)
            return
        if isinstance(node, ast.Try):
            for child in node.body:
                walk(child, scope, guards + ('try; puede haber manejo de excepcion',), function_columns)
            for handler in node.handlers:
                for child in handler.body:
                    walk(child, scope, guards + ('except; rama alternativa',), function_columns)
            for child in node.orelse + node.finalbody:
                walk(child, scope, guards + ('else/finally de try',), function_columns)
            return
        # Expressions were already inspected above; recurse only into containers
        # to avoid emitting every nested subexpression a second time.
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.stmt):
                walk(child, scope, guards, function_columns)
    walk(tree)
    return dict(reglas_unidad=rule_rows, formatos_modelos=model_rows, limitaciones=sorted(limits))


def write_consumer_code_evidence(folder, inspections):
    unit_rows, model_rows, issues = [], [], []
    for inspection in inspections:
        metadata = dict(digest=inspection['digest'], cobertura_archivos=inventory_coverage(inspection))
        for result in inspection.get('auditoria_consumidor', []):
            unit_rows.extend(dict(**metadata, **r) for r in result['reglas_unidad'])
            model_rows.extend(dict(**metadata, **r) for r in result['formatos_modelos'])
            issues.extend(dict(archivo_codigo=result['archivo_codigo'], motivo=reason) for reason in result['limitaciones'])
        issues.extend(dict(archivo_codigo=f['archivo'], motivo=f['referencia']) for f in inspection.get('codigo', [])
                      if f['archivo'].startswith(APPLICATION_ROOT) and f['tipo'] == 'NO ANALIZADO')
    base = ['digest', 'cobertura_archivos', 'archivo_codigo', 'linea', 'funcion', 'expresion', 'condiciones', 'ejecucion']
    folder = Path(folder)
    for name, rows, extra in [('reglas_unidad.csv', unit_rows, ['categoria', 'columnas', 'operaciones', 'asociacion', 'regla_activada']),
                              ('formatos_modelos.csv', model_rows, ['emisor', 'variables', 'correlacion'])]:
        with (folder/name).open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=base+extra)
            writer.writeheader()
            writer.writerows(rows)
    context = dict(reglas_unidad=unit_rows, formatos_modelos=model_rows, limitaciones=issues,
        alcance='Evidencia estatica proyectada; filtros/formatos no prueban ejecucion, elegibilidad universal ni correlacion de procesos. No activa reglas por tipo.',
        fuera_de_alcance=['Comprobar acceso desde ECS', 'Validar resultados de prediccion en S3'])
    (folder/'contexto_auditoria_codigo.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\nAUDITORIA DE CODIGO | Referencias de unidades: {len(unit_rows)} | Formatos de modelos: {len(model_rows)} | Limitaciones: {len(issues)}')
    print('Evidencia por funcion/linea; requiere interpretacion antes de activar reglas o correlacionar logs.')
    print(f'Auditoria local: {folder}/reglas_unidad.csv | formatos_modelos.csv | contexto_auditoria_codigo.json')


def inventory_files(files):
    packages, manifests, code, data_files, operations, column_usage, audit = [], [], [], [], [], [], []
    for path, item in sorted(files.items()):
        if item is None:
            continue
        kind, content = item
        if kind == 'archivo_datos':
            data_files.append(dict(archivo=path, **json.loads(content)))
        elif kind == 'paquete':
            metadata = Parser().parsestr(content, headersonly=True)
            name, version = metadata.get('Name', ''), metadata.get('Version', '')
            if not re.fullmatch(r'[A-Za-z0-9_.-]{1,160}', name) or not re.fullmatch(r'[A-Za-z0-9_.+!~-]{1,120}', version):
                continue
            packages.append(dict(paquete=name, version=version, archivo=path,
                python_requerido=sanitize_message(metadata.get('Requires-Python', '')),
                requisitos='; '.join(sanitize_message(v) for v in metadata.get_all('Requires-Dist', [])[:100])))
        elif kind == 'manifiesto':
            # No guardar URLs de instalacion, credenciales ni el contenido original.
            declarations = []
            if PurePosixPath(path).name.lower().startswith('requirements'):
                for line in content.splitlines():
                    match = re.fullmatch(r'\s*([A-Za-z0-9][A-Za-z0-9_.-]*(?:\[[A-Za-z0-9_,.-]+\])?)\s*([<>=!~][A-Za-z0-9.,+!*<>=~ -]+)?\s*(?:#.*)?', line)
                    if match:
                        declarations.append(match.group(1) + (match.group(2) or '').strip())
            manifests.append(dict(archivo=path, sha256=hashlib.sha256(content.encode()).hexdigest(),
                                  declaraciones='; '.join(declarations[:300])))
        else:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', SyntaxWarning)
                    warnings.simplefilter('ignore', DeprecationWarning)
                    tree = ast.parse(content)
            except (SyntaxError, ValueError, RecursionError):
                code.append(dict(archivo=path, linea='', tipo='NO ANALIZADO', referencia='Sintaxis no compatible con el analizador local'))
                continue
            operations.extend(file_operations(tree, path))
            if path.startswith(APPLICATION_ROOT):
                try:
                    usage = column_consumption(tree, path)
                except RecursionError:
                    usage = dict(referencias=[], limitaciones=['Limite de recursion del analizador de columnas'])
                column_usage.append(dict(archivo_codigo=path, **usage))
                try:
                    evidence = consumer_code_evidence(tree, path)
                except RecursionError:
                    evidence = dict(reglas_unidad=[], formatos_modelos=[], limitaciones=['Limite de recursion de auditoria'])
                audit.append(dict(archivo_codigo=path, **evidence))
            for node in ast.walk(tree):
                references = []
                if isinstance(node, ast.Import):
                    references = [('Import declarado', n.name) for n in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    references = [('Import declarado', node.module)]
                elif isinstance(node, ast.ClassDef) and node.name in MODELS:
                    references = [('Clase de modelo', node.name)]
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    value = node.value
                    if re.fullmatch(r'[A-Za-z0-9_./{}%:+-]{1,160}', value) and re.search(r'UnidadesBase|\.(?:geojson|csv|pkl)$', value):
                        references = [('Ruta literal; uso no probado', value)]
                for kind_ref, value in references:
                    code.append(dict(archivo=path, linea=node.lineno, tipo=kind_ref, referencia=value))
    return dict(paquetes=packages, manifiestos=manifests, codigo=code,
                archivos_datos=data_files, operaciones_archivos=operations, consumo_columnas=column_usage, auditoria_consumidor=audit)


def inventory_coverage(inspection):
    """La lectura de capas no depende de la profundidad del analisis de rutas."""
    return inspection.get('cobertura_archivos', inspection['estado'])


def write_auxiliary_report(folder, inspections):
    rows = []
    for inspection in inspections:
        for op in inspection.get('operaciones_archivos', []):
            matches = []
            if op['uso'] == 'LECTURA' and op['origen'] == 'LOCAL' and op['ruta_estatica'] == 'SI':
                base = PurePosixPath(op['referencia']).name
                matches = [f for f in inspection.get('archivos_datos', []) if PurePosixPath(f['archivo']).name == base]
            regular = [f['archivo'] for f in matches if f['regular']]
            status = ('SALIDA; NO EXIGIDA' if op['uso'] == 'ESCRITURA' else
                      'COINCIDENCIA POR NOMBRE' if regular and inventory_coverage(inspection) == 'LEIDA' else 'NO EVALUADO')
            prior_write = any(other['uso'] == 'ESCRITURA' and other['referencia'] == op['referencia']
                and other['archivo_codigo'] == op['archivo_codigo'] and other['funcion'] == op['funcion']
                and other.get('llamada_desde') == op.get('llamada_desde')
                and int(other['linea']) < int(op['linea']) for other in inspection.get('operaciones_archivos', [])
                if other['linea'] and op['linea']) if op['uso'] == 'LECTURA' and op['ruta_estatica'] == 'SI' else False
            category = ('SALIDA' if op['uso'] == 'ESCRITURA' else 'NO CLASIFICADA' if op['uso'] != 'LECTURA' else
                'LECTURA TRAS ESCRITURA' if prior_write else
                'POSIBLE TEMPORAL' if re.search(r'(?:^|/)(?:tmp[^/]*|[^/]*tmp\.geojson)$', op['referencia'], re.I) else
                'ENTRADA CANDIDATA' if op['ruta_estatica'] == 'SI' else 'LECTURA POR RESOLVER')
            rows.append(dict(digest=inspection['digest'], cobertura=inspection['estado'], **op,
                tipo_referencia=category, estado=status, copias_en_imagen='; '.join(sorted(regular)),
                acceso_desde_tarea='NO EVALUADO',
                observacion='Coincidencia por nombre no prueba ruta efectiva ni obligatoriedad' if regular else
                    'No se valida salida' if op['uso'] == 'ESCRITURA' else
                    'Puede descargarse/generarse/montarse o depender de parametros; no se declara ausencia'))
    folder = Path(folder)
    columns = ['digest', 'cobertura', 'archivo_codigo', 'linea', 'funcion', 'condicional', 'llamada', 'uso', 'origen', 'referencia', 'ruta_estatica', 'ejecucion', 'llamada_desde', 'tipo_referencia', 'estado', 'copias_en_imagen', 'acceso_desde_tarea', 'observacion']
    with (folder/'auxiliares.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    not_analyzed = [dict(digest=i['digest'], **f) for i in inspections for f in i.get('codigo', [])
                    if f['archivo'].startswith(APPLICATION_ROOT) and f['tipo'] == 'NO ANALIZADO']
    context = dict(referencias=rows, archivos_datos=[dict(digest=i['digest'], **f) for i in inspections for f in i.get('archivos_datos', [])],
        archivos_no_analizados=not_analyzed,
        limites_analisis=[r for r in rows if r['uso'] == 'NO ANALIZADO'],
        alcance='Seguimiento simbolico por modulo, maximo 4 funciones/200 expansiones/2000 operaciones. Limites no certifican cobertura; sin resolver imports ni condiciones. Usos estaticos en el generador; ejecucion, necesidad y acceso no confirmados. Sin consultas a buckets de negocio.')
    (folder/'contexto_auxiliares.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    counts = Counter(r['uso'] for r in rows)
    print(f'\nAUXILIARES | Lecturas referenciadas: {counts["LECTURA"]} | Escrituras separadas: {counts["ESCRITURA"]} | Otros: {len(rows)-counts["LECTURA"]-counts["ESCRITURA"]}')
    candidates = {r['referencia'] for r in rows if r['tipo_referencia'] == 'ENTRADA CANDIDATA'}
    print(f'Rutas concretas candidatas distintas: {len(candidates)} | Trazabilidad en llamada_desde; plantillas siguen pendientes.')
    print('Lecturas son referencias de codigo, no archivos obligatorios confirmados. No se comprueban permisos ni se validan salidas.')
    if not_analyzed:
        print(f'Archivos del generador sin analisis AST: {len(not_analyzed)}; ver contexto_auxiliares.json.')
    print(f'Auxiliares locales: {folder}/auxiliares.csv | contexto_auxiliares.json')


class ImageRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        parsed = urlsplit(newurl)
        if parsed.scheme != 'https' or not parsed.hostname or not (parsed.hostname.endswith('.amazonaws.com') or parsed.hostname.endswith('.amazonaws.com.cn')):
            raise ValueError('Redireccion de capa fuera de HTTPS/AWS')
        redirected = super().redirect_request(request, fp, code, message, headers, newurl)
        if redirected is not None and urlsplit(request.full_url).netloc != parsed.netloc:
            redirected.remove_header('Authorization')
        return redirected


def open_image_layer(location):
    return build_opener(ImageRedirectHandler()).open(location, timeout=30)


def read_image_startup(ecr, reference, digest, architecture, tokens):
    """Leer solo config OCI verificada por digest, sin capas ni Docker."""
    _, _, manifest = image_manifest(ecr, reference, digest, architecture)
    descriptor = manifest.get('config', {})
    config_digest, size = descriptor.get('digest', ''), descriptor.get('size')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', config_digest) or not isinstance(size, int) or not 0 <= size <= 2*1024**2:
        raise ValueError('Descriptor de configuracion OCI no soportado o mayor de 2 MiB')
    key = reference['cuenta'], reference['region']
    if key not in tokens:
        credentials = ecr.get_authorization_token().get('authorizationData', [])
        if not credentials or not credentials[0].get('authorizationToken'):
            raise ValueError('Token de ECR no disponible')
        tokens[key] = credentials[0]['authorizationToken']
    suffix = 'amazonaws.com.cn' if reference['region'].startswith('cn-') else 'amazonaws.com'
    registry = f'{reference["cuenta"]}.dkr.ecr.{reference["region"]}.{suffix}'
    if not re.fullmatch(r'[a-z0-9][a-z0-9_./-]*', reference['repositorio']):
        raise ValueError('Ruta de repositorio no reconocida')
    request = Request(f'https://{registry}/v2/{reference["repositorio"]}/blobs/{config_digest}',
                      headers={'Authorization': 'Basic ' + tokens[key]})
    with open_image_layer(request) as stream:
        data = stream.read(2*1024**2+1)
    if len(data) != size or 'sha256:' + hashlib.sha256(data).hexdigest() != config_digest:
        raise ValueError('Integridad/tamano de configuracion OCI no verificados')
    configuration = json.loads(data).get('config', {})
    # Solo en memoria: Cmd/Env pueden contener secretos. No exportar el original.
    return {key: configuration.get(key) for key in ('Entrypoint', 'Cmd', 'Env', 'WorkingDir')}


def startup_summary(task, container, image):
    """Proyectar referencias de arranque; no confundirlas con procesos observados."""
    override = next((c for c in task.get('overrides', {}).get('containerOverrides', []) if c.get('name') == container['name']), {})
    if 'command' in override:
        command, command_source = override['command'], 'override de tarea'
    elif 'command' in container:
        command, command_source = container['command'], 'definicion ECS'
    else:
        command, command_source = image.get('Cmd') or [], 'imagen' if image else 'NO DISPONIBLE'
    entrypoint = container['entryPoint'] if 'entryPoint' in container else image.get('Entrypoint') or []
    entry_source = 'definicion ECS' if 'entryPoint' in container else 'imagen' if image else 'NO DISPONIBLE'
    # shlex solo tokeniza, no ejecuta el shell. No exportar el argumento -c completo.
    tokens = []
    for value in list(entrypoint or []) + list(command or []):
        if not isinstance(value, str):
            continue
        try:
            tokens.extend(shlex.split(value))
        except ValueError:
            continue
    # Omitir valores de argumentos con nombres sensibles antes de proyectar rutas.
    safe_tokens, hide_next = [], False
    for token in tokens:
        if hide_next:
            hide_next = False
            continue
        if token.startswith('-') and re.search(r'password|token|secret|credential|key|authorization', token, re.I):
            hide_next = '=' not in token
            continue
        safe_tokens.append(token)
    tokens = safe_tokens
    interpreters, scripts, command_envs = set(), set(), set()
    for index, token in enumerate(tokens):
        if re.fullmatch(r'(?:/[A-Za-z0-9_.+-]+)*/python(?:\d+(?:\.\d+)*)?', token) or re.fullmatch(r'python(?:\d+(?:\.\d+)*)?', token):
            interpreters.add(token)
            match = re.fullmatch(r'(.+)/bin/python(?:\d+(?:\.\d+)*)?', token)
            if match:
                command_envs.add(match.group(1))
        if re.fullmatch(r'[A-Za-z0-9_./-]{1,180}\.(?:py|sh)', token):
            scripts.add(token)
        if PurePosixPath(token).name == 'conda' and index+1 < len(tokens) and tokens[index+1] == 'run':
            for offset in range(index+2, len(tokens)-1):
                flag, value = tokens[offset:offset+2]
                if flag in ('-n', '--name') and re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', value):
                    command_envs.add('conda:' + value)
                    break
                if flag in ('-p', '--prefix') and re.fullmatch(r'/[A-Za-z0-9_./-]{1,180}', value):
                    command_envs.add(value)
                    break
                if flag not in ('--no-capture-output', '--live-stream', '-v', '-vv', '-vvv', '--debug-wrapper-scripts'):
                    break
        if PurePosixPath(token).name in ('conda', 'source') and index+2 < len(tokens) and tokens[index+1] == 'activate':
            value = tokens[index+2]
            if re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', value):
                command_envs.add('conda:' + value)
    environment = {}
    for value in image.get('Env') or []:
        name, sep, content = value.partition('=')
        if sep:
            environment[name] = content
    for name, (value, _) in effective_environment(container, task).items():
        environment[name] = value
    # Valores inyectados como secretos se desconocen; no usar defaults anteriores.
    for secret in container.get('secrets', []):
        environment.pop(secret.get('name'), None)
    projected = {}
    for name in ('CONDA_PREFIX', 'CONDA_DEFAULT_ENV', 'VIRTUAL_ENV'):
        value = environment.get(name, '')
        if re.fullmatch(r'[A-Za-z0-9_./-]{1,180}', value):
            projected[name] = value
    working = container.get('workingDirectory', image.get('WorkingDir') or '')
    if not re.fullmatch(r'/[A-Za-z0-9_./-]{0,180}', working):
        working = ''
    python_paths = [v for v in environment.get('PATH', '').split(':')
                    if re.fullmatch(r'/[A-Za-z0-9_./-]{1,180}', v) and
                    ('conda' in v or 'venv' in v or v in ('/usr/bin', '/usr/local/bin'))]
    return dict(fuente_command=command_source, fuente_entrypoint=entry_source,
        interpretes_referenciados='; '.join(sorted(interpreters)) or 'NO IDENTIFICADO',
        scripts_referenciados='; '.join(sorted(scripts)),
        entornos_en_comando='; '.join(sorted(command_envs)) or 'NO IDENTIFICADO',
        variables_entorno_proyectadas=json.dumps(projected, ensure_ascii=False, sort_keys=True),
        directorio_configurado=working, configuracion_imagen='LEIDA' if image else 'NO LEIDA',
        paths_python_declarados='; '.join(python_paths),
        entorno_efectivo_proceso='NO EVALUADO',
        variables_externas='NO LEIDAS' if container.get('environmentFiles') or override.get('environmentFiles') or container.get('secrets') else 'NO DECLARADAS',
        ecs_exec_habilitado='SI' if task.get('enableExecuteCommand') else 'NO',
        alcance='Referencias de lanzamiento; no se observa /proc, no se ejecuta ECS Exec')


def inspect_image(ecr, reference, digest, architecture, max_bytes):
    original, resolved, manifest = image_manifest(ecr, reference, digest, architecture)
    files, issues, downloaded, read = {}, [], 0, 0
    layers = manifest['layers']
    sizes = [layer.get('size') for layer in layers]
    total = sum(sizes) if all(isinstance(size, int) and size >= 0 for size in sizes) else None
    if total is not None:
        needed = (total + 1024**2 - 1)//1024**2
        print(f'Tamano comprimido total: {total/1024**2:.1f} MiB | presupuesto: {max_bytes/1024**2:.0f} MiB', flush=True)
        if total > max_bytes:
            print(f'Cobertura parcial prevista por presupuesto; para todas las capas se necesitan al menos --max-mib {needed}.', flush=True)
    with tempfile.TemporaryDirectory(prefix='inspeccion-ecr-') as temporary:
        for index, layer in enumerate(layers, 1):
            size = layer.get('size')
            if not isinstance(size, int) or size < 0:
                issues.append(f'Capa {index}: tamano desconocido/no valido')
                continue
            if size > max_bytes - downloaded:
                detail = f'Capa {index}: {size/1024**2:.1f} MiB; quedan {(max_bytes-downloaded)/1024**2:.1f} MiB de presupuesto'
                issues.append(detail)
                print('[OMITIDA] ' + detail, flush=True)
                continue
            print(f'Inspeccion: capa {index}/{len(layers)} | {size/1024**2:.1f} MiB...', flush=True)
            response = ecr.get_download_url_for_layer(registryId=reference['cuenta'],
                repositoryName=reference['repositorio'], layerDigest=layer['digest'])
            location = response['downloadUrl']
            parsed = urlsplit(location)
            if parsed.scheme != 'https' or not parsed.hostname or not (parsed.hostname.endswith('.amazonaws.com') or parsed.hostname.endswith('.amazonaws.com.cn')):
                raise ValueError('Destino de descarga ECR no reconocido')
            path = Path(temporary)/'capa.tar'
            digest_bytes, layer_bytes = hashlib.sha256(), 0
            try:
                with open_image_layer(location) as source, path.open('wb') as target:
                    if urlsplit(source.geturl()).scheme != 'https':
                        raise ValueError('Redireccion fuera de HTTPS')
                    while True:
                        block = source.read(1024*1024)
                        if not block:
                            break
                        layer_bytes += len(block)
                        downloaded += len(block)
                        if downloaded > max_bytes or layer_bytes > size:
                            raise ValueError('Descarga supera el presupuesto o tamano del manifiesto')
                        digest_bytes.update(block)
                        target.write(block)
                if 'sha256:' + digest_bytes.hexdigest() != layer['digest'] or layer_bytes != size:
                    raise ValueError('Integridad de la capa no verificada')
                with path.open('rb') as handle:
                    issues.extend(scan_layer(handle, files))
                read += 1
            except HTTPError as exc:
                issues.append(f'Capa {index}: HTTP {exc.code}')
            except (URLError, OSError, tarfile.TarError, ValueError) as exc:
                # No imprimir excepciones de red: pueden contener URLs firmadas.
                detail = str(exc)[:180] if isinstance(exc, ValueError) else type(exc).__name__
                issues.append(f'Capa {index}: lectura incompleta ({detail})')
            finally:
                path.unlink(missing_ok=True)
    file_issues = list(issues)
    inventory = inventory_files(files)
    route_issues = [op['archivo_codigo'] + ': ' + op['referencia'] for op in inventory['operaciones_archivos'] if op['uso'] == 'NO ANALIZADO']
    route_issues.extend(f['archivo'] + ': ' + f['referencia'] for f in inventory['codigo']
                        if f['archivo'].startswith(APPLICATION_ROOT) and f['tipo'] == 'NO ANALIZADO')
    issues.extend(route_issues)
    return dict(digest=original, digest_plataforma=resolved, estado='PARCIAL' if issues else 'LEIDA',
        cobertura_archivos='PARCIAL' if file_issues else 'LEIDA',
        cobertura_rutas='PARCIAL' if file_issues or route_issues else 'LEIDA',
        limitaciones_archivos=file_issues, limitaciones_rutas=route_issues,
        capas_total=len(layers), capas_leidas=read, bytes_descargados=downloaded,
        bytes_total_capas=total, rutas_excluidas=list(EXCLUDED_IMAGE_PREFIXES),
        limitaciones=issues, **inventory)


def canonical_model(value, allow_unknown=False):
    """Normalizar nombres conocidos; conservar nuevos solo en declaraciones."""
    if not isinstance(value, str):
        return ''
    name = ' '.join(value.split())
    known = next((model for model in MODELS if model.lower() == name.lower()), '')
    if known or not allow_unknown:
        return known
    # Solo nombres acotados, sin URLs, rutas, credenciales o frases negadas.
    if (len(name) > 80 or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_.+-]*(?: [A-Za-z0-9][A-Za-z0-9_.+-]*){0,7}', name)
            or re.search(r'password|secret|token|credential|access.?key', name, re.I)
            or re.search(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b', name)
            or re.search(r'\b(?:no|not|none|null|unknown|sin|ninguno|pending|candidate|candidato|selected|chosen)\b', name, re.I)):
        return ''
    return name


def reported_model(value):
    """El sufijo numerico observado no pertenece al nombre ni identifica unidad."""
    name = re.sub(r'\s+-\s+\d+\s+-\s+\d+\s*$', '', value.strip())
    return canonical_model(name, allow_unknown=True)


def sanitize_message(message):
    """Muestra corta para diagnostico; suprimir credenciales y datos identificables."""
    text = str(message)[:16000]
    text = re.sub(r'(?i)\b(?:https?|s3)://[^\s<>]+', '[URL/RUTA]', text)
    text = re.sub(r'(?i)\bBearer\s+[^\s,;]+', 'Bearer [OCULTO]', text)
    text = re.sub(r'''(?ix)(["']?(?:password|passwd|token|authorization|secret|api_key|access_key|aws_secret_access_key|credential)[\w-]*["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}]+)''', r'\1[OCULTO]', text)
    text = re.sub(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b', '[CLAVE]', text)
    text = re.sub(r'[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}', '[CORREO]', text)
    text = re.sub(r'\b\d{1,3}(?:\.\d{1,3}){3}\b', '[IP]', text)
    text = re.sub(r'(?<!\w)/(?:[\w.~-]+/)+[^\s,;]*', '[RUTA]', text)
    return ' '.join(text.split())[:300]


def inspect_event(message):
    """Separar declaraciones explicitas de menciones; no certificar exito global."""
    try:
        data = json.loads(message)
    except (TypeError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    findings = []
    for key in ('selected_model', 'chosen_model', 'modelo_seleccionado', 'modelo_elegido'):
        model = canonical_model(data.get(key), allow_unknown=True)
        if model:
            findings.append(('Seleccion reportada', model, '', 'campo JSON: ' + key))
    # Texto solo cuando la declaracion es afirmativa, sin negaciones/candidatos.
    match = re.search(r'(?im)^\s*(?:\[(?:INFO|DEBUG)\]\s*)?(?:modelo seleccionado|modelo elegido|selected model|chosen model)\s*[:=]\s*([^\r\n]+?)\s*$', message)
    model = reported_model(match.group(1)) if match else ''
    if model:
        findings.append(('Seleccion reportada', model, '', 'declaracion textual'))
    # Formatos observados en las muestras reales aportadas desde CloudShell.
    # Los numeros posteriores no se interpretan como unidad, dia ni turno.
    prefix = r'(?im)^\s*(?:\[(?:INFO|DEBUG|WARNING|WARN)\]\s*)?'
    match = re.search(prefix + r'The best model to run the prediction is\s+([^\r\n]+?)\s*$', message)
    model = reported_model(match.group(1)) if match else ''
    if model:
        findings.append(('Seleccion reportada', model, '', 'formato observado: best model'))
    match = re.search(prefix + r'Not enough records in the train data for\s+([^\r\n]+?)\s*$', message)
    model = reported_model(match.group(1)) if match else ''
    if model:
        findings.append(('Datos insuficientes', model,
                         'Registros insuficientes para entrenamiento', 'formato observado: train data'))
    fallback_model = canonical_model(data.get('fallback_model'))
    if fallback_model or data.get('fallback_used') is True or data.get('uso_respaldo') is True:
        reason = data.get('fallback_reason', data.get('motivo_respaldo', ''))
        reason = sanitize_message(reason) if isinstance(reason, str) else ''
        findings.append(('Respaldo reportado', fallback_model, reason, 'campo JSON'))
    for error in ERRORS:
        if re.search(r'(?<!\w)' + re.escape(error) + r'(?!\w)', message):
            findings.append(('Error de dependencia mencionado', '', error, 'clase en log'))
    # Respaldo en texto permanece indicio hasta conocer el formato real.
    if re.search(r'\bUniform\b', message, re.I) and re.search(r'\b(?:fallback|respaldo|insufficient|insuficientes|zero matrix)\b', message, re.I):
        findings.append(('Indicio de respaldo', 'Uniform', '', 'texto; requiere revision'))
    if not findings and any(re.search(r'(?<!\w)' + re.escape(m) + r'(?!\w)', message, re.I) for m in MODELS):
        findings.append(('Mencion sin seleccion', '', '', 'texto sin declaracion explicita'))
    # Alias del mismo campo JSON no representan dos selecciones del modelo.
    unique = {}
    for finding in findings:
        unique.setdefault(finding[:3], finding)
    return list(unique.values())


def event_evidence(events, max_samples):
    counts, samples, sampled = Counter(), [], Counter()
    for event in events:
        message = str(event.get('message', ''))
        for kind, model, reason, source in inspect_event(message):
            counts[(kind, model or reason or 'Sin identificar')] += 1
            key = kind, model
            if sampled[key] < max_samples:
                sampled[key] += 1
                stamp = event.get('timestamp')
                samples.append(dict(tipo=kind, modelo=model, motivo=reason, fuente=source,
                    instante_utc=datetime.fromtimestamp(stamp/1000, timezone.utc).isoformat() if isinstance(stamp, (int, float)) else '',
                    mensaje_saneado=sanitize_message(message)))
    return counts, samples


def effective_environment(container, task):
    values = {v['name']: (v.get('value', ''), 'definicion') for v in container.get('environment', [])}
    for override in task.get('overrides', {}).get('containerOverrides', []):
        if override.get('name') == container['name']:
            values.update({v['name']: (v.get('value', ''), 'ejecucion') for v in override.get('environment', [])})
    return values


def safe_reference(name, value):
    """Valores de referencias acotadas; nunca exportar el entorno completo."""
    if re.search(r'secret|password|token|credential|access.?key|private', name, re.I):
        return None
    key = name.upper()
    if key in ('CAD', 'SIGLA', 'SIGLA_FISICA') and re.fullmatch(r'[A-Za-z][A-Za-z0-9]{2,11}', value):
        return 'Sigla', value.upper()
    if key in ('MODEL', 'MODELO', 'MODEL_NAME', 'MODELO_NOMBRE') and value in MODELS:
        return 'Modelo configurado', value
    if re.search(r'URL|ENDPOINT', key):
        try:
            parsed = urlsplit(value)
            if parsed.scheme in ('https', 'http') and parsed.hostname:
                # No conservar credenciales, query, fragmento ni rutas.
                host = parsed.hostname
                if parsed.port:
                    host += ':' + str(parsed.port)
                return 'Host API configurado', host
        except ValueError:
            return None
    if re.search(r'BUCKET|DATABASE|DB_NAME|TABLE|WORKGROUP|REGION', key):
        if re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value):
            return 'Recurso configurado', value
    return None


def log_references(events):
    counts = Counter()
    for event in events:
        message = str(event.get('message', ''))
        for model in MODELS:
            if re.search(r'(?<![\w])' + re.escape(model) + r'(?![\w])', message):
                counts[('Modelo mencionado', model)] += 1
        for error in ERRORS:
            if re.search(r'(?<![\w])' + re.escape(error) + r'(?![\w])', message):
                counts[('Error mencionado', error)] += 1
    return counts


def model_event_details(events):
    """Contexto explicito del mismo evento; nunca unir mensajes por proximidad."""
    rows, seen = [], set()
    aliases = {'proceso': ('id_process', 'process_id', 'id_proceso'),
               'unidad': ('unidad_base', 'unidad_id', 'unit_id'),
               'fecha_objetivo': ('fecha_objetivo', 'prediction_date')}
    for event in events:
        event_id = event.get('eventId')
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        message = str(event.get('message', ''))
        try:
            data = json.loads(message)
        except (TypeError, ValueError):
            data = {}
        maps = [data] if isinstance(data, dict) and data else []
        if maps:
            maps += [data[k] for k in ('context', 'contexto', 'metadata', 'data')
                     if isinstance(data.get(k), dict)]
        context, conflicts = {}, []
        for field, keys in aliases.items():
            values = set()
            invalid = False
            for mapping in maps:
                for key in keys:
                    if key not in mapping or mapping[key] is None:
                        continue
                    value = mapping[key]
                    if isinstance(value, (str, int)) and not isinstance(value, bool):
                        value = str(value)
                        if re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value):
                            values.add(value)
                            continue
                    invalid = True
            # Texto plano: solo etiquetas explicitas, no numeros sueltos.
            if not maps:
                pattern = r'(?<![\w])(?:' + '|'.join(keys) + r')\s*[=:]\s*([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})(?=\s|[,;]|$)'
                values.update(re.findall(pattern, message))
            if invalid or len(values) > 1:
                conflicts.append(field)
            context[field] = next(iter(values)) if len(values) == 1 and not invalid else ''
        process = context.pop('proceso')
        context['proceso_huella'] = hashlib.sha256(process.encode()).hexdigest()[:16] if process else ''
        context['alcance'] = ('PROCESO/UNIDAD' if process and context['unidad'] else
                              'PROCESO' if process else 'UNIDAD' if context['unidad'] else 'TAREA/SIGLA')
        context['contexto_ambiguo'] = ', '.join(conflicts)
        stamp = event.get('timestamp')
        instant = datetime.fromtimestamp(stamp/1000, timezone.utc).isoformat() if isinstance(stamp, (int, float)) else ''
        for kind, model, reason, source in inspect_event(message):
            rows.append(dict(**context, evento_id=event_id or '', instante_utc=instant,
                             tipo=kind, modelo=model, motivo=reason, fuente=source))
    return rows


def write_model_correlation(folder, events, coverage):
    """Conteos de declaraciones, no conteos de predicciones terminadas."""
    detail_fields = ['tarea', 'sigla', 'contenedor', 'stream', 'cobertura', 'evento_id',
        'instante_utc', 'proceso_huella', 'unidad', 'fecha_objetivo', 'alcance',
        'contexto_ambiguo', 'tipo', 'modelo', 'motivo', 'fuente']
    group_fields = ['tarea', 'sigla', 'contenedor', 'stream', 'cobertura',
                    'proceso_huella', 'unidad', 'fecha_objetivo', 'alcance', 'tipo', 'modelo']
    grouped = {}
    for event in events:
        key = tuple(event[k] for k in group_fields)
        row = grouped.setdefault(key, dict(zip(group_fields, key), declaraciones=0,
                                           primera_utc=event['instante_utc'], ultima_utc=event['instante_utc']))
        row['declaraciones'] += 1
        row['primera_utc'] = min(row['primera_utc'], event['instante_utc'])
        row['ultima_utc'] = max(row['ultima_utc'], event['instante_utc'])
    for name, fields, rows in (
            ('eventos_modelos.csv', detail_fields, events),
            ('resumen_modelos.csv', group_fields + ['declaraciones', 'primera_utc', 'ultima_utc'], grouped.values())):
        with (folder/name).open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    print('\nMODELOS | Declaraciones observadas en la ventana')
    print('Tarea        | Sigla      | Seleccion | Uniform | Respaldo | Con proceso* | Lectura')
    print('-------------+------------+-----------+---------+----------+--------------+----------------')
    for item in coverage:
        selected = [e for e in events if e['tarea'] == item['tarea'] and e['contenedor'] == item['contenedor']]
        declarations = [e for e in selected if e['tipo'] in ('Seleccion reportada', 'Respaldo reportado')]
        counts = Counter(e['tipo'] for e in declarations)
        uniform = sum(e['tipo'] == 'Seleccion reportada' and e['modelo'] == 'Uniform' for e in declarations)
        linked = sum(bool(e['proceso_huella']) for e in declarations)
        print(f"{item['tarea'][:12] or 'CONFIG':12} | {item['sigla'][:10]:10} | {counts['Seleccion reportada']:9} | {uniform:7} | {counts['Respaldo reportado']:8} | {str(linked)+'/'+str(len(declarations)):12} | {item['logs']}")
    print('* Con proceso: declaraciones con ID explicito en el mismo evento; ID guardado como huella.')
    print('Uniform no implica respaldo. Sin contexto no se atribuye proceso/unidad; no se unen mensajes independientes.')
    print('Los conteos son eventos/declaraciones, no predicciones unicas ni finalizadas. Cero puede significar ausencia de evidencia.')
    print(f'Modelos locales: {folder}/eventos_modelos.csv | resumen_modelos.csv; cobertura en contexto.json')


def read_references(logs, group, stream, start, end, limit, max_samples=3, details=None):
    events = []
    for page in logs.get_paginator('filter_log_events').paginate(
            logGroupName=group, logStreamNames=[stream], filterPattern=LOG_FILTER,
            startTime=int(start.timestamp()*1000), endTime=int(end.timestamp()*1000),
            PaginationConfig={'MaxItems': limit+1, 'PageSize': min(limit+1, 10000)}):
        events.extend(page.get('events', []))
    selected = events[:limit]
    if details is not None:
        details.extend(model_event_details(selected))
    counts, samples = event_evidence(selected, max_samples)
    counts.update(log_references(selected))
    return counts, len(events) > limit, len(selected), samples


def stopped_log_window(task, start, now):
    began, stopped = task.get('createdAt') or task.get('startedAt'), task.get('stoppedAt')
    if (task.get('lastStatus') == 'STOPPED' and isinstance(began, datetime) and began.tzinfo
            and isinstance(stopped, datetime) and stopped.tzinfo and began <= stopped <= now):
        return began, min(stopped + timedelta(seconds=1), now)
    return start, now


def collect_tasks(ecs, cluster, family, limit, include_stopped=False):
    arns = []
    issues = []
    for status in ('RUNNING', 'STOPPED') if include_stopped else ('RUNNING',):
        found = []
        for page in ecs.get_paginator('list_tasks').paginate(
                cluster=cluster, family=family, desiredStatus=status,
                PaginationConfig={'MaxItems': limit+1, 'PageSize': min(limit+1, 100)}):
            found.extend(page.get('taskArns', []))
        if len(found) > limit:
            issues.append(f'Tareas {status} truncadas; aumentar --max-tareas')
        arns.extend(found[:limit])
    arns = list(dict.fromkeys(arns))
    tasks = []
    for offset in range(0, len(arns), 100):
        response = ecs.describe_tasks(cluster=cluster, tasks=arns[offset:offset+100])
        tasks.extend(response.get('tasks', []))
        for failure in response.get('failures', []):
            issues.append('DescribeTasks incompleto: ' + str(failure.get('reason', 'sin motivo')))
    return tasks, issues


def task_lifecycle_rows(tasks, definitions):
    rows = []
    def instant(value):
        return value.astimezone(BOGOTA).isoformat() if isinstance(value, datetime) and value.tzinfo else ''
    for task in tasks:
        definition = definitions.get(task['taskDefinitionArn'], {})
        siglas = set()
        for container in definition.get('containerDefinitions', []):
            for name, (value, _) in effective_environment(container, task).items():
                ref = safe_reference(name, value)
                if ref and ref[0] == 'Sigla':
                    siglas.add(ref[1])
        exits = {c.get('name', '?'): c['exitCode'] for c in task.get('containers', []) if 'exitCode' in c}
        stopped = task.get('lastStatus') == 'STOPPED'
        if not stopped:
            outcome = task.get('lastStatus', 'SIN ESTADO')
        elif task.get('stopCode') == 'TaskFailedToStart':
            outcome = 'ERROR ARRANQUE'
        elif any(code != 0 for code in exits.values()):
            outcome = 'SALIDA NO CERO'
        elif exits:
            outcome = 'SALIDAS 0 OBSERVADAS'
        else:
            outcome = 'SIN CODIGO DE SALIDA'
        rows.append(dict(tarea=task['taskArn'].rsplit('/', 1)[-1], task_arn=task['taskArn'],
            siglas=','.join(sorted(siglas)) or 'SIN DATO', estado_ecs=task.get('lastStatus', 'SIN ESTADO'),
            creacion=instant(task.get('createdAt')), inicio=instant(task.get('startedAt')),
            fin=instant(task.get('stoppedAt')), resultado_contenedores=outcome,
            codigos_salida=json.dumps(exits, ensure_ascii=False), stop_code=task.get('stopCode', ''),
            motivo_parada=sanitize_message(task.get('stoppedReason', '')),
            motivos_contenedores='; '.join(c.get('name', '?')+': '+sanitize_message(c['reason'])
                for c in task.get('containers', []) if c.get('reason'))))
    return rows


def print_task_lifecycle(rows):
    print('\nTAREAS VISIBLES EN ECS | Horas de Bogota')
    print('Tarea        | Siglas          | Estado ECS | Inicio              | Fin')
    print('-------------+-----------------+------------+---------------------+--------------------')
    for row in rows:
        print(f"{row['tarea'][:12]:12} | {row['siglas'][:15]:15} | {row['estado_ecs']:10} | {(row['inicio'][:19] or '-'):19} | {row['fin'][:19] or '-'}")
    counts = Counter(row['estado_ecs'] for row in rows)
    print('Estados observados: '+(', '.join(f'{k}={v}' for k, v in sorted(counts.items())) or 'sin tareas'))
    print('STOPPED significa detenida; no certifica prediccion completa. Codigos y motivos en tareas.csv.')
    print('Solo tareas que ECS aun conserva; no es historial completo del domingo ni de la semana.')


def print_table(rows):
    widths = (12, 10, 24, 32, 22, 7)
    def render(values):
        print(' | '.join(textwrap.shorten(str(v), width=w, placeholder='...').ljust(w)
                         for v, w in zip(values, widths)))
    render(('Tarea', 'Sigla', 'Tipo', 'Referencia', 'Evidencia', 'Eventos'))
    print('-+-'.join('-'*w for w in widths))
    for row in rows:
        render((row['tarea'][:12] or 'CONFIG', row['sigla'], row['tipo'], row['referencia'], row['evidencia'], row['eventos'] if row['eventos'] != '' else '-'))


INPUT_BUCKET = 'pon-prod-ml-iapol'
INPUT_KEYS = ('EstructuraNacionaOperativalPONAL.csv', 'utilidades/MaestroMdP.csv')
INPUT_HEADER_BYTES = 64 * 1024
# Perfil de referencia, no deteccion del entorno ni prueba de ejecucion.
SELECTOR_SOURCE_DIGEST = 'sha256:83ea3bbbfef167bfa0882de5b188a6baa6a1ffef3ae327f6be1c70287b76bbc8'
# pandas 1.1.3 read_csv defaults: na_filter=True, keep_default_na=True.
# https://pandas.pydata.org/pandas-docs/version/1.1.3/reference/api/pandas.read_csv.html
SELECTOR_NA_VALUES = frozenset(('', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN',
    '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null'))


def csv_input_header(data, total_bytes, encoding, separator):
    """Leer solo el primer registro CSV completo dentro de una muestra acotada.

    No exportar ni analizar las filas de negocio. Cada separador se prueba con
    un generador nuevo para no decodificar filas posteriores a la cabecera.
    """
    def lines():
        decoder = codecs.getincrementaldecoder(encoding)(errors='strict')
        parts = data.splitlines(keepends=True)
        consumed = 0
        for raw in parts:
            consumed += len(raw)
            if not raw.endswith((b'\n', b'\r')) and consumed == len(data) and total_bytes > len(data):
                raise ValueError('La cabecera no termina dentro de 64 KiB; no se evalua')
            yield decoder.decode(raw, final=consumed == len(data) and total_bytes <= len(data))
    delimiters = (',', ';', '\t', '|') if separator == 'auto' else ('\t' if separator == 'tab' else separator,)
    candidates = []
    for delimiter in delimiters:
        try:
            header = next(csv.reader(lines(), delimiter=delimiter, strict=True))
            if header and (len(header) > 1 or separator != 'auto'):
                candidates.append((delimiter, header))
        except (csv.Error, UnicodeError, ValueError, StopIteration):
            continue
    if len(candidates) != 1:
        detail = 'separador ambiguo' if len(candidates) > 1 else 'cabecera, encoding o separador no reconocidos'
        raise ValueError(detail + '; usar --encoding-entradas/--separador-entradas si se conocen')
    delimiter, header = candidates[0]
    if len(header) > 256 or any(len(name) > 160 for name in header):
        raise ValueError('Cabecera supera el limite de 256 columnas/160 caracteres por nombre')
    if any('\x00' in name or any(ord(ch) < 32 for ch in name) for name in header):
        raise ValueError('Cabecera contiene caracteres no interpretables como nombres de columnas')
    return delimiter, header


def input_row_statistics(data, header, delimiter, required, encoding, group_structure=False, selector=False):
    """Contar vacios; solo exportar siglas/tipos agregados, no registros ni IDs."""
    total = valid = empty = inconsistent = 0
    blanks = [0] * len(header)
    complete, reason = False, ''
    group_columns = ('SIGLA_FISICA', 'SIGLA_PAPA', 'TIUN_CODIGO', 'UNIDAD_ID', 'UNDE_CONSECUTIVO')
    group_missing = [name for name in group_columns if header.count(name) != 1]
    positions = {name: header.index(name) for name in group_columns if header.count(name) == 1}
    groups, grouped_by_limit, projected_rows = {}, 0, 0
    selector_columns = ('SIGLA_PAPA', 'UNIDAD_ID')
    selector_missing = [name for name in selector_columns if header.count(name) != 1]
    selector_positions = {name: header.index(name) for name in selector_columns if header.count(name) == 1}
    selector_groups, selector_unprojected = {}, 0
    def project_dimension(cell, is_type=False):
        text = cell.strip()
        if not text:
            return '[VACIA]'
        if is_type:
            return text if re.fullmatch(r'[0-9]{1,12}(?:\.[0-9]{1,6})?', text) else '[NO PROYECTADO]'
        text = text.upper()
        return text if re.fullmatch(r'[A-Z0-9_-]{1,24}', text) else '[NO PROYECTADO]'
    old_limit = csv.field_size_limit()
    try:
        csv.field_size_limit(max(old_limit, len(data)))
        text = data.decode(encoding, errors='strict')
        reader = csv.reader(io.StringIO(text, newline=''), delimiter=delimiter, strict=True)
        if next(reader, None) != header:
            raise ValueError('Cabecera diferente de la interpretada inicialmente')
        for values in reader:
            if total >= 100000:
                reason = 'Limite de 100000 registros CSV; conteos parciales'
                break
            total += 1
            is_empty = not values or all(not v.strip() for v in values)
            empty += int(is_empty)
            if not values:
                continue
            if len(values) != len(header):
                inconsistent += 1
                continue
            if is_empty:
                continue
            valid += 1
            for index, cell in enumerate(values):
                blanks[index] += int(not cell.strip())
            if selector and not selector_missing:
                parent = values[selector_positions['SIGLA_PAPA']]
                # Igualdad del consumidor: no convertir mayusculas ni quitar espacios.
                unprojected = not re.fullmatch(r'[A-Za-z0-9_-]{1,24}', parent) or parent in SELECTOR_NA_VALUES
                if unprojected:
                    parent = '[SIGLA NO PROYECTADA]'
                if parent not in selector_groups and len(selector_groups) >= 10000:
                    parent = '[SIGLA NO PROYECTADA]'
                    unprojected = True
                selector_unprojected += int(bool(unprojected))
                group = selector_groups.setdefault(parent, Counter(registros=0, id_nulos=0, candidatos=0, id_solo_espacios=0))
                identifier = values[selector_positions['UNIDAD_ID']]
                is_na = identifier in SELECTOR_NA_VALUES
                group['registros'] += 1
                group['id_nulos'] += int(is_na)
                group['candidatos'] += int(not is_na)
                group['id_solo_espacios'] += int(bool(identifier) and not identifier.strip())
            if group_structure and not group_missing:
                dimensions = (project_dimension(values[positions['SIGLA_FISICA']]),
                              project_dimension(values[positions['SIGLA_PAPA']]),
                              project_dimension(values[positions['TIUN_CODIGO']], is_type=True))
                projected_rows += int('[NO PROYECTADO]' in dimensions)
                if dimensions not in groups and len(groups) >= 10000:
                    dimensions = ('[SIN DESGLOSE POR LIMITE]',) * 3
                    grouped_by_limit += 1
                counts = groups.setdefault(dimensions, Counter(registros=0, unidad_id_vacios=0, unde_consecutivo_vacios=0, ambos_vacios=0))
                unit_empty = not values[positions['UNIDAD_ID']].strip()
                parent_empty = not values[positions['UNDE_CONSECUTIVO']].strip()
                counts['registros'] += 1
                counts['unidad_id_vacios'] += int(unit_empty)
                counts['unde_consecutivo_vacios'] += int(parent_empty)
                counts['ambos_vacios'] += int(unit_empty and parent_empty)
        else:
            complete = True
    except (UnicodeError, csv.Error, ValueError):
        # No guardar excepciones con fragmentos de datos de negocio.
        reason = 'Lectura de filas no completada: encoding, sintaxis o limite CSV; conteos parciales'
    finally:
        csv.field_size_limit(old_limit)
    unique = {name: header.index(name) for name in required if header.count(name) == 1}
    missing = sorted(set(required) - set(header))
    ambiguous = sorted({name for name in required if header.count(name) > 1})
    counts = {name: blanks[index] for name, index in unique.items()}
    unnamed = [dict(posicion=index+1, filas_evaluadas=valid, celdas_no_vacias=valid-blanks[index])
               for index, name in enumerate(header) if not name.strip()]
    breakdown = [dict(sigla_fisica=key[0], sigla_papa=key[1], tiun_codigo=key[2], **counts,
        filas_con_algun_vacio=counts['unidad_id_vacios']+counts['unde_consecutivo_vacios']-counts['ambos_vacios'],
        porcentaje_unidad_id_vacios=round(100*counts['unidad_id_vacios']/counts['registros'], 2))
        for key, counts in groups.items()]
    breakdown.sort(key=lambda r: (-r['unidad_id_vacios'], -r['unde_consecutivo_vacios'], r['sigla_fisica'], r['sigla_papa'], r['tiun_codigo']))
    group_coverage = 'NO EVALUADO' if not group_structure or group_missing else 'LEIDA' if complete and not grouped_by_limit and not projected_rows else 'PARCIAL'
    group_reason = ('No aplica a este archivo' if not group_structure else
        'Columnas ausentes o ambiguas: ' + ', '.join(group_missing) if group_missing else
        'Conteos por SIGLA_FISICA/SIGLA_PAPA y TIUN_CODIGO; codigos sin interpretacion de negocio')
    selector_rows = [dict(sigla_papa=name, **values) for name, values in sorted(selector_groups.items())]
    selector_coverage = 'NO EVALUADO' if not selector or selector_missing else 'LEIDA' if complete and not inconsistent and not selector_unprojected else 'PARCIAL'
    selector_reason = ('Perfil no solicitado' if not selector else
        'Columnas ausentes o ambiguas: ' + ', '.join(selector_missing) if selector_missing else
        'Referencia: filtrar SIGLA_PAPA exacta y quitar UNIDAD_ID nulos; read_csv con defaults de pandas 1.1.3')
    return dict(cobertura_selector=selector_coverage, motivo_selector=selector_reason,
        selector_unidades=json.dumps(selector_rows, ensure_ascii=False),
        selector_siglas_no_proyectadas=selector_unprojected,
        cobertura_desglose=group_coverage, motivo_desglose=group_reason,
        filas_agrupadas_por_limite=grouped_by_limit, filas_dimensiones_no_proyectadas=projected_rows,
        desglose_unidad_id=json.dumps(breakdown, ensure_ascii=False),
        filas_contenido='EVALUADO' if complete else 'NO EVALUADO',
        cobertura_filas='LEIDA' if complete else 'PARCIAL', filas_totales=total,
        filas_datos_validas=valid, filas_vacias=empty, filas_campos_inconsistentes=inconsistent,
        celdas_vacias_configuradas=sum(counts.values()) if unique else '',
        vacios_por_columna=json.dumps(counts, ensure_ascii=False),
        columnas_sin_nombre=json.dumps(unnamed, ensure_ascii=False),
        columnas_ambiguas=json.dumps(ambiguous, ensure_ascii=False),
        motivo_filas=reason or ('Sin filas de datos con estructura valida' if valid == 0 else
            'Conteos completos; vacios por columna sobre filas no vacias con estructura valida'),
        aviso_filas=bool(empty or inconsistent or any(counts.values()) or not valid or missing or ambiguous or group_structure and group_coverage != 'LEIDA'))


def read_input_header(s3, owner, key, required, encoding, separator, row,
                      validate_rows=False, max_input_bytes=5*1024**2, selector=False):
    """HEAD y GET del mismo ETag; cabecera o filas optativas con limite de lectura."""
    request = dict(Bucket=INPUT_BUCKET, Key=key, ExpectedBucketOwner=owner)
    head = s3.head_object(**request)
    size = head['ContentLength']
    row.update(existencia='OK', tamano_bytes=size, etag=head.get('ETag', ''),
               version=head.get('VersionId', ''), ultima_modificacion=head.get('LastModified').isoformat() if head.get('LastModified') else '')
    if size == 0:
        row.update(estado='FALLA', cabecera='VACIA', motivo='Objeto candidato vacio; no se declara fallo de prediccion')
        return
    if not head.get('ETag'):
        row.update(estado='NO EVALUADO', motivo='Sin ETag para vincular metadata y lectura')
        return
    full_read = validate_rows and size <= max_input_bytes
    limit = size if full_read else INPUT_HEADER_BYTES
    response = s3.get_object(**request, Range=f'bytes=0-{limit-1}', IfMatch=head['ETag'])
    body = response['Body']
    try:
        data = body.read(limit)
    finally:
        body.close()
    row['bytes_leidos'] = len(data)
    if len(data) != min(size, limit) or response.get('ETag') != head['ETag']:
        row.update(estado='NO EVALUADO', motivo='Lectura incompleta o ETag distinto; no se evalua contenido')
        return
    row['lectura_cloudshell'] = 'OK'
    # Aun al leer filas, la cabecera conserva el limite propio de 64 KiB.
    delimiter, header = csv_input_header(data[:INPUT_HEADER_BYTES], size, encoding, separator)
    row.update(separador='tab' if delimiter == '\t' else delimiter,
               columnas=json.dumps(header, ensure_ascii=False), numero_columnas=len(header))
    header_warning = any(not name.strip() for name in header) or len({name.strip() for name in header}) != len(header)
    row['cabecera'] = 'REVISAR' if header_warning else 'OK'
    missing = sorted(set(required) - set(header))
    row['columnas_faltantes'] = json.dumps(missing, ensure_ascii=False)
    if not required:
        reason = 'Cabecera con nombres vacios o repetidos; esquema pendiente' if header_warning else 'Cabecera legible; columnas obligatorias no confirmadas en el consumidor'
        row.update(estado='AVISO', motivo=reason)
    elif missing:
        row.update(estado='FALLA', columnas_requeridas='FALTANTES', motivo='Faltan columnas del contrato configurado; no certifica fallo de tarea')
    elif header_warning:
        row.update(estado='AVISO', columnas_requeridas='OK', motivo='Columnas configuradas presentes; cabecera con nombres vacios o repetidos requiere revision')
    else:
        row.update(estado='OK', columnas_requeridas='OK', motivo='Cumple cabecera y columnas configuradas')
    if validate_rows:
        if not full_read:
            row.update(motivo_filas='Objeto supera --max-mib-entradas; se leyo solo cabecera, filas NO EVALUADAS')
            if row['estado'] != 'FALLA':
                row.update(estado='NO EVALUADO', motivo=row['motivo'] + '; filas fuera del presupuesto')
            return
        stats = input_row_statistics(data, header, delimiter, required, encoding, group_structure=key == INPUT_KEYS[0],
                                     selector=selector and key == INPUT_KEYS[0])
        warning = stats.pop('aviso_filas')
        row.update(stats)
        if row['estado'] != 'FALLA':
            if row['cobertura_filas'] != 'LEIDA':
                row.update(estado='NO EVALUADO', motivo=row['motivo'] + '; lectura de filas parcial')
            elif warning:
                row.update(estado='AVISO', motivo=row['motivo'] + '; revisar conteos de filas/vacios')


def write_structure_breakdown(folder, row):
    groups = json.loads(row['desglose_unidad_id'])
    coverage = row['cobertura_desglose']
    columns = ['cobertura', 'filas_base', 'sigla_fisica', 'sigla_papa', 'tiun_codigo', 'registros',
               'unidad_id_vacios', 'porcentaje_unidad_id_vacios', 'unde_consecutivo_vacios',
               'ambos_vacios', 'filas_con_algun_vacio', 'motivo']
    path = Path(folder)/'desglose_estructura.csv'
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        metadata = dict(cobertura=coverage, filas_base=row['filas_datos_validas'], motivo=row['motivo_desglose'])
        writer.writerows(dict(**metadata, **g) for g in groups)
        if not groups:
            # Un desglose no evaluado no se convierte en cero grupos confirmados.
            writer.writerow(metadata)
    affected = [g for g in groups if g['filas_con_algun_vacio']]
    group_count = len(groups) if coverage != 'NO EVALUADO' else '-'
    affected_count = len(affected) if coverage != 'NO EVALUADO' else '-'
    print(f'\nDESGLOSE ESTRUCTURA | {coverage} | Grupos: {group_count} | Con vacios de ID/UNDE: {affected_count}')
    print('Fisica       | Papa         | TIUN     | Filas | Sin UNIDAD_ID | % grupo | Sin UNDE')
    print('-------------+--------------+----------+-------+---------------+---------+---------')
    for group in affected[:10]:
        values = (group['sigla_fisica'], group['sigla_papa'], group['tiun_codigo'])
        a, b, c = (textwrap.shorten(v, width=w, placeholder='...') for v, w in zip(values, (12, 12, 8)))
        print(f'{a:12} | {b:12} | {c:8} | {group["registros"]:5} | {group["unidad_id_vacios"]:13} | {group["porcentaje_unidad_id_vacios"]:7.2f} | {group["unde_consecutivo_vacios"]:8}')
    if not groups:
        print(row['motivo_desglose'] or 'Sin desglose evaluable; revisar cobertura de filas.')
    if len(affected) > 10:
        print(f'Se muestran 10 de {len(affected)} grupos con vacios; detalle completo en CSV.')
    if row['filas_agrupadas_por_limite']:
        print('Filas sin grupo individual por limite: ' + str(row['filas_agrupadas_por_limite']))
    if row['filas_dimensiones_no_proyectadas']:
        print('Filas con sigla/tipo no proyectados: ' + str(row['filas_dimensiones_no_proyectadas']))
    print('TIUN_CODIGO es un codigo observado; no determina si UNIDAD_ID deberia existir.')
    print(f'Desglose local: {path}')


def write_selector_units(folder, row):
    groups = json.loads(row['selector_unidades'])
    fields = ['digest_referencia', 'cobertura', 'sigla_papa', 'registros', 'id_nulos', 'candidatos', 'id_solo_espacios', 'motivo']
    path = Path(folder)/'selector_unidades.csv'
    metadata = dict(digest_referencia=SELECTOR_SOURCE_DIGEST, cobertura=row['cobertura_selector'], motivo=row['motivo_selector'])
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(**metadata, **g) for g in groups)
        if not groups:
            writer.writerow(metadata)
    print(f'\nSELECTOR DE REFERENCIA | {row["cobertura_selector"]} | Codigo {SELECTOR_SOURCE_DIGEST[7:19]}')
    if groups:
        total = sum(g['registros'] for g in groups)
        excluded = sum(g['id_nulos'] for g in groups)
        candidates = sum(g['candidatos'] for g in groups)
        print(f'Filas evaluadas: {total} | Nulos segun perfil: {excluded} | Candidatos no nulos: {candidates}')
        without = sum(g['candidatos'] == 0 for g in groups if g['sigla_papa'] != '[SIGLA NO PROYECTADA]')
        print(f'SIGLA_PAPA sin candidatos: {without}; no demuestra que deba ejecutarse una prediccion para ellas.')
        spaces = sum(g['id_solo_espacios'] for g in groups)
        if spaces:
            print(f'[AVISO] IDs de solo espacios: {spaces}; no se equiparan automaticamente a nulos de pandas.')
    else:
        print(row['motivo_selector'] or 'Selector no evaluado; verificar lectura y columnas.')
    print('Perfil de referencia, no ejecucion del consumidor. Sin excepciones por TIUN ni garantia de ID valido.')
    print(f'Selector local: {path}')


def validate_candidate_inputs(session, config, identity, args, error_types):
    """Orquestacion independiente: errores por objeto no ocultan el otro control."""
    folder = Path(args.salida)/'modelos_dependencias'/datetime.now(timezone.utc).astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    s3 = session.client('s3', config=config)
    requirements = (args.columnas_estructura, args.columnas_maestro)
    rows = []
    print('ENTRADAS CANDIDATAS | S3 | Permisos de la sesion CloudShell | Solo lectura')
    for key, required in zip(INPUT_KEYS, requirements):
        row = dict(recurso='s3://' + INPUT_BUCKET + '/' + key, estado='NO EVALUADO', existencia='NO EVALUADO',
            tamano_bytes='', bytes_leidos=0, etag='', version='', ultima_modificacion='',
            lectura_cloudshell='NO EVALUADO', cabecera='NO EVALUADO', numero_columnas='', columnas='[]',
            separador='', encoding=args.encoding_entradas, columnas_requeridas='NO EVALUADO',
            contrato_configurado=json.dumps(required, ensure_ascii=False), columnas_faltantes='[]',
            filas_contenido='NO EVALUADO', cobertura_filas='NO EVALUADO',
            filas_totales='', filas_datos_validas='', filas_vacias='', filas_campos_inconsistentes='',
            celdas_vacias_configuradas='', vacios_por_columna='{}', columnas_sin_nombre='[]',
            columnas_ambiguas='[]', motivo_filas='', cobertura_desglose='NO EVALUADO', motivo_desglose='',
            filas_agrupadas_por_limite='', filas_dimensiones_no_proyectadas='', desglose_unidad_id='[]',
            cobertura_selector='NO EVALUADO', motivo_selector='', selector_unidades='[]',
            selector_siglas_no_proyectadas='', motivo='')
        try:
            read_input_header(s3, args.cuenta, key, required, args.encoding_entradas, args.separador_entradas, row,
                              args.validar_filas, args.max_mib_entradas*1024**2, selector=args.selector_unidades)
        except error_types as exc:
            response = getattr(exc, 'response', {})
            code = str(response.get('Error', {}).get('Code', type(exc).__name__))
            # 403 no prueba ausencia. HEAD 404 tampoco prueba que el candidato
            # sea obligatorio para la modalidad actual.
            if code in ('404', 'NoSuchKey', 'NotFound') and row['existencia'] != 'OK':
                row.update(existencia='AUSENTE', estado='AVISO', motivo='Objeto candidato no encontrado; obligatoriedad no confirmada')
            elif code in ('412', 'PreconditionFailed'):
                row.update(motivo='El objeto cambio entre HEAD y GET; repetir la validacion')
            else:
                row.update(motivo='Lectura no completada: ' + code + '; no demuestra ausencia ni fallo ECS')
        except (ValueError, csv.Error, UnicodeError) as exc:
            row.update(estado='NO EVALUADO', motivo=str(exc))
        rows.append(row)
    with (folder/'control_entradas.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    context = dict(captura_utc=datetime.now(timezone.utc).isoformat(), cuenta=identity['Account'],
        principal=identity.get('Arn', ''), controles=rows, limite_bytes_por_objeto=args.max_mib_entradas*1024**2 if args.validar_filas else INPUT_HEADER_BYTES,
        modo='FILAS' if args.validar_filas else 'CABECERAS',
        perfil_selector=dict(digest_referencia=SELECTOR_SOURCE_DIGEST,
            filtro='SIGLA_PAPA == cad; dropna(subset=[UNIDAD_ID])',
            supuestos='read_csv con NA defaults de pandas 1.1.3; sin inferencia de tipos ni replica completa del parser',
            codigos_tiun_no_excluidos=True, ejecucion_del_consumidor='NO EVALUADA') if args.selector_unidades else None,
        alcance='Dos entradas candidatas; HEAD y GET acotado con IfMatch. Filas solo con --validar-filas; exporta conteos y siglas/tipos agregados, no registros ni IDs. No consulta resultados/ECS/ECR ni escribe AWS.',
        pendientes=['Contrato de columnas del consumidor si no se configura', 'Necesidad por modalidad/sigla',
                    'Reglas de elegibilidad de tipos y significado de los vacios'],
        fuera_de_alcance=['Comprobar acceso desde ECS', 'Validar resultados de prediccion en S3'],
        referencias=['https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html',
                     'https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html'])
    (folder/'contexto_entradas.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Archivo                                  | Existe       | Lectura      | Columnas | Control')
    print('-----------------------------------------+--------------+--------------+----------+-------------')
    for row in rows:
        key = row['recurso'].split('/', 3)[-1]
        print(f'{key:40} | {row["existencia"]:12} | {row["lectura_cloudshell"]:12} | {str(row["numero_columnas"]):8} | {row["estado"]}')
        print('  ' + row['motivo'])
    if args.validar_filas:
        print('\nFILAS | Conteos agregados; no valores de negocio')
        print('Archivo          | Cobertura    | Registros | Vacias | Desajustes | Celdas vacias*')
        print('-----------------+--------------+-----------+--------+------------+---------------')
        for label, row in zip(('Estructura PONAL', 'MaestroMdP'), rows):
            print(f'{label:16} | {row["cobertura_filas"]:12} | {str(row["filas_totales"]):9} | {str(row["filas_vacias"]):6} | {str(row["filas_campos_inconsistentes"]):10} | {row["celdas_vacias_configuradas"]}')
            if row['motivo_filas']:
                print('  ' + row['motivo_filas'])
            unnamed = json.loads(row['columnas_sin_nombre'])
            for column in unnamed[:3]:
                print(f'  Columna sin nombre #{column["posicion"]}: no vacias={column["celdas_no_vacias"]}/{column["filas_evaluadas"]} filas evaluadas con estructura valida.')
            if len(unnamed) > 3:
                print('  Mas columnas sin nombre: detalle en CSV.')
        print('* Solo columnas configuradas y unicas, sobre filas no vacias con estructura valida; detalle en CSV.')
    else:
        print('Solo cabeceras; filas NO EVALUADAS.')
    print('Sin umbrales de antiguedad.')
    if args.validar_filas:
        write_structure_breakdown(folder, rows[0])
    if args.selector_unidades:
        write_selector_units(folder, rows[0])
    print(f'Reportes: {folder}/control_entradas.csv | contexto_entradas.json')
    if any(r['estado'] == 'NO EVALUADO' or r['existencia'] != 'OK' or args.validar_filas and (r['cobertura_filas'] != 'LEIDA' or r['recurso'].endswith('/' + INPUT_KEYS[0]) and r['cobertura_desglose'] != 'LEIDA') or args.selector_unidades and r['recurso'].endswith('/' + INPUT_KEYS[0]) and r['cobertura_selector'] != 'LEIDA' for r in rows):
        return 3
    return 1 if any(r['estado'] == 'FALLA' for r in rows) else 0


def docker_command(command, timeout, password=None):
    """Sin shell, salida acotada y credenciales solo por stdin."""
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        result = subprocess.run(command, input=password, stdout=out, stderr=err,
                                timeout=timeout, check=False)
        out.seek(0)
        # El motivo suele estar al final, despues del progreso de descarga.
        err.seek(0, 2)
        err.seek(max(0, err.tell()-4096))
        return result.returncode, out.read(65536).decode('utf-8', errors='replace'), err.read(4096).decode('utf-8', errors='replace')


def docker_failure_detail(code, output, error):
    lines = [line.strip() for line in (error or output).splitlines() if line.strip()]
    detail = sanitize_message(lines[-1]) if lines else 'Docker no devolvio un motivo'
    return f'Docker pull fallo (salida {code}): {detail}'


def import_container_command(docker, docker_config, image, interpreter, module, name):
    return [docker, '--config', docker_config, 'run', '--rm', '--pull', 'never', '--no-healthcheck', '--name', name,
        '--network', 'none', '--read-only', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges', '--pids-limit', '128',
        '--memory', '1g', '--cpus', '1', '--user', '65534:65534',
        '--tmpfs', '/tmp:rw,nosuid,nodev,size=67108864', '--workdir', '/tmp',
        '--env', 'HOME=/tmp', '--env', 'MPLCONFIGDIR=/tmp/matplotlib',
        '--env', 'MPLBACKEND=Agg', '--env', 'OPENBLAS_NUM_THREADS=1',
        '--env', 'OMP_NUM_THREADS=1', '--env', 'MKL_NUM_THREADS=1',
        '--env', 'AWS_EC2_METADATA_DISABLED=true', '--entrypoint', interpreter,
        image, '-I', '-B', '-c', IMPORT_PROBE, module]


def import_result(code, output, interpreter):
    lines = [line.partition('=')[2] for line in output.splitlines()
             if line.startswith('PONAL_IMPORT_RESULT=')]
    if len(lines) != 1:
        return dict(estado='NO EVALUADO', error='Sin resultado interpretable', detalle=f'Salida contenedor={code}')
    try:
        result = json.loads(lines[0])
    except ValueError:
        return dict(estado='NO EVALUADO', error='Resultado JSON no interpretable', detalle='')
    if (not isinstance(result, dict) or result.get('interprete') != interpreter or
            result.get('entorno_real') != posixpath.dirname(posixpath.dirname(interpreter))):
        return dict(estado='NO EVALUADO', error='Interprete no confirmado', detalle='')
    if code == 0 and result.get('estado') == 'OK':
        return dict(estado='OK', python=str(result.get('python', ''))[:40], entorno_real=result['entorno_real'],
                    version=sanitize_message(str(result.get('version', ''))), error='', detalle='Import correcto en copia local')
    if code == 1 and result.get('estado') == 'FALLA':
        return dict(estado='FALLA', python=str(result.get('python', ''))[:40], entorno_real=result['entorno_real'], version='',
                    error=sanitize_message(str(result.get('error', ''))), detalle=sanitize_message(str(result.get('detalle', ''))))
    return dict(estado='NO EVALUADO', error='Contenedor interrumpido o resultado incoherente', detalle=f'Salida contenedor={code}')


def probe_image_imports(session, config, identity, args, aws_errors):
    """Imagen por digest observado; Docker local, sin ECS Exec/RunTask ni red en el probe."""
    now = datetime.now(timezone.utc)
    folder = Path(args.salida)/'modelos_dependencias'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    rows, issues, targets = [], [], {}
    modules = list(dict.fromkeys(args.modulo_import or IMPORT_MODULES))
    docker = shutil.which('docker')
    interpreter = '/'+args.entorno.strip('/')+'/bin/python'
    print('IMPORTS | Copia local de la imagen del predictor | Sin modificar ECS', flush=True)
    try:
        if not docker:
            raise ValueError('Docker no disponible en esta sesion; imports NO EVALUADOS')
        code, output, _ = docker_command([docker, 'info', '--format', '{{.Architecture}}'], 15)
        if code:
            raise ValueError('Docker no accesible; no se inicia ni modifica el daemon')
        host_arch = output.strip()
        ecs = session.client('ecs', config=config)
        tasks, task_issues = collect_tasks(ecs, args.cluster, args.familia, args.max_tareas)
        issues.extend(task_issues)
        if not tasks:
            raise ValueError('Sin tareas visibles: no se conoce un digest ejecutado para probar')
        definitions = {}
        for task in tasks:
            arn = task['taskDefinitionArn']
            if arn not in definitions:
                definitions[arn] = ecs.describe_task_definition(taskDefinition=arn)['taskDefinition']
            definition = definitions[arn]
            arch = definition.get('runtimePlatform', {}).get('cpuArchitecture', 'X86_64')
            for container in definition.get('containerDefinitions', []):
                actual = next((c for c in task.get('containers', []) if c.get('name') == container['name']), {})
                digest = actual.get('imageDigest', '')
                ref = ecr_reference(actual.get('image') or container.get('image', ''), digest)
                if not ref or not digest:
                    issues.append('Sin digest ECS/ECR observado: ' + container['name'])
                    continue
                registry = (actual.get('image') or container['image']).split('/')[0]
                image = registry+'/'+ref['repositorio']+'@'+digest
                values = effective_environment(container, task)
                siglas = {r[1] for key, (value, _) in values.items()
                          for r in [safe_reference(key, value)] if r and r[0] == 'Sigla'}
                target = targets.setdefault(image, dict(referencia=ref, digest=digest,
                    arquitectura=arch, siglas=set(), tareas=set()))
                target['siglas'].update(siglas or {'SIN DATO'})
                target['tareas'].add(task['taskArn'].rsplit('/', 1)[-1])
        clients = {}
        # Credenciales de Docker efimeras: no toca ~/.docker/config.json.
        with tempfile.TemporaryDirectory(prefix='ponal-imports-') as docker_config:
            for image, target in targets.items():
                base = dict(digest=target['digest'], siglas=','.join(sorted(target['siglas'])),
                    tareas=','.join(sorted(target['tareas'])), entorno=args.entorno,
                    interprete=interpreter, arquitectura=target['arquitectura'])
                try:
                    compatible = (target['arquitectura'] == 'X86_64' and host_arch in ('amd64', 'x86_64') or
                                  target['arquitectura'] == 'ARM64' and host_arch in ('arm64', 'aarch64'))
                    if not compatible:
                        raise ValueError('Arquitectura local distinta de ECS; no se usa emulacion')
                    ref = target['referencia']
                    if ref['region'] not in clients:
                        clients[ref['region']] = session.client('ecr', region_name=ref['region'], config=config)
                    ecr = clients[ref['region']]
                    metadata = describe_ecr_image(ecr, ref)
                    code, _, _ = docker_command([docker, '--config', docker_config, 'image', 'inspect', image], 15)
                    if code:
                        size = metadata.get('bytes')
                        if not isinstance(size, int) or size > args.max_mib*1024**2:
                            raise ValueError('Imagen supera --max-mib o ECR no informa tamano; no se descarga')
                        import base64
                        credentials = ecr.get_authorization_token()['authorizationData']
                        endpoint = 'https://'+image.split('/')[0]
                        token = next((c['authorizationToken'] for c in credentials if c.get('proxyEndpoint') == endpoint), '')
                        if not token:
                            raise ValueError('ECR no devolvio token para el registro esperado')
                        user, separator, password = base64.b64decode(token).partition(b':')
                        if user != b'AWS' or not separator:
                            raise ValueError('Token ECR no interpretable')
                        code, _, _ = docker_command([docker, '--config', docker_config, 'login', '--username', 'AWS',
                            '--password-stdin', image.split('/')[0]], 30, password)
                        del token, password, credentials
                        if code:
                            raise ValueError('Docker login ECR fallo; credenciales omitidas')
                        print(f'Descargando imagen por digest {target["digest"][7:19]} ({size/1024**2:.1f} MiB comprimidos); puede tardar...', flush=True)
                        code, pull_output, pull_error = docker_command([docker, '--config', docker_config, 'pull', image], 900)
                        if code:
                            raise ValueError(docker_failure_detail(code, pull_output, pull_error))
                    for module in modules:
                        print(f'Importando {module} | {base["siglas"]}...', flush=True)
                        name = 'ponal-import-'+uuid.uuid4().hex
                        try:
                            code, output, _ = docker_command(import_container_command(docker, docker_config, image,
                                interpreter, module, name), args.timeout_import)
                            result = import_result(code, output, interpreter)
                        except subprocess.TimeoutExpired:
                            result = dict(estado='NO EVALUADO', error='Timeout de prueba', detalle='Limite tecnico del probe; no es limite de prediccion')
                        finally:
                            # Solo el contenedor creado por este probe, incluso ante Ctrl+C.
                            docker_command([docker, '--config', docker_config, 'rm', '--force', name], 15)
                        rows.append(dict(base, modulo=module, **result))
                except aws_errors + (ValueError, OSError, subprocess.SubprocessError) as exc:
                    reason = type(exc).__name__ if isinstance(exc, aws_errors) else sanitize_message(str(exc))
                    issues.append(base['digest'][7:19]+': '+reason)
                    completed = {r['modulo'] for r in rows if r['digest'] == base['digest']}
                    rows.extend(dict(base, modulo=module, estado='NO EVALUADO', error=reason,
                                     detalle='Prueba no completada') for module in modules if module not in completed)
    except aws_errors + (ValueError, OSError, subprocess.SubprocessError) as exc:
        issues.append(type(exc).__name__ if isinstance(exc, aws_errors) else sanitize_message(str(exc)))
    if not rows:
        rows.append(dict(digest='', siglas='', tareas='', entorno=args.entorno, interprete=interpreter,
            arquitectura='', modulo='[PRUEBA]', estado='NO EVALUADO', error='Sin imports ejecutados',
            detalle='Ver limitaciones en contexto_imports.json'))
    fields = ['digest', 'siglas', 'tareas', 'entorno', 'interprete', 'arquitectura',
              'modulo', 'estado', 'python', 'entorno_real', 'version', 'error', 'detalle']
    with (folder/'imports.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    context = dict(captura_utc=now.isoformat(), cuenta=identity['Account'], entorno=args.entorno,
        modulos=modules, resultados=rows, limitaciones=issues, timeout_segundos=args.timeout_import,
        alcance='Imports en copia Docker local por digest ECS; no en el proceso activo',
        limites='Sin aplicacion, datos, red, ECS Exec, RunTask, instalacion de paquetes ni validacion de predicciones')
    (folder/'contexto_imports.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print('\nDigest       | Siglas               | Modulo                  | Control      | Python')
    print('-------------+----------------------+-------------------------+--------------+-------')
    for row in rows:
        print(f"{row['digest'][7:19]:12} | {row['siglas'][:20]:20} | {row['modulo']:23} | {row['estado']:12} | {row.get('python', '-')}")
    counts = Counter(row['estado'] for row in rows)
    print(f"Imports: {counts['OK']} OK | {counts['FALLA']} FALLA | {counts['NO EVALUADO']} NO EVALUADO")
    for issue in issues:
        print('[INCOMPLETO] '+issue)
    print('Prueba aislada de imports; no certifica el proceso activo ni todos los modelos o combinaciones de librerias.')
    print(f'Reportes: {folder}/imports.csv | contexto_imports.json')
    return 3 if issues or counts['NO EVALUADO'] else 1 if counts['FALLA'] else 0


def history_labels(message):
    """Solo CAD/sigla y HORA/TURNO explicitos; nunca inferirlos del taskId."""
    siglas, modes = set(), set()

    def record(key, value):
        key = re.sub(r'[^a-z]', '', str(key).lower())
        if not isinstance(value, str):
            return
        value = value.strip().upper()
        if key in ('cad', 'sigla', 'siglafisica') and re.fullmatch(r'[A-Z][A-Z0-9]{2,11}', value):
            if value not in ('NONE', 'NULL', 'UNKNOWN', 'TODAS', 'TODOS'):
                siglas.add(value)
        if key in ('timeframe', 'modalidad') and value in ('HORA', 'TURNO'):
            modes.add(value)

    def walk(value, depth=0):
        if depth > 8:
            return
        if isinstance(value, dict):
            record(value.get('name', ''), value.get('value'))
            for key, child in value.items():
                if re.search(r'secret|password|token|credential|access.?key', str(key), re.I):
                    continue
                record(key, child)
                if isinstance(child, (dict, list)):
                    walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                walk(child, depth + 1)

    try:
        walk(json.loads(message))
    except (ValueError, TypeError, RecursionError):
        pass
    # Cabecera real del predictor; el taskId y el nombre del stream no son CAD.
    for match in re.finditer(
            r'(?im)^\s*(?:\[(?:INFO|DEBUG|WARNING|WARN)\]\s*)?Version de C[oó]digo:\s*\S+\s+'
            r'Predicci[oó]n para:\s*([A-Z][A-Z0-9]{2,11})\s*-\s*\d+\s+en la fecha:', message):
        record('CAD', match.group(1))
    # Formato textual acotado; requiere etiqueta y separador en el mismo evento.
    for key, value in re.findall(
            r'''(?i)(?<![\w])\b(CAD|sigla(?:_fisica)?|time_?frame|modalidad)['"]?\s*[:=]\s*['"]?([A-Z][A-Z0-9]{2,11})(?=[\s'",;}\]]|$)''', message):
        record(key, value)
    return siglas, modes


def empty_history_log():
    return dict(eventos=0, seleccion=Counter(), insuficientes=Counter(), errores_dependencia=Counter(),
        siglas=set(), modalidades=set(), cierres=0, unidades_cierre=set(),
        indicios_error=0, primero=None, ultimo=None, cobertura='LEIDA', error='')


def history_log_summary(logs, group, stream, start, end, limit, sdk_errors):
    """Agregacion por stream con memoria acotada; retiene evidencia entre errores."""
    result = empty_history_log()
    seen, raw_count = set(), 0
    try:
        for page in logs.get_paginator('filter_log_events').paginate(
                logGroupName=group, logStreamNames=[stream],
                startTime=int(start.timestamp()*1000), endTime=int(end.timestamp()*1000),
                PaginationConfig={'MaxItems': limit + 1, 'PageSize': min(limit + 1, 10000)}):
            for event in page.get('events', []):
                if raw_count >= limit:
                    result['cobertura'] = 'PARCIAL'
                    return result
                raw_count += 1
                event_id = event.get('eventId')
                if event_id and event_id in seen:
                    continue
                if event_id:
                    seen.add(event_id)
                result['eventos'] += 1
                message = str(event.get('message', ''))
                siglas, modes = history_labels(message)
                result['siglas'].update(siglas)
                result['modalidades'].update(modes)
                findings = inspect_event(message)
                result['seleccion'].update({model for kind, model, _, _ in findings if kind == 'Seleccion reportada'})
                result['insuficientes'].update({model for kind, model, _, _ in findings if kind == 'Datos insuficientes'})
                result['errores_dependencia'].update({reason for kind, _, reason, _ in findings
                                                     if kind == 'Error de dependencia mencionado'})
                closures = len(re.findall(r'\bEnd\s+Script\s*!', message, re.I))
                result['cierres'] += closures
                if closures:
                    units = set(re.findall(r'''(?i)\bsubunidad['"]?\s*[:=]?\s*['"]?(\d+)\b''', message))
                    if len(units) == 1:
                        result['unidades_cierre'].update(units)
                try:
                    data = json.loads(message)
                except (ValueError, TypeError):
                    data = {}
                level = str(data.get('level', data.get('levelname', data.get('severity', '')))).upper() if isinstance(data, dict) else ''
                result['indicios_error'] += int(level in ('ERROR', 'FATAL', 'CRITICAL') or bool(re.search(
                    r'(?:^|\s|\[)(?:ERROR|FATAL|CRITICAL)(?:\]|\s*[:|]|\s)|Traceback \(most recent call last\)|^\s*[\w.]+(?:Error|Exception):',
                    message, re.M)))
                stamp = event.get('timestamp')
                if isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
                    result['primero'] = stamp if result['primero'] is None else min(result['primero'], stamp)
                    result['ultimo'] = stamp if result['ultimo'] is None else max(result['ultimo'], stamp)
    except sdk_errors as exc:
        result.update(cobertura='PARCIAL' if result['eventos'] else 'ERROR',
                      error=getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__))
    return result


def sample_stream_cad(logs, group, stream, start, end, limit, sdk_errors, deadline=None):
    """Dos muestras como maximo, sin paginar ni analizar modelos: inicio y final.

    GetLogEvents puede devolver paginas parciales/vacias: estas muestras nunca
    se presentan como lectura completa del stream ni prueba de ausencia de CAD.
    """
    result = empty_history_log()
    result['cobertura'] = 'MUESTRA CAD'
    budget = min(limit, 1000)
    for from_head in (True, False):
        if deadline is not None and time.monotonic() >= deadline:
            result['cobertura'] = 'PENDIENTE'
            break
        if budget <= 0:
            break
        amount = min(budget, max(1, (limit + 1)//2), 500)
        try:
            response = logs.get_log_events(logGroupName=group, logStreamName=stream,
                startTime=int(start.timestamp()*1000), endTime=int(end.timestamp()*1000),
                startFromHead=from_head, limit=amount)
            for event in response.get('events', [])[:amount]:
                result['eventos'] += 1
                siglas, modes = history_labels(str(event.get('message', '')))
                result['siglas'].update(siglas)
                result['modalidades'].update(modes)
                stamp = event.get('timestamp')
                if isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
                    result['primero'] = stamp if result['primero'] is None else min(result['primero'], stamp)
                    result['ultimo'] = stamp if result['ultimo'] is None else max(result['ultimo'], stamp)
            budget -= amount
            if result['siglas']:
                break
        except sdk_errors as exc:
            result.update(cobertura='ERROR',
                error=getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__))
            break
    return result


def mapping_stream_key(candidate):
    return json.dumps([candidate['region'], candidate['grupo'], candidate['stream']])


def mapping_cached_labels(record):
    if not isinstance(record, dict):
        return set(), set()
    return history_labels(json.dumps({'CAD': record.get('cad'), 'modalidad': record.get('modalidad')}))


def discover_history_streams(logs, group, prefix, limit, sdk_errors, deadline=None):
    """Priorizar actividad reciente; AWS no permite combinar ese orden y prefijo.

    Los prefijos se filtran localmente. No se corta por timestamps de metadata,
    que tienen consistencia eventual, ni se infiere fin desde LastEventTime.
    """
    rows, seen, issues = [], set(), []
    prefixes = (prefix,) if isinstance(prefix, str) else tuple(prefix)
    if deadline is not None and time.monotonic() >= deadline:
        return rows, ['Presupuesto de tiempo agotado antes de listar streams']
    try:
        for page in logs.get_paginator('describe_log_streams').paginate(
                logGroupName=group, orderBy='LastEventTime', descending=True,
                PaginationConfig={'MaxItems': limit + 1, 'PageSize': min(limit + 1, 50)}):
            if deadline is not None and time.monotonic() >= deadline:
                issues.append('Presupuesto de tiempo agotado durante el listado de streams')
                return rows, issues
            for item in page.get('logStreams', []):
                name = item.get('logStreamName', '')
                if name in seen:
                    continue
                seen.add(name)
                if len(seen) > limit:
                    issues.append('Listado de streams truncado; aumentar --max-streams')
                    return rows, issues
                matched = next((item for item in prefixes if name.startswith(item)), None)
                if matched is None:
                    continue
                suffix = name[len(matched):]
                if not re.fullmatch(r'[a-fA-F0-9]{32}|[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}', suffix):
                    issues.append('Stream omitido: sufijo distinto de un taskId ECS')
                    continue
                rows.append(dict(tarea=suffix, stream=name, grupo=group))
    except sdk_errors as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
        issues.append('DescribeLogStreams: ' + code)
    return rows, issues


def history_task_row(candidate, task, definition, logs, metadata_error=False):
    """ECS confirma parada; End Script solo es evidencia de cierre de subunidades."""
    siglas, modes = set(), set()
    for container in definition.get('containerDefinitions', []):
        for name, (value, source) in effective_environment(container, task).items():
            ref_siglas, ref_modes = history_labels(json.dumps({name: value}))
            siglas.update(ref_siglas)
            modes.update(ref_modes)
    # Overrides siguen siendo evidencia si la revision exacta no se pudo describir.
    for container in task.get('overrides', {}).get('containerOverrides', []):
        for variable in container.get('environment', []):
            ref_siglas, ref_modes = history_labels(json.dumps({variable.get('name', ''): variable.get('value', '')}))
            siglas.update(ref_siglas)
            modes.update(ref_modes)
    direct = set(siglas)
    siglas.update(logs['siglas'])
    modes.update(logs['modalidades'])
    label = next(iter(siglas)) if len(siglas) == 1 else 'CONFLICTO' if siglas else 'SIN DATO'
    mode = next(iter(modes)) if len(modes) == 1 else 'CONFLICTO' if modes else 'SIN DATO'
    state = task.get('lastStatus', 'NO DISPONIBLE')
    total = sum(logs['seleccion'].values())
    uniform = logs['seleccion']['Uniform']
    exits = {c.get('name', '?'): c['exitCode'] for c in task.get('containers', []) if 'exitCode' in c}
    essential = {c['name'] for c in definition.get('containerDefinitions', []) if c.get('essential', True)}
    stopped_reason = str(task.get('stoppedReason', ''))
    failure = (task.get('stopCode') == 'TaskFailedToStart' or any(exits.get(name) not in (None, 0) for name in essential)
               or stopped_reason.startswith(('OutOfMemoryError', 'CannotPullContainerError',
                                              'ResourceInitializationError', 'CannotStartContainerError')))
    if failure:
        closure = 'FALLA ECS'
    elif state == 'STOPPED':
        closure = 'DETENIDA EN ECS; RESULTADO NO VALIDADO'
    else:
        closure = 'CIERRES DE SUBUNIDAD EN LOGS' if logs['cierres'] else 'NO CONFIRMADA'
    notices = []
    if label in ('SIN DATO', 'CONFLICTO'):
        notices.append('Sigla sin atribucion unica')
    if logs['indicios_error'] or logs['errores_dependencia']:
        notices.append('Indicios de error en logs')
    if logs['insuficientes']:
        notices.append('Avisos de datos insuficientes')
    if total and uniform == total:
        notices.append('100% Uniform en las selecciones observadas; no certifica toda la tarea')
    if logs['cobertura'] != 'LEIDA':
        notices.append('Lectura de logs incompleta')
    if metadata_error:
        notices.append('Metadata ECS incompleta')
    if not failure and any(value not in (None, 0) for value in exits.values()):
        notices.append('Contenedor con salida no cero; revisar esencialidad')
    if task.get('stopCode') in ('UserInitiated', 'ServiceSchedulerInitiated', 'SpotInterruption', 'TerminationNotice'):
        notices.append('Parada/interrupcion reportada por ECS')
    if not task:
        notices.append('ECS ya no aporta estado/codigos; no se infiere finalizacion')

    def instant(value):
        return value.astimezone(BOGOTA).isoformat() if isinstance(value, datetime) and value.tzinfo else ''

    def event_instant(value):
        return datetime.fromtimestamp(value/1000, timezone.utc).astimezone(BOGOTA).isoformat() if value is not None else ''

    return dict(**candidate, sigla=label, siglas_evidencia=','.join(sorted(siglas)),
        sigla_fuente='ECS y logs' if direct and logs['siglas'] else 'ECS' if direct else 'logs explicitos' if siglas else '',
        modalidad=mode, estado_ecs=state, inicio_ecs=instant(task.get('startedAt')),
        fin_ecs=instant(task.get('stoppedAt')), codigos_salida=json.dumps(exits, sort_keys=True),
        stop_code=task.get('stopCode', ''), motivo_parada=sanitize_message(stopped_reason),
        primera_emision=event_instant(logs['primero']), ultima_emision=event_instant(logs['ultimo']),
        finalizacion=closure, cierres_reportados=logs['cierres'],
        subunidades_con_cierre_explicito=len(logs['unidades_cierre']),
        selecciones_por_modelo=json.dumps(dict(sorted(logs['seleccion'].items()))),
        total_selecciones=total, uniform=uniform, porcentaje_uniform_observado=round(100*uniform/total, 2) if total else '',
        avisos_datos_insuficientes=json.dumps(dict(sorted(logs['insuficientes'].items()))),
        indicios_error=logs['indicios_error'], errores_dependencia=json.dumps(dict(logs['errores_dependencia'])),
        eventos_leidos=logs['eventos'], lectura_logs=logs['cobertura'], error_lectura=logs['error'],
        control='FALLA' if failure else 'AVISO' if notices else 'INFO', detalle='; '.join(notices))


def collect_log_history(ecs, client_for_region, args, now, sdk_errors,
                        mapping_state=None, deadline=None, progress=None):
    """Descubrimiento independiente de ListTasks; no ejecuta ECS/ECR/S3."""
    start = now - timedelta(days=args.dias_logs)
    targets, candidates, issues = set(), [], []
    def notify(message, rows):
        if progress is not None:
            progress(message, rows)
    notify('Descubriendo grupos y streams...', [])
    if args.grupo_logs:
        targets.add((args.region, args.grupo_logs, args.prefijo_logs.rstrip('/') + '/'))
    else:
        definition = ecs.describe_task_definition(taskDefinition=args.familia)['taskDefinition']
        for container in definition.get('containerDefinitions', []):
            log = container.get('logConfiguration', {})
            options = log.get('options', {})
            if log.get('logDriver') == 'awslogs' and options.get('awslogs-group') and options.get('awslogs-stream-prefix'):
                targets.add((options.get('awslogs-region', args.region), options['awslogs-group'],
                             options['awslogs-stream-prefix'].rstrip('/') + '/' + container['name'] + '/'))
    if not targets:
        issues.append('No se encontro un grupo/prefijo awslogs; usar --grupo-logs con el nombre completo')
    grouped = {}
    for region, group, prefix in sorted(targets):
        grouped.setdefault((region, group), set()).add(prefix)
    for (region, group), prefixes in grouped.items():
        found, errors = discover_history_streams(client_for_region(region), group, prefixes, args.max_streams, sdk_errors, deadline)
        candidates.extend(dict(**item, region=region) for item in found)
        issues.extend(group + ': ' + error for error in errors)
    ids = list(dict.fromkeys(candidate['tarea'] for candidate in candidates))
    notify(f'{len(candidates)} streams candidatos; consultando metadata ECS...', [])
    tasks, failed_ids = {}, set()
    for offset in range(0, len(ids), 100):
        if deadline is not None and time.monotonic() >= deadline:
            issues.append('Presupuesto de tiempo agotado durante consulta ECS')
            failed_ids.update(ids[offset:])
            break
        batch = ids[offset:offset + 100]
        try:
            response = ecs.describe_tasks(cluster=args.cluster, tasks=batch)
            tasks.update({t['taskArn'].rsplit('/', 1)[-1]: t for t in response.get('tasks', [])})
            for failure in response.get('failures', []):
                # MISSING es esperado para historial que ECS ya no conserva.
                if failure.get('reason') != 'MISSING':
                    failed_ids.add(failure.get('arn', '').rsplit('/', 1)[-1])
                    issues.append('DescribeTasks: ' + str(failure.get('reason', 'sin motivo')))
        except sdk_errors as exc:
            failed_ids.update(batch)
            issues.append('DescribeTasks: ' + getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__))
    definitions = {}
    for task in tasks.values():
        arn = task.get('taskDefinitionArn')
        if arn and arn not in definitions:
            if deadline is not None and time.monotonic() >= deadline:
                issues.append('Presupuesto de tiempo agotado durante consulta de definiciones ECS')
                break
            try:
                definitions[arn] = ecs.describe_task_definition(taskDefinition=arn)['taskDefinition']
            except sdk_errors as exc:
                definitions[arn] = {}
                issues.append('Revision ECS: ' + getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__))
    if mapping_state is not None:
        def priority(candidate):
            saved = mapping_state['streams'].get(mapping_stream_key(candidate))
            cached, _ = mapping_cached_labels(saved)
            return (0, '') if cached else (1, '') if saved is None else (2, saved.get('consultada_utc', ''))
        candidates.sort(key=priority)
    rows, omitted, pending = [], 0, 0
    for number, candidate in enumerate(candidates, 1):
        task = tasks.get(candidate['tarea'], {})
        task_family = task.get('taskDefinitionArn', '').rsplit('/', 1)[-1].rsplit(':', 1)[0]
        expected_family = args.familia.rsplit('/', 1)[-1].rsplit(':', 1)[0]
        if task_family and task_family != expected_family:
            issues.append(candidate['tarea'][:12] + ': ECS identifica otra familia; stream omitido')
            omitted += 1
            continue
        # Solo la revision exacta observada en ECS sirve para defaults historicos.
        definition = definitions.get(task.get('taskDefinitionArn'), {})
        cache_source = ''
        if mapping_state is None:
            log = history_log_summary(client_for_region(candidate['region']), candidate['grupo'], candidate['stream'],
                                      start, now, args.max_eventos, sdk_errors)
            if not log['eventos'] and log['cobertura'] == 'LEIDA' and not task:
                omitted += 1
                continue
            if log['cobertura'] != 'LEIDA':
                issues.append(candidate['tarea'][:12] + ': lectura de logs ' + log['cobertura'])
        else:
            log = empty_history_log()
            direct = history_task_row(candidate, task, definition, log)
            saved = mapping_state['streams'].get(mapping_stream_key(candidate), {})
            cached, cached_modes = mapping_cached_labels(saved)
            if direct['sigla'] not in ('SIN DATO', 'CONFLICTO'):
                log['cobertura'] = 'CAD ECS; SIN LECTURA LOG'
            elif cached:
                log.update(siglas=cached, modalidades=cached_modes, cobertura='CACHE ASOCIACION')
                cache_source = 'CACHE: ' + ('ECS' if saved.get('fuente') == 'ECS' else 'MUESTRA LOGS')
            elif deadline is not None and time.monotonic() >= deadline:
                log['cobertura'] = 'PENDIENTE'
            else:
                notify(f'Consultando CAD: stream {number}/{len(candidates)} ({candidate["tarea"][:12]})...', rows)
                log = sample_stream_cad(client_for_region(candidate['region']), candidate['grupo'], candidate['stream'],
                                        start, now, args.max_eventos, sdk_errors, deadline)
            if log['cobertura'] == 'ERROR':
                issues.append(candidate['tarea'][:12] + ': muestra CAD: ' + log['error'])
            if log['cobertura'] == 'PENDIENTE':
                pending += 1
        row = history_task_row(candidate, task, definition, log,
                              candidate['tarea'] in failed_ids or bool(task and not definition))
        if cache_source:
            row['sigla_fuente'] = cache_source
        elif mapping_state is not None and log['siglas']:
            row['sigla_fuente'] = 'MUESTRA LOGS'
        rows.append(row)
        if mapping_state is not None and log['cobertura'] != 'PENDIENTE':
            mapping_state['streams'][mapping_stream_key(candidate)] = dict(
                cad=row['sigla'] if row['sigla'] not in ('SIN DATO', 'CONFLICTO') else '',
                modalidad=row['modalidad'] if row['modalidad'] in ('HORA', 'TURNO') else '',
                fuente='ECS' if row['sigla_fuente'] in ('ECS', 'CACHE: ECS') else 'MUESTRA LOGS',
                consultada_utc=now.isoformat())
        notify(f'Resueltos {number}/{len(candidates)} streams; {pending} pendientes de muestra', rows)
    if pending:
        issues.append(f'Presupuesto de tiempo agotado; {pending} streams pendientes. Repetir el comando para continuar con la cache')
    return rows, dict(captura_utc=now.isoformat(), desde_utc=start.isoformat(), hasta_utc=now.isoformat(),
        streams_candidatos=len(candidates), streams_sin_evidencia_en_ventana=omitted,
        tareas_distintas=len({r['tarea'] for r in rows}), lecturas_incompletas=issues,
        streams_pendientes=pending, modo='ASOCIACION RAPIDA' if mapping_state is not None else 'HISTORIAL MODELOS',
        limitaciones=['Una fila por stream de contenedor; varios streams pueden pertenecer a una tarea.',
            'Primer/ultimo log no son hora de inicio/fin ECS. Silencio no demuestra parada.',
            'End Script es cierre reportado de subunidad, no finalizacion global ni cobertura esperada.',
            'Los conteos son declaraciones de la ventana/muestra, no solicitudes ni predicciones unicas.',
            'Sin campos explicitos no se inventa sigla, HORA/TURNO, subunidad ni proceso.',
            'No usa la definicion actual para atribuir siglas a tareas que ECS ya no conserva.',
            'La retencion del grupo limita los eventos disponibles; streams vacios pueden persistir.',
            'DescribeLogStreams ordena por actividad reciente; max-streams limita streams examinados por grupo, incluidos otros prefijos.',
            'No confirma familia/cluster historico de una tarea ausente en ECS; origen es el prefijo configurado.'])


def summarize_history_siglas(rows, incomplete=False):
    """Una fila por sigla, sin filtrar por RUNNING ni duplicar tareas por stream."""
    labels = {row['sigla'] for row in rows if row['sigla'] not in ('SIN DATO', 'CONFLICTO')}
    unresolved = [row for row in rows if row['sigla'] in ('SIN DATO', 'CONFLICTO')]
    result = []
    for sigla in sorted(labels):
        selected, seen = [], set()
        for row in rows:
            key = (row.get('region', ''), row.get('grupo', ''), row.get('stream', ''))
            if row['sigla'] == sigla and key not in seen:
                seen.add(key)
                selected.append(row)
        tasks = {}
        for row in selected:
            tasks.setdefault((row.get('region', ''), row['tarea']), set()).add(row['estado_ecs'])
        active = stopped = unknown = 0
        for states in tasks.values():
            states.discard('NO DISPONIBLE')
            if states == {'STOPPED'}:
                stopped += 1
            elif states and states <= {'PROVISIONING', 'PENDING', 'ACTIVATING', 'RUNNING',
                                       'DEACTIVATING', 'STOPPING', 'DEPROVISIONING'}:
                active += 1
            else:
                unknown += 1
        models, insufficient, dependencies = Counter(), Counter(), Counter()
        for row in selected:
            models.update(json.loads(row['selecciones_por_modelo']))
            insufficient.update(json.loads(row['avisos_datos_insuficientes']))
            dependencies.update(json.loads(row['errores_dependencia']))
        total = sum(models.values())
        uniform = models['Uniform']
        coverage = ('PARCIAL' if incomplete or unresolved or any(row['lectura_logs'] != 'LEIDA' for row in selected) else 'LEIDA')
        control = ('FALLA' if any(row['control'] == 'FALLA' for row in selected) else 'AVISO'
                   if unknown or coverage != 'LEIDA' or any(row['control'] == 'AVISO' for row in selected) else 'INFO')
        notes = []
        if unknown:
            notes.append('Tareas recuperadas de logs sin estado ECS; finalizacion no confirmada')
        if total and uniform == total:
            notes.append('100% Uniform observado; no prueba toda la prediccion si la cobertura es parcial')
        if unresolved:
            notes.append('Hay streams sin sigla unica; detalle conservado sin asignarlos arbitrariamente')
        starts = [row['inicio_ecs'] for row in selected if row['inicio_ecs']]
        ends = [row['fin_ecs'] for row in selected if row['fin_ecs']]
        result.append(dict(sigla=sigla, tareas=len(tasks), activas_ecs=active, detenidas_ecs=stopped, sin_estado_ecs=unknown,
            modalidades=', '.join(sorted({row['modalidad'] for row in selected
                                         if row['modalidad'] not in ('SIN DATO', 'CONFLICTO')})),
            primer_inicio_ecs=min(starts) if starts else '', ultimo_fin_ecs=max(ends) if ends else '',
            selecciones_por_modelo=json.dumps(dict(sorted(models.items()))),
            total_selecciones=total, uniform=uniform,
            porcentaje_uniform_observado=round(100*uniform/total, 2) if total else '',
            avisos_datos_insuficientes=json.dumps(dict(sorted(insufficient.items()))),
            indicios_error=sum(row['indicios_error'] for row in selected),
            errores_dependencia=json.dumps(dict(sorted(dependencies.items()))),
            cierres_reportados=sum(row['cierres_reportados'] for row in selected),
            streams=len(selected), lectura_logs=coverage, control=control, detalle='; '.join(notes)))
    return result


def write_history_report(folder, rows, summaries):
    """Un solo CSV: resumen por sigla y detalle trazable por tarea/stream."""
    fields = list(dict.fromkeys(['tipo_registro'] + [key for row in summaries + rows for key in row]))
    if len(fields) == 1:
        fields += ['sigla', 'tarea', 'estado_ecs', 'lectura_logs', 'control']
    path = folder/'historial_tareas.csv'
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(dict(tipo_registro='RESUMEN_SIGLA', **row) for row in summaries)
        writer.writerows(dict(tipo_registro='DETALLE_TAREA', **row) for row in rows)
    return path


def collect_history_from_session(session, config, args, sdk_errors, **options):
    clients = {}
    def client_for_region(region):
        if region not in clients:
            clients[region] = session.client('logs', region_name=region, config=config)
        return clients[region]
    return collect_log_history(session.client('ecs', config=config), client_for_region,
                               args, datetime.now(timezone.utc), sdk_errors, **options)


def read_stream_structure_siglas(s3, args):
    """SIGLA_PAPA es la dimension filtrada por CAD en el selector inspeccionado."""
    row = {}
    read_input_header(s3, args.cuenta, INPUT_KEYS[0], ('SIGLA_PAPA', 'UNIDAD_ID'),
                      args.encoding_entradas, args.separador_entradas, row,
                      validate_rows=True, max_input_bytes=args.max_mib_entradas*1024**2, selector=True)
    groups = json.loads(row.get('selector_unidades', '[]'))
    siglas = sorted({item['sigla_papa'] for item in groups
                     if re.fullmatch(r'[A-Z][A-Z0-9]{2,11}', item['sigla_papa'])})
    return siglas, dict(fuente='s3://' + INPUT_BUCKET + '/' + INPUT_KEYS[0], columna='SIGLA_PAPA',
        cobertura=row.get('cobertura_selector', 'NO EVALUADO') if siglas else 'NO EVALUADO',
        siglas=len(siglas), etag=row.get('etag', ''),
        ultima_modificacion=row.get('ultima_modificacion', ''),
        alcance='Todas las SIGLA_PAPA proyectables; su presencia no define ejecuciones esperadas')


def build_sigla_stream_mapping(rows, structure_siglas):
    """Una fila por relacion CAD/stream; sin asociar por orden o similitud."""
    catalog = set(structure_siglas)
    observed = {row['sigla'] for row in rows if row['sigla'] not in ('SIN DATO', 'CONFLICTO')}
    result = []
    for sigla in sorted(catalog | observed):
        selected = [row for row in rows if row['sigla'] == sigla]
        if not selected:
            result.append(dict(sigla=sigla, stream='', grupo='', region='', tarea='', modalidad='',
                estado_ecs='NO DISPONIBLE', fuente_cad='', en_estructura='SI',
                asociacion='SIN STREAM ASOCIADO', lectura_logs='NO EVALUADO', ultima_emision='',
                detalle='Sin relacion CAD/stream confirmada en la ventana; no demuestra falta de ejecucion'))
        for row in selected:
            result.append(dict(sigla=sigla, stream=row['stream'], grupo=row['grupo'], region=row['region'],
                tarea=row['tarea'], modalidad=row['modalidad'], estado_ecs=row['estado_ecs'],
                fuente_cad=row['sigla_fuente'], en_estructura='SI' if sigla in catalog else 'NO',
                asociacion='ASOCIADO', lectura_logs=row['lectura_logs'], ultima_emision=row['ultima_emision'],
                detalle='' if sigla in catalog else 'CAD observado fuera de la estructura leida'))
    for row in rows:
        if row['sigla'] in ('SIN DATO', 'CONFLICTO'):
            result.append(dict(sigla='SIN CAD CONFIRMADO' if row['sigla'] == 'SIN DATO' else 'CONFLICTO CAD',
                stream=row['stream'], grupo=row['grupo'], region=row['region'], tarea=row['tarea'],
                modalidad=row['modalidad'], estado_ecs=row['estado_ecs'], fuente_cad=row['sigla_fuente'],
                en_estructura='NO EVALUADO', asociacion='SIN ATRIBUCION UNICA', lectura_logs=row['lectura_logs'],
                ultima_emision=row['ultima_emision'], detalle='No asignar a una sigla; evidencias=' + row['siglas_evidencia']))
    return list({(row['sigla'], row['region'], row['grupo'], row['stream']): row for row in result}.values())


def atomic_mapping_text(path, text):
    temporary = path.with_name('.' + path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        temporary.write_text(text, encoding='utf-8')
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def load_mapping_state(path, scope):
    empty = dict(version=1, scope=scope, streams={})
    if not path.exists():
        return empty
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('version') != 1 or data.get('scope') != scope or not isinstance(data.get('streams'), dict):
            raise ValueError('Formato de cache incompatible')
        for key, row in data['streams'].items():
            if isinstance(key, str) and isinstance(row, dict):
                empty['streams'][key] = dict(cad=row.get('cad') if isinstance(row.get('cad'), str) else '',
                    modalidad=row.get('modalidad') if isinstance(row.get('modalidad'), str) else '',
                    fuente='ECS' if row.get('fuente') == 'ECS' else 'MUESTRA LOGS',
                    consultada_utc=str(row.get('consultada_utc') or ''))
        return empty
    except (OSError, ValueError, TypeError, AttributeError):
        print('[AVISO] Cache no interpretable; se reconstruira sin reutilizar asociaciones.', flush=True)
        return empty


def inspect_stream_mapping(session, config, args, sdk_errors):
    print('MAPA CAD / STREAM | Asociacion rapida | Todas las siglas de estructura | Solo lectura', flush=True)
    now = datetime.now(timezone.utc)
    deadline = time.monotonic() + getattr(args, 'max_segundos_mapeo', 180)
    base = Path(args.salida).expanduser()/'modelos_dependencias'
    folder = base/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    path = folder/'mapa_siglas_streams.csv'
    scope = {name: getattr(args, name, '') for name in ('cuenta', 'region', 'cluster', 'familia', 'grupo_logs', 'prefijo_logs')}
    scope_id = hashlib.sha256(json.dumps(scope, sort_keys=True).encode()).hexdigest()[:16]
    cache_path = base/('cache_mapa_streams_' + scope_id + '.json')
    state = (dict(version=1, scope=scope, streams={}) if getattr(args, 'reiniciar_mapeo', False)
             else load_mapping_state(cache_path, scope))
    context = dict(captura_utc=now.isoformat(), configuracion=vars(args), en_proceso=True,
        lecturas_incompletas=[], cache=str(cache_path),
        alcance='Solo asociacion CAD/stream; muestras limitadas y cache, sin analisis de modelos')
    print(f'Avance y resultados: {path}', flush=True)
    siglas, rows, latest_rows = [], [], []
    structure = dict(cobertura='NO EVALUADO')
    try:
        print('Leyendo siglas de estructura...', flush=True)
        siglas, structure = read_stream_structure_siglas(session.client('s3', config=config), args)
        if structure['cobertura'] != 'LEIDA':
            context['lecturas_incompletas'].append('Estructura incompleta; no se certifica lista completa de siglas')
    except (*sdk_errors, OSError, ValueError, csv.Error) as exc:
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
        structure = dict(cobertura='ERROR', codigo=code)
        context['lecturas_incompletas'].append('Lectura de estructura: ' + code)

    def persist(current, provisional, running=True):
        mapping = build_sigla_stream_mapping(current, siglas)
        if provisional:
            for row in mapping:
                if row['asociacion'] == 'SIN STREAM ASOCIADO':
                    row['asociacion'] = 'PENDIENTE DE MAPEO'
        handle = io.StringIO(newline='')
        writer = csv.DictWriter(handle, fieldnames=list(mapping[0]) if mapping else ['sigla', 'stream', 'asociacion'])
        writer.writeheader()
        writer.writerows(mapping)
        atomic_mapping_text(path, '\ufeff' + handle.getvalue())
        context.update(estructura=structure, relaciones=mapping, en_proceso=running, mapa_parcial=provisional)
        atomic_mapping_text(folder/'contexto_streams.json', json.dumps(context, ensure_ascii=False, indent=2))
        atomic_mapping_text(cache_path, json.dumps(state, ensure_ascii=False))
        return mapping

    persist([], True)
    saved_at, printed_at, saved_size = time.monotonic(), 0.0, 0
    def progress(message, current):
        nonlocal latest_rows, saved_at, printed_at, saved_size
        latest_rows = current
        tick = time.monotonic()
        if tick - printed_at >= 2:
            print(message, flush=True)
            printed_at = tick
        if len(current) == 1 or len(current) >= saved_size + 5 or tick - saved_at >= 2:
            persist(current, True)
            saved_at, saved_size = tick, len(current)

    interrupted = False
    try:
        rows, collected = collect_history_from_session(session, config, args, sdk_errors,
            mapping_state=state, deadline=deadline, progress=progress)
        collected['lecturas_incompletas'] = context['lecturas_incompletas'] + collected['lecturas_incompletas']
        context.update(collected)
    except KeyboardInterrupt:
        interrupted = True
        rows = latest_rows
        context['lecturas_incompletas'].append('Interrumpido; repetir el mismo comando reutiliza la cache guardada')
    except sdk_errors as exc:
        rows = latest_rows
        code = getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)
        context['lecturas_incompletas'].append('Descubrimiento de streams: ' + code)
    provisional = interrupted or bool(context['lecturas_incompletas'])
    mapping = persist(rows, provisional, running=False)
    width = 19
    print('Sigla / CAD'.ljust(width) + ' | Stream')
    print('-'*width + '-+-' + '-'*90)
    for row in mapping:
        print(row['sigla'].ljust(width) + ' | ' + (row['stream'] or row['asociacion']))
    print(f'Siglas de estructura: {len(siglas)} | Asociaciones CAD/stream: {sum(row["asociacion"] == "ASOCIADO" for row in mapping)}')
    print('Muestras limitadas para identificar CAD; no analiza modelos ni certifica ausencia de ejecucion.')
    for issue in context['lecturas_incompletas']:
        print('[INCOMPLETO] ' + issue)
    print(f'Informe: {path} | contexto_streams.json')
    print(f'Cache: {cache_path}; repetir el comando continua sin releer asociaciones confirmadas.')
    return 130 if interrupted else 3 if context['lecturas_incompletas'] else 0


def inspect_log_history(session, config, args, sdk_errors):
    print(f'MONITOREO POR SIGLA | {args.cluster} | Ultimos {args.dias_logs} dias | Solo lectura', flush=True)
    now = datetime.now(timezone.utc)
    rows, context = collect_history_from_session(session, config, args, sdk_errors)
    context['universo_siglas'] = dict(fuente='Streams de logs y metadata ECS vinculada',
        alcance='Solo siglas identificadas en la evidencia disponible; no permite detectar las nunca observadas',
        catalogo_externo=False)
    summaries = summarize_history_siglas(rows, incomplete=bool(context['lecturas_incompletas']))
    columns = ('sigla', 'tareas', 'activas_ecs', 'detenidas_ecs', 'sin_estado_ecs',
               'selecciones_por_modelo', 'total_selecciones', 'porcentaje_uniform_observado',
               'avisos_datos_insuficientes', 'indicios_error', 'lectura_logs', 'control')
    widths = (12, 6, 7, 8, 8, 26, 7, 10, 22, 7, 13, 7)
    def render(values):
        cells = [textwrap.wrap(str(value), width=width, break_on_hyphens=False) or ['-']
                 for value, width in zip(values, widths)]
        for index in range(max(map(len, cells))):
            print(' | '.join((cell[index] if index < len(cell) else '').ljust(width)
                             for cell, width in zip(cells, widths)))
    render(('Sigla', 'Tareas', 'Activas', 'Detenidas', 'Sin ECS', 'Modelos (logs)', 'Total', '% Uniform*', 'Pocos datos (logs)', 'Errores', 'Lectura', 'Control'))
    print('-+-'.join('-'*width for width in widths))
    for row in summaries:
        values = [row[name] for name in columns]
        values[5] = '; '.join(f'{model}:{count}' for model, count in json.loads(row['selecciones_por_modelo']).items())
        values[8] = '; '.join(f'{model}:{count}' for model, count in json.loads(row['avisos_datos_insuficientes']).items())
        render(values)
    folder = Path(args.salida)/'modelos_dependencias'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
    folder.mkdir(parents=True, exist_ok=True)
    path = write_history_report(folder, rows, summaries)
    context['siglas_resumidas'] = len(summaries)
    context['streams_sin_sigla_unica'] = sum(row['sigla'] in ('SIN DATO', 'CONFLICTO') for row in rows)
    context['configuracion'] = vars(args)
    (folder/'contexto_historial.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
    print('* Porcentaje sobre declaraciones observadas, no solicitudes. Detenida no equivale a prediccion validada.')
    print('Incluye activas, detenidas y recuperadas desde logs. Sin ECS no significa finalizada.')
    print('Alcance: siglas identificadas en la evidencia disponible; sin catalogo S3. Las nunca observadas no se pueden detectar.')
    print(f'Siglas: {len(summaries)} | Streams sin sigla unica: {context["streams_sin_sigla_unica"]}; conservados en DETALLE_TAREA.')
    print(f'Streams candidatos: {context["streams_candidatos"]} | Tareas con evidencia: {context["tareas_distintas"]}')
    for issue in context['lecturas_incompletas']:
        print('[INCOMPLETO] ' + issue)
    print(f'Informe: {path} | Filtrar tipo_registro=RESUMEN_SIGLA; detalle en el mismo CSV.')
    return 3 if context['lecturas_incompletas'] else 1 if any(r['control'] == 'FALLA' for r in rows) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cluster', default=CLUSTER)
    parser.add_argument('--familia', default=FAMILY)
    parser.add_argument('--cuenta', default='926162397524')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--max-tareas', type=int, default=100, help='Maximo por estado consultado; con finalizadas, hasta este limite para RUNNING y otro para STOPPED')
    parser.add_argument('--incluir-finalizadas', action='store_true', help='Incluye STOPPED aun visibles en ECS; no recupera historial eliminado ni certifica exito')
    view = parser.add_mutually_exclusive_group()
    view.add_argument('--historial-logs', action='store_true', help='Resumen por siglas observadas: historial ECS/Logs (modo predeterminado, sin catalogo S3)')
    view.add_argument('--por-tarea', action='store_true', help='Mapeo anterior por tarea ECS; inspecciones opcionales conservan su modo')
    view.add_argument('--mapear-streams', action='store_true', help='Tabla CAD/stream para todas las SIGLA_PAPA de la estructura actual S3')
    parser.add_argument('--max-segundos-mapeo', type=int, default=180, help='Presupuesto tecnico del mapa entre llamadas AWS: 10..900 s; no es umbral de prediccion')
    parser.add_argument('--reiniciar-mapeo', action='store_true', help='Ignora asociaciones guardadas del mapa y las vuelve a consultar')
    parser.add_argument('--dias-logs', type=int, default=30, help='Ventana historica: 1..30 dias; default 30, sujeto a retencion; no es un SLA')
    parser.add_argument('--max-streams', type=int, default=1000, help='Maximo de streams examinados por grupo, orden reciente; 1..10000')
    parser.add_argument('--grupo-logs', help='Nombre completo del grupo; sin el flag se descubre desde la familia ECS')
    parser.add_argument('--prefijo-logs', default='pon-prod-xcrime-prediccion/pon-prod-prediccion/', help='Prefijo completo con contenedor, para --grupo-logs')
    parser.add_argument('--ventana-minutos', type=int, default=60)
    parser.add_argument('--max-eventos', type=int, help='Default mapa: 200 muestreados; historial: 100000; por tarea: 500 filtrados')
    parser.add_argument('--muestras', type=int, default=3, help='Muestras saneadas por tipo/modelo/motivo; 0 desactiva muestras')
    parser.add_argument('--sin-logs', action='store_true')
    parser.add_argument('--probar-imports', action='store_true', help='Prueba imports en copia Docker local de digests observados; sin ECS Exec ni ejecutar la aplicacion')
    parser.add_argument('--modulo-import', action='append', default=[], metavar='MODULO', help='Modulo a probar; repetir. Default: 16 librerias numericas/geograficas y de modelos')
    parser.add_argument('--timeout-import', type=int, default=45, help='Limite tecnico por import, 5..120 segundos; no es umbral de prediccion')
    parser.add_argument('--correlacionar-modelos', action='store_true', help='Detalla declaraciones por tarea/sigla y contexto explicito del mismo evento; sin inferir proceso por proximidad')
    parser.add_argument('--sin-ecr', action='store_true', help='Omite la verificacion del digest/imagen en ECR')
    parser.add_argument('--inspeccionar-imagen', action='store_true', help='Lee capas sin ejecutar la imagen; genera paquetes.csv y archivos_imagen.csv')
    parser.add_argument('--mapear-arranque', action='store_true', help='Lee config OCI de hasta 2 MiB y referencias de arranque; no descarga capas')
    parser.add_argument('--mapear-auxiliares', action='store_true', help='Inspecciona capas para mapear lecturas/escrituras y copias de datos en imagen')
    parser.add_argument('--auditar-codigo', action='store_true', help='Proyecta filtros de UNIDAD_ID/TIUN y formatos de logs de modelos desde la imagen; sin ejecutar codigo')
    parser.add_argument('--mapear-columnas', action='store_true', help='Inspecciona codigo de la imagen y traza columnas de las dos entradas; sin leer sus datos S3')
    parser.add_argument('--validar-entradas', action='store_true', help='Valida dos entradas S3 candidatas sin ECS/ECR ni descargar capas')
    parser.add_argument('--selector-unidades', action='store_true', help='Implica --validar-filas; aplica perfil de referencia SIGLA_PAPA + UNIDAD_ID no nulo, sin reglas por TIUN')
    parser.add_argument('--validar-filas', action='store_true', help='Implica --validar-entradas; revisa filas y agrupa vacios por siglas/tipo sin exportar registros ni IDs')
    parser.add_argument('--max-mib-entradas', type=int, default=5, help='Presupuesto por CSV de entrada para --validar-filas; 1..20 MiB, default 5')
    parser.add_argument('--encoding-entradas', choices=('utf-8-sig', 'cp1252', 'latin-1'), default='utf-8-sig')
    parser.add_argument('--separador-entradas', choices=('auto', ',', ';', 'tab', '|'), default='auto')
    parser.add_argument('--columnas-estructura', action='append', default=[], metavar='COLUMNA', help='Columna requerida confirmada; repetir por columna, sin contrato predeterminado')
    parser.add_argument('--columnas-maestro', action='append', default=[], metavar='COLUMNA', help='Columna requerida confirmada; repetir por columna')
    parser.add_argument('--max-mib', type=int, default=512, help='Presupuesto de descarga comprimida por imagen (default 512 MiB)')
    parser.add_argument('--salida', default='salida')
    parser.add_argument('--desde-reportes', help='Carpeta con paquetes.csv y archivos_imagen.csv; analiza sin AWS ni boto3')
    parser.add_argument('--entorno', default=DEFAULT_ENVIRONMENT, help='Entorno objetivo en la imagen; no implica que sea el proceso activo')
    parser.add_argument('--requirements-imagen', default=DEFAULT_REQUIREMENTS, help='Ruta del manifiesto a comparar dentro de la imagen')
    args = parser.parse_args()
    if args.selector_unidades:
        args.validar_filas = True
    if args.validar_filas:
        args.validar_entradas = True
    if not args.por_tarea and not args.mapear_streams and not any((args.validar_entradas, args.desde_reportes, args.probar_imports,
            args.sin_logs, args.mapear_arranque, args.inspeccionar_imagen, args.mapear_auxiliares,
            args.mapear_columnas, args.auditar_codigo)):
        args.historial_logs = True
    if args.max_eventos is None:
        args.max_eventos = 200 if args.mapear_streams else 100000 if args.historial_logs else 500
    if not 10 <= args.max_segundos_mapeo <= 900:
        parser.error('--max-segundos-mapeo: 10..900')
    if args.reiniciar_mapeo and not args.mapear_streams:
        parser.error('--reiniciar-mapeo requiere --mapear-streams')
    if not 1 <= args.dias_logs <= 30 or not 1 <= args.max_streams <= 10000:
        parser.error('--dias-logs: 1..30; --max-streams: 1..10000')
    if args.grupo_logs and (not (args.historial_logs or args.mapear_streams) or '...' in args.grupo_logs or '…' in args.grupo_logs):
        parser.error('--grupo-logs requiere historial/mapa de streams y el nombre completo, sin puntos suspensivos')
    if (args.historial_logs or args.mapear_streams) and (args.sin_logs or args.validar_entradas or args.desde_reportes or args.probar_imports
            or args.mapear_arranque or args.inspeccionar_imagen or args.mapear_auxiliares or args.mapear_columnas or args.auditar_codigo):
        parser.error('Historial/mapa de streams son modos independientes; no combinar con inspeccion, entradas, imports ni --sin-logs')
    if args.incluir_finalizadas and (args.validar_entradas or args.desde_reportes or args.probar_imports):
        parser.error('--incluir-finalizadas requiere un modo de mapeo ECS; no combinar con entradas, reportes locales o prueba de imports')
    if not 5 <= args.timeout_import <= 120:
        parser.error('--timeout-import debe estar entre 5 y 120 segundos')
    if len(args.modulo_import) > 50 or any(not re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*', module) or len(module) > 120 for module in args.modulo_import):
        parser.error('--modulo-import: hasta 50 nombres Python de maximo 120 caracteres')
    if args.modulo_import and not args.probar_imports:
        parser.error('--modulo-import requiere --probar-imports')
    if not re.fullmatch(r'/?[A-Za-z0-9_-]+(?:/[A-Za-z0-9_.-]+)*', args.entorno) or '..' in args.entorno.split('/'):
        parser.error('--entorno debe ser una ruta de entorno sin segmentos ..')
    if args.probar_imports and (args.correlacionar_modelos or args.validar_entradas or args.desde_reportes or args.mapear_arranque or args.inspeccionar_imagen or args.mapear_auxiliares or args.mapear_columnas or args.auditar_codigo or args.sin_ecr):
        parser.error('--probar-imports es independiente y requiere ECR; no combinar con otros modos ni --sin-ecr')
    if args.correlacionar_modelos and (args.sin_logs or args.validar_entradas or args.desde_reportes):
        parser.error('--correlacionar-modelos requiere logs y no se combina con validacion de entradas ni --desde-reportes')
    if not 1 <= args.max_mib_entradas <= 20:
        parser.error('--max-mib-entradas debe estar entre 1 y 20')
    if args.validar_entradas and (args.mapear_auxiliares or args.mapear_columnas or args.auditar_codigo or args.inspeccionar_imagen or args.mapear_arranque or args.desde_reportes):
        parser.error('--validar-entradas es independiente; no combinar con mapeos/inspeccion/--desde-reportes')
    if any(not name.strip() or len(name) > 160 for name in args.columnas_estructura + args.columnas_maestro):
        parser.error('Las columnas requeridas deben tener entre 1 y 160 caracteres')
    if (args.columnas_estructura or args.columnas_maestro) and not args.validar_entradas:
        parser.error('Los contratos de columnas requieren --validar-entradas')
    if args.mapear_auxiliares or args.mapear_columnas or args.auditar_codigo:
        args.inspeccionar_imagen = True
    max_events_limit = 1000 if args.mapear_streams else 100000 if args.historial_logs else 10000
    if not 1 <= args.max_tareas <= 1000 or not 1 <= args.max_eventos <= max_events_limit or not 1 <= args.ventana_minutos <= 1440:
        parser.error(f'Limites: --max-tareas 1..1000; --max-eventos 1..{max_events_limit}; --ventana-minutos 1..1440')
    if not 0 <= args.muestras <= 10:
        parser.error('--muestras debe estar entre 0 y 10')
    if not 1 <= args.max_mib <= 4096:
        parser.error('--max-mib debe estar entre 1 y 4096')
    if (args.inspeccionar_imagen or args.mapear_arranque) and args.sin_ecr:
        parser.error('La inspeccion y el mapeo de arranque requieren ECR; quitar --sin-ecr')
    if args.desde_reportes:
        if args.inspeccionar_imagen or args.mapear_arranque:
            parser.error('--desde-reportes no se combina con --inspeccionar-imagen ni --mapear-arranque')
        try:
            now = datetime.now(timezone.utc)
            folder = Path(args.salida)/'controles_modelos'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
            print('CONTROLES MODELOS Y DEPENDENCIAS | Reportes locales | Sin consultas AWS')
            return controls_from_folder(args.desde_reportes, folder, args.entorno, args.requirements_imagen)
        except (OSError, ValueError, csv.Error) as exc:
            print(f'[BLOQUEADO] {type(exc).__name__}: {exc}', file=sys.stderr)
            return 3
        except KeyboardInterrupt:
            print('[AVISO] Analisis local interrumpido.', file=sys.stderr)
            return 130
    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print('[BLOQUEADO] Ejecutar en CloudShell con boto3/botocore.', file=sys.stderr)
        return 3
    try:
        config = (Config(connect_timeout=3, read_timeout=5, retries={'total_max_attempts': 2, 'mode': 'standard'})
                  if args.mapear_streams else Config(connect_timeout=5, read_timeout=20,
                      retries={'total_max_attempts': 3, 'mode': 'standard'}))
        session = boto3.Session(region_name=args.region)
        identity = session.client('sts', config=config).get_caller_identity()
        if identity['Account'] != args.cuenta:
            print('[BLOQUEADO] Cuenta distinta de la esperada; no se consultan recursos.', file=sys.stderr)
            return 3
        if args.mapear_streams:
            return inspect_stream_mapping(session, config, args, (ClientError, BotoCoreError))
        if args.historial_logs:
            return inspect_log_history(session, config, args, (ClientError, BotoCoreError))
        if args.validar_entradas:
            return validate_candidate_inputs(session, config, identity, args, (ClientError, BotoCoreError, OSError))
        if args.probar_imports:
            return probe_image_imports(session, config, identity, args, (ClientError, BotoCoreError))
        ecs = session.client('ecs', config=config)
        print(f'MAPEO MODELOS Y DEPENDENCIAS | {args.cluster} | Solo lectura', flush=True)
        tasks, issues = collect_tasks(ecs, args.cluster, args.familia, args.max_tareas, args.incluir_finalizadas)
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=args.ventana_minutos)
        rows, coverage, definitions, clients, evidence_rows = [], [], {}, {}, []
        model_events = []
        image_results, ecr_clients, ecr_cache = [], {}, {}
        inspections = {}
        startup_rows, startup_cache, auth_tokens = [], {}, {}
        targets = sorted({t['taskDefinitionArn'] for t in tasks}) or [args.familia]
        for arn in targets:
            try:
                definitions[arn] = ecs.describe_task_definition(taskDefinition=arn)['taskDefinition']
            except (ClientError, BotoCoreError) as exc:
                code = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                issues.append(f'Definicion {arn}: {code}')
        # Sin tareas se describe la ultima revision ACTIVE: configuracion, no ejecucion.
        entries = tasks or [dict(taskDefinitionArn=args.familia)]
        for task in entries:
            arn = task['taskDefinitionArn']
            task_id = task.get('taskArn', '').rsplit('/', 1)[-1]
            definition = definitions.get(arn, {})
            for container in definition.get('containerDefinitions', []):
                refs = [(name, source, safe_reference(name, value))
                        for name, (value, source) in effective_environment(container, task).items()]
                siglas = sorted({ref[1] for _, _, ref in refs if ref and ref[0] == 'Sigla'})
                sigla = siglas[0] if len(siglas) == 1 else 'MULTIPLES' if siglas else 'SIN DATO'
                def add(kind, reference, evidence, count=''):
                    rows.append(dict(tarea=task_id, task_arn=task.get('taskArn', ''), sigla=sigla,
                        contenedor=container['name'], definicion=definition.get('taskDefinitionArn', arn),
                        tipo=kind, referencia=reference, evidencia=evidence, eventos=count))
                image = container.get('image', '')
                add('Imagen configurada', image, 'definicion ECS')
                actual = next((c for c in task.get('containers', []) if c.get('name') == container['name']), {})
                if actual.get('imageDigest'):
                    add('Digest observado', actual['imageDigest'], 'tarea ECS')
                image_result = dict(tarea=task_id, contenedor=container['name'], estado='NO CONSULTADA')
                image_results.append(image_result)
                image_startup = {}
                if not args.sin_ecr:
                    try:
                        reference = ecr_reference(actual.get('image') or image, actual.get('imageDigest', ''))
                        if reference:
                            key = (reference['cuenta'], reference['region'], reference['repositorio'],
                                   tuple(reference['identificador'].items()))
                            if key not in ecr_cache:
                                region_ecr = reference['region']
                                if region_ecr not in ecr_clients:
                                    ecr_clients[region_ecr] = session.client('ecr', region_name=region_ecr, config=config)
                                ecr_cache[key] = describe_ecr_image(ecr_clients[region_ecr], reference)
                            image_result.update(ecr_cache[key], origen=reference['origen'])
                            add('Imagen en ECR', 'ENCONTRADA', reference['origen'])
                            if args.mapear_arranque:
                                architecture = definition.get('runtimePlatform', {}).get('cpuArchitecture', 'X86_64')
                                startup_key = key + (architecture,)
                                try:
                                    if startup_key not in startup_cache:
                                        print(f'Consultando configuracion de arranque: {sigla} (sin capas)...', flush=True)
                                        startup_cache[startup_key] = read_image_startup(ecr_clients[reference['region']], reference,
                                            image_result['digest'], architecture, auth_tokens)
                                    image_startup = startup_cache[startup_key]
                                except (ClientError, BotoCoreError, OSError, ValueError) as exc:
                                    code = (exc.response['Error']['Code'] if isinstance(exc, ClientError)
                                            else 'HTTP ' + str(exc.code) if isinstance(exc, HTTPError) else type(exc).__name__)
                                    image_result['error_arranque'] = code
                                    issues.append(f'{task_id[:12] or "CONFIG"} / configuracion de arranque: {code}')
                            if args.inspeccionar_imagen:
                                architecture = definition.get('runtimePlatform', {}).get('cpuArchitecture', 'X86_64')
                                operating_system = definition.get('runtimePlatform', {}).get('operatingSystemFamily', 'LINUX')
                                inspection_key = key + (architecture,)
                                try:
                                    if operating_system != 'LINUX':
                                        raise ValueError('Inspeccion soportada solo para imagenes Linux')
                                    if inspection_key not in inspections:
                                        print(f'Inspeccionando imagen de {sigla} por digest; presupuesto {args.max_mib} MiB...', flush=True)
                                        inspections[inspection_key] = inspect_image(ecr_clients[reference['region']], reference,
                                            image_result['digest'], architecture, args.max_mib*1024**2)
                                    inspection = inspections[inspection_key]
                                    image_result['inspeccion'] = inspection['estado']
                                    add('Capas de imagen', inventory_coverage(inspection), f'{inspection["capas_leidas"]}/{inspection["capas_total"]} capas')
                                    if args.mapear_auxiliares:
                                        add('Analisis de rutas', inspection.get('cobertura_rutas', inspection['estado']), 'profundidad y sintaxis; ver contexto_auxiliares.json')
                                    add('Metadata de paquetes', str(len(inspection['paquetes'])), 'no prueba imports/compatibilidad')
                                    add('Manifiestos internos', str(len(inspection['manifiestos'])), 'requirements / configuracion')
                                    if inspection['limitaciones']:
                                        detail = 'lectura de archivos incompleta' if inventory_coverage(inspection) != 'LEIDA' else 'analisis de rutas limitado; capas leidas'
                                        issues.append(f'{task_id[:12] or "CONFIG"} / inspeccion: {detail}; ver contexto.json')
                                except (ClientError, BotoCoreError, OSError, ValueError, tarfile.TarError) as exc:
                                    code = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                                    image_result.update(inspeccion='NO COMPLETADA', error_inspeccion=code)
                                    if isinstance(exc, ValueError):
                                        image_result['detalle_inspeccion'] = str(exc)[:180]
                                    add('Capas de imagen', 'NO COMPLETADA', code)
                                    issues.append(f'{task_id[:12] or "CONFIG"} / inspeccion: {code}')
                        else:
                            image_result['estado'] = 'REGISTRO NO SOPORTADO'
                            add('Imagen en ECR', 'NO VERIFICADA', 'imagen fuera de ECR privado reconocido')
                    except (ClientError, BotoCoreError, ValueError) as exc:
                        code = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                        image_result.update(estado='NO VERIFICADA', error=code)
                        add('Imagen en ECR', 'NO VERIFICADA', code)
                        issues.append(f'{task_id[:12] or "CONFIG"} / ECR: {code}')
                if args.mapear_arranque:
                    startup = startup_summary(task, container, image_startup)
                    startup_rows.append(dict(tarea=task_id, sigla=sigla, contenedor=container['name'],
                        digest=image_result.get('digest', actual.get('imageDigest', '')), **startup))
                    add('Interprete referenciado', startup['interpretes_referenciados'], 'configuracion; no proceso observado')
                    add('Entorno en comando', startup['entornos_en_comando'], 'referencia; no activacion probada')
                    add('Entorno declarado', startup['variables_entorno_proyectadas'], 'variables de lanzamiento; no proceso')
                    add('Scripts referenciados', startup['scripts_referenciados'] or 'NO IDENTIFICADOS', 'configuracion; argumentos omitidos')
                for name, source, ref in refs:
                    if ref:
                        add(ref[0], ref[1], f'{source}: {name}')
                for field in ('taskRoleArn', 'executionRoleArn'):
                    if definition.get(field):
                        add('Rol configurado', definition[field], field)
                # Solo nombres de variables: no sus valores ni referencias a secretos.
                add('Variables declaradas', ', '.join(sorted(effective_environment(container, task))), 'nombres; valores omitidos')
                if container.get('environmentFiles'):
                    add('Entorno externo', 'No inspeccionado', 'environmentFiles declarado')
                if container.get('secrets'):
                    add('Secretos inyectados', 'No inspeccionados', 'secrets declarado')
                log = container.get('logConfiguration', {})
                opts = log.get('options', {})
                group, prefix = opts.get('awslogs-group'), opts.get('awslogs-stream-prefix')
                region = opts.get('awslogs-region', args.region)
                item = dict(tarea=task_id, sigla=sigla, contenedor=container['name'], logs='NO CONFIGURADOS', eventos_filtrados=0)
                coverage.append(item)
                if group:
                    add('Grupo de logs', group, 'logConfiguration')
                if args.sin_logs or not task_id:
                    item['logs'] = 'NO CONSULTADOS'
                    continue
                if log.get('logDriver') != 'awslogs' or not group or not prefix:
                    if args.correlacionar_modelos:
                        issues.append(f'{task_id[:12]} / {container["name"]}: sin stream awslogs identificable; modelos no evaluados')
                    continue
                try:
                    if region not in clients:
                        clients[region] = session.client('logs', region_name=region, config=config)
                    stream = f'{prefix}/{container["name"]}/{task_id}'
                    print(f'Consultando referencias: {task_id[:12]} / {sigla}...', flush=True)
                    details = [] if args.correlacionar_modelos else None
                    log_start, log_end = stopped_log_window(task, start, now)
                    counts, truncated, read, samples = read_references(clients[region], group, stream, log_start, log_end, args.max_eventos, args.muestras, details)
                    if details is not None:
                        model_events.extend(dict(tarea=task_id, sigla=sigla, contenedor=container['name'],
                            stream=stream, cobertura='PARCIAL' if truncated else 'LEIDA', **event) for event in details)
                    evidence_rows.extend(dict(tarea=task_id, sigla=sigla, contenedor=container['name'], **sample) for sample in samples)
                    item.update(logs='TRUNCADOS' if truncated else 'LEIDOS', eventos_filtrados=read,
                                grupo=group, stream=stream, region=region,
                                desde_utc=log_start.isoformat(), hasta_utc=log_end.isoformat())
                    for (kind, name), count in sorted(counts.items()):
                        add(kind, name, 'declaracion en logs' if kind in ('Seleccion reportada', 'Respaldo reportado') else 'referencia en logs', count)
                    if truncated:
                        issues.append(f'{task_id[:12]} / {container["name"]}: referencias truncadas; aumentar --max-eventos o reducir ventana')
                except (ClientError, BotoCoreError) as exc:
                    code = exc.response['Error']['Code'] if isinstance(exc, ClientError) else type(exc).__name__
                    item.update(logs='ERROR LECTURA', error=code)
                    issues.append(f'{task_id[:12]} / logs: {code}')
        lifecycle = task_lifecycle_rows(tasks, definitions) if args.incluir_finalizadas else []
        if args.incluir_finalizadas:
            print_task_lifecycle(lifecycle)
            print(f'Logs activas: ultimos {args.ventana_minutos} minutos. STOPPED: vida de la tarea visible; metadata en contexto.json.')
        if not args.correlacionar_modelos:
            print_table(rows)
        print(f'\nTareas observadas: {len(tasks)} | Definiciones descritas: {len(definitions)} | Lecturas incompletas: {len(issues)}')
        if not tasks:
            print('Sin tareas visibles: se describe la ultima revision ACTIVE, no una imagen en ejecucion.')
        for issue in issues:
            print('[INCOMPLETO] ' + issue)
        print('Seleccion/respaldo reportados describen eventos de la muestra, no un modelo unico de toda la tarea ni exito final.')
        print('Mencion o indicio no confirma seleccion/respaldo. Librerias instaladas/compatibles siguen sin verificar.')
        if not args.sin_ecr:
            print('DescribeImages verifica metadata en ECR; no prueba que el rol de la tarea pueda descargarla.')
        if args.inspeccionar_imagen:
            print('Inspeccion estatica: metadata de paquetes y codigo legible. No ejecuta imports ni certifica compatibilidad o acceso desde ECS.')
        if args.mapear_arranque:
            print('Arranque configurado: referencias de interprete/entorno; el proceso efectivo sigue NO EVALUADO. No usa ECS Exec.')
        print('Sin menciones en la ventana no significa que un modelo no se haya usado. No es un monitor de salud.')
        folder = Path(args.salida)/'modelos_dependencias'/now.astimezone(BOGOTA).strftime('%Y-%m-%d/%H-%M-%S_%f')
        folder.mkdir(parents=True, exist_ok=True)
        if args.incluir_finalizadas:
            with (folder/'tareas.csv').open('w', encoding='utf-8-sig', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['tarea', 'task_arn', 'siglas', 'estado_ecs',
                    'creacion', 'inicio', 'fin', 'resultado_contenedores', 'codigos_salida',
                    'stop_code', 'motivo_parada', 'motivos_contenedores'])
                writer.writeheader()
                writer.writerows(lifecycle)
        with (folder/'mapa.csv').open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=['tarea', 'task_arn', 'sigla', 'contenedor', 'definicion', 'tipo', 'referencia', 'evidencia', 'eventos'])
            writer.writeheader()
            writer.writerows(rows)
        with (folder/'evidencias.csv').open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=['tarea', 'sigla', 'contenedor', 'tipo', 'modelo', 'motivo', 'fuente', 'instante_utc', 'mensaje_saneado'])
            writer.writeheader()
            writer.writerows(evidence_rows)
        if args.inspeccionar_imagen:
            with (folder/'paquetes.csv').open('w', encoding='utf-8-sig', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['digest', 'cobertura', 'paquete', 'version', 'archivo', 'python_requerido', 'requisitos'])
                writer.writeheader()
                for inspection in inspections.values():
                    writer.writerows(dict(digest=inspection['digest'], cobertura=inventory_coverage(inspection), **p) for p in inspection['paquetes'])
            with (folder/'archivos_imagen.csv').open('w', encoding='utf-8-sig', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['digest', 'cobertura', 'archivo', 'tipo', 'referencia', 'linea'])
                writer.writeheader()
                for inspection in inspections.values():
                    for file in inspection['manifiestos']:
                        writer.writerow(dict(digest=inspection['digest'], cobertura=inventory_coverage(inspection), archivo=file['archivo'],
                            tipo='Manifiesto presente', referencia=file['declaraciones'], linea=''))
                    for file in inspection['codigo']:
                        writer.writerow(dict(digest=inspection['digest'], cobertura=inventory_coverage(inspection), **file))
        dependency_exit = 0
        if args.inspeccionar_imagen and inspections:
            dependency_exit = controls_from_folder(folder, folder, args.entorno, args.requirements_imagen)
        if args.mapear_auxiliares:
            write_auxiliary_report(folder, list(inspections.values()))
        if args.mapear_columnas:
            write_column_consumption(folder, list(inspections.values()))
        if args.auditar_codigo:
            write_consumer_code_evidence(folder, list(inspections.values()))
        if args.mapear_arranque:
            with (folder/'arranque.csv').open('w', encoding='utf-8-sig', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=list(startup_rows[0]) if startup_rows else ['tarea', 'sigla', 'entorno_efectivo_proceso'])
                writer.writeheader()
                writer.writerows(startup_rows)
        context = dict(captura_utc=now.isoformat(), cuenta=identity['Account'], configuracion=vars(args),
            ventana_utc=dict(desde=start.isoformat(), hasta=now.isoformat()), filas=rows,
            cobertura=coverage, imagenes=image_results, inspecciones=list(inspections.values()), consultas_completas=not issues, limitaciones=issues,
            controles_estaticos_exit=dependency_exit,
            arranque=startup_rows,
            tareas_observadas=lifecycle,
            alcance_tareas='RUNNING y STOPPED aun visibles, limitado por estado; no es historial semanal' if args.incluir_finalizadas else 'RUNNING visibles',
            correlacion_modelos=dict(habilitada=args.correlacionar_modelos,
                registros=len(model_events), regla='Solo contexto explicito dentro del mismo evento',
                seleccion_uniform_no_implica_respaldo=True,
                cobertura_global='PARCIAL' if issues or any(c['logs'] != 'LEIDOS' for c in coverage) else 'LEIDA',
                sin_tareas=not bool(tasks)),
            pendientes=['Imports en el entorno objetivo'],
            fuera_de_alcance=['Comprobar acceso desde ECS', 'Validar resultados de prediccion en S3'])
        (folder/'contexto.json').write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding='utf-8')
        if args.correlacionar_modelos:
            write_model_correlation(folder, model_events, coverage)
        print(f'Mapa local: {folder}/mapa.csv | evidencias.csv | contexto.json')
        if args.incluir_finalizadas:
            print(f'Tareas locales: {folder}/tareas.csv')
        if args.inspeccionar_imagen:
            print(f'Inspeccion local: {folder}/paquetes.csv | archivos_imagen.csv')
        if args.mapear_arranque:
            print(f'Arranque local: {folder}/arranque.csv')
        return 3 if issues else dependency_exit
    except (ClientError, BotoCoreError, OSError, ValueError, csv.Error) as exc:
        print(f'[BLOQUEADO] {type(exc).__name__}: {exc}', file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print('[AVISO] Lectura interrumpida; no se detuvo ninguna tarea.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    sys.exit(main())
