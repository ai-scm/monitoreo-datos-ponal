"""Utilidades compartidas de la validacion SECAD del mapa. Solo lectura.

Sesion AWS, ejecucion de consultas Athena, catalogo de unidades (el mismo CSV con que el
backend arma el arbol del mapa) y las reglas de ese arbol. Lo importan descubrir_secad.py y
validar_secad.py; no se despliega nada.
"""

import csv
import io
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, ProfileNotFound

CUENTA = '926162397524'
REGION = 'us-east-1'
DATABASE = 'pon-prod-capas-iapol'
# Mismo archivo que lee ArbolJerarquicoServicio.java (bucket aws.bucketName.geoJsonBase).
CATALOGO_S3 = 's3://pon-prod-ml-iapol/EstructuraNacionaOperativalPONAL.csv'
UTC_MENOS_5 = timezone(timedelta(hours=-5))  # Colombia no tiene horario de verano

# Capas SECAD del mapa (LAYER_CONFIGS / capas_config.json) -> tabla Athena que lee el BFF.
CAPAS = {
    'secad-rinas': ('capa_13', 'SECAD: Rinas'),
    'secad-hurtos-violento-y-homicidio': ('capa_11', 'SECAD: Hurto violento y homicidio'),
    'secad-hurtos-general': ('capa_12', 'SECAD: Hurto general'),
}

# Columnas que usa el BFF (capas_config.json, athena_service.py). Se pueden cambiar con --col.
COLUMNAS = dict(sigla='sigla', unidad='codigo_unidad', fecha='hora_caso', lat='latitud',
                lon='longitud', anio='anio', mes='mes', dia='dia')
COLUMNA_LLAMADA = 'nume_llamada'  # opcional: solo se usa si existe en la tabla

# Rango de coordenadas que deja pasar geojson_service.py; fuera de el el BFF descarta la fila.
LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = -82, -66, -5, 15

# Reglas del arbol del mapa (ArbolJerarquicoServicio.java y quitarUnidadesSinPoligono).
TIPOS_FILTRADOS = {'1.0', '308.0', '164.0', '187.0', '6.0'}  # el arbol los omite y asciende sus hijas
UNIDADES_SIN_POLIGONO = {'64780'}  # solo se quitan como hijas
TIPOS_SIGLA = {'4.0', '5.0'}  # departamento / metropolitana: el mapa pide solo por sigla

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=60, retries={'total_max_attempts': 3, 'mode': 'standard'})


class Blocked(RuntimeError):
    """No se pudo ejecutar (credenciales, permisos, esquema, catalogo): exit 3, no es una falla de datos."""


def ahora_colombia():
    return datetime.now(UTC_MENOS_5).replace(tzinfo=None)


def identifier(valor):
    return '"' + valor.replace('"', '""') + '"'


def literal(valor):
    return "'" + valor.replace("'", "''") + "'"


def detalle_error(exc):
    if isinstance(exc, ClientError):
        e = exc.response.get('Error', {})
        return f'{e.get("Code", "ClientError")}: {e.get("Message", "")}'.strip(': ')
    return f'{type(exc).__name__}: {exc}'


# --------------------------------------------------------------------------------------
# Sesion e identidad
# --------------------------------------------------------------------------------------
def crear_sesion(profile=None, region=REGION):
    try:
        return boto3.Session(profile_name=profile, region_name=region)
    except ProfileNotFound as exc:
        raise Blocked(f'Perfil AWS no encontrado: {exc}') from exc


def verificar_identidad(sesion, cuenta=CUENTA):
    try:
        ident = sesion.client('sts', config=BOTO_CONFIG).get_caller_identity()
    except NoCredentialsError as exc:
        raise Blocked('Sin credenciales AWS. Inicia sesion (aws sso login / aws login) o exporta '
                      'AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y AWS_SESSION_TOKEN; --profile elige un perfil.') from exc
    except (ClientError, BotoCoreError) as exc:
        raise Blocked(f'No se pudo obtener la identidad AWS: {detalle_error(exc)}') from exc
    if cuenta and ident['Account'] != cuenta:
        raise Blocked(f'La cuenta {ident["Account"]} no es la esperada ({cuenta}); use --cuenta si es intencional.')
    return ident


# --------------------------------------------------------------------------------------
# Glue
# --------------------------------------------------------------------------------------
def columnas_tabla(glue, database, tabla):
    """Devuelve (columnas {nombre: tipo}, claves de particion [nombres], tabla Glue completa)."""
    try:
        t = glue.get_table(DatabaseName=database, Name=tabla)['Table']
    except glue.exceptions.EntityNotFoundException as exc:
        raise Blocked(f'No existe {database}.{tabla} en Glue (region/cuenta/permisos).') from exc
    except (ClientError, BotoCoreError) as exc:
        raise Blocked(f'No se pudo leer {database}.{tabla} en Glue: {detalle_error(exc)}') from exc
    cols = {c['Name'].lower(): c.get('Type', '') for c in t.get('StorageDescriptor', {}).get('Columns', [])}
    claves = [c['Name'].lower() for c in t.get('PartitionKeys', [])]
    for c in t.get('PartitionKeys', []):
        cols[c['Name'].lower()] = c.get('Type', '')
    return cols, claves, t


TERMINALES = ('SUCCEEDED', 'FAILED', 'ERROR', 'TIMEOUT', 'STOPPED')  # estados finales de una corrida de Glue


def fecha_run_utc(run):
    return run['StartedOn'].astimezone(timezone.utc).date()  # Athena evalua current_date en UTC


def leer_corridas(glue, job, desde):
    """Corridas del job, de la mas reciente hacia atras, hasta cubrir `desde` (con dos dias de margen)."""
    corridas, token = [], None
    limite = desde - timedelta(days=2)
    while True:
        kw = dict(JobName=job, MaxResults=200)
        if token:
            kw['NextToken'] = token
        r = glue.get_job_runs(**kw)
        corridas += r['JobRuns']
        token = r.get('NextToken')
        if not token or not corridas or fecha_run_utc(corridas[-1]) < limite:
            return corridas


# --------------------------------------------------------------------------------------
# Athena
# --------------------------------------------------------------------------------------
class Athena:
    """Lanza varias consultas a la vez, espera, y devuelve las filas como diccionarios."""

    def __init__(self, sesion, database=DATABASE, workgroup='primary', output_s3=None, timeout=300, log=print):
        self.client = sesion.client('athena', config=BOTO_CONFIG)
        self.database = database
        self.workgroup = workgroup
        self.output_s3 = output_s3
        self.timeout = timeout
        self.log = log

    def ejecutar(self, consultas):
        """consultas: {nombre: sql}. Devuelve {nombre: (filas, info_athena)}."""
        ids = {}
        try:
            for nombre, sql in consultas.items():
                ids[nombre] = self._iniciar(sql)
                self.log(f'  [athena] {nombre}: {ids[nombre]}')
            info = self._esperar(ids)
            return {nombre: (self._leer(ids[nombre]), info[nombre]) for nombre in consultas}
        except (KeyboardInterrupt, TimeoutError):
            self._cancelar(ids.values())
            raise

    def _iniciar(self, sql):
        kwargs = dict(QueryString=sql, WorkGroup=self.workgroup,
                      QueryExecutionContext={'Database': self.database, 'Catalog': 'AwsDataCatalog'})
        if self.output_s3:
            kwargs['ResultConfiguration'] = {'OutputLocation': self.output_s3}
        try:
            return self.client.start_query_execution(**kwargs)['QueryExecutionId']
        except (ClientError, BotoCoreError) as exc:
            raise Blocked(f'Athena rechazo la consulta: {detalle_error(exc)}') from exc

    def _esperar(self, ids):
        pendientes, info = dict(ids), {}
        limite = time.monotonic() + self.timeout
        while pendientes:
            for nombre, qid in list(pendientes.items()):
                q = self.client.get_query_execution(QueryExecutionId=qid)['QueryExecution']
                estado = q['Status']['State']
                if estado == 'SUCCEEDED':
                    info[nombre] = q
                    del pendientes[nombre]
                elif estado in ('FAILED', 'CANCELLED'):
                    del pendientes[nombre]
                    self._cancelar(pendientes.values())
                    raise Blocked(f'Athena {estado} en {nombre}: {q["Status"].get("StateChangeReason", "sin detalle")} '
                                  f'(query {qid})')
            if pendientes:
                if time.monotonic() >= limite:
                    raise TimeoutError(f'Tiempo de espera agotado ({self.timeout}s); pendientes: {", ".join(pendientes)}')
                time.sleep(2)
        return info

    def _leer(self, qid):
        filas, primera = [], True
        for pagina in self.client.get_paginator('get_query_results').paginate(QueryExecutionId=qid):
            rs = pagina['ResultSet']
            cols = [c['Name'] for c in rs['ResultSetMetadata']['ColumnInfo']]
            for fila in rs.get('Rows', []):
                if primera:  # Athena repite los nombres de columna como primera fila de un SELECT
                    primera = False
                    continue
                filas.append(dict(zip(cols, [d.get('VarCharValue') for d in fila.get('Data', [])])))
        return filas

    def _cancelar(self, ids):
        for qid in ids:
            try:
                self.client.stop_query_execution(QueryExecutionId=qid)
                self.log(f'  [athena] cancelacion solicitada: {qid}')
            except (ClientError, BotoCoreError):
                pass


# --------------------------------------------------------------------------------------
# Catalogo de unidades y arbol del mapa
# --------------------------------------------------------------------------------------
@dataclass
class Unidad:
    id: str
    nombre: str
    sigla: str
    tipo: str = ''
    padre: str = ''
    cuadrantes: tuple = ()  # CUADRANTES_ID del catalogo: cuadrantes que pertenecen a la unidad


# El primer nombre de cada lista es el del CSV real del backend (EstructuraNacionaOperativalPONAL.csv):
# NOMBRE, TIUN_CODIGO, CONSECUTIVO, SIGLA_FISICA, UNDE_CONSECUTIVO = posiciones 1, 2, 3, 6, 8 que lee
# ArbolJerarquicoServicio. Ojo: id_unidad del mapa es CONSECUTIVO, no UNIDAD_ID (esa columna solo
# viene en las unidades operativas). El resto de nombres cubre otros CSV derivados del mismo.
_ENCABEZADOS = dict(
    id=('CONSECUTIVO', 'UNIDAD_ID', 'ID_UNIDAD'),
    nombre=('NOMBRE', 'DEPENDENCIA', 'NOMBRE_UNIDAD'),
    sigla=('SIGLA_FISICA', 'SIGLA_DMCE', 'SIGLA'),
    tipo=('TIUN_CODIGO', 'TIPO', 'TIPO_UNIDAD'),
    padre=('UNDE_CONSECUTIVO', 'ID_UNIDAD_PADRE', 'UNIDAD_PADRE_ID', 'ID_PADRE', 'UNIDAD_ID_PADRE', 'PADRE_ID', 'UNIDAD_PADRE'),
    cuadrantes=('CUADRANTES_ID',),
)


def _lista_cuadrantes(valor):
    return tuple(c.strip() for c in re.split(r'[,;]', valor or '') if c.strip())


def normalizar_id(valor):
    """'20435.0' -> '20435'; lo que no es un id numerico -> ''."""
    v = (valor or '').strip()
    return v.split('.')[0] if re.fullmatch(r'\d+(\.0+)?', v) else ''


def normalizar_tipo(valor):
    v = (valor or '').strip()
    try:
        return f'{float(v):.1f}'
    except ValueError:
        return v


def _texto_catalogo(origen, sesion):
    if origen.startswith('s3://'):
        bucket, _, key = origen[5:].partition('/')
        try:
            cuerpo = sesion.client('s3', config=BOTO_CONFIG).get_object(Bucket=bucket, Key=key)['Body'].read()
        except (ClientError, BotoCoreError) as exc:
            raise Blocked(f'No se pudo leer el catalogo {origen}: {detalle_error(exc)}. '
                          'Descargalo y usa --catalogo <archivo local>, o --sin-catalogo.') from exc
    else:
        try:
            with open(origen, 'rb') as f:
                cuerpo = f.read()
        except OSError as exc:
            raise Blocked(f'No se pudo abrir el catalogo {origen}: {exc}') from exc
    try:
        return cuerpo.decode('utf-8-sig')
    except UnicodeDecodeError:
        return cuerpo.decode('latin-1')


def parsear_catalogo(texto):
    """Dos formatos: con encabezados (UNIDAD_ID, DEPENDENCIA, SIGLA_FISICA, ...) o el CSV sin
    encabezados del backend, por posicion: [1]=nombre [2]=tipo [3]=id [6]=sigla [8]=padre."""
    try:
        delim = csv.Sniffer().sniff(texto[:12000], delimiters=',;\t|').delimiter
    except csv.Error:
        delim = ','
    filas = list(csv.reader(io.StringIO(texto), delimiter=delim))
    if not filas:
        raise Blocked('El catalogo esta vacio.')
    cabecera = [c.strip().upper().lstrip('﻿') for c in filas[0]]
    indices = {k: next((cabecera.index(n) for n in nombres if n in cabecera), None) for k, nombres in _ENCABEZADOS.items()}
    unidades = []
    if indices['id'] is not None:
        if indices['nombre'] is None or indices['sigla'] is None:
            raise Blocked('Catalogo con encabezados: se requieren CONSECUTIVO (o UNIDAD_ID), NOMBRE (o DEPENDENCIA) y SIGLA_FISICA.')

        def celda(fila, clave):
            i = indices[clave]
            return fila[i].strip() if i is not None and i < len(fila) else ''

        for f in filas[1:]:
            uid = normalizar_id(celda(f, 'id'))
            if uid:
                unidades.append(Unidad(uid, celda(f, 'nombre'), celda(f, 'sigla').upper(),
                                       normalizar_tipo(celda(f, 'tipo')), normalizar_id(celda(f, 'padre')),
                                       _lista_cuadrantes(celda(f, 'cuadrantes'))))
        formato = 'encabezados' + ('' if indices['padre'] is not None else ' (sin columna de padre: jerarquia plana)')
    else:
        for f in filas:
            if len(f) > 8 and f[0].strip():
                uid = normalizar_id(f[3])
                if uid:
                    unidades.append(Unidad(uid, f[1].strip(), f[6].strip().upper(), normalizar_tipo(f[2]), normalizar_id(f[8]),
                                           _lista_cuadrantes(f[12]) if len(f) > 12 else ()))
        formato = 'posicional (formato del backend)'
    if not unidades:
        raise Blocked('El catalogo no tiene unidades con id numerico; revisa el formato o el delimitador.')
    return unidades, formato


def cargar_catalogo(origen, sesion=None):
    return parsear_catalogo(_texto_catalogo(origen, sesion))


class Arbol:
    """Arbol de unidades tal como lo muestra el mapa y como arma el payload de las capas."""

    def __init__(self, unidades, tipos_filtrados=TIPOS_FILTRADOS, sin_poligono=UNIDADES_SIN_POLIGONO):
        self.por_id = {}
        for u in unidades:  # el backend deduplica por id: gana la primera fila
            self.por_id.setdefault(u.id, u)
        self.tipos_filtrados = set(tipos_filtrados)
        self.sin_poligono = set(sin_poligono)
        self._pe = {i: self._padre_efectivo(u) for i, u in self.por_id.items()}
        self.visibles = {i for i in self.por_id if self._es_visible(i)}
        self.hijos = defaultdict(list)
        self.raices = []
        for i, u in self.por_id.items():
            if i not in self.visibles:
                continue
            pe = self._pe[i]
            (self.hijos[pe] if pe is not None else self.raices).append(u)
        self._desc = {}

    def _padre_efectivo(self, u):
        """Primer ancestro no filtrado (el arbol asciende las hijas de los tipos filtrados)."""
        vistos, p = set(), u.padre
        while p and p not in vistos:
            vistos.add(p)
            pu = self.por_id.get(p)
            if pu is None:
                return None
            if pu.tipo not in self.tipos_filtrados:
                return p
            p = pu.padre
        return None

    def _es_visible(self, id_):
        vistos, actual = set(), id_
        while actual is not None and actual not in vistos:
            vistos.add(actual)
            u = self.por_id[actual]
            if u.tipo in self.tipos_filtrados:
                return False
            if actual in self.sin_poligono and self._pe[actual] is not None:
                return False
            actual = self._pe[actual]
        return True

    def ids_con_descendientes(self, id_):
        """La unidad y todas sus descendientes visibles: lo que manda el mapa en id_unidad."""
        if id_ not in self._desc:
            res, pila, vistos = [], [id_], set()
            while pila:
                actual = pila.pop()
                if actual in vistos:
                    continue
                vistos.add(actual)
                res.append(actual)
                pila.extend(h.id for h in self.hijos.get(actual, []))
            self._desc[id_] = res
        return self._desc[id_]

    def cuadrante_a_unidad(self):
        """{cuadrante: id_unidad}. Si un cuadrante figurara en dos unidades gana la primera del catalogo."""
        mapa = {}
        for u in self.por_id.values():
            for q in u.cuadrantes:
                mapa.setdefault(q, u.id)
        return mapa

    def es_sigla_completa(self, id_):
        """El mapa pide solo por sigla (sin id_unidad) para departamentos y metropolitanas."""
        return self.por_id[id_].tipo in TIPOS_SIGLA

    def siglas_con_territorio(self):
        """Siglas con una unidad departamento/metropolitana; si el catalogo no trae tipos, todas las visibles."""
        con_tipo = {self.por_id[i].sigla for i in self.visibles if self.por_id[i].tipo in TIPOS_SIGLA}
        return con_tipo or {self.por_id[i].sigla for i in self.visibles if self.por_id[i].sigla}

    def recorrido(self):
        """[(unidad, nivel)] en orden de arbol; el nivel cuenta ancestros con la misma sigla."""
        salida, vistos = [], set()

        def visitar(u, nivel):
            if u.id in vistos:
                return
            vistos.add(u.id)
            salida.append((u, nivel))
            for h in sorted(self.hijos.get(u.id, []), key=lambda x: (x.sigla, x.nombre)):
                visitar(h, nivel + 1 if h.sigla == u.sigla else 0)

        for r in sorted(self.raices, key=lambda x: (x.sigla, x.nombre)):
            visitar(r, 0)
        return salida
