"""Nombres legibles para los CSV del informe de calidad de fuentes.

Uso: python columnas.py <cal_detalle.csv> <salida_detalle.csv> <salida_diccionario.csv>
Convierte los contadores cortos de cal_resumen.py (c_cero, e_sin_canal, ...) en columnas que se entienden solas,
en un orden de lectura: totales, identificador, fecha, coordenadas y luego los demas campos.
"""
import csv, sys

FUENTES = {
    'secad_cadpedidos':      ('SECAD regionales (llamadas 123)',          'Oracle SECAD <SIGLA> - USR_CAD.CADPEDIDOS'),
    'mebog_premierone':      ('PremierOne MEBOG (llamadas Bogota)',        'Archivos Excel de GUTIC (no hay base conectada)'),
    'meval_hexagon':         ('Hexagon MEVAL (llamadas Cali)',             'Archivos Parquet de Hexagon (no hay base conectada)'),
    'rnmc_hechos':           ('RNMC comparendos / hechos',                 'Oracle DBOPER - USR_CNP.CNP_HECHOS'),
    'rnmc_mediaciones_sala': ('RNMC mediaciones en sala',                  'Oracle DBOPER - USR_CNP.CNP_SOLICI_MEDIA'),
    'antecedentes_persona':  ('Antecedentes - consultas de personas',      'Oracle SIVICC - USR_MNVCC.AUDITORIA_CONS_PERSONA'),
    'antecedentes_vehiculo': ('Antecedentes - consultas de vehiculos',     'Oracle SIVICC - USR_MNVCC.AUDITORIA_CONS_VEHICULO'),
    'antecedentes_imei':     ('Antecedentes - consultas de IMEI',          'Oracle SIVICC - USR_MNVCC.AUDITORIA_CONS_IMEI'),
    'gescov_evos_planes':    ('GESCOV planes de vigilancia',               'Oracle DISEC - USR_DISEC.V_EVOS_PLANES'),
    'mtrs':                  ('MTRS mesas tecnicas de riesgo',             'Oracle DISEC - USR_DISEC.V_MTRS_RIESGO_UBICACION'),
    'iris_p1':               ('IRIS P1',                                   'Oracle DISEC - USR_DISEC.V_GENERAL_IRISP'),
    'simep':                 ('SIMEP autos / medidas',                     'Oracle DBOPER - USR_SIMEP.V_SIMEP_AUTOS_ARCGIS'),
    'siedco':                ('SIEDCO delitos',                            'Oracle SIEDCO - USR_ESTA.MOTIVOSPOLICIA'),
    'sirap':                 ('SIRAP actividades',                         'Oracle GEOPOL - USR_PMU.ACTIVIDADES'),
}

# columna corta -> (columna legible, que cuenta, campo revisado en cada fuente)
COLUMNAS = [
    ('n',          'total_registros_6_meses', 'Registros de la fuente entre 2026-05-03 y 2026-11-02.', 'Todas'),
    ('critico',    'registros_criticos_sin_id_fecha_o_coordenada_valida',
     'Registros que no se pueden ubicar en el mapa: sin ID, o sin fecha, o con coordenada vacia, en 0, no numerica, invertida o fuera de Colombia. Un registro cuenta una sola vez aunque tenga varios problemas.', 'Todas'),
    ('pct',        'pct_registros_criticos', 'registros_criticos / total_registros x 100.', 'Todas'),
    ('id_nulo',    'sin_id_del_registro', 'El identificador del registro viene vacio.',
     'SECAD: SITIO_GRABA o NUME_LLAMADA; PremierOne: no_incidente; Hexagon: cod_incidente; RNMC: ID_HECHOS / ID_SOLICITUD; Antecedentes: ID_AUDITORIA_PERSONA / ID_VEHICULO / ID_IMEI; GESCOV: PLAN_ID-PUNTOS_ID; MTRS: RIESGO_ID; IRIS: CRIMINALIDAD_ID; SIMEP: AUTO_ID; SIEDCO: HECHOS_ID; SIRAP: ID_ACTIVIDAD'),
    ('ids',        'ids_distintos', 'Cantidad de identificadores diferentes.', 'Mismo campo de sin_id_del_registro'),
    ('duplicados', 'registros_con_id_repetido', 'Registros de mas que comparten el mismo ID (total - sin_id - ids_distintos). En SIEDCO es normal: un hecho trae una fila por persona o bien.', 'Mismo campo de sin_id_del_registro'),
    ('f_nula',     'sin_fecha_o_fecha_ilegible', 'Fecha vacia o en un formato que no se puede leer.',
     'SECAD: HORA_CASO; PremierOne: fecha_inicial; Hexagon: fecha; RNMC: FECHA / FECHA_SOLICITUD; Antecedentes: FECHA / FECHA_CREACION; GESCOV: FECHA_INICIO_PLAN; MTRS: FECHA_INICIO_MESA_TECNICA; IRIS: FECHA_CREACION_IRISP1; SIMEP: FECHA_EXPEDICION; SIEDCO: FECHA_HECHO; SIRAP: FECHA_INICIO'),
    ('f_futura',   'fecha_posterior_a_hoy', 'Fecha mayor que la fecha de la consulta.', 'Mismo campo de sin_fecha_o_fecha_ilegible'),
    ('c_ok',       'coordenada_valida_dentro_de_colombia', 'Latitud entre -4,3 y 13,6 y longitud entre -82 y -66.', 'LATITUD / LONGITUD (SECAD: LATITUD_CASO / LONGITUD_CASO)'),
    ('c_nula',     'coordenada_vacia', 'Latitud o longitud vacia.', 'LATITUD / LONGITUD'),
    ('c_cero',     'coordenada_en_cero', 'Latitud o longitud igual a 0 (el sistema guardo 0 en vez de la ubicacion).', 'LATITUD / LONGITUD'),
    ('c_no_numerica', 'coordenada_con_texto_no_numerico', 'La coordenada trae texto que no es un numero.', 'LATITUD / LONGITUD'),
    ('c_invertida',   'coordenada_latitud_y_longitud_invertidas', 'La latitud esta en la longitud y viceversa.', 'LATITUD / LONGITUD'),
    ('c_fuera',       'coordenada_fuera_de_colombia', 'Coordenada numerica que cae fuera de Colombia.', 'LATITUD / LONGITUD'),
    ('e_sin_canal',   'llamada_no_despachada_sin_canal', 'Llamada sin registro en CADPEDIDOS_CANALES: no se envio patrulla (en su mayoria TIPO_PEDIDO 01). No es perdida para el mapa.', 'SECAD: CADPEDIDOS_CANALES'),
    ('e_tipo_nulo',   'sin_tipo_de_incidente', 'El tipo o descripcion del incidente viene vacio.', 'SECAD: CODI_PEDIDO; PremierOne: descripcion_tipo; Hexagon: desc_incidente'),
    ('e_tipo_sin_catalogo', 'tipo_de_incidente_no_existe_en_catalogo', 'El codigo del incidente no esta en la tabla de casos.', 'SECAD: CODI_PEDIDO sin fila en CADCASOS'),
    ('e_sin_direccion',     'sin_direccion', 'Direccion vacia.', 'SECAD: DIRE_CASO; PremierOne: direccion; Hexagon: direccion'),
    ('e_sin_cai',           'sin_cai', 'CAI vacio.', 'PremierOne: cai'),
    ('e_sin_cuadrante',     'sin_cuadrante', 'Cuadrante vacio (o "NO HA..." en RNMC).',
     'Hexagon: cuadrante; RNMC: CUADRANTE2 / CUADRANTE; Antecedentes: ID_CUADRANTE; MTRS: CUADRANTE; IRIS: NRO_CUADRANTE; SIEDCO: PERTE_DEPENDENCIA; SIRAP: CUADRANTE_ID'),
    ('e_sin_sigla',         'sin_sigla_de_unidad', 'Sigla de la unidad vacia.', 'RNMC mediaciones: SIGLA_FISICA; GESCOV/IRIS: SIGLA_UNIDAD; MTRS: SIGLA; SIMEP: SIGLA_FISICA; SIEDCO: SIGLA_UNDEPERTENECE'),
    ('e_sin_unidad',        'sin_unidad', 'Unidad vacia o 0. En Antecedentes es la unidad que hizo la consulta.', 'Antecedentes: UNDELABORANDO; GESCOV/SIMEP: ID_UNIDAD; SIRAP: UNIDAD'),
    ('e_sin_comportamiento', 'sin_articulo_de_comportamiento', 'Articulo del Codigo de Policia vacio o 0.', 'RNMC: CNP_ART'),
    ('e_sin_infractor',     'sin_infractor_registrado', 'El hecho no tiene persona tipo 350 (infractor).', 'RNMC: CNP_PERSONAS.ID_TIPO_PERSONA = 350'),
    ('e_no_vigente',        'registro_no_vigente', 'Registro marcado como no vigente (anulado).', 'RNMC: VIGENTE <> 1'),
    ('e_sin_identificacion', 'sin_documento_consultado', 'Documento de la persona consultada vacio.', 'Antecedentes personas: IDENTIFICACION'),
    ('e_sin_placa',         'sin_placa_consultada', 'Placa consultada vacia.', 'Antecedentes vehiculos: PLACA'),
    ('e_sin_imei',          'sin_imei_consultado', 'IMEI consultado vacio.', 'Antecedentes IMEI: IMEI'),
    ('e_resultado_nulo',    'sin_resultado_de_antecedentes', 'No dice si la persona/vehiculo/IMEI tiene antecedentes.', 'Antecedentes: TIENE_ANTECEDENTES'),
    ('e_cumplimiento_nulo', 'sin_estado_de_cumplimiento', 'Plan sin estado de cumplimiento (en el mes en curso es normal).', 'GESCOV: ESTADO_CUMPLIMIENTO'),
    ('e_id_parcial',        'id_de_plan_o_punto_incompleto', 'Falta PLAN_ID o PUNTOS_ID.', 'GESCOV: PLAN_ID / PUNTOS_ID'),
    ('e_sin_delito',        'sin_delito', 'Delito vacio.', 'MTRS: DELITO; IRIS: DELITO_PRINCIPAL'),
    ('e_sin_conducta',      'sin_tipo_de_conducta', 'Tipo de conducta vacio.', 'SIMEP: TIPO_CONDUCTA'),
    ('e_sin_tematica',      'sin_tematica', 'Tematica vacia.', 'SIEDCO: TEMATICA'),
    ('e_sin_hora',          'sin_hora', 'Hora del hecho vacia.', 'SIEDCO: HORA_24'),
    ('e_sin_descripcion',   'sin_descripcion', 'Descripcion vacia.', 'SIRAP: DESCRIPCION'),
]
CABECERA = ['fuente', 'tabla_en_el_sistema_del_cliente', 'validador_de_capa', 'sigla_o_unidad']


def convertir(entrada, salida, diccionario):
    filas = list(csv.DictReader(open(entrada, encoding='utf-8-sig')))
    presentes = [c for c in COLUMNAS if c[0] == 'pct' or c[0] in filas[0]]
    with open(salida, 'w', newline='', encoding='utf-8-sig') as fh:
        w = csv.writer(fh)
        w.writerow(CABECERA + [c[1] for c in presentes])
        for r in filas:
            nombre, tabla = FUENTES.get(r['fuente'], (r['fuente'], ''))
            n = int(r['n'] or 0)
            r['pct'] = f"{100 * int(r['critico'] or 0) / n:.1f}" if n else ''
            w.writerow([nombre, tabla, r['validador'], r['grupo']] + [r.get(c[0], '') for c in presentes])
    with open(diccionario, 'w', newline='', encoding='utf-8-sig') as fh:
        w = csv.writer(fh)
        w.writerow(['columna', 'que_cuenta', 'campo_revisado_en_cada_fuente'])
        w.writerow(['fuente', 'Sistema del cliente de donde vienen los datos.', ''])
        w.writerow(['tabla_en_el_sistema_del_cliente', 'Base y tabla de origen.', ''])
        w.writerow(['validador_de_capa', 'Validador del mapa que usa la fuente.', ''])
        w.writerow(['sigla_o_unidad', 'Sigla de la metropolitana/departamento o unidad. TOTAL = sin desglose.', ''])
        for _, nuevo, que, campo in presentes:
            w.writerow([nuevo, que, campo])
    print(f'{len(filas)} filas, {len(presentes)} columnas de conteo')


if __name__ == '__main__':
    convertir(*sys.argv[1:4])
