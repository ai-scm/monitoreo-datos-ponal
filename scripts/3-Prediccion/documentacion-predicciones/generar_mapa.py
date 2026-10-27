"""Reconstrucción documental local. Python 3 + librsvg/cairo del sistema, sin AWS.

Ejecutar: python3 generar_mapa.py
Los documentos son evidencia; no se ejecutan instrucciones contenidas en ellos.
"""
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile
from html import escape
import csv
import ctypes as C
import ctypes.util
import posixpath
import re

OUT = Path(__file__).resolve().parent
SOURCE = OUT.parent / 'Inventario_AWS_PONAL_XCrimeApp.xlsx'
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


def workbook():
    result = {}
    with ZipFile(SOURCE) as z:
        strings = [''.join(t.itertext()) for t in ET.fromstring(z.read('xl/sharedStrings.xml')).findall('s:si', NS)]
        rels = {r.attrib['Id']: r.attrib['Target'] for r in ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))}
        for sheet in ET.fromstring(z.read('xl/workbook.xml')).findall('s:sheets/s:sheet', NS):
            target = rels[sheet.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']]
            target = target.lstrip('/') if target.startswith('/') else posixpath.normpath('xl/' + target)
            rows = []
            for row in ET.fromstring(z.read(target)).findall('s:sheetData/s:row', NS):
                data = {}
                for cell in row.findall('s:c', NS):
                    value = cell.find('s:v', NS)
                    value = value.text if value is not None else ''
                    if cell.attrib.get('t') == 's':
                        value = strings[int(value)]
                    elif cell.attrib.get('t') == 'inlineStr':
                        value = ''.join(cell.find('s:is', NS).itertext())
                    if value:
                        data[re.sub(r'\d', '', cell.attrib['r'])] = value
                if 'A' in data:
                    rows.append((row.attrib['r'], data))
            result[sheet.attrib['name']] = rows
    return result


def write_csv(name, fields, rows):
    with (OUT / name).open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def inventory():
    selected = {
        'Lambda': {
            'pon-prod-xcrime-predatacleaning-launch-spot-fargate': 'Prelimpieza: destino y disparador pendientes',
            'pon-prod-xcrime-datacleaning-inicializador-spotFargate': 'Limpieza: destino y salida pendientes',
            'pon-prod-xcrime-prediccion-launch-fargate': 'Lanzamiento automático candidato; no sustituye la llamada directa de SER documentada',
            'validar-predicciones-prod': 'Validador con logs en inventario; código local de referencia lee S3 y genera reportes',
            'pon-prod-xcrime-update-estructura-nal': 'Datos maestros: relación exacta con UnidadesBase pendiente',
            'pon-prod-xcrime-update-geometrias-nal': 'Geometrías: relación exacta con UnidadesBase pendiente',
            'pon-prod-xcrime-copy-geometrias-nal': 'Copia de geometrías: destinos pendientes',
            'pon_prod_xcrime_informes_anticipaciones': 'Informes: entradas y salidas pendientes',
            'pon_prod_xcrime_copy_predicciones': 'Copia histórica: inventario la clasifica inactiva',
        },
        'Glue Jobs': {
            'pon-prod-xcrime-ejecucion-fusion-demanda-job': 'Fusión de demanda: relación con resultados pendiente',
            'pon-prod-xcrime-ejecucion-optimizaciones-fusion-job': 'Optimización/fusión: entradas y destino pendientes',
            'pon-prod-xcrime-copia-optimizaciones-fusion-job': 'Copia de optimizaciones: entradas y destino pendientes',
            'pon-prod-xcrime-prediccion-demanda-update-delitos-unidad-job': 'Configuración delito/unidad: consumidor pendiente',
            'pon-prod-xcrime-copy-predicciones': 'Copia: sin fecha ni resultado de corrida en la fila; estado inconsistente',
            'pon-prod-raw-siedco_etl': 'Ingesta candidata: no está probada su conexión a mdp_limpio',
            'pon-prod-homogenize-raw-siedco': 'Homogeneización candidata: conexión a mdp_limpio pendiente',
            'pon-prod-premier-one-transform-data-mdp': 'Transformación candidata: conexión a mdp_limpio pendiente',
            'pon-prod-hexagon-transform-data-mdp': 'Transformación candidata: conexión a mdp_limpio pendiente',
        },
        'Glue Crawlers': {
            'pon-prod-xcrime-datacleaning-glue-crawler': 'Sin evidencia de corrida; no prueba refresco de mdp_limpio',
            'pon-prod-siedco-datacleaning': 'Sin evidencia de corrida; conexión pendiente',
            'pon-prod-mdp2023-crawler': 'Sin evidencia de corrida; conexión pendiente',
            'informe_anticipaciones_prediccion': 'Sin evidencia de corrida; fuera del núcleo confirmado',
        },
        'ECS': {
            'pred-demanda-ser-ECSCluster': 'SER del PDF: correspondencia fuerte por nombre; endpoints no inspeccionados',
            'pon-prod-xcrime-predicciones-cluster': 'Ejecutor candidato: mismo repositorio ECR del PDF; falta RunTask/task ARN para unión exacta',
            'pred-demanda-ECSCluster': 'Frontend candidato por nombre; ruta real pendiente',
            'pred-demanda-bff-ECSCluster': 'BFF adicional: no confundir con SER Flask; ubicación en ruta pendiente',
            'pon-prod-xcrime-datacleaning-cluster': 'Cero tareas/servicios en la captura; puede ser efímero; no concluye abandono',
            'pon-prod-xcrime-prediccion-demanda-cluster': 'Cero tareas/servicios y sin definición conocida; destino RunTask pendiente',
        },
        'EC2': {
            'autoprediccion.prod.mdp.ponal': 'Instancia running en captura; cron, código y relación con ECS no investigados',
        },
    }
    rows = []
    for sheet, wanted in selected.items():
        for number, r in workbook()[sheet]:
            if r['A'] not in wanted:
                continue
            ecs = sheet == 'ECS'
            rows.append(dict(hoja=sheet, fila=number, recurso=r['A'], papel_y_limite=wanted[r['A']],
                             estado_captura=r.get('B', ''),
                             ultima_evidencia=r.get('L' if ecs else 'D', '') if sheet != 'EC2' else '',
                             resultado=r.get('E', '') if sheet.startswith('Glue') else '',
                             log_group=r.get('M' if ecs else 'G' if sheet.startswith('Glue') else 'E', '') if sheet != 'EC2' else '',
                             definicion=r.get('G', '') if ecs else r.get('C', '') if sheet == 'EC2' else '',
                             imagen_o_script=r.get('K' if ecs else 'H', '') if ecs or sheet == 'Glue Jobs' else '',
                             corte='2026-09-22'))
    write_csv('matriz-recursos.csv', list(rows[0]), rows)
    return rows


class Diagram:
    def __init__(self, title, subtitle, height):
        self.height = height
        self.parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="{height}" viewBox="0 0 1600 {height}" role="img" aria-label="{escape(title)}">
<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 Z" fill="#325c76"/></marker></defs>
<style>text{{font-family:DejaVu Sans,sans-serif;fill:#173346}} .title{{font-size:34px;font-weight:bold}} .sub{{font-size:17px;fill:#4b6373}} .node-title{{font-size:22px;font-weight:bold}} .body{{font-size:17px}} .label{{font-size:15px;paint-order:stroke;stroke:#f7fafc;stroke-width:6px;stroke-linejoin:round}} .small{{font-size:15px;fill:#526978}}</style>
<rect width="1600" height="{height}" fill="#f7fafc"/>
<rect width="1600" height="9" fill="#087f8c"/>
<text x="40" y="60" class="small">PONAL / XCRIMEAPP · RECONSTRUCCIÓN DOCUMENTAL · 19 OCT 2026</text>
<text x="40" y="108" class="title">{escape(title)}</text>
<text x="40" y="143" class="sub">{escape(subtitle)}</text>''']

    def text(self, x, y, content, cls='body'):
        self.parts.append(f'<text x="{x}" y="{y}" class="{cls}">{escape(content)}</text>')

    def box(self, x, y, w, h, title, lines, tone='core'):
        colors = {'core': ('#e6f3f5', '#178593'), 'store': ('#edf0fb', '#7181b8'),
                  'pending': ('#fff5e6', '#c38728'), 'model': ('#eaf4eb', '#529168')}
        fill, stroke = colors[tone]
        dash = 'stroke-dasharray="7 5"' if tone == 'pending' else ''
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}" stroke-width="2" {dash}/>')
        self.text(x+19, y+32, title, 'node-title')
        for i, line in enumerate(lines):
            self.text(x+19, y+61+i*25, line)

    def edge(self, path, label='', x=0, y=0):
        self.parts.append(f'<path d="{path}" fill="none" stroke="#325c76" stroke-width="2.5" marker-end="url(#arrow)"/>')
        if label:
            self.text(x, y, label, 'label')

    def finish(self, name):
        self.text(40, self.height-49, 'Fuentes: Mapa-del-Pipeline.pdf (pp. 1–4) + inventario XLSX (corte 22 sep 2026) + referencias locales.', 'small')
        self.text(40, self.height-24, 'Línea continua: flujo documentado. Recuadro ámbar: relación pendiente. No se verificó AWS en vivo.', 'small')
        path = OUT / name
        path.write_text('\n'.join(self.parts) + '\n</svg>', encoding='utf-8')
        ET.parse(path)
        return path


def overview():
    d = Diagram('Pipeline de predicciones · servicios y datos', 'Núcleo confirmado por el PDF: timeFrame=TURNO vía POST /pred/completo. Los nombres AWS se cruzan con el inventario.', 1390)
    d.box(40, 220, 300, 150, 'Frontend', ['Solicita una predicción', 'Recibe id_proceso', 'Consulta estado y mapa'])
    d.box(440, 220, 390, 150, 'SER · Flask / Waitress', ['pon-xcrimePredictionDemanda-', 'services · puerto 8080', 'Candidato: pred-demanda-ser'])
    d.box(980, 220, 540, 150, 'Tarea efímera · ECS / Fargate', ['Sci-XCrimeGeneradorCuadrantes', 'ECR: pon-prod-xcrime-predicciones-repository', 'Unidad × día × turno; sin endpoint público'])
    d.edge('M340 260 H440', 'POST', 353, 245)
    d.edge('M440 331 H340', '200 + id', 347, 315)
    d.edge('M830 276 H980', 'ecs:RunTask', 851, 259)
    d.text(865, 307, 'asíncrono', 'small')
    d.box(40, 500, 300, 145, 'Athena · mdp_limpio', ['Histórico por CAD, unidad', 'y fechas de entrenamiento', 'Lectura SQL → CSV'], 'store')
    d.box(440, 500, 390, 145, 'DynamoDB · estado', ['pon-prod-prediccion-demanda-{cad}', 'SER: procesando → tarea: final/error', 'Índice: fecha_unidad_global_index'], 'store')
    d.box(980, 500, 540, 145, 'S3 · pon-prod-ml-iapol', ['Lee: UnidadesBase/{CAD}-UnidadesBase.geojson', 'Escribe: PrediccionMdP/ → GeoJSON + PKL', 'Prediccion_* y PuntosApoyo_*'], 'store')
    d.edge('M190 500 V420 H1060 V370', 'histórico → tarea (consulta iniciada por Fargate)', 260, 411)
    d.edge('M600 370 V500', 'put_item / query', 612, 468)
    d.edge('M1110 370 V454 H785 V500', 'update_item', 841, 447)
    d.edge('M1340 370 V500', 'GET maestros / PUT resultados', 1250, 482)
    d.edge('M980 606 H875 V350 H830', 'GET GeoJSON', 878, 665)
    d.text(40, 694, 'Consulta posterior desde frontend a SER:', 'body')
    d.text(40, 726, '/info/unidad_fecha · /geo/disponibles_unidad_fecha', 'small')
    d.text(40, 752, '/geo/one_path · combina unidades hijas si es estación', 'small')
    d.box(980, 745, 540, 130, 'API PONAL · autenticación y turnos', ['rnmc2web.policia.gov.co:5074', 'Si no responde en 30 s: turnos 91 / 89 / 90'], 'store')
    d.edge('M1520 790 H1550 V310 H1520', 'turnos', 1479, 696)
    d.text(40, 815, '200 OK confirma el lanzamiento; el resultado se consulta después.', 'node-title')
    d.text(40, 849, 'finalizado tampoco acredita calidad estadística: el resultado puede provenir de Uniform.', 'body')
    d.text(40, 927, 'RECURSOS RELACIONADOS DEL INVENTARIO · CONEXIONES POR CONFIRMAR', 'node-title')
    d.box(40, 955, 480, 190, '01 · Preparación / automatización', ['Lambdas pre/datacleaning + launch-fargate', 'EC2 autoprediccion.prod.mdp.ponal', 'Falta confirmar cron, destinos ECS y', 'productor/catálogo de mdp_limpio.'], 'pending')
    d.box(555, 955, 480, 190, '02 · Fusión / optimización / copia', ['Glue: fusion-demanda, optimizaciones,', 'copia-optimizaciones y copy-predicciones', 'Existen recursos; no se ha probado', 'la secuencia ni los prefijos de intercambio.'], 'pending')
    d.box(1070, 955, 490, 190, '03 · Validación de resultados', ['validar-predicciones-prod: logs en XLSX', 'Referencia local: S3 → reportes + SNS', 'EventBridge y SNS: wiring desplegado', 'pendiente; validadores manuales en repo.'], 'pending')
    d.text(40, 1190, 'Cruce clave: ECS!13 usa el mismo ECR que el PDF; falta la evidencia RunTask para confirmar el ejecutor exacto.', 'body')
    d.text(40, 1223, 'ECS!6 respalda SER. ECS!24 y ECS!32 son frontend/BFF candidatos; el PDF no demuestra su encadenamiento.', 'body')
    d.text(40, 1256, 'ECS!36 y ECS!38: 0 tareas en la captura no implica fallo en cargas efímeras. Véase matriz-recursos.csv.', 'body')
    return d.finish('01-pipeline-servicios.svg')


def models():
    d = Diagram('Dentro de Fargate · selección y respaldo de modelos', 'Comportamiento descrito en el PDF (pp. 2–3). Código del generador y ejecución productiva no inspeccionados.', 1190)
    d.box(40, 215, 350, 155, '1 · Preparar por unidad', ['Auth + turnos (respaldo en 30 s)', 'Athena: hasta 5 reintentos', 'Grilla 200 × 200 enmascarada'])
    d.box(470, 215, 460, 155, '2 · Filtrar / particionar', ['Día de semana × turno: 21 combinaciones', 'Ventanas móviles de 20 semanas', 'Se omiten candidatos sin datos suficientes'])
    d.box(1010, 215, 550, 155, '3 · Entrenar / evaluar', ['Counter · KDE · KDEPerDayKernelSize · SEEP', 'Hit-rate para coberturas de área de 10–100 %', 'PAI = hit-rate / cobertura de área'])
    d.edge('M390 290 H470')
    d.edge('M930 290 H1010')
    d.box(1010, 465, 550, 155, '4 · Ordenar y predecir', ['Promedio de PAI entre particiones → top 3', 'Candidato #1 → #2 → #3 si falla o se cuelga', 'Cada intento de predict(): máximo 30 s'])
    d.edge('M1280 370 V465')
    d.box(470, 465, 460, 155, '5 · Matriz de intensidad', ['Si hay predicción válida: continuar', 'Si la matriz queda toda en cero: Uniform', 'Recorte, centroides, umbral y ranking top 3'])
    d.edge('M1010 540 H930', 'éxito', 946, 524)
    d.box(40, 465, 350, 155, 'Uniform · respaldo', ['Sin particiones/candidatos válidos,', 'fallan los 3 o predicen todo en cero', 'Probabilidad igual en las celdas'], 'model')
    d.edge('M470 579 H390', 'cero', 414, 562)
    d.edge('M700 370 V416 H210 V465', 'sin entrenamiento válido', 425, 407)
    d.edge('M1280 620 V673 H210 V620', 'fallan los 3 candidatos', 775, 665)
    d.box(470, 750, 1090, 140, '6 · Publicar resultados y cerrar estado', ['GeoJSON de predicción + puntos de apoyo + matrices PKL → S3', 'DynamoDB → finalizado | error. SER consulta después el estado y los archivos.'], 'store')
    d.edge('M700 620 V750')
    d.edge('M210 620 V820 H470', 'continúa', 300, 807)
    d.text(40, 947, 'Cómo interpretar un mapa uniforme', 'node-title')
    d.text(40, 983, 'Puede ser el respaldo Uniform por escasez de datos; el color por sí solo no prueba la causa ni el éxito del pipeline.', 'body')
    d.text(40, 1016, 'El PDF cita mínimos: Counter ≥2, KDE ≥3 y SEEP ≥5 + vecinos; se aplican tras filtrar, no al total histórico.', 'body')
    d.text(40, 1049, 'El PDF señala totalCalls = len(dfToUse) / 13: ampliar fechas puede cambiar la escala. Validar en el código vigente.', 'body')
    d.text(40, 1082, 'Monitoreo sugerido: modelo elegido, motivo de respaldo, tamaño de muestra, cobertura de archivos y estado final.', 'body')
    return d.finish('02-seleccion-modelos.svg')


def render(paths):
    """Renderiza SVG vectorial con librsvg a PNG y PDF de dos páginas."""
    r = C.CDLL(ctypes.util.find_library('rsvg-2'))
    c = C.CDLL(ctypes.util.find_library('cairo'))
    g = C.CDLL(ctypes.util.find_library('gobject-2.0'))
    def bind(lib, name, args, result):
        fn = getattr(lib, name); fn.argtypes = args; fn.restype = result
        return fn
    ptr, integer, double = C.c_void_p, C.c_int, C.c_double
    load = bind(r, 'rsvg_handle_new_from_file', [C.c_char_p, ptr], ptr)
    draw = bind(r, 'rsvg_handle_render_cairo', [ptr, ptr], integer)
    unref = bind(g, 'g_object_unref', [ptr], None)
    image = bind(c, 'cairo_image_surface_create', [integer, integer, integer], ptr)
    pdf = bind(c, 'cairo_pdf_surface_create', [C.c_char_p, double, double], ptr)
    size = bind(c, 'cairo_pdf_surface_set_size', [ptr, double, double], None)
    create = bind(c, 'cairo_create', [ptr], ptr)
    destroy = bind(c, 'cairo_destroy', [ptr], None)
    finish = bind(c, 'cairo_surface_finish', [ptr], None)
    drop = bind(c, 'cairo_surface_destroy', [ptr], None)
    scale = bind(c, 'cairo_scale', [ptr, double, double], None)
    page = bind(c, 'cairo_show_page', [ptr], None)
    png = bind(c, 'cairo_surface_write_to_png', [ptr, C.c_char_p], integer)
    pdf_surface = pdf(str(OUT / 'Pipeline-Predicciones-PONAL.pdf').encode(), 800, 695)
    for path in paths:
        root = ET.parse(path).getroot()
        w, h = int(root.attrib['width']), int(root.attrib['height'])
        handle = load(str(path).encode(), None)
        assert handle, path
        surface = image(0, w, h); context = create(surface)
        assert draw(handle, context), path
        assert png(surface, str(path.with_suffix('.png')).encode()) == 0
        destroy(context); drop(surface)
        size(pdf_surface, w/2, h/2)
        context = create(pdf_surface); scale(context, .5, .5)
        assert draw(handle, context), path
        page(context); destroy(context); unref(handle)
    finish(pdf_surface); drop(pdf_surface)


if __name__ == '__main__':
    resources = inventory()
    render([overview(), models()])
    print(f'Generados 2 SVG, 2 PNG, PDF de 2 páginas y matriz de {len(resources)} recursos en {OUT}')
