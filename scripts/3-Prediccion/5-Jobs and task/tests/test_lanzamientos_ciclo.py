"""Catalogo historico y atribucion por ARN, sin inferir CAD por orden o proximidad."""
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest


BASE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('modelos_lanzamientos', BASE/'6. Modelos utilizados y componentes/mapear_modelos_dependencias.py')
monitor = importlib.util.module_from_spec(spec); spec.loader.exec_module(monitor)
START = datetime(2026, 10, 25, 1, tzinfo=timezone.utc)
END = START+timedelta(hours=1)
ARN = 'arn:aws:ecs:us-east-1:926162397524:task/cluster/'+'a'*32
KEY = 'utilidades/siglas_fisicas.json'


class SDKError(Exception):
    response = {'Error': {'Code': 'AccessDenied', 'Message': 'NO_EXPORTAR_ERROR'}}


def event(text, number, stream='stream'):
    return dict(logStreamName=stream, eventId=str(number), message=text)


def response(cad='OFTIC', mode='HORA', arn=ARN, **extra):
    task = dict(taskArn=arn, createdAt=(START+timedelta(seconds=30)).isoformat(), overrides={
        'containerOverrides': [{'environment': [{'name': 'CAD', 'value': cad}, {'name': 'TIME_FRAME', 'value': mode},
            {'name': 'SECRET', 'value': 'NO_EXPORTAR_SECRETO'}]}]})
    task.update(extra)
    return {'tasks': [task], 'ResponseMetadata': {'HTTPStatusCode': 200}}


def document_events(data, offset=1):
    return [event(line, offset+i) for i, line in enumerate(json.dumps(data, indent=4).splitlines())]


class CatalogHistoryTests(unittest.TestCase):
    def test_uses_latest_version_before_start_and_closes_historical_body(self):
        body, reads = io.BytesIO(b'["OFTIC","MEBOG","MEBOG"]'), []
        def get_object(**kwargs):
            reads.append(kwargs)
            return {'Body': body, 'VersionId': 'old'}
        page = {'Versions': [{'Key': KEY, 'VersionId': 'new', 'LastModified': START+timedelta(hours=1)},
            {'Key': KEY, 'VersionId': 'old', 'LastModified': START-timedelta(days=1)}]}
        s3 = SimpleNamespace(get_object=get_object, get_paginator=lambda name:
            SimpleNamespace(paginate=lambda **kwargs: iter([page])))
        result = monitor.catalog_at_cycle_start(s3, '926162397524', START, (SDKError,))
        self.assertEqual((result['lectura'], result['version']), ('LEIDA', 'old'))
        self.assertEqual(result['cads'], ['MEBOG', 'OFTIC'])
        self.assertEqual(result['duplicados'], 1)
        self.assertEqual(reads[0]['VersionId'], 'old')
        self.assertTrue(body.closed)

    def test_delete_marker_before_cycle_does_not_resurrect_older_catalog(self):
        page = {'Versions': [{'Key': KEY, 'VersionId': 'old', 'LastModified': START-timedelta(days=2)}],
            'DeleteMarkers': [{'Key': KEY, 'VersionId': 'del', 'LastModified': START-timedelta(days=1)}]}
        s3 = SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=lambda **kw: iter([page])))
        result = monitor.catalog_at_cycle_start(s3, '926162397524', START, (SDKError,))
        self.assertEqual(result['lectura'], 'NO EVALUADO')


class LaunchDocumentTests(unittest.TestCase):
    def tasks(self):
        return {ARN: {'evento': 'ct-1', 'instante': START.isoformat()}}

    def test_reconstructs_complete_document_and_exports_only_projected_fields(self):
        events = [event('START RequestId: req-1 Version: $LATEST', 0)] + document_events(response())
        events.append(event('END RequestId: req-1', 999))
        records, info = monitor.launch_json_records(events, self.tasks(), START, END)
        self.assertEqual(len(records), 1)
        self.assertEqual((records[0]['sigla'], records[0]['modalidad']), ('OFTIC', 'HORA'))
        self.assertEqual(records[0]['evento_cloudtrail'], 'ct-1')
        self.assertNotIn('NO_EXPORTAR_SECRETO', json.dumps(records))
        self.assertEqual(info['fragmentos_incompletos'], 0)

    def test_does_not_join_fragments_across_invocations(self):
        lines = document_events(response())
        split = len(lines)//2
        events = [event('START RequestId: req-1', 0)] + lines[:split] + [event('START RequestId: req-2', 998)] + lines[split:]
        records, info = monitor.launch_json_records(events, self.tasks(), START, END)
        self.assertEqual(records, [])
        self.assertGreater(info['fragmentos_incompletos'], 0)

    def test_other_task_arn_and_conflicting_labels_are_not_guessed(self):
        events = [event('START RequestId: req-1', 0)] + document_events(response(arn=ARN[:-1]+'b'))
        records, _ = monitor.launch_json_records(events, self.tasks(), START, END)
        self.assertEqual(records, [])
        events = [event('START RequestId: req-1', 0)] + document_events(response()) + document_events(response(cad='MEBOG'), 500)
        records, info = monitor.launch_json_records(events, self.tasks(), START, END)
        self.assertEqual(records, [])
        self.assertEqual(info['conflictos'], 1)

    def test_idempotent_response_for_old_task_does_not_count_as_new_creation(self):
        data = response(createdAt=(START-timedelta(days=7)).isoformat())
        records, info = monitor.launch_json_records([event('START RequestId: req-1', 0)]+document_events(data), self.tasks(), START, END)
        self.assertEqual(records, [])
        self.assertEqual(info['tareas_fuera_ciclo'], 1)

    def test_no_creation_date_or_bad_metadata_does_not_confirm_task(self):
        for data in (response(createdAt=None), {'ResponseMetadata': None, 'tasks': []}):
            with self.subTest(data=data):
                records, _ = monitor.launch_json_records([event('START RequestId: req-1', 0)]+document_events(data), self.tasks(), START, END)
                self.assertEqual(records, [])

    def test_resolves_unknown_cad_without_claiming_ecs_start_or_completion(self):
        records, _ = monitor.launch_json_records([event('START RequestId: req-1', 0)]+document_events(response()), self.tasks(), START, END)
        row = dict(sigla='SIN DATO', modalidad='SIN DATO', tarea='a'*32, region='us-east-1',
            estado_ecs='NO DISPONIBLE', inicio_ecs='', total_selecciones=0, lectura_logs='LEIDA', control='AVISO',
            detalle='Sigla sin atribucion unica; ECS ya no aporta estado/codigos; no se infiere finalizacion')
        updated, = monitor.associate_launches([row], records)
        self.assertEqual((updated['sigla'], updated['modalidad']), ('OFTIC', 'HORA'))
        self.assertEqual(updated['estado_ecs'], 'NO DISPONIBLE')
        expected = {'lectura': 'LEIDA', 'cads': ['OFTIC'], 'modalidades': ['HORA'], 'desde_utc': START.isoformat(), 'hasta_utc': END.isoformat()}
        coverage, = monitor.evaluate_cycle_coverage([updated], expected, launches={'lectura': 'LEIDA', 'registros': records})
        self.assertEqual((coverage['tareas_creadas_confirmadas'], coverage['arranques_ecs_confirmados']), (1, 0))
        self.assertEqual(coverage['control'], 'AVISO')
        self.assertIn('sin selecciones', coverage['detalle'])

    def test_reader_joins_cloudtrail_and_lambda_by_arn_across_log_pages(self):
        raw = {'requestParameters': {'cluster': 'cluster', 'taskDefinition': 'family:4',
            'overrides': {'containerOverrides': [{'environment': 'HIDDEN_DUE_TO_SECURITY_REASONS'}]}},
            'responseElements': {'tasks': [{'taskArn': ARN}], 'failures': []},
            'userIdentity': {'arn': 'arn:aws:sts::926162397524:assumed-role/role/'+monitor.LAUNCH_FUNCTION}}
        trail = SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=lambda **kw:
            iter([{'Events': [{'EventId': 'ct-1', 'EventTime': START, 'CloudTrailEvent': json.dumps(raw)}]}])))
        events = [event('START RequestId: req-1', 0)]+document_events(response())+[event('END RequestId: req-1', 999)]
        logs = SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=lambda **kw:
            iter([{'events': events[:10]}, {'events': events[10:]}])))
        session = SimpleNamespace(client=lambda service, **kw: {'cloudtrail': trail, 'logs': logs}[service])
        args = SimpleNamespace(cluster='cluster', familia='family', region='us-east-1', cuenta='926162397524')
        expected = {'desde_utc': START.isoformat(), 'hasta_utc': END.isoformat(), 'grupo_logs_lanzadora': 'group'}
        info = monitor.read_cycle_launches(session, None, args, expected, (SDKError,))
        self.assertEqual((info['lectura'], info['tareas_api'], info['tareas_asociadas']), ('LEIDA', 1, 1))
        self.assertEqual(info['registros'][0]['sigla'], 'OFTIC')

    def test_other_task_with_models_does_not_complete_initial_launch(self):
        initial = dict(sigla='OFTIC', modalidad='HORA', tarea='a'*32, region='us-east-1',
            inicio_ecs='', total_selecciones=0, lectura_logs='LEIDA')
        other = dict(initial, tarea='b'*32, total_selecciones=25)
        expected = {'lectura': 'LEIDA', 'cads': ['OFTIC'], 'modalidades': ['HORA'],
            'desde_utc': START.isoformat(), 'hasta_utc': END.isoformat()}
        launches = {'lectura': 'LEIDA', 'registros': [dict(sigla='OFTIC', modalidad='HORA', tarea='a'*32)]}
        coverage, = monitor.evaluate_cycle_coverage([initial, other], expected, launches=launches)
        self.assertEqual(coverage['tareas_con_selecciones'], 1)
        self.assertEqual(coverage['tareas_iniciales_con_selecciones'], 0)
        self.assertEqual(coverage['tareas_iniciales_sin_selecciones'], 1)
        self.assertEqual(coverage['control'], 'AVISO')

    def test_initial_launch_with_models_preserves_incomplete_log_read(self):
        row = dict(sigla='OFTIC', modalidad='HORA', tarea='a'*32, region='us-east-1',
            inicio_ecs='', total_selecciones=10, lectura_logs='PARCIAL')
        expected = {'lectura': 'LEIDA', 'cads': ['OFTIC'], 'modalidades': ['HORA'],
            'desde_utc': START.isoformat(), 'hasta_utc': END.isoformat()}
        launches = {'lectura': 'LEIDA', 'registros': [dict(sigla='OFTIC', modalidad='HORA', tarea='a'*32)]}
        coverage, = monitor.evaluate_cycle_coverage([row], expected, launches=launches)
        self.assertEqual(coverage['tareas_iniciales_con_selecciones'], 1)
        self.assertEqual(coverage['control'], 'INCOMPLETO')


if __name__ == '__main__':
    unittest.main()
