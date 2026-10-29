"""Disparos esperados y CAD ausentes: clientes simulados, sin AWS ni boto3."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


BASE = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 29, 18, tzinfo=timezone.utc)
SUNDAY = datetime(2026, 10, 25, 1, tzinfo=timezone.utc)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


trigger = load('disparo_ciclos', BASE/'2. Programación y lanzamiento/monitor_disparo_semanal.py')
models = load('modelos_ciclos', BASE/'6. Modelos utilizados y componentes/mapear_modelos_dependencias.py')


class SDKError(Exception):
    response = {'Error': {'Code': 'AccessDeniedException', 'Message': 'NO_EXPORTAR'}}


class FakeMetrics:
    def __init__(self, values=None):
        self.values, self.calls = values or {}, []

    def get_metric_statistics(self, **args):
        self.calls.append(args)
        value = self.values.get((args['MetricName'], args['StartTime']), self.values.get(args['MetricName']))
        if isinstance(value, Exception):
            raise value
        return {'Datapoints': [] if value is None else [{'Sum': value, 'Timestamp': args['StartTime']}]}


class ExpectedTriggersTests(unittest.TestCase):
    def inspect(self, cw, now=NOW, start=None):
        reader, controls = trigger.Reader((SDKError,)), []
        args = SimpleNamespace(ventana_disparo_minutos=60, espera_metricas_minutos=15)
        rows = trigger.inspect_expected_triggers(reader, cw, 'rule',
            {'ScheduleExpression': 'cron(0 1 ? * SUN *)'}, start or now-timedelta(days=8), now, args, controls)
        return rows, controls, reader

    def test_sunday_utc_is_saturday_evening_in_bogota(self):
        rows, _, _ = self.inspect(FakeMetrics({'Invocations': 1}))
        self.assertEqual(rows[0]['esperado_bogota'], '2026-10-24T20:00:00-05:00')
        self.assertEqual(rows[0]['control'], 'INFO')

    def test_missing_datapoints_warn_without_fabricating_zero(self):
        rows, _, _ = self.inspect(FakeMetrics())
        self.assertEqual(rows[0]['control'], 'AVISO')
        self.assertIn('SIN EVIDENCIA', rows[0]['detalle'])
        self.assertIsNone(rows[0]['metricas']['Invocations']['valor'])

    def test_each_week_has_independent_evidence(self):
        first = SUNDAY - timedelta(days=7)
        cw = FakeMetrics({('Invocations', SUNDAY): 1})
        rows, _, _ = self.inspect(cw, start=first)
        self.assertEqual([r['control'] for r in rows], ['AVISO', 'INFO'])

    def test_window_and_metric_wait_do_not_report_premature_absence(self):
        for offset in (0, 5, 65):
            with self.subTest(minutes=offset):
                rows, _, _ = self.inspect(FakeMetrics(), now=SUNDAY+timedelta(minutes=offset))
                self.assertEqual(rows[-1]['control'], 'PENDIENTE')

    def test_failed_invocations_remain_failure_with_partial_other_metrics(self):
        rows, controls, reader = self.inspect(FakeMetrics({'Invocations': SDKError(), 'FailedInvocations': 1}))
        self.assertEqual(rows[0]['control'], 'FALLA')
        self.assertEqual(trigger.row_status(controls, reader, 'rule'), 'FALLA')
        self.assertTrue(reader.issues)

    def test_denied_metrics_are_incomplete_not_missing_activity(self):
        rows, _, reader = self.inspect(FakeMetrics({'Invocations': SDKError()}))
        self.assertEqual(rows[0]['control'], 'INCOMPLETO')
        self.assertNotIn('NO_EXPORTAR', json.dumps(reader.issues))

    def test_invalid_metric_values_never_indicate_healthy_activity(self):
        for value in (-1, float('nan'), float('inf'), '1', True):
            with self.subTest(value=value):
                rows, _, _ = self.inspect(FakeMetrics({'Invocations': value}))
                self.assertEqual(rows[0]['control'], 'INCOMPLETO')

    def test_weekly_parser_handles_named_numeric_days_and_rejects_other_schedules(self):
        for expr in ('cron(0 1 ? * SUN *)', 'cron(0 1 ? * 1 *)'):
            self.assertEqual(trigger.weekly_slots(expr, SUNDAY, NOW), [SUNDAY])
        for expr in ('rate(7 days)', 'cron(0 1 1 * ? *)', 'cron(99 25 ? * SUN *)', 'cron(0 1 ? * FOO *)'):
            with self.subTest(expr=expr), self.assertRaises(ValueError):
                trigger.weekly_slots(expr, SUNDAY, NOW)

    def test_csv_retains_cycle_checks_in_one_report(self):
        rows, controls, _ = self.inspect(FakeMetrics())
        with tempfile.TemporaryDirectory() as folder:
            path = trigger.write_report(Path(folder), [], controls, [], {'disparos': rows})
            with path.open(encoding='utf-8-sig') as handle:
                data = list(csv.DictReader(handle))
            self.assertEqual(len(list(Path(folder).glob('*.csv'))), 1)
            self.assertTrue(any(r['tipo_registro'] == 'CONTROL' and r['estado'] == 'AVISO' for r in data))


class ExpectedCADTests(unittest.TestCase):
    def make_session(self, cads=None, payload=None, sha=None, transformer=None, deny=False):
        body = io.BytesIO(json.dumps(cads if cads is not None else ['MEBOG', 'DESAB', 'DESAB']).encode())
        def describe(**args):
            if deny:
                raise SDKError()
            return {'ScheduleExpression': 'cron(0 1 ? * SUN *)', 'State': 'ENABLED'}
        target = dict(Arn='arn:aws:lambda:us-east-1:926162397524:function:'+models.LAUNCH_FUNCTION)
        if payload is not None:
            target['Input'] = json.dumps(payload)
        if transformer:
            target['InputTransformer'] = transformer
        events = SimpleNamespace(describe_rule=describe, get_paginator=lambda operation:
            SimpleNamespace(paginate=lambda **args: iter([{'Targets': [target]}])))
        lambdas = SimpleNamespace(get_function_configuration=lambda **args:
            dict(CodeSha256=sha or models.LAUNCH_CODE_SHA256, LastModified='2026-10-27T13:07:18.000+0000'))
        s3 = SimpleNamespace(get_object=lambda **args:
            dict(Body=body, LastModified=SUNDAY+timedelta(hours=1), VersionId='v1', ETag='etag'),
            get_paginator=lambda operation: SimpleNamespace(paginate=lambda **args: iter([{}])))
        session = SimpleNamespace(client=lambda service, **kwargs: {'events': events, 'lambda': lambdas, 's3': s3}[service])
        return session, body

    def expectation(self, **kwargs):
        session, body = self.make_session(**kwargs)
        args = SimpleNamespace(region='us-east-1', cuenta='926162397524')
        result = models.load_cycle_expectations(session, None, args, NOW, (SDKError,))
        return result, body

    def test_uses_deduplicated_catalog_and_both_modes_and_closes_body(self):
        result, body = self.expectation()
        self.assertEqual(result['cads'], ['DESAB', 'MEBOG'])
        self.assertEqual(result['combinaciones_configuradas'], 4)
        self.assertEqual(result['duplicados_catalogo'], 1)
        self.assertTrue(body.closed)
        self.assertEqual(result['desde_utc'], SUNDAY.isoformat())

    def test_configuration_modified_after_cycle_is_not_a_historical_contract(self):
        result, _ = self.expectation()
        self.assertEqual(result['cambios_posteriores_al_disparo'], ['codigo', 'catalogo'])
        self.assertFalse(result['configuracion_historica_confirmada'])

    def test_explicit_target_subset_and_mode_override_catalog(self):
        result, body = self.expectation(payload={'cads': ['MEPAS'], 'time_frame': 'TURNO'})
        self.assertEqual((result['cads'], result['modalidades']), (['MEPAS'], ['TURNO']))
        self.assertFalse(body.closed)  # No se consulta S3 al existir cads en el target.

    def test_changed_generator_or_transformed_input_do_not_guess_contract(self):
        for params in ({'sha': 'otro'}, {'transformer': {'InputTemplate': '{}'}},
                       {'cads': ['MEBOG', '']}, {'payload': {'time_frame': 'OTRO'}}):
            with self.subTest(params=params):
                result, _ = self.expectation(**params)
                self.assertEqual(result['lectura'], 'NO EVALUADO')
                self.assertNotIn('cads', result)

    def test_access_denied_is_preserved_without_exporting_sdk_message(self):
        result, _ = self.expectation(deny=True)
        self.assertEqual(result['lectura'], 'ERROR')
        self.assertNotIn('NO_EXPORTAR', json.dumps(result))

    def row(self, mode='HORA', start=None, **extra):
        return dict(sigla='DESAB', modalidad=mode, region='us-east-1', tarea='a',
                    inicio_ecs=start or '', lectura_logs='LEIDA', **extra)

    def test_missing_cad_and_missing_mode_are_independent(self):
        expected, _ = self.expectation()
        rows = models.evaluate_cycle_coverage([self.row()], expected)
        present = [r for r in rows if r['tareas_con_actividad']]
        self.assertEqual([(r['sigla'], r['modalidad']) for r in present], [('DESAB', 'HORA')])
        self.assertEqual(sum(r['control'] == 'AVISO' for r in rows), 3)
        self.assertTrue(all('expectativa historica' in r['detalle'] for r in rows))

    def test_old_running_task_does_not_confirm_new_launch_and_streams_deduplicate(self):
        expected, _ = self.expectation()
        row = self.row(start=(SUNDAY-timedelta(days=7)).isoformat())
        result = models.evaluate_cycle_coverage([row, row], expected)[0]
        self.assertEqual((result['tareas_con_actividad'], result['arranques_ecs_confirmados']), (1, 0))

    def test_partial_query_or_unidentified_stream_cannot_confirm_absence(self):
        expected, _ = self.expectation()
        for rows, incomplete in (([], True), ([dict(sigla='SIN DATO', modalidad='SIN DATO')], False)):
            result = models.evaluate_cycle_coverage(rows, expected, incomplete)
            self.assertTrue(all(r['control'] == 'INCOMPLETO' for r in result))

    def test_exports_cycle_detail_in_same_csv(self):
        expected, _ = self.expectation()
        coverage = models.evaluate_cycle_coverage([], expected)
        with tempfile.TemporaryDirectory() as folder:
            path = models.write_history_report(Path(folder), [], [], coverage)
            with path.open(encoding='utf-8-sig') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 4)
            self.assertEqual({r['tipo_registro'] for r in rows}, {'COBERTURA_CICLO'})

    def test_unreadable_contract_stops_before_starting_a_logs_scan(self):
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(verificar_ciclo=True, cluster='cluster', dias_logs=7, salida=folder)
            with patch.object(models, 'load_cycle_expectations', return_value={'lectura': 'ERROR', 'detalle': 'AccessDenied'}), \
                    patch.object(models, 'upload_csv_reports', return_value={}), \
                    patch.object(models, 'collect_history_from_session', side_effect=AssertionError('No escanear')), \
                    redirect_stdout(io.StringIO()) as output:
                code = models.inspect_log_history(None, None, args, (SDKError,))
            self.assertEqual(code, 3)
            self.assertIn('NO EVALUADA', output.getvalue())
            self.assertNotIn('0/0 con actividad', output.getvalue())

    def test_query_uses_cycle_boundary_and_resume_keeps_original_window(self):
        calls = []
        logs = SimpleNamespace(start_query=lambda **kw: calls.append(kw) or {'queryId': 'cycle-query'},
            get_query_results=lambda **kw: {'status': 'Complete', 'results': []})
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            args = SimpleNamespace(cuenta='926162397524', region='us-east-1', cluster='cluster',
                familia='predictor', dias_logs=30, salida=folder, ciclo_desde_utc=SUNDAY.isoformat())
            _, first = models.history_query_result(logs, 'group', {'prefix/'}, args, NOW, time.monotonic()+3, (SDKError,))
            _, second = models.history_query_result(logs, 'group', {'prefix/'}, args,
                NOW+timedelta(hours=1), time.monotonic()+3, (SDKError,))
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['startTime'], int(SUNDAY.timestamp()))
            self.assertEqual(second['hasta_utc'], first['hasta_utc'])

    def test_explicit_exception_is_grouped_without_exporting_its_message(self):
        stream = 'pon-prod-xcrime-prediccion/pon-prod-prediccion/'+'a'*32
        result = [[{'field': k, 'value': str(v)} for k, v in {
            '@logStream': stream, 'pn_exception': 'NameError', 'eventos': 125,
            'indicios_error': 125, 'cad_header': 'DESAB', 'cad_header_distintos': 1}.items()]]
        items, issues = models.decode_history_aggregates(result, 'group', 'us-east-1', {stream[:-32]})
        self.assertFalse(issues)
        self.assertEqual(items[0]['log']['errores_aplicacion'], {'NameError': 125})

    def test_only_missing_historical_ecs_is_distinguished_from_application_error(self):
        log = models.empty_history_log()
        log['siglas'].add('DESAB')
        log['modalidades'].add('HORA')
        candidate = dict(tarea='a'*32, region='us-east-1', grupo='group', stream='stream')
        row = models.history_task_row(candidate, {}, {}, log)
        summary, = models.summarize_history_siglas([row])
        self.assertEqual(summary['motivos_aviso'], 'SIN_METADATA_ECS_HISTORICA')
        log['errores_aplicacion']['NameError'] = 10
        log['indicios_error'] = 30
        row = models.history_task_row(candidate, {}, {}, log)
        summary, = models.summarize_history_siglas([row])
        self.assertIn('ERROR_APLICACION_REPORTADO', summary['motivos_aviso'])
        self.assertEqual(json.loads(summary['errores_aplicacion']), {'NameError': 10})


if __name__ == '__main__':
    unittest.main()
