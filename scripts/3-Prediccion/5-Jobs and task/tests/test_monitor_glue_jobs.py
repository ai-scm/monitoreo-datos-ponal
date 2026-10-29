"""Regresion de la unificacion Glue; stdlib, sin credenciales ni consultas AWS."""
import argparse
from contextlib import redirect_stderr, redirect_stdout
import csv
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


BASE = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 28, 17, tzinfo=timezone.utc)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


monitor = load('monitor_glue_jobs', BASE / 'Glue-Jobs/monitor_glue_jobs.py')
upstream = load('monitor_procesos_upstream', BASE / 'Procesos-Upstream/monitor_procesos_upstream.py')


class SDKError(Exception):
    def __init__(self, code='AccessDeniedException'):
        self.response = {'Error': {'Code': code, 'Message': 'NO_EXPORTAR_MENSAJE_SDK'}}


class MissingJob(SDKError):
    pass


class FakeGlue:
    exceptions = SimpleNamespace(EntityNotFoundException=MissingJob)

    def __init__(self, plans=None):
        self.plans = plans or {}
        self.calls = []

    def get_paginator(self, operation):
        if operation != 'get_job_runs':
            raise AssertionError(operation)
        return self

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        count = 0
        cap = kwargs['PaginationConfig'].get('MaxItems')
        for item in self.plans.get(kwargs['JobName'], []):
            if isinstance(item, Exception):
                raise item
            rows = item if cap is None else item[:cap - count]
            yield {'JobRuns': rows}
            count += len(rows)
            if cap is not None and count >= cap:
                return


def run(run_id='jr_1', state='SUCCEEDED', days=1, **extra):
    data = dict(Id=run_id, Attempt=0, JobRunState=state,
                StartedOn=NOW - timedelta(days=days), ExecutionTime=30)
    if state not in monitor.ACTIVE:
        data['CompletedOn'] = data['StartedOn'] + timedelta(seconds=60)
    data.update(extra)
    return data


class GlueMonitorTests(unittest.TestCase):
    def collect(self, pages, limit=1000):
        glue = FakeGlue({'custom': pages})
        result = monitor.collect_report(glue, monitor.select_jobs(names=['custom']),
                                        NOW, max_runs=limit, sdk_errors=(SDKError,))
        return result, glue

    def test_default_catalog_has_seven_unique_jobs_and_three_groups(self):
        jobs = monitor.select_jobs()
        self.assertEqual(len(jobs), 7)
        self.assertEqual(len({job['job'] for job in jobs}), 7)
        self.assertEqual({job['grupo'] for job in jobs}, set(monitor.GROUPS))

    def test_group_and_custom_selections_are_deduplicated(self):
        self.assertEqual(len(monitor.select_jobs(['entrenamiento', 'informes', 'informes'])), 4)
        jobs = monitor.select_jobs(names=['custom', 'custom', 'pon_prod_xcrime_train_models_job'])
        self.assertEqual([job['grupo'] for job in jobs], ['personalizado', 'entrenamiento'])

    def test_cli_filters_are_exclusive_and_limits_validated(self):
        self.assertTrue(monitor.parse_args(['--historial-completo']).historial_completo)
        for args in (['--grupo', 'informes', '--job', 'foo'], ['--dias', '0'],
                     ['--max-ejecuciones', '10001'], ['--job', '  '], ['--job', 'a\nb']):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                monitor.parse_args(args)
            self.assertEqual(exc.exception.code, 2)

    def test_success_report_has_bogota_times_and_distinct_durations(self):
        report, _ = self.collect([[run()]])
        row = report['resumen'][0]
        self.assertEqual((row['control'], row['cobertura']), ('INFO', 'COMPLETA'))
        self.assertEqual(row['inicio'], '2026-10-27T12:00:00-05:00')
        self.assertEqual((row['duracion_segundos'], row['execution_time_segundos']), (60, 30))
        self.assertEqual(monitor.exit_code(report), 0)

    def test_previous_failure_is_reported_even_if_latest_succeeded(self):
        report, _ = self.collect([[run(), run('jr_2', 'FAILED', days=2)]])
        row = report['resumen'][0]
        self.assertEqual((row['estado'], row['control'], row['fallas_ventana']), ('SUCCEEDED', 'FALLA', 1))
        self.assertEqual(monitor.exit_code(report), 1)

    def test_old_failure_is_visible_but_not_a_failure_in_current_window(self):
        report, _ = self.collect([[run(state='FAILED', days=30)]])
        row = report['resumen'][0]
        self.assertEqual((row['estado'], row['en_ventana'], row['control']), ('FAILED', 'NO', 'INFO'))
        self.assertEqual(row['fallas_ventana'], 0)
        self.assertEqual(report['detalle'][0]['control'], 'FALLA')

    def test_old_run_that_just_finished_is_counted(self):
        report, _ = self.collect([[run(days=30, CompletedOn=NOW - timedelta(hours=1))]])
        self.assertEqual(report['resumen'][0]['ejecuciones_ventana'], 1)

    def test_old_active_run_is_not_lost_after_old_terminal_runs(self):
        report, _ = self.collect([[run(days=30)], [run('jr_old', 'RUNNING', days=60)]], limit=None)
        self.assertEqual(json.loads(report['resumen'][0]['estados_ventana']), {'RUNNING': 1})
        active = next(row for row in report['detalle'] if row['estado'] == 'RUNNING')
        self.assertEqual(active['duracion_tipo'], 'HASTA CAPTURA')
        self.assertEqual(active['duracion_segundos'], 60 * 86400)

    def test_bounded_pagination_reads_one_extra_and_marks_partial(self):
        report, glue = self.collect([[run('a'), run('b'), run('c')]], limit=2)
        self.assertEqual(report['resumen'][0]['cobertura'], 'PARCIAL')
        self.assertEqual(report['contexto']['recursos'][0]['runs_recibidos'], 2)
        self.assertEqual(glue.calls[0]['PaginationConfig'], {'PageSize': 3, 'MaxItems': 3})
        self.assertEqual(monitor.exit_code(report), 3)

    def test_exact_limit_without_more_runs_is_complete(self):
        report, _ = self.collect([[run('a'), run('b')]], limit=2)
        self.assertEqual(report['resumen'][0]['cobertura'], 'COMPLETA')

    def test_page_size_is_at_most_200_and_full_history_has_no_cap(self):
        _, bounded = self.collect([], limit=1000)
        report, full = self.collect([[run('a')], [run('b')]], limit=None)
        self.assertEqual(bounded.calls[0]['PaginationConfig'], {'PageSize': 200, 'MaxItems': 1001})
        self.assertEqual(full.calls[0]['PaginationConfig'], {'PageSize': 200})
        self.assertEqual(report['resumen'][0]['ejecuciones_ventana'], 2)

    def test_duplicate_run_attempts_are_counted_once(self):
        report, _ = self.collect([[run(), run()], [run(Attempt=1)]])
        self.assertEqual(report['resumen'][0]['ejecuciones_ventana'], 2)
        self.assertEqual(report['contexto']['recursos'][0]['runs_distintos'], 2)

    def test_late_page_error_preserves_failure_and_other_jobs(self):
        jobs = monitor.select_jobs(names=['failed', 'complete'])
        glue = FakeGlue({'failed': [[run(state='FAILED')], SDKError()], 'complete': [[run()]]})
        report = monitor.collect_report(glue, jobs, NOW, sdk_errors=(SDKError,))
        first, second = report['resumen']
        self.assertEqual((first['control'], first['cobertura']), ('FALLA', 'PARCIAL'))
        self.assertEqual((second['control'], second['cobertura']), ('INFO', 'COMPLETA'))
        self.assertNotIn('NO_EXPORTAR_MENSAJE_SDK', json.dumps(report))
        self.assertEqual(monitor.exit_code(report), 3)

    def test_missing_job_is_different_from_denied_access(self):
        missing, _ = self.collect([MissingJob('EntityNotFoundException')])
        denied, _ = self.collect([SDKError()])
        self.assertEqual((missing['resumen'][0]['control'], missing['resumen'][0]['cobertura']),
                         ('AUSENTE', 'AUSENTE'))
        self.assertEqual(monitor.exit_code(missing), 1)
        self.assertEqual((denied['resumen'][0]['control'], denied['resumen'][0]['cobertura']),
                         ('NO EVALUADO', 'ERROR'))
        self.assertIsNone(denied['resumen'][0]['ejecuciones_ventana'])
        self.assertEqual(monitor.exit_code(denied), 3)

    def test_missing_terminal_finish_does_not_hide_failure_or_invent_duration(self):
        report, _ = self.collect([[run(state='FAILED', CompletedOn=None)]])
        row = report['resumen'][0]
        self.assertEqual((row['control'], row['cobertura']), ('FALLA', 'PARCIAL'))
        self.assertIsNone(row['duracion_segundos'])

    def test_active_run_with_finish_is_incomplete(self):
        report, _ = self.collect([[run(state='RUNNING', CompletedOn=NOW)]])
        self.assertEqual(report['resumen'][0]['cobertura'], 'PARCIAL')
        self.assertIsNone(report['resumen'][0]['duracion_segundos'])

    def test_bad_times_are_incomplete_and_never_get_a_duration(self):
        for change in ({'StartedOn': None}, {'StartedOn': NOW.replace(tzinfo=None)},
                       {'StartedOn': NOW + timedelta(days=1)},
                       {'CompletedOn': NOW - timedelta(days=2)},
                       {'CompletedOn': NOW + timedelta(seconds=1)}):
            with self.subTest(change=change):
                report, _ = self.collect([[run(**change)]])
                self.assertEqual(report['resumen'][0]['cobertura'], 'PARCIAL')
                self.assertIsNone(report['resumen'][0]['duracion_segundos'])

    def test_unknown_state_and_stopped_are_warnings(self):
        for state in ('STOPPED', 'NEW_STATE'):
            with self.subTest(state=state):
                report, _ = self.collect([[run(state=state)]])
                self.assertEqual(report['resumen'][0]['control'], 'AVISO')

    def test_no_runs_is_not_a_missed_schedule(self):
        report, _ = self.collect([])
        row = report['resumen'][0]
        self.assertEqual((row['estado'], row['control'], row['ejecuciones_ventana']),
                         ('SIN EJECUCIONES', 'INFO', 0))

    def test_csv_contains_all_sections_but_no_arguments_or_raw_error(self):
        report, _ = self.collect([[run(state='FAILED', ErrorMessage='IndexError: NO_EXPORTAR_DATO',
                                      Arguments={'password': 'NO_EXPORTAR_ARGUMENTO'})]])
        with tempfile.TemporaryDirectory() as temp:
            path = monitor.write_report(temp, report)
            text = path.read_text(encoding='utf-8-sig')
            with path.open(encoding='utf-8-sig') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(list(Path(temp).iterdir())), 1)
        self.assertEqual({r['tipo_registro'] for r in rows}, {'RESUMEN', 'DETALLE', 'CONTROL', 'CONTEXTO'})
        self.assertIn('IndexError', text)
        self.assertNotIn('NO_EXPORTAR', text)
        self.assertNotIn('Arguments', json.dumps(report['detalle']))

    def test_cli_checks_account_before_glue_and_writes_nothing_if_wrong(self):
        session = FakeSession(account='otra')
        with tempfile.TemporaryDirectory() as temp, redirect_stderr(io.StringIO()):
            code = monitor.run_cli(session, None, monitor.parse_args(['--salida', temp]), (SDKError,))
            self.assertEqual(list(Path(temp).iterdir()), [])
        self.assertEqual((code, session.clients), (3, ['sts']))

    def test_cli_reads_seven_jobs_with_one_glue_client_and_writes_one_csv(self):
        session = FakeSession()
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            code = monitor.run_cli(session, None, monitor.parse_args(['--salida', temp]), (SDKError,))
            self.assertEqual(len(list(Path(temp).rglob('*.csv'))), 1)
        self.assertEqual(code, 0)
        self.assertEqual(session.clients, ['sts', 'glue'])
        self.assertEqual(len(session.glue.calls), 7)


class FakeSession:
    def __init__(self, account='926162397524'):
        self.account = account
        self.glue = FakeGlue()
        self.clients = []

    def client(self, name, **kwargs):
        self.clients.append(name)
        if name == 'sts':
            return SimpleNamespace(get_caller_identity=lambda: {'Account': self.account})
        if name == 'glue':
            return self.glue
        raise AssertionError('Servicio no autorizado: ' + name)


class UpstreamRegressionTests(unittest.TestCase):
    def test_upstream_keeps_five_resources_and_never_queries_job_runs(self):
        class EmptyPaginator:
            def paginate(self, **kwargs):
                yield {}

        class ECS:
            def describe_clusters(self, **kwargs):
                return {'clusters': [{'status': 'ACTIVE', 'settings': [
                    {'name': 'containerInsights', 'value': 'enabled'}]}]}

            def get_paginator(self, operation):
                if operation != 'list_tasks':
                    raise AssertionError(operation)
                return EmptyPaginator()

        class CrawlerOnly:
            def get_crawler(self, **kwargs):
                return {'Crawler': {'State': 'READY', 'LastCrawl': {'Status': 'SUCCEEDED'}}}

        class DDB:
            def describe_table(self, **kwargs):
                return {'Table': {'TableStatus': 'ACTIVE'}}

        class CW:
            def get_paginator(self, operation):
                if operation != 'list_metrics':
                    raise AssertionError(operation)
                return EmptyPaginator()

            def get_metric_statistics(self, **kwargs):
                return {'Datapoints': []}

        clients = {'ecs': ECS(), 'glue': CrawlerOnly(), 'dynamodb': DDB(), 'cloudwatch': CW()}
        session = SimpleNamespace(client=lambda name, **kwargs: clients[name])
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            args = argparse.Namespace(cluster=upstream.CLUSTER, crawler=upstream.CRAWLER,
                tabla=None, dias=8, max_tareas=100, salida=temp, region='us-east-1', cuenta='926162397524')
            code = upstream.inspect(session, None, {'Account': args.cuenta}, args, (SDKError,))
            report = next(Path(temp).rglob('informe_procesos_upstream.csv'))
            with report.open(encoding='utf-8-sig') as handle:
                rows = [row for row in csv.DictReader(handle) if row['tipo_registro'] == 'RESUMEN']
        self.assertEqual(code, 0)
        self.assertEqual(len(rows), 5)
        self.assertEqual({row['tipo'] for row in rows}, {'ECS limpieza', 'Glue crawler', 'DynamoDB'})

    def test_essential_container_failure_is_still_detected(self):
        result, _ = upstream.task_result({'lastStatus': 'STOPPED',
            'containers': [{'name': 'limpieza', 'exitCode': 1}]}, {'limpieza': True})
        self.assertEqual(result, 'FALLA')


if __name__ == '__main__':
    unittest.main()
