"""Historial ECS/CloudWatch con clientes simulados; no usa AWS ni credenciales."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import sys


BASE = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 29, 18, tzinfo=timezone.utc)
GROUP = 'Anticipaciones-Automaticas-Stack-containerLogGroup14238BF8-VYMB21QxRjoB'
PREFIX = 'pon-prod-xcrime-prediccion/pon-prod-prediccion/'
TASK = 'f475c4a7' + '0'*24
SECOND_TASK = '1'*32


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


models = load('models_history', BASE/'6. Modelos utilizados y componentes/mapear_modelos_dependencias.py')
execution = load('execution_history', BASE/'5. Ejecución de las predicciones/mapear_inicio_progreso.py')


class SDKError(Exception):
    def __init__(self, code='AccessDeniedException'):
        self.response = {'Error': {'Code': code, 'Message': 'NO_EXPORTAR_ERROR_SDK'}}


def event(message, days=1):
    return dict(message=message, timestamp=int((NOW - timedelta(days=days)).timestamp()*1000))


class FakeLogs:
    def __init__(self, streams=None, events=None):
        self.stream_pages = streams if streams is not None else [[PREFIX + TASK]]
        self.event_pages = events or {}
        self.calls = []

    def get_log_events(self, **kwargs):
        self.calls.append(('get_log_events', kwargs))
        messages = []
        for page in self.event_pages.get(kwargs['logStreamName'], []):
            if isinstance(page, Exception):
                raise page
            messages.extend(page)
        limit = kwargs['limit']
        return {'events': messages[:limit] if kwargs['startFromHead'] else messages[-limit:]}

    def get_paginator(self, operation):
        if operation not in ('describe_log_streams', 'filter_log_events'):
            raise AssertionError(operation)
        def paginate(**kwargs):
            self.calls.append((operation, kwargs))
            pages = (self.stream_pages if operation == 'describe_log_streams' else
                     self.event_pages.get(kwargs['logStreamNames'][0], []))
            cap = kwargs['PaginationConfig'].get('MaxItems')
            used = 0
            for page in pages:
                if isinstance(page, Exception):
                    raise page
                selected = page if cap is None else page[:cap-used]
                if operation == 'describe_log_streams':
                    yield {'logStreams': [{'logStreamName': name} for name in selected]}
                else:
                    yield {'events': selected}
                used += len(selected)
                if cap is not None and used >= cap:
                    return
        return SimpleNamespace(paginate=paginate)


class FakeECS:
    def __init__(self, tasks=None, failure=None, definition_fail=False):
        self.tasks = tasks or {}
        self.failure = failure
        self.definition_fail = definition_fail
        self.batches = []

    def describe_task_definition(self, taskDefinition):
        if taskDefinition != 'predictor' and self.definition_fail:
            raise SDKError()
        # CAD de la revision actual no debe atribuirse a tareas desaparecidas.
        return {'taskDefinition': {'taskDefinitionArn': taskDefinition, 'containerDefinitions': [
            {'name': 'pon-prod-prediccion', 'environment': [{'name': 'CAD', 'value': 'DECAU'}],
             'logConfiguration': {'logDriver': 'awslogs', 'options': {
                 'awslogs-group': GROUP, 'awslogs-stream-prefix': 'pon-prod-xcrime-prediccion'}}}]}}

    def describe_tasks(self, cluster, tasks):
        self.batches.append(tasks)
        if self.failure:
            raise self.failure
        return {'tasks': [self.tasks[t] for t in tasks if t in self.tasks],
                'failures': [{'arn': t, 'reason': 'MISSING'} for t in tasks if t not in self.tasks]}


def arguments(**extra):
    data = dict(cluster='cluster', familia='predictor', region='us-east-1', dias_logs=8,
                grupo_logs=None, prefijo_logs=PREFIX, max_streams=1000, max_eventos=1000, salida='salida',
                cuenta='926162397524', encoding_entradas='utf-8-sig',
                separador_entradas='auto', max_mib_entradas=5)
    data.update(extra)
    return SimpleNamespace(**data)


def task(task_id=TASK, **extra):
    data = dict(taskArn='arn:aws:ecs:us-east-1:926162397524:task/cluster/' + task_id,
                taskDefinitionArn='predictor:4', lastStatus='STOPPED',
                createdAt=NOW - timedelta(days=2, minutes=1), startedAt=NOW - timedelta(days=2),
                stoppedAt=NOW - timedelta(days=1), containers=[{'name': 'pon-prod-prediccion', 'exitCode': 0}],
                overrides={'containerOverrides': [{'name': 'pon-prod-prediccion', 'environment': [
                    {'name': 'CAD', 'value': 'DESAB'}, {'name': 'TIME_FRAME', 'value': 'TURNO'}]}]})
    data.update(extra)
    return data


class HistoryTests(unittest.TestCase):
    def collect(self, pages, ecs=None, **options):
        logs = FakeLogs(events={PREFIX+TASK: pages})
        rows, context = models.collect_log_history(ecs or FakeECS(), lambda region: logs,
                                                  arguments(**options), NOW, (SDKError,))
        return rows, context, logs

    def test_recovers_expired_task_from_stream_and_logs(self):
        rows, context, logs = self.collect([[event('CAD=DESAB timeFrame=TURNO'),
            event('The best model to run the prediction is Uniform - 5 - 17'),
            event('subunidad 64779 Version de Codigo 4 End Script!')]])
        row = rows[0]
        self.assertEqual((row['sigla'], row['modalidad'], row['estado_ecs']), ('DESAB', 'TURNO', 'NO DISPONIBLE'))
        self.assertEqual((row['total_selecciones'], row['porcentaje_uniform_observado']), (1, 100))
        self.assertEqual((row['cierres_reportados'], row['subunidades_con_cierre_explicito']), (1, 1))
        self.assertEqual((row['inicio_ecs'], row['fin_ecs']), ('', ''))
        self.assertEqual(row['finalizacion'], 'CIERRES DE SUBUNIDAD EN LOGS')
        self.assertEqual(context['lecturas_incompletas'], [])
        self.assertEqual(logs.calls[0][1]['orderBy'], 'LastEventTime')
        self.assertTrue(logs.calls[0][1]['descending'])
        self.assertNotIn('logStreamNamePrefix', logs.calls[0][1])

    def test_no_current_definition_default_is_used_for_absent_task(self):
        rows, _, _ = self.collect([[event('End Script!')]])
        self.assertEqual(rows[0]['sigla'], 'SIN DATO')
        self.assertEqual(rows[0]['modalidad'], 'SIN DATO')

    def test_explicit_ecs_overrides_confirm_sigla_and_stop(self):
        rows, _, _ = self.collect([[event('End Script!')]], FakeECS({TASK: task()}))
        self.assertEqual((rows[0]['sigla'], rows[0]['modalidad']), ('DESAB', 'TURNO'))
        self.assertEqual(rows[0]['estado_ecs'], 'STOPPED')
        self.assertIn('DETENIDA EN ECS', rows[0]['finalizacion'])
        self.assertNotEqual(rows[0]['fin_ecs'], '')

    def test_conflicting_siglas_are_not_attributed_to_one(self):
        rows, _, _ = self.collect([[event('CAD=MEBOG')]], FakeECS({TASK: task()}))
        self.assertEqual(rows[0]['sigla'], 'CONFLICTO')
        self.assertEqual(rows[0]['siglas_evidencia'], 'DESAB,MEBOG')

    def test_unhealthy_ecs_exit_stays_failure_even_if_log_read_is_partial(self):
        ended = task(containers=[{'name': 'pon-prod-prediccion', 'exitCode': 1}])
        rows, context, _ = self.collect([[event('End Script!')], SDKError()], FakeECS({TASK: ended}))
        self.assertEqual((rows[0]['control'], rows[0]['lectura_logs']), ('FALLA', 'PARCIAL'))
        self.assertTrue(context['lecturas_incompletas'])

    def test_truncation_keeps_partial_counts_and_denominator(self):
        rows, context, _ = self.collect([[event('CAD=DESAB'),
            event('The best model to run the prediction is Uniform - 1 - 2'),
            event('The best model to run the prediction is KDE - 1 - 2')]], max_eventos=2)
        self.assertEqual(rows[0]['total_selecciones'], 1)
        self.assertEqual(rows[0]['lectura_logs'], 'PARCIAL')
        self.assertIn('observadas', rows[0]['detalle'])
        self.assertTrue(context['lecturas_incompletas'])

    def test_empty_page_does_not_stop_event_pagination(self):
        rows, _, _ = self.collect([[], [event('CAD=DESAB')]])
        self.assertEqual(rows[0]['sigla'], 'DESAB')

    def test_duplicate_end_messages_do_not_invent_unique_completed_units(self):
        rows, _, _ = self.collect([[event('subunidad 64779 End Script!'),
                                   event('subunidad 64779 End Script!'), event('End Script!')]])
        self.assertEqual(rows[0]['cierres_reportados'], 3)
        self.assertEqual(rows[0]['subunidades_con_cierre_explicito'], 1)

    def test_mixed_models_and_insufficient_data_are_counted_separately(self):
        rows, _, _ = self.collect([[event('CAD=DESAB'),
            event('The best model to run the prediction is Uniform - 1 - 2'),
            event('The best model to run the prediction is KDE - 1 - 2'),
            event('The best model to run the prediction is KDE - 1 - 3'),
            event('Not enough records in the train data for Counter')]])
        self.assertEqual(rows[0]['total_selecciones'], 3)
        self.assertEqual(rows[0]['porcentaje_uniform_observado'], 33.33)
        self.assertEqual(json.loads(rows[0]['avisos_datos_insuficientes']), {'Counter': 1})

    def test_json_context_labels_and_errors(self):
        rows, _, _ = self.collect([[event(json.dumps({'environment': [{'name': 'CAD', 'value': 'DESAB'}],
                     'date_params': {'timeFrame': 'HORA'}, 'level': 'ERROR', 'message': 'detalle'}))]])
        self.assertEqual((rows[0]['sigla'], rows[0]['modalidad'], rows[0]['indicios_error']), ('DESAB', 'HORA', 1))

    def test_no_model_mentions_are_not_counted_as_selections(self):
        rows, _, _ = self.collect([[event('CAD=DESAB'), event('Training KDE; error_rate=0')]])
        self.assertEqual(rows[0]['total_selecciones'], 0)
        self.assertEqual(rows[0]['porcentaje_uniform_observado'], '')
        self.assertEqual(rows[0]['indicios_error'], 0)

    def test_same_event_id_is_not_counted_twice_across_pages(self):
        declaration = dict(event('The best model to run the prediction is Uniform - 1 - 2'), eventId='same')
        rows, _, _ = self.collect([[declaration], [declaration]])
        self.assertEqual(rows[0]['eventos_leidos'], 1)
        self.assertEqual(rows[0]['total_selecciones'], 1)

    def test_explicit_ecs_start_error_is_failure_without_definition(self):
        failed = task(stoppedReason='CannotPullContainerError: NO_EXPORTAR_TOKEN token=secret',
                      containers=[])
        rows, _, _ = self.collect([[event('CAD=DESAB')]], FakeECS({TASK: failed}, definition_fail=True))
        self.assertEqual(rows[0]['control'], 'FALLA')
        self.assertIn('CannotPullContainerError', rows[0]['motivo_parada'])

    def test_absent_exit_code_is_not_a_confirmed_ecs_failure(self):
        active = task(lastStatus='RUNNING', containers=[{'name': 'pon-prod-prediccion', 'exitCode': None}])
        rows, _, _ = self.collect([[event('CAD=DESAB')]], FakeECS({TASK: active}))
        self.assertNotEqual(rows[0]['control'], 'FALLA')

    def test_stream_scan_is_bounded_and_reports_truncation(self):
        logs = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK]])
        rows, issues = models.discover_history_streams(logs, GROUP, PREFIX, 1, (SDKError,))
        self.assertEqual(len(rows), 1)
        self.assertIn('truncado', issues[0])
        config = logs.calls[0][1]['PaginationConfig']
        self.assertEqual(config, {'MaxItems': 2, 'PageSize': 2})

    def test_listing_failure_keeps_streams_already_found(self):
        logs = FakeLogs(streams=[[PREFIX+TASK], SDKError()])
        rows, issues = models.discover_history_streams(logs, GROUP, PREFIX, 100, (SDKError,))
        self.assertEqual(len(rows), 1)
        self.assertIn('AccessDeniedException', issues[0])

    def test_invalid_stream_suffix_is_not_treated_as_task_id(self):
        logs = FakeLogs(streams=[[PREFIX+'not-an-ecs-task', PREFIX+TASK]])
        rows, issues = models.discover_history_streams(logs, GROUP, PREFIX, 100, (SDKError,))
        self.assertEqual([r['tarea'] for r in rows], [TASK])
        self.assertTrue(issues)

    def test_other_container_prefix_is_ignored(self):
        logs = FakeLogs(streams=[['other/container/'+SECOND_TASK, PREFIX+TASK]])
        rows, issues = models.discover_history_streams(logs, GROUP, PREFIX, 100, (SDKError,))
        self.assertEqual([r['tarea'] for r in rows], [TASK])
        self.assertEqual(issues, [])

    def test_empty_expired_stream_is_omitted_without_claiming_failure(self):
        rows, context, _ = self.collect([])
        self.assertEqual(rows, [])
        self.assertEqual(context['streams_sin_evidencia_en_ventana'], 1)
        self.assertEqual(context['lecturas_incompletas'], [])

    def test_failed_event_read_is_not_reported_as_no_activity(self):
        rows, context, _ = self.collect([SDKError()])
        self.assertEqual(rows[0]['lectura_logs'], 'ERROR')
        self.assertTrue(context['lecturas_incompletas'])

    def test_wrong_family_visible_in_ecs_is_excluded(self):
        rows, context, _ = self.collect([[event('CAD=DESAB')]], FakeECS({TASK: task(taskDefinitionArn='other:4')}))
        self.assertEqual(rows, [])
        self.assertIn('otra familia', context['lecturas_incompletas'][0])

    def test_ecs_description_error_does_not_block_log_evidence(self):
        rows, context, _ = self.collect([[event('CAD=DESAB')]], FakeECS(failure=SDKError()))
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertTrue(context['lecturas_incompletas'])

    def test_exact_revision_failure_preserves_override_but_marks_partial(self):
        rows, context, _ = self.collect([[event('End Script!')]], FakeECS({TASK: task()}, definition_fail=True))
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertIn('Metadata ECS incompleta', rows[0]['detalle'])
        self.assertTrue(context['lecturas_incompletas'])

    def test_explicit_group_does_not_require_current_family_definition(self):
        ecs = FakeECS(definition_fail=True)
        def refuse_definition(**kwargs):
            raise AssertionError('No debe leer definicion actual si se dio grupo explicito')
        ecs.describe_task_definition = refuse_definition
        rows, _, _ = self.collect([[event('CAD=DESAB')]], ecs, grupo_logs=GROUP)
        self.assertEqual(rows[0]['sigla'], 'DESAB')

    def test_no_s3_ecr_or_task_list_dependency_and_no_raw_messages_in_reports(self):
        logs = FakeLogs(events={PREFIX+TASK: [[event('CAD=DESAB token=NO_EXPORTAR_TOKEN')]]})
        clients = []
        def client(name, **kwargs):
            clients.append(name)
            if name == 'ecs':
                return FakeECS()
            if name == 'logs':
                return logs
            raise AssertionError(name)
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            code = models.inspect_log_history(SimpleNamespace(client=client), None,
                                             arguments(salida=temp), (SDKError,))
            report = next(Path(temp).rglob('historial_tareas.csv'))
            contents = report.read_text(encoding='utf-8-sig')
            self.assertEqual(len(list(Path(temp).rglob('*.csv'))), 1)
            with report.open(encoding='utf-8-sig') as handle:
                row = next(csv.DictReader(handle))
        self.assertEqual(code, 0)
        self.assertEqual(clients, ['ecs', 'logs'])
        self.assertNotIn('NO_EXPORTAR_TOKEN', contents)
        self.assertEqual(row['sigla'], 'DESAB')

    def test_describe_tasks_batches_never_exceed_100(self):
        ids = [format(index, '032x') for index in range(205)]
        ecs = FakeECS()
        logs = FakeLogs(streams=[[PREFIX + item for item in ids]])
        models.collect_log_history(ecs, lambda region: logs, arguments(), NOW, (SDKError,))
        self.assertEqual([len(batch) for batch in ecs.batches], [100, 100, 5])


class ExecutionWindowTests(unittest.TestCase):
    def test_both_monitors_read_old_stopped_task_lifetime(self):
        recent = NOW - timedelta(minutes=60)
        stopped = task()
        for function in (models.stopped_log_window, execution.task_log_window):
            with self.subTest(function=function.__name__):
                start, end = function(stopped, recent, NOW)
                self.assertEqual(start, stopped['createdAt'])
                self.assertEqual(end, stopped['stoppedAt'] + timedelta(seconds=1))

    def test_running_task_keeps_recent_window_and_bad_dates_do_not_expand_it(self):
        recent = NOW - timedelta(minutes=60)
        for function in (models.stopped_log_window, execution.task_log_window):
            for candidate in (task(lastStatus='RUNNING'), task(stoppedAt=NOW+timedelta(hours=1)),
                              task(createdAt=None, startedAt=None)):
                with self.subTest(function=function.__name__, candidate=candidate):
                    self.assertEqual(function(candidate, recent, NOW), (recent, NOW))

    def test_end_script_is_informational_not_global_completion(self):
        result = execution.analyze_logs([event('subunidad 64779 End Script!'), event('End Script!')])
        self.assertEqual(result['cierres_reportados'], 2)
        self.assertEqual(result['errores'], 0)
        self.assertEqual(result['avance'], '')


def detail(sigla, task_id, state='NO DISPONIBLE', selections=None, stream=None, **changes):
    candidate = dict(tarea=task_id, stream=stream or PREFIX+task_id, grupo=GROUP, region='us-east-1')
    logs = dict(eventos=1, seleccion=models.Counter(selections or {}), insuficientes=models.Counter(),
        errores_dependencia=models.Counter(), siglas={sigla} if sigla != 'SIN DATO' else set(),
        modalidades=set(), cierres=0, unidades_cierre=set(), indicios_error=0,
        primero=1, ultimo=1, cobertura='LEIDA', error='')
    ecs_task = {} if state == 'NO DISPONIBLE' else task(task_id, lastStatus=state,
        overrides={'containerOverrides': [{'name': 'pon-prod-prediccion',
                   'environment': [{'name': 'CAD', 'value': sigla}]}]})
    row = models.history_task_row(candidate, ecs_task, {}, logs)
    row.update(changes)
    return row


class SiglaSummaryTests(unittest.TestCase):
    def test_no_observations_do_not_invent_missing_siglas(self):
        self.assertEqual(models.summarize_history_siglas([]), [])

    def test_all_states_appear_in_one_row_per_sigla(self):
        rows = [detail('DESAB', TASK, 'RUNNING'), detail('MEBOG', SECOND_TASK, 'STOPPED'),
                detail('MEPAS', '2'*32)]
        result = models.summarize_history_siglas(rows)
        self.assertEqual([r['sigla'] for r in result], ['DESAB', 'MEBOG', 'MEPAS'])
        self.assertEqual(result[0]['activas_ecs'], 1)
        self.assertEqual(result[1]['detenidas_ecs'], 1)
        self.assertEqual(result[2]['sin_estado_ecs'], 1)

    def test_one_sigla_combines_active_stopped_and_logs_with_weighted_percentage(self):
        rows = [detail('DESAB', TASK, 'RUNNING', {'Uniform': 1}),
                detail('DESAB', SECOND_TASK, 'STOPPED', {'KDE': 9}),
                detail('DESAB', '2'*32, selections={'Counter': 10})]
        summary, = models.summarize_history_siglas(rows)
        self.assertEqual((summary['tareas'], summary['activas_ecs'], summary['detenidas_ecs'], summary['sin_estado_ecs']),
                         (3, 1, 1, 1))
        self.assertEqual((summary['total_selecciones'], summary['porcentaje_uniform_observado']), (20, 5))

    def test_multiple_streams_do_not_duplicate_task_count_or_duplicate_same_stream(self):
        first = detail('DESAB', TASK, 'RUNNING', {'Uniform': 2})
        other = detail('DESAB', TASK, 'RUNNING', {'KDE': 3}, stream='other/container/'+TASK)
        summary, = models.summarize_history_siglas([first, first, other])
        self.assertEqual((summary['tareas'], summary['streams'], summary['total_selecciones']), (1, 2, 5))


    def test_unknown_sigla_is_not_arbitrarily_assigned(self):
        summaries = models.summarize_history_siglas([detail('SIN DATO', TASK, selections={'Uniform': 4})])
        self.assertEqual(summaries, [])

    def test_known_failure_survives_partial_discovery(self):
        row = detail('DESAB', TASK, control='FALLA')
        summary, = models.summarize_history_siglas([row], incomplete=True)
        self.assertEqual((summary['control'], summary['lectura_logs']), ('FALLA', 'PARCIAL'))


    def test_unknown_ecs_state_is_not_counted_as_an_active_task(self):
        summary, = models.summarize_history_siglas([detail('DESAB', TASK, estado_ecs=None)])
        self.assertEqual((summary['activas_ecs'], summary['sin_estado_ecs']), (0, 1))
        self.assertEqual(summary['control'], 'AVISO')

    def test_default_summary_collects_all_observed_states_without_s3(self):
        third = '2'*32
        logs = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK, PREFIX+third]], events={
            PREFIX+TASK: [[event('CAD=DESAB')]], PREFIX+SECOND_TASK: [[event('CAD=MEBOG')]],
            PREFIX+third: [[event('CAD=MEPAS')]]})
        ecs = FakeECS({
            TASK: task(lastStatus='RUNNING'),
            SECOND_TASK: task(SECOND_TASK, overrides={'containerOverrides': [
                {'name': 'pon-prod-prediccion', 'environment': [{'name': 'CAD', 'value': 'MEBOG'}]}]})})
        called = []
        def client(name, **kwargs):
            called.append(name)
            if name not in ('ecs', 'logs'):
                raise AssertionError('Este monitoreo no consulta catalogos ni S3: ' + name)
            return {'ecs': ecs, 'logs': logs}[name]
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(output):
            code = models.inspect_log_history(SimpleNamespace(client=client), None,
                arguments(salida=temp), (SDKError,))
            path = next(Path(temp).rglob('*.csv'))
            with path.open(encoding='utf-8-sig') as handle:
                records = list(csv.DictReader(handle))
            self.assertEqual(len(list(Path(temp).rglob('*.csv'))), 1)
        self.assertEqual(code, 0)
        summaries = [row for row in records if row['tipo_registro'] == 'RESUMEN_SIGLA']
        details = [row for row in records if row['tipo_registro'] == 'DETALLE_TAREA']
        self.assertEqual([row['sigla'] for row in summaries], ['DESAB', 'MEBOG', 'MEPAS'])
        self.assertEqual(len(details), 3)
        self.assertEqual(called, ['ecs', 'logs'])
        self.assertEqual(summaries[0]['activas_ecs'], '1')
        self.assertEqual(summaries[1]['detenidas_ecs'], '1')
        self.assertEqual(summaries[2]['sin_estado_ecs'], '1')
        for sigla in ('DESAB', 'MEBOG', 'MEPAS'):
            self.assertIn(sigla, output.getvalue())


    def test_default_cli_uses_summary_mode_instead_of_only_running_tasks(self):
        session = SimpleNamespace(client=lambda *args, **kwargs: SimpleNamespace(
            get_caller_identity=lambda: {'Account': '926162397524'}))
        sdk = {'boto3': SimpleNamespace(Session=lambda **kwargs: session),
               'botocore.config': SimpleNamespace(Config=lambda **kwargs: None),
               'botocore.exceptions': SimpleNamespace(BotoCoreError=SDKError, ClientError=SDKError)}
        with patch.dict(sys.modules, sdk), patch.object(sys, 'argv', ['mapear_modelos_dependencias.py']), \
                patch.object(models, 'inspect_log_history', return_value=0) as history:
            self.assertEqual(models.main(), 0)
        args = history.call_args.args[2]
        self.assertTrue(args.historial_logs)
        self.assertEqual(args.dias_logs, 30)
        self.assertEqual(args.max_eventos, 100000)


class StreamStructureTests(unittest.TestCase):
    class S3:
        def __init__(self, data=None, error=False):
            self.data = (data if data is not None else
                b'SIGLA_PAPA;SIGLA_FISICA;TIUN_CODIGO;UNIDAD_ID;UNDE_CONSECUTIVO\n'
                b'DESAB;DESAB;6;123;1\nMEBOG;MEBOG;6;456;1\nMEPAS;MEPAS;6;;1\n')
            self.error = error
            self.body = None
            self.requests = []

        def head_object(self, **kwargs):
            if self.error:
                raise SDKError()
            self.requests.append(kwargs)
            return {'ContentLength': len(self.data), 'ETag': 'same', 'LastModified': NOW}

        def get_object(self, **kwargs):
            self.requests.append(kwargs)
            self.body = io.BytesIO(self.data)
            return {'Body': self.body, 'ETag': 'same'}

    def test_structure_includes_all_siglas_even_without_unit_candidates(self):
        s3 = self.S3()
        siglas, metadata = models.read_stream_structure_siglas(s3, arguments())
        self.assertEqual(siglas, ['DESAB', 'MEBOG', 'MEPAS'])
        self.assertEqual(metadata['columna'], 'SIGLA_PAPA')
        self.assertEqual(metadata['cobertura'], 'LEIDA')
        self.assertTrue(s3.body.closed)
        self.assertEqual(s3.requests[1]['IfMatch'], 'same')
        self.assertEqual(s3.requests[1]['ExpectedBucketOwner'], '926162397524')

    def test_repeated_cad_has_one_row_for_each_distinct_stream(self):
        first = detail('MEBOG', TASK, 'RUNNING')
        second = detail('MEBOG', SECOND_TASK, 'STOPPED')
        records = models.build_sigla_stream_mapping([first, first, second], ['MEBOG', 'MEPAS'])
        bogota = [row for row in records if row['sigla'] == 'MEBOG']
        self.assertEqual([row['stream'] for row in bogota], [PREFIX+TASK, PREFIX+SECOND_TASK])
        self.assertEqual(len(records), 3)
        absent = next(row for row in records if row['sigla'] == 'MEPAS')
        self.assertEqual(absent['asociacion'], 'SIN STREAM ASOCIADO')
        self.assertEqual(absent['stream'], '')

    def test_unknown_and_conflicting_cad_are_never_assigned_to_structure(self):
        unknown = detail('SIN DATO', TASK)
        conflict = detail('DESAB', SECOND_TASK)
        conflict['sigla'] = 'CONFLICTO'
        records = models.build_sigla_stream_mapping([unknown, conflict], ['MEBOG'])
        matched = next(row for row in records if row['sigla'] == 'MEBOG')
        self.assertEqual(matched['stream'], '')
        self.assertEqual({row['asociacion'] for row in records},
                         {'SIN STREAM ASOCIADO', 'SIN ATRIBUCION UNICA'})

    def test_cad_outside_structure_is_preserved_and_labeled(self):
        records = models.build_sigla_stream_mapping([detail('DECUN', TASK)], ['MEBOG'])
        row = next(record for record in records if record['sigla'] == 'DECUN')
        self.assertEqual(row['en_estructura'], 'NO')
        self.assertEqual(row['stream'], PREFIX+TASK)

    def test_empty_or_header_only_structure_is_not_reported_complete(self):
        for data in (b'', b'SIGLA_PAPA;UNIDAD_ID\n'):
            with self.subTest(data=data):
                siglas, metadata = models.read_stream_structure_siglas(self.S3(data), arguments())
                self.assertEqual(siglas, [])
                self.assertNotEqual(metadata['cobertura'], 'LEIDA')

    def test_cli_table_and_csv_show_all_structure_cads_and_full_stream_names(self):
        logs = FakeLogs(events={PREFIX+TASK: [[event('CAD=DESAB')]]})
        clients = {'ecs': FakeECS(), 'logs': logs, 's3': self.S3()}
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(output):
            code = models.inspect_stream_mapping(SimpleNamespace(client=lambda name, **kwargs: clients[name]),
                                                  None, arguments(salida=temp), (SDKError,))
            path = next(Path(temp).rglob('mapa_siglas_streams.csv'))
            with path.open(encoding='utf-8-sig') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(list(Path(temp).rglob('*.csv'))), 1)
        self.assertEqual(code, 0)
        self.assertEqual([row['sigla'] for row in rows], ['DESAB', 'MEBOG', 'MEPAS'])
        self.assertEqual(rows[0]['stream'], PREFIX+TASK)
        self.assertIn(PREFIX+TASK, output.getvalue())
        self.assertIn('MEBOG', output.getvalue())
        self.assertIn('SIN STREAM ASOCIADO', output.getvalue())

    def test_structure_read_error_is_visible_and_keeps_observed_associations(self):
        clients = {'ecs': FakeECS(), 'logs': FakeLogs(events={PREFIX+TASK: [[event('CAD=DESAB')]]}),
                   's3': self.S3(error=True)}
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(output):
            code = models.inspect_stream_mapping(SimpleNamespace(client=lambda name, **kwargs: clients[name]),
                                                  None, arguments(salida=temp), (SDKError,))
        self.assertEqual(code, 3)
        self.assertIn(PREFIX+TASK, output.getvalue())
        self.assertIn('[INCOMPLETO]', output.getvalue())

    def test_mapping_cli_does_not_switch_to_default_history_mode(self):
        session = SimpleNamespace(client=lambda *args, **kwargs: SimpleNamespace(
            get_caller_identity=lambda: {'Account': '926162397524'}))
        sdk = {'boto3': SimpleNamespace(Session=lambda **kwargs: session),
               'botocore.config': SimpleNamespace(Config=lambda **kwargs: None),
               'botocore.exceptions': SimpleNamespace(BotoCoreError=SDKError, ClientError=SDKError)}
        with patch.dict(sys.modules, sdk), \
                patch.object(sys, 'argv', ['mapear_modelos_dependencias.py', '--mapear-streams']), \
                patch.object(models, 'inspect_stream_mapping', return_value=0) as mapping:
            self.assertEqual(models.main(), 0)
        args = mapping.call_args.args[2]
        self.assertTrue(args.mapear_streams)
        self.assertFalse(args.historial_logs)
        self.assertEqual(args.max_eventos, 200)


class FastMappingTests(unittest.TestCase):
    def collect(self, logs, ecs=None, state=None, deadline=None):
        state = {'streams': {}} if state is None else state
        rows, context = models.collect_log_history(ecs or FakeECS(), lambda region: logs,
            arguments(max_eventos=200), NOW, (SDKError,), mapping_state=state, deadline=deadline)
        return rows, context, state

    def test_ecs_cad_needs_no_log_messages_or_model_analysis(self):
        logs = FakeLogs()
        with patch.object(models, 'inspect_event', side_effect=AssertionError('No analizar modelos')):
            rows, context, state = self.collect(logs, FakeECS({TASK: task()}))
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertEqual(rows[0]['lectura_logs'], 'CAD ECS; SIN LECTURA LOG')
        self.assertEqual([call[0] for call in logs.calls], ['describe_log_streams'])
        self.assertEqual(context['modo'], 'ASOCIACION RAPIDA')

    def test_head_sample_stops_after_cad_and_never_uses_full_log_paginator(self):
        logs = FakeLogs(events={PREFIX+TASK: [[event('CAD=DESAB')]*20000]})
        with patch.object(models, 'history_log_summary', side_effect=AssertionError('No recorrer historial')):
            rows, _, _ = self.collect(logs)
        calls = [kwargs for name, kwargs in logs.calls if name == 'get_log_events']
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['limit'], 100)
        self.assertTrue(calls[0]['startFromHead'])
        self.assertEqual(rows[0]['sigla'], 'DESAB')

    def test_tail_sample_can_identify_cad_without_scanning_middle(self):
        events = [event('procesando')]*20000 + [event('CAD=MEBOG')]
        logs = FakeLogs(events={PREFIX+TASK: [events]})
        rows, _, _ = self.collect(logs)
        calls = [kwargs for name, kwargs in logs.calls if name == 'get_log_events']
        self.assertEqual(len(calls), 2)
        self.assertEqual([call['startFromHead'] for call in calls], [True, False])
        self.assertEqual(rows[0]['sigla'], 'MEBOG')
        self.assertLessEqual(rows[0]['eventos_leidos'], 200)

    def test_no_cad_is_a_bounded_sample_not_proof_of_absence(self):
        logs = FakeLogs(events={PREFIX+TASK: [[event('procesando')]*20000]})
        rows, _, state = self.collect(logs)
        self.assertEqual(rows[0]['sigla'], 'SIN DATO')
        self.assertEqual(rows[0]['lectura_logs'], 'MUESTRA CAD')
        self.assertEqual(sum(name == 'get_log_events' for name, _ in logs.calls), 2)
        self.assertEqual(next(iter(state['streams'].values()))['cad'], '')

    def test_sample_read_error_is_reported_without_model_scan_or_raw_sdk_message(self):
        logs = FakeLogs(events={PREFIX+TASK: [SDKError()]})
        rows, context, _ = self.collect(logs)
        self.assertEqual(rows[0]['lectura_logs'], 'ERROR')
        self.assertIn('AccessDeniedException', context['lecturas_incompletas'][0])
        self.assertNotIn('NO_EXPORTAR_ERROR_SDK', json.dumps(context))
        self.assertEqual(sum(name == 'get_log_events' for name, _ in logs.calls), 1)

    def test_cache_reuses_only_association_without_claiming_old_ecs_state(self):
        candidate = dict(region='us-east-1', grupo=GROUP, stream=PREFIX+TASK)
        state = {'streams': {models.mapping_stream_key(candidate):
                            dict(cad='DESAB', modalidad='TURNO', fuente='ECS', consultada_utc=NOW.isoformat())}}
        logs = FakeLogs()
        rows, _, _ = self.collect(logs, state=state)
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertEqual(rows[0]['estado_ecs'], 'NO DISPONIBLE')
        self.assertEqual(rows[0]['sigla_fuente'], 'CACHE: ECS')
        self.assertFalse(any(name == 'get_log_events' for name, _ in logs.calls))

    def test_current_ecs_cad_takes_precedence_over_an_old_cached_association(self):
        candidate = dict(region='us-east-1', grupo=GROUP, stream=PREFIX+TASK)
        state = {'streams': {models.mapping_stream_key(candidate): dict(cad='MEBOG', fuente='MUESTRA LOGS')}}
        rows, _, state = self.collect(FakeLogs(), FakeECS({TASK: task()}), state)
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertEqual(next(iter(state['streams'].values()))['cad'], 'DESAB')

    def test_unattempted_streams_are_tried_before_previously_unidentified_samples(self):
        first = dict(region='us-east-1', grupo=GROUP, stream=PREFIX+TASK)
        state = {'streams': {models.mapping_stream_key(first): dict(cad='', consultada_utc=NOW.isoformat())}}
        logs = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK]])
        self.collect(logs, state=state)
        calls = [kwargs for name, kwargs in logs.calls if name == 'get_log_events']
        self.assertEqual(calls[0]['logStreamName'], PREFIX+SECOND_TASK)

    def test_deadline_keeps_confirmed_rows_and_resume_only_reads_pending_stream(self):
        logs = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK]], events={
            PREFIX+TASK: [[event('CAD=DESAB')]], PREFIX+SECOND_TASK: [[event('CAD=MEBOG')]]})
        clock = {'tick': 0}
        original = logs.get_log_events
        def expire_after_read(**kwargs):
            response = original(**kwargs)
            clock['tick'] = 20
            return response
        logs.get_log_events = expire_after_read
        with patch.object(models.time, 'monotonic', side_effect=lambda: clock['tick']):
            rows, context, state = self.collect(logs, deadline=10)
        self.assertEqual(context['streams_pendientes'], 1)
        self.assertEqual(rows[0]['sigla'], 'DESAB')
        self.assertEqual(rows[1]['lectura_logs'], 'PENDIENTE')
        resumed = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK]], events={
            PREFIX+SECOND_TASK: [[event('CAD=MEBOG')]]})
        rows, context, _ = self.collect(resumed, state=state)
        self.assertEqual({row['sigla'] for row in rows}, {'DESAB', 'MEBOG'})
        calls = [kwargs for name, kwargs in resumed.calls if name == 'get_log_events']
        self.assertEqual([call['logStreamName'] for call in calls], [PREFIX+SECOND_TASK])
        self.assertEqual(context['streams_pendientes'], 0)

    def test_checkpoint_exists_before_first_log_read_and_survives_interrupt(self):
        logs = FakeLogs(streams=[[PREFIX+TASK, PREFIX+SECOND_TASK]], events={PREFIX+TASK: [[event('CAD=DESAB')]]})
        clients = {'ecs': FakeECS(), 'logs': logs, 's3': StreamStructureTests.S3()}
        original = logs.get_log_events
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            def interrupted(**kwargs):
                self.assertTrue(list(Path(temp).rglob('mapa_siglas_streams.csv')))
                if kwargs['logStreamName'] == PREFIX+SECOND_TASK:
                    raise KeyboardInterrupt()
                return original(**kwargs)
            logs.get_log_events = interrupted
            code = models.inspect_stream_mapping(SimpleNamespace(client=lambda name, **kwargs: clients[name]),
                                                  None, arguments(salida=temp, max_eventos=200), (SDKError,))
            cache = json.loads(next(Path(temp).rglob('cache_mapa_streams_*.json')).read_text())
            self.assertEqual(code, 130)
            self.assertIn('DESAB', [entry['cad'] for entry in cache['streams'].values()])
            context = json.loads(next(Path(temp).rglob('contexto_streams.json')).read_text())
            self.assertFalse(context['en_proceso'])
            self.assertTrue(context['mapa_parcial'])
            logs.get_log_events = original
            logs.calls.clear()
            code = models.inspect_stream_mapping(SimpleNamespace(client=lambda name, **kwargs: clients[name]),
                                                  None, arguments(salida=temp, max_eventos=200), (SDKError,))
            self.assertEqual(code, 0)
            calls = [kwargs for name, kwargs in logs.calls if name == 'get_log_events']
            self.assertTrue(calls)
            self.assertTrue(all(call['logStreamName'] == PREFIX+SECOND_TASK for call in calls))

    def test_invalid_cache_is_rebuilt_without_reusing_its_contents(self):
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            path = Path(temp)/'cache.json'
            path.write_text('{bad json')
            self.assertEqual(models.load_mapping_state(path, {'cuenta': 'x'})['streams'], {})

    def test_atomic_write_preserves_old_checkpoint_if_replace_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'cache.json'
            path.write_text('old')
            with patch.object(Path, 'replace', side_effect=OSError('disk error')), self.assertRaises(OSError):
                models.atomic_mapping_text(path, 'new')
            self.assertEqual(path.read_text(), 'old')
            self.assertEqual(list(Path(temp).iterdir()), [path])


if __name__ == '__main__':
    unittest.main()
