"""Publicacion de CSV por monitor: destino, errores, copia local y reanudacion."""
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


BASE = Path(__file__).resolve().parents[1]
SOURCES = [
    ('2. Programación y lanzamiento/monitor_disparo_semanal.py', '02-MonitorDisparoSemanal'),
    ('3. Preparación de datos/monitor_procesos_upstream.py', '03-MonitorProcesosUpstream'),
    ('4. Referencias, entrenamiento e informes/monitor_glue_jobs.py', '04-MonitorGlueJobs'),
    ('5. Ejecución de las predicciones/mapear_inicio_progreso.py', '05-MonitorInicioProgreso'),
    ('6. Modelos utilizados y componentes/mapear_modelos_dependencias.py', '06-MonitorModelosDependencias'),
    ('7. Actualidad de la tabla de probabilidades/monitor_freshness_probabilidaddelito.py', '07-MonitorFreshnessProbabilidaddelito'),
]


def load(index, source):
    spec = importlib.util.spec_from_file_location('s3_monitor_'+str(index), BASE/source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MODULES = [(load(i, source), name) for i, (source, name) in enumerate(SOURCES)]


class Denied(Exception):
    response = {'Error': {'Code': 'AccessDenied', 'Message': 'NO_EXPORTAR_RESPUESTA'}}


class CaptureS3:
    def __init__(self, denied=False):
        self.objects, self.denied = {}, denied

    def upload_fileobj(self, body, bucket, key, ExtraArgs):
        if self.denied:
            raise Denied()
        self.objects[(bucket, key)] = (body.read(), ExtraArgs)


class ReportPublicationTests(unittest.TestCase):
    def test_six_monitors_publish_only_csv_to_distinct_script_date_run_prefixes(self):
        prefixes = set()
        for module, name in MODULES:
            with self.subTest(monitor=name), tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
                folder = Path(temp)/'2026-10-29'/'15-00-00_123456'
                folder.mkdir(parents=True)
                files = {'general.csv': '\ufeffestado,mensaje\r\nAVISO,Revisión\r\n',
                         'diario.csv': '\ufefffecha,total\r\n2026-10-28,2\r\n'}
                for filename, text in files.items():
                    (folder/filename).write_bytes(text.encode('utf-8'))
                (folder/'contexto.json').write_text('{}')
                (folder/'consulta.sql').write_text('SELECT 1')
                nested = folder/'otra_carpeta'; nested.mkdir()
                (nested/'ajeno.csv').write_text('No publicar')
                s3, calls = CaptureS3(), []
                def client(service, **kwargs):
                    calls.append(service)
                    self.assertEqual(service, 's3')
                    return s3
                locations = module.upload_csv_reports(SimpleNamespace(client=client), None, folder)
                self.assertEqual(calls, ['s3'])
                self.assertEqual(set(locations), set(files))
                self.assertEqual(len(s3.objects), 2)
                for (bucket, key), (data, extra) in s3.objects.items():
                    self.assertEqual(bucket, 'pon-prod-ml-iapol')
                    self.assertTrue(key.startswith('ValidationReports/'+name+'/2026-10-29/15-00-00_123456/'))
                    self.assertEqual(data, (folder/key.rsplit('/', 1)[-1]).read_bytes())
                    self.assertEqual(extra['ContentType'], 'text/csv; charset=utf-8')
                prefixes.add(module.REPORTS_PREFIX)
        self.assertEqual(len(prefixes), 6)

    def test_access_denied_preserves_local_csv_and_does_not_report_success(self):
        for module, name in MODULES:
            with self.subTest(monitor=name), tempfile.TemporaryDirectory() as temp:
                folder = Path(temp)/'2026-10-29'/'15-00-00_123456'; folder.mkdir(parents=True)
                path = folder/'control.csv'; path.write_bytes(b'estado\nFALLA\n')
                output = io.StringIO()
                with redirect_stdout(output), self.assertRaises(Denied):
                    module.upload_csv_reports(SimpleNamespace(client=lambda *a, **k: CaptureS3(denied=True)), None, folder)
                self.assertEqual(path.read_bytes(), b'estado\nFALLA\n')
                self.assertNotIn('CSV guardados', output.getvalue())

    def test_no_csv_does_not_create_client_or_placeholder_folder_objects(self):
        for module, name in MODULES:
            with self.subTest(monitor=name), tempfile.TemporaryDirectory() as temp:
                (Path(temp)/'contexto.json').write_text('{}')
                session = SimpleNamespace(client=lambda *a, **k: self.fail('No crear cliente sin CSV'))
                self.assertEqual(module.upload_csv_reports(session, None, Path(temp)), {})

    def test_model_query_is_not_consumed_until_s3_publication_succeeds(self):
        models = MODULES[4][0]
        started = []
        logs = SimpleNamespace(start_query=lambda **kw: started.append(kw) or {'queryId': 'q1'},
            get_query_results=lambda **kw: {'status': 'Complete', 'results': []})
        s3 = CaptureS3(denied=True)
        services = {'ecs': object(), 'logs': logs, 's3': s3}
        session = SimpleNamespace(client=lambda service, **kwargs: services[service])
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            args = SimpleNamespace(cluster='cluster', familia='family', cuenta='926162397524', region='us-east-1',
                grupo_logs='group', prefijo_logs='prefix/', dias_logs=7, timeout_consulta=180, salida=temp,
                max_streams=1000, max_eventos=100000, leer_eventos=False, verificar_ciclo=False)
            with self.assertRaises(Denied):
                models.inspect_log_history(session, None, args, (Denied,))
            state_path = next(Path(temp).glob('modelos_dependencias/consultas/*.json'))
            self.assertFalse(json.loads(state_path.read_text())['consumida'])
            s3.denied = False
            self.assertEqual(models.inspect_log_history(session, None, args, (Denied,)), 0)
            self.assertTrue(json.loads(state_path.read_text())['consumida'])
            self.assertEqual(len(started), 1)
            self.assertEqual(len(s3.objects), 1)

    def test_ecs_no_export_skips_s3_and_upload_failure_returns_incomplete_with_local_copy(self):
        monitor = MODULES[3][0]
        s3, calls = CaptureS3(denied=True), []
        def client(name, **kwargs):
            calls.append(name)
            return {'sts': SimpleNamespace(get_caller_identity=lambda: {'Account': '926162397524'}),
                'ecs': SimpleNamespace(describe_clusters=lambda **kw: {'clusters': [{'status': 'ACTIVE'}]}),
                'cloudwatch': object(), 's3': s3}[name]
        session = SimpleNamespace(client=client)
        sdk = {'boto3': SimpleNamespace(Session=lambda **kwargs: session),
               'botocore.config': SimpleNamespace(Config=lambda **kwargs: None),
               'botocore.exceptions': SimpleNamespace(BotoCoreError=Denied, ClientError=Denied)}
        with tempfile.TemporaryDirectory() as temp, patch.dict(sys.modules, sdk), \
                patch.object(monitor, 'collect_tasks', return_value=([], [])), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with patch.object(sys, 'argv', ['monitor.py', '--salida', temp, '--no-export']):
                self.assertEqual(monitor.main(), 0)
            self.assertNotIn('s3', calls)
            self.assertEqual(list(Path(temp).iterdir()), [])
            with patch.object(sys, 'argv', ['monitor.py', '--salida', temp]):
                self.assertEqual(monitor.main(), 3)
            self.assertEqual(len(list(Path(temp).rglob('tareas.csv'))), 1)


if __name__ == '__main__':
    unittest.main()
