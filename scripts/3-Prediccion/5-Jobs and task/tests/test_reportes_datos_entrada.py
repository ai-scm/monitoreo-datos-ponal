"""Exportacion general/diario: conserva controles y usa dos CSV sin consultas extra."""
from contextlib import redirect_stdout
import csv
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1]/'1. Datos antes de predecir/monitor_datos_entrada.py'
spec = importlib.util.spec_from_file_location('datos_entrada_reportes', PATH)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
NOW = datetime(2026, 10, 29, 12, tzinfo=timezone(timedelta(hours=-5)))


class CaptureS3:
    def __init__(self):
        self.objects = {}

    def upload_fileobj(self, body, bucket, key, **kwargs):
        self.objects[(bucket, key)] = body.read()


def read_csv(data):
    return list(csv.DictReader(io.StringIO(data.decode('utf-8-sig'))))


class InputReportTests(unittest.TestCase):
    def args(self):
        parser = monitor.parser()
        args = parser.parse_args(['--database', 'pon-prod-mdp', '--fecha', 'fecha_hora',
            '--unidad', 'unidad_base', '--sigla', 'sigla', '--fuente', 'bd',
            '--latitud', 'latitud', '--longitud', 'longitud', '--comparar-periodos', '--ejecutar'])
        monitor.validate(parser, args)
        return args

    def run_period(self, prefix, records, args):
        s3 = CaptureS3()
        with patch.object(monitor, 'execute', return_value=(records, {'query_id': 'query'})) as execute, \
                redirect_stdout(io.StringIO()):
            code = monitor.run_period_mode(args, NOW, {}, None, prefix,
                {'Account': '926162397524'}, s3)
        execute.assert_called_once()
        return code, s3

    def test_period_report_exports_two_csv_with_same_content_locally_and_in_s3(self):
        args = self.args()
        days = monitor.detail_days(args, NOW)
        record = dict(sigla='MEPAS', unidad='CAI MORASURCO', fuente='RNMC',
            total='30', ultima_fecha='2026-10-28 18:00:00', coordenadas_invalidas='0')
        record.update({f'd_{day:%Y%m%d}': '0' for day in days})
        record[f'd_{days[0]:%Y%m%d}'] = '20'
        record[f'd_{days[-1]:%Y%m%d}'] = '10'
        with tempfile.TemporaryDirectory() as folder:
            prefix = Path(folder)/'datos_entrada_2026-10-29_12-00-00_000001'
            code, s3 = self.run_period(prefix, [record], args)
            self.assertEqual(code, 0)
            self.assertEqual({p.name for p in Path(folder).glob('*.csv')},
                {monitor.csv_path(prefix, name).name for name in ('general', 'diario')})
            self.assertEqual(len(s3.objects), 2)
            self.assertEqual({key.rsplit('/', 1)[-1] for bucket, key in s3.objects}, {'general.csv', 'diario.csv'})
            for (bucket, key), data in s3.objects.items():
                self.assertEqual(bucket, 'pon-prod-ml-iapol')
                self.assertTrue(key.startswith('ValidationReports/01-MonitorDatosEntrada/2026-10-29/12-00-00_000001/'))
                local = monitor.csv_path(prefix, key.rsplit('/', 1)[-1][:-4])
                self.assertEqual(data, local.read_bytes())
            general = read_csv(monitor.csv_path(prefix, 'general').read_bytes())
            summary = next(r for r in general if r['tipo_registro'] == 'RESUMEN' and r['bd'] == 'RNMC')
            self.assertEqual((summary['registros_anteriores'], summary['registros_recientes']), ('20', '10'))
            controls = [r for r in general if r['tipo_registro'] == 'CONTROL']
            self.assertTrue(any(r['recurso'] == 'ALCANCE' and r['control'] == 'Calidad de fechas' for r in controls))
            self.assertTrue(any(r['estado'] == 'AVISO' and r['mensaje'] for r in controls))
            context = json.loads(prefix.with_suffix('.json').read_text())
            self.assertEqual(set(context['reportes_s3']), {'general', 'diario'})

    def test_no_data_still_exports_controls_and_daily_header_without_fabricating_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            prefix = Path(folder)/'datos_entrada_2026-10-29_12-00-00_000002'
            code, s3 = self.run_period(prefix, [], self.args())
            self.assertEqual(code, 1)
            self.assertEqual(len(list(Path(folder).glob('*.csv'))), 2)
            general = read_csv(monitor.csv_path(prefix, 'general').read_bytes())
            self.assertTrue(any(r['estado'] == 'FALLA' for r in general))
            self.assertEqual({r['tipo_registro'] for r in general}, {'CONTROL'})
            daily = monitor.csv_path(prefix, 'diario').read_bytes()
            self.assertEqual(read_csv(daily), [])
            self.assertIn('fecha', next(csv.reader(io.StringIO(daily.decode('utf-8-sig')))))
            self.assertEqual(len(s3.objects), 2)

    def test_no_export_never_uploads_reports(self):
        args = self.args()
        args.no_export = True
        code, s3 = self.run_period(None, [], args)
        self.assertEqual(code, 1)
        self.assertEqual(s3.objects, {})

    def test_historical_detail_retains_all_source_and_control_fields_in_general(self):
        reports = monitor.input_reports([{'total': 12, 'unidad': 'unidad'}],
            [{'dia': '2026-10-28', 'fuente': 'RNMC', 'registros': 4}],
            [{'recurso': 'GENERAL', 'control': 'Lectura', 'estado': 'FALLA', 'mensaje': 'Detalle parcial'}],
            [{'fuente': 'RNMC', 'total_historico': 12}])
        with tempfile.TemporaryDirectory() as folder:
            prefix = Path(folder)/'datos_entrada_2026-10-29_12-00-00_000003'
            monitor.save_local_reports(prefix, reports)
            general = read_csv(monitor.csv_path(prefix, 'general').read_bytes())
            self.assertEqual({r['tipo_registro'] for r in general}, {'RESUMEN', 'RESUMEN_FUENTE', 'CONTROL'})
            self.assertEqual(next(r for r in general if r['tipo_registro'] == 'RESUMEN_FUENTE')['total_historico'], '12')
            self.assertEqual(next(r for r in general if r['tipo_registro'] == 'CONTROL')['mensaje'], 'Detalle parcial')
            self.assertEqual(len(list(Path(folder).glob('*.csv'))), 2)


if __name__ == '__main__':
    unittest.main()
