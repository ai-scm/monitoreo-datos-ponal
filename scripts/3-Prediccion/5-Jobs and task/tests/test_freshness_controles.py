"""Presencia temporal: no confundir metadata, datos invalidos ni fechas objetivo futuras."""
from datetime import date
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest


PATH = Path(__file__).resolve().parents[1]/'7. Actualidad de la tabla de probabilidades/monitor_freshness_probabilidaddelito.py'
spec = importlib.util.spec_from_file_location('freshness_controles', PATH)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class FreshnessTests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(database='pon-prod-mdp', tabla='probabilidaddelito',
            fecha='fecha', desde=date(2026, 11, 2), hasta=date(2026, 11, 8), formato_fecha=None)

    def result(self, **extra):
        result = dict(total='10', fechas_no_interpretables='0', primera_fecha='2026-11-02',
            ultima_fecha='2026-11-08', registros_periodo='10', dias_con_datos='7',
            primera_fecha_periodo='2026-11-02', ultima_fecha_periodo='2026-11-08')
        result.update(extra)
        return result

    def test_schema_without_time_cannot_fabricate_a_freshness_query(self):
        table = {'StorageDescriptor': {'Columns': [{'Name': 'probability', 'Type': 'double'}]}, 'PartitionKeys': []}
        with self.assertRaises(monitor.ReadBlocked):
            monitor.build_sql(self.args(), table)

    def test_explicit_future_target_period_is_valid_not_a_generation_time_error(self):
        rows, _ = monitor.evaluate(self.result(), self.args(), 'table')
        self.assertEqual(rows[0]['control'], 'INFO')

    def test_no_records_is_a_failure_only_when_dates_can_be_evaluated(self):
        result = self.result(primera_fecha='2020-01-01', ultima_fecha='2020-01-02',
            registros_periodo='0', dias_con_datos='0', primera_fecha_periodo=None, ultima_fecha_periodo=None)
        rows, _ = monitor.evaluate(result, self.args(), 'table')
        self.assertEqual(rows[0]['control'], 'FALLA')
        result['fechas_no_interpretables'] = '2'
        rows, _ = monitor.evaluate(result, self.args(), 'table')
        self.assertEqual(rows[0]['control'], 'INCOMPLETO')

    def test_incoherent_counts_are_not_a_success(self):
        with self.assertRaises(monitor.ReadBlocked):
            monitor.evaluate(self.result(total='1'), self.args(), 'table')


if __name__ == '__main__':
    unittest.main()
