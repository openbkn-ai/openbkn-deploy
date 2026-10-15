import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest import mock


# Unit tests import the script without requiring a database driver.
sys.modules.setdefault("pymysql", types.ModuleType("pymysql"))
MODULE_PATH = Path(__file__).with_name("migrate_object_metrics.py")
SPEC = importlib.util.spec_from_file_location("migrate_object_metrics", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def legacy_metric(**overrides):
    row = {
        "f_id": "metric-1",
        "f_name": "Order count",
        "f_comment": "",
        "f_unit_type": "countUnit",
        "f_unit": "1",
        "f_metric_type": "atomic",
        "f_scope_type": "object_type",
        "f_scope_ref": "orders",
        "f_calculation_formula": '{"aggregation":{"aggr":"count"}}',
        "f_analysis_dimensions": "[]",
        "f_time_dimension": None,
        "f_create_time": 0,
        "f_update_time": 0,
    }
    row.update(overrides)
    return row


class MigrationTranslationTest(unittest.TestCase):
    def test_maps_current_time_dimension_property(self):
        decision = MODULE.translate(legacy_metric(
            f_time_dimension='{"property":"paid_at","default_range_policy":"none"}',
        ))

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.definition["query_capabilities"]["time_dimensions"], [{
            "object_type_id": "orders",
            "property_name": "paid_at",
        }])

    def test_maps_legacy_time_dimension_field(self):
        decision = MODULE.translate(legacy_metric(f_time_dimension='{"field":"paid_at"}'))

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(
            decision.definition["query_capabilities"]["time_dimensions"][0]["property_name"],
            "paid_at",
        )

    def test_requires_manual_review_for_default_range_policy(self):
        decision = MODULE.translate(legacy_metric(
            f_time_dimension='{"property":"paid_at","default_range_policy":"last_1h"}',
        ))

        self.assertEqual(decision.action, "manual_review")

    def test_marks_derived_and_qualified_metrics_for_manual_review(self):
        for overrides in (
            {"f_metric_type": "derived"},
            {"f_metric_type": "composite"},
            {"f_calculation_formula": '{"aggregation":{"aggr":"sum","property":"amount"},"condition":{"field":"region","value":"EU"}}'},
        ):
            with self.subTest(overrides=overrides):
                self.assertEqual(MODULE.translate(legacy_metric(**overrides)).action, "manual_review")

    def test_malformed_and_unknown_shapes_never_abort_batch(self):
        for overrides in (
            {"f_calculation_formula": "{"},
            {"f_calculation_formula": "[]"},
            {"f_calculation_formula": '{"aggregation":[]}'},
            {"f_calculation_formula": '{"aggregation":{"aggr":"sum","property":"amount"},"unknown":"semantic"}'},
            {"f_analysis_dimensions": "{"},
            {"f_analysis_dimensions": '[{"name":3}]'},
            {"f_time_dimension": '{"property":"paid_at","unknown":"policy"}'},
        ):
            with self.subTest(overrides=overrides):
                self.assertEqual(MODULE.translate(legacy_metric(**overrides)).action, "manual_review")

    def test_does_not_invent_decimal_rounding(self):
        decision = MODULE.translate(legacy_metric(
            f_calculation_formula='{"aggregation":{"aggr":"sum","property":"amount"}}',
            f_unit_type="currencyUnit", f_unit="CNY",
        ))

        self.assertEqual(decision.action, "manual_review")
        self.assertIn("rounding", decision.reason)

    def test_count_unit_is_valid_ucum_dimensionless_code(self):
        decision = MODULE.translate(legacy_metric())

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.definition["result_semantics"]["value"]["unit"], {
            "system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1,
        })

    def test_incompatible_count_unit_requires_review(self):
        decision = MODULE.translate(legacy_metric(f_unit_type="currencyUnit", f_unit="CNY"))

        self.assertEqual(decision.action, "manual_review")

    def test_defaults_to_openbkn_database(self):
        with mock.patch.object(sys, "argv", ["migrate_object_metrics.py"]), mock.patch.dict(
            os.environ, {}, clear=True
        ):
            self.assertEqual(MODULE.arguments().database, "openbkn")

    def test_identifies_legacy_metric_logic_properties(self):
        bindings, malformed = MODULE.legacy_metric_bindings([{
            "f_kn_id": "kn-1", "f_branch": "main", "f_id": "orders",
            "f_logic_properties": '[{"name":"total","type":"metric","data_source":{"type":"metric","id":"m-1"}}]',
        }])
        self.assertEqual(bindings[("kn-1", "main", "m-1")], ["orders.total"])
        self.assertEqual(malformed, set())
        decision = MODULE.review_bound_metric(
            {"f_kn_id": "kn-1", "f_branch": "main", "f_id": "m-1"},
            MODULE.Decision("m-1", "migrate", "lossless atomic metric", {}), bindings, malformed,
        )
        self.assertEqual(decision.action, "manual_review")
        self.assertIn("orders.total", decision.reason)

    def test_malformed_logic_properties_are_not_silently_ignored(self):
        bindings, malformed = MODULE.legacy_metric_bindings([{
            "f_kn_id": "kn-1", "f_branch": "main", "f_id": "orders", "f_logic_properties": "{",
        }])
        self.assertEqual(bindings, {})
        self.assertEqual(malformed, {("kn-1", "main")})


if __name__ == "__main__":
    unittest.main()
