import importlib.util
import json
import os
from pathlib import Path
import sys
import subprocess
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
        "f_kn_id": "kn-1",
        "f_branch": "main",
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

    def test_treats_empty_legacy_time_dimension_as_absent(self):
        for time_dimension in ("null", "{}", '{"property":null,"default_range_policy":"none"}'):
            with self.subTest(time_dimension=time_dimension):
                decision = MODULE.translate(legacy_metric(f_time_dimension=time_dimension))
                self.assertEqual(decision.action, "migrate")
                self.assertEqual(decision.definition["query_capabilities"]["time_dimensions"], [])

    def test_treats_json_null_analysis_dimensions_as_empty(self):
        decision = MODULE.translate(legacy_metric(f_analysis_dimensions="null"))
        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.definition["query_capabilities"]["analysis_dimensions"], [])

    def test_requires_manual_review_for_default_range_policy(self):
        decision = MODULE.translate(legacy_metric(
            f_time_dimension='{"property":"paid_at","default_range_policy":"last_1h"}',
        ))

        self.assertEqual(decision.action, "manual_review")

    def test_marks_legacy_derived_metrics_for_manual_review(self):
        for overrides in (
            {"f_metric_type": "derived"},
            {"f_metric_type": "composite"},
        ):
            with self.subTest(overrides=overrides):
                self.assertEqual(MODULE.translate(legacy_metric(**overrides)).action, "manual_review")

    def test_omits_having_as_query_time_condition(self):
        decision = MODULE.translate(legacy_metric(
            f_calculation_formula='{"aggregation":{"aggr":"count"},"having":{"field":"__value","operation":">","value":1}}',
        ))

        self.assertEqual(decision.action, "migrate")
        self.assertIn("having omitted as query-time condition", decision.lost_capabilities)

    def test_having_does_not_override_other_manual_review_reasons(self):
        decision = MODULE.translate(legacy_metric(
            f_calculation_formula='{"aggregation":{"aggr":"count"},"group_by":[{"property":"region"}],"having":{"field":"__value","operation":">","value":1}}',
        ))

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.lost_capabilities, [
            "having omitted as query-time condition",
            "group_by omitted as query-time analysis setting",
        ])

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

    def test_applies_explicit_default_decimal_precision(self):
        decision = MODULE.translate(legacy_metric(
            f_calculation_formula='{"aggregation":{"aggr":"sum","property":"amount"}}',
            f_unit_type="currencyUnit", f_unit="CNY",
        ))

        self.assertEqual(decision.action, "migrate")
        value = decision.definition["result_semantics"]["value"]
        self.assertEqual(value["physical_type"], "decimal")
        self.assertEqual(value["precision"], 18)
        self.assertEqual(value["scale"], 2)
        self.assertEqual(value["rounding_mode"], "half_up")
        self.assertEqual(value["unit"]["system"], "ISO_4217")

    def test_converts_condition_to_pre_aggregation_derived_metric(self):
        decision = MODULE.translate(
            legacy_metric(
                f_calculation_formula='{"aggregation":{"aggr":"sum","property":"amount"},"condition":{"field":"status","operation":"!=","value_from":"const","value":"cancelled","RemainCfg":null}}',
                f_unit_type="currencyUnit", f_unit="CNY",
            ),
            {"amount": "decimal", "status": "string"},
        )

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(len(decision.definitions), 2)
        base, derived = decision.definitions
        self.assertEqual(base["metric_type"], "atomic")
        self.assertEqual(derived["metric_type"], "derived")
        self.assertEqual(derived["id"], "metric-1")
        self.assertEqual(derived["specification"]["base_metric"]["metric_id"], base["id"])
        self.assertEqual(
            derived["specification"]["business_qualifiers"]["filters"]["comparison_operator"],
            "neq",
        )
        self.assertEqual(
            derived["specification"]["business_qualifiers"]["filters"]["right"],
            [{"source": "literal", "value_type": "string", "value": "cancelled"}],
        )

    def test_preserves_last_24_hours_as_derived_time_window(self):
        decision = MODULE.translate(legacy_metric(
            f_time_dimension='{"property":"paid_at","default_range_policy":"last_24h"}',
        ))

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(len(decision.definitions), 2)
        qualifiers = decision.definition["specification"]["business_qualifiers"]
        self.assertEqual(qualifiers["event_time"]["property_name"], "paid_at")
        self.assertEqual(qualifiers["time_window"], {"mode": "rolling", "length": 24, "unit": "hour"})

    def test_defaults_missing_count_unit_to_ucum_one(self):
        for unit_type, unit in (("numUnit", "none"), (None, None), ("countUnit", "")):
            with self.subTest(unit_type=unit_type, unit=unit):
                decision = MODULE.translate(legacy_metric(f_unit_type=unit_type, f_unit=unit))
                self.assertEqual(decision.action, "migrate")
                self.assertEqual(decision.definition["result_semantics"]["value"]["unit"], {
                    "system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1,
                })
                self.assertIn("missing count unit defaulted to UCUM 1", decision.normalizations)

    def test_converts_integer_and_boolean_condition_literals_by_property_type(self):
        cases = (
            ('{"field":"age","operation":">","value_from":"const","value":"90","RemainCfg":null}', "integer", 90, "int64"),
            ('{"field":"active","operation":"==","value_from":"const","value":"true","RemainCfg":null}', "boolean", True, "boolean"),
        )
        for condition, property_type, expected, value_type in cases:
            with self.subTest(property_type=property_type):
                decision = MODULE.translate(
                    legacy_metric(f_calculation_formula='{"aggregation":{"aggr":"count"},"condition":' + condition + '}'),
                    {"age": "integer", "active": "boolean"},
                )
                self.assertEqual(decision.action, "migrate")
                literal = decision.definition["specification"]["business_qualifiers"]["filters"]["right"][0]
                self.assertEqual(literal["value"], expected)
                self.assertEqual(literal["value_type"], value_type)

    def test_missing_condition_property_is_not_silently_migrated(self):
        decision = MODULE.translate(
            legacy_metric(f_calculation_formula='{"aggregation":{"aggr":"count"},"condition":{"field":"missing","operation":"==","value_from":"const","value":"x","RemainCfg":null}}'),
            {},
        )
        self.assertEqual(decision.action, "manual_review")
        self.assertIn("property does not exist", decision.reason)

    def test_count_unit_is_valid_ucum_dimensionless_code(self):
        decision = MODULE.translate(legacy_metric())

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.definition["result_semantics"]["value"]["unit"], {
            "system": "UCUM", "quantity_kind": "count", "code": "1", "scale_factor": 1,
        })

    def test_preserves_legacy_count_unit_label(self):
        decision = MODULE.translate(legacy_metric(f_unit="piece"))

        self.assertEqual(decision.action, "migrate")
        self.assertEqual(decision.definition["result_semantics"]["value"]["unit"], {
            "system": "registered", "quantity_kind": "count", "code": "piece", "scale_factor": 1,
        })

    def test_incompatible_count_unit_requires_review(self):
        decision = MODULE.translate(legacy_metric(f_unit_type="currencyUnit", f_unit="CNY"))

        self.assertEqual(decision.action, "manual_review")

    def test_does_not_fall_back_to_a_local_database(self):
        with mock.patch.object(sys, "argv", ["migrate_object_metrics.py"]), mock.patch.dict(
            os.environ, {}, clear=True
        ):
            args = MODULE.arguments()
            self.assertIsNone(args.host)
            self.assertIsNone(args.database)
            self.assertEqual(args.namespace, "openbkn")
            self.assertEqual(args.release, "bkn-backend")

    def test_discovers_database_settings_from_helm_release(self):
        helm_values = {
            "depServices": {"rds": {
                "host": "mariadb.resource.svc.cluster.local",
                "port": 3306,
                "user": "openbkn",
                "password": "sensitive-value",
                "database": "openbkn",
            }}
        }
        completed = subprocess.CompletedProcess([], 0, json.dumps(helm_values), "")
        with mock.patch.object(sys, "argv", ["migrate_object_metrics.py"]), \
                mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/helm"), \
                mock.patch.object(MODULE.subprocess, "run", return_value=completed) as run:
            args = MODULE.arguments()
            MODULE.resolve_database_config(args)

        self.assertEqual(args.host, "mariadb.resource.svc.cluster.local")
        self.assertEqual(args.port, 3306)
        self.assertEqual(args.user, "openbkn")
        self.assertEqual(args.password, "sensitive-value")
        self.assertEqual(args.database, "openbkn")
        run.assert_called_once_with([
            "helm", "get", "values", "bkn-backend", "--namespace", "openbkn",
            "--output", "json",
        ], check=False, capture_output=True, text=True)

    def test_complete_environment_configuration_skips_helm(self):
        environment = {
            "BKN_DB_HOST": "db.example.com",
            "BKN_DB_PORT": "3307",
            "BKN_DB_USER": "metric-reader",
            "BKN_DB_PASSWORD": "env-secret",
            "BKN_DB_NAME": "openbkn",
        }
        with mock.patch.object(sys, "argv", ["migrate_object_metrics.py"]), \
                mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.object(MODULE.shutil, "which") as which:
            args = MODULE.arguments()
            MODULE.resolve_database_config(args)

        self.assertEqual(args.host, "db.example.com")
        self.assertEqual(args.port, 3307)
        self.assertEqual(args.user, "metric-reader")
        self.assertEqual(args.password, "env-secret")
        which.assert_not_called()

    def test_cli_overrides_environment_and_helm_fills_missing_fields(self):
        helm_values = {"depServices": {"rds": {
            "host": "helm-db", "port": 3306, "user": "helm-user",
            "password": "helm-secret", "database": "helm-db-name",
        }}}
        completed = subprocess.CompletedProcess([], 0, json.dumps(helm_values), "")
        environment = {"BKN_DB_HOST": "environment-db", "BKN_DB_USER": "environment-user"}
        with mock.patch.object(sys, "argv", [
            "migrate_object_metrics.py", "--host", "cli-db", "--user", "cli-user",
        ]), mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/helm"), \
                mock.patch.object(MODULE.subprocess, "run", return_value=completed):
            args = MODULE.arguments()
            MODULE.resolve_database_config(args)

        self.assertEqual(args.host, "cli-db")
        self.assertEqual(args.user, "cli-user")
        self.assertEqual(args.port, 3306)
        self.assertEqual(args.password, "helm-secret")
        self.assertEqual(args.database, "helm-db-name")

    def test_helm_context_namespace_and_release_are_forwarded(self):
        helm_values = {"depServices": {"rds": {
            "host": "db", "port": 3306, "user": "user",
            "password": "secret", "database": "openbkn",
        }}}
        completed = subprocess.CompletedProcess([], 0, json.dumps(helm_values), "")
        with mock.patch.object(sys, "argv", [
            "migrate_object_metrics.py", "--namespace", "tenant", "--release", "custom-backend",
            "--context", "cluster-a",
        ]), mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/helm"), \
                mock.patch.object(MODULE.subprocess, "run", return_value=completed) as run:
            args = MODULE.arguments()
            MODULE.resolve_database_config(args)

        self.assertEqual(args.kube_context, "cluster-a")
        self.assertEqual(run.call_args.args[0], [
            "helm", "get", "values", "custom-backend", "--namespace", "tenant",
            "--output", "json", "--kube-context", "cluster-a",
        ])

    def test_helm_failure_does_not_fall_back_to_local_mysql_or_log_password(self):
        failed = subprocess.CompletedProcess([], 1, "", "failed, password=should-not-appear")
        with mock.patch.object(sys, "argv", ["migrate_object_metrics.py"]), \
                mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/helm"), \
                mock.patch.object(MODULE.subprocess, "run", return_value=failed):
            args = MODULE.arguments()
            with self.assertRaises(RuntimeError) as raised:
                MODULE.resolve_database_config(args)

        self.assertIn("failed to read Helm values", str(raised.exception))
        self.assertNotIn("should-not-appear", str(raised.exception))
        self.assertIsNone(args.host)

    def test_recognizes_cluster_service_dns_for_database(self):
        self.assertEqual(
            MODULE.cluster_database_service("mariadb.resource.svc.cluster.local"),
            ("mariadb", "resource"),
        )
        self.assertIsNone(MODULE.cluster_database_service("mysql.customer.example.com"))

    def test_collects_legacy_metric_logic_property_locations_for_report(self):
        bindings, malformed = MODULE.legacy_metric_bindings([{
            "f_kn_id": "kn-1", "f_branch": "main", "f_id": "orders",
            "f_logic_properties": '[{"name":"total","type":"metric","data_source":{"type":"metric","id":"m-1"}}]',
        }])
        self.assertEqual(bindings[("kn-1", "main", "m-1")], ["orders.total"])
        self.assertEqual(malformed, set())

    def test_malformed_logic_properties_are_not_silently_ignored(self):
        bindings, malformed = MODULE.legacy_metric_bindings([{
            "f_kn_id": "kn-1", "f_branch": "main", "f_id": "orders", "f_logic_properties": "{",
        }])
        self.assertEqual(bindings, {})
        self.assertEqual(malformed, {("kn-1", "main")})

    def test_json_null_logic_properties_mean_no_bindings(self):
        bindings, malformed = MODULE.legacy_metric_bindings([{
            "f_kn_id": "kn-1", "f_branch": "main", "f_id": "orders", "f_logic_properties": "null",
        }])
        self.assertEqual(bindings, {})
        self.assertEqual(malformed, set())


if __name__ == "__main__":
    unittest.main()
