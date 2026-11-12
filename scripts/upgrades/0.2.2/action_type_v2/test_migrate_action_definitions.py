# Copyright openbkn.ai

import copy
import argparse
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from migrate_action_definitions import (
    MigrationError, apply_plan, apply_snapshot_path, convert_document, load_rollback_snapshots,
    main, migrate_action, migration_report, plan_rows, private_json,
    ROLLBACK_DEFAULT, cluster_database_service, configure_artifact_paths, resolve_database_config,
    rollback_snapshot_paths, select_rollback_changes, validate_artifact_paths, write_migration_snapshot,
)


class Connection:
    def __init__(self, rowcounts=(1,)):
        self.rowcounts = iter(rowcounts)
        self.commands = []
        self.committed = self.rolled_back = self.begun = False
        self.rowcount = 0

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, values):
        self.commands.append((sql, values))
        self.rowcount = next(self.rowcounts)

    def begin(self):
        self.begun = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class ActionMigrationTests(unittest.TestCase):
    def test_default_paths_separate_dry_run_apply_and_rollback(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {"OPENBKN_MIGRATION_WORKDIR": directory}, clear=True,
        ):
            args = argparse.Namespace(
                input=None, rollback=None, apply=False, output=None, report=None,
                host="db.example.com", port=3306, database="openbkn", namespace="openbkn",
                release="bkn-backend", kube_context="cluster", kn_id=None,
            )
            configure_artifact_paths(args)
            self.assertEqual("dry-run-plan.json", args.output.name)
            self.assertEqual("dry-run-report.json", args.report.name)
            self.assertEqual(0o700, args.output.parent.stat().st_mode & 0o777)
            target = args.output.parent
            args.apply, args.output, args.report = True, None, None
            configure_artifact_paths(args)
            self.assertEqual(target / "rollback.json", args.output)
            self.assertEqual(target / "apply-report.json", args.report)
            args.rollback, args.output, args.report = ROLLBACK_DEFAULT, None, None
            configure_artifact_paths(args)
            self.assertEqual(target / "rollback.json", args.rollback)
            self.assertEqual(target / "rollback-result.json", args.output)
            self.assertEqual(target / "rollback-report.json", args.report)

    def test_offline_cli_generates_paths_without_output_or_report_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "actions.json"
            source.write_text(json.dumps({"action_types": [self.old_action()]}))
            environment = dict(os.environ, OPENBKN_MIGRATION_WORKDIR=str(root / "artifacts"))
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("migrate_action_definitions.py")), "--input", str(source)],
                capture_output=True, text=True, env=environment,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            paths = dict(line.split(": ", 1) for line in result.stdout.splitlines())
            self.assertTrue(Path(paths["converted"]).exists())
            self.assertEqual(["notify"], json.loads(Path(paths["report"]).read_text())["changed"])

    def test_deploy_database_discovery_uses_explicit_values_before_helm(self):
        args = argparse.Namespace(
            host="db.example.com", port=3307, user="cli-user", password="cli-password",
            database="cli-db", namespace="openbkn", release="bkn-backend", kube_context="",
        )
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "migrate_action_definitions.helm_database_values"
        ) as helm:
            resolve_database_config(args)
        helm.assert_not_called()
        self.assertEqual(("db.example.com", 3307, "cli-user", "cli-db"),
                         (args.host, args.port, args.user, args.database))

    def test_deploy_database_discovery_fills_only_missing_values(self):
        args = argparse.Namespace(
            host=None, port=None, user="cli-user", password=None, database=None,
            namespace="openbkn", release="bkn-backend", kube_context="cluster",
        )
        values = {"host": "db.openbkn.svc", "port": "3306", "password": "secret", "database": "openbkn"}
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "migrate_action_definitions.helm_database_values", return_value=values,
        ) as helm:
            resolve_database_config(args)
        helm.assert_called_once_with(args, ["host", "port", "password", "database"])
        self.assertEqual("cli-user", args.user)
        self.assertEqual(("db", "openbkn"), cluster_database_service(args.host))
        self.assertIsNone(cluster_database_service("10.109.185.14"))

    def old_action(self, bound=True):
        return {
            "id": "notify", "name": "Notify", "object_type_id": "orders" if bound else "",
            "action_type": "modify", "parameters": [{"name": "message", "type": "string", "value_from": "input"}],
            "action_source": {"type": "mcp", "mcp_id": "m1", "tool_name": "notify"},
        }

    def row(self):
        return {
            "f_id": "notify", "f_name": "Notify", "f_kn_id": "kn", "f_branch": "main", "f_action_type": "modify",
            "f_action_intent": "", "f_object_type_id": "orders", "f_operation_config": None,
            "f_parameters": json.dumps(self.old_action()["parameters"]), "f_condition": None,
            "f_bkn_raw_content": "---\ntype: action_type\n---\n## ActionType: Notify\n", "f_update_time": 123,
        }

    def test_bound_and_independent_definitions_convert_once(self):
        for bound, kind in ((True, "object"), (False, "tool")):
            source = self.old_action(bound)
            before = copy.deepcopy(source)
            migrated = migrate_action(source)
            self.assertEqual({"type": kind}, migrated["operation_config"])
            self.assertEqual("modify", migrated["action_intent"])
            self.assertEqual(source, before)
            self.assertEqual(migrated, migrate_action(migrated))
            self.assertEqual(source["action_source"], migrated["action_source"])

    def test_invalid_definitions_report_without_silent_value_loss(self):
        source = self.old_action(False)
        source["parameters"][0] = {"name": "id", "value_from": "property", "value": "id"}
        with self.assertRaises(MigrationError):
            migrate_action(source)
        bindings = [{"id": "send", "capability_type": "function", "sources": [{"kind": "manual"}]}]
        document = {"object_types": [{"id": "orders"}], "capability_bindings": bindings,
                    "action_types": [self.old_action(), source]}
        converted, report = convert_document(document)
        self.assertEqual(["notify"], report["changed"])
        self.assertEqual(1, len(report["errors"]))
        self.assertEqual(document["object_types"], converted["object_types"])
        self.assertEqual(bindings, converted["capability_bindings"])
        self.assertNotIn("operation_config", document["action_types"][0])

    def test_typed_constants_preserve_json_types_and_exact_integers(self):
        action = self.old_action()
        action["parameters"] = [
            {"name": "flag", "type": "boolean", "value_from": "const", "value": "false"},
            {"name": "id", "type": "integer", "value_from": "const", "value": "9223372036854775807"},
            {"name": "text", "type": "string", "value_from": "const", "value": "false"},
        ]
        values = [item["value"] for item in migrate_action(action)["parameters"]]
        self.assertEqual([False, 9223372036854775807, "false"], values)
        action["parameters"][0]["value"] = '"false"'
        with self.assertRaises(MigrationError): migrate_action(action)

    def test_incomplete_migrated_definitions_and_scheduled_targets_are_refused(self):
        action = self.old_action()
        action["operation_config"] = {"type": "relation", "source_parameter": "id"}
        with self.assertRaises(MigrationError): migrate_action(action)
        action = self.old_action()
        action["schedule"] = {"cron": "* * * * *"}
        with self.assertRaises(MigrationError): migrate_action(action)

    def test_implicit_mcp_inputs_require_an_explicit_tool_schema_snapshot(self):
        action = self.old_action(False)
        action["parameters"] = []
        with self.assertRaises(MigrationError): migrate_action(action)
        action["tool_input_schema"] = {"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}
        migrated = migrate_action(action)
        self.assertEqual([{"name": "message", "type": "string", "value_from": "input"}], migrated["parameters"])
        self.assertNotIn("tool_input_schema", migrated)
        row = self.row()
        row["f_object_type_id"] = ""
        row["f_parameters"] = "[]"
        row["f_action_source"] = json.dumps({"type": "mcp", "mcp_id": "m1", "tool_name": "send"})
        self.assertTrue(plan_rows([row])[1])
        plan, errors = plan_rows([row], {("kn", "main", "notify"): action["tool_input_schema"]})
        self.assertFalse(errors)
        self.assertEqual("message", json.loads(plan[0]["after"]["f_parameters"])[0]["name"])

    def test_legacy_missing_types_are_recovered_before_constant_conversion(self):
        action = self.old_action(False)
        action["parameters"] = [
            {"name": "body.count", "value_from": "const", "value": "0"},
            {"name": "enabled", "value_from": "input"},
        ]
        with self.assertRaises(MigrationError):
            migrate_action(action)
        action["tool_input_schema"] = {"type": "object", "properties": {
            "body": {"type": "object", "properties": {"count": {"type": "integer"}}},
            "enabled": {"type": "boolean"},
        }}
        migrated = migrate_action(action)
        self.assertEqual(0, migrated["parameters"][0]["value"])
        self.assertEqual("integer", migrated["parameters"][0]["type"])
        self.assertEqual("boolean", migrated["parameters"][1]["type"])
        self.assertNotIn("tool_input_schema", migrated)
        self.assertNotIn("type", action["parameters"][0])

    def test_missing_or_ambiguous_schema_types_are_not_guessed(self):
        action = self.old_action(False)
        action["parameters"] = [{"name": "message", "value_from": "input"}]
        for field in ({"$ref": "#/schemas/Message"}, {"oneOf": [{"type": "string"}, {"type": "number"}]}, {"type": ["string", "null"]}):
            action["tool_input_schema"] = {"type": "object", "properties": {"message": field}}
            with self.assertRaises(MigrationError):
                migrate_action(action)


    def test_database_plan_refuses_implicit_scheduled_targets(self):
        row = self.row()
        row["_unsafe_schedule_ids"] = ["timer1"]
        changes, errors = plan_rows([row])
        self.assertEqual([], changes)
        self.assertIn("scheduled", errors[0]["reason"])

    def test_skipped_report_identifies_action_tool_and_reason_without_values(self):
        good = self.row()
        bad = {**self.row(), "f_id": "broken", "f_name": "发送通知"}
        bad["f_action_source"] = json.dumps({"type": "mcp", "mcp_id": "server-1", "tool_name": "send", "token": "secret"})
        bad["f_parameters"] = json.dumps([{"name": "message", "value_from": "const", "value": "private value"}])
        changes, errors = plan_rows([good, bad])
        self.assertEqual(["notify"], [item["id"] for item in changes])
        self.assertEqual("发送通知", errors[0]["name"])
        self.assertEqual({"type": "mcp", "mcp_id": "server-1", "tool_name": "send"}, errors[0]["tool_source"])
        report = migration_report(changes, errors, applied=True)
        self.assertEqual(1, report["applied_count"])
        self.assertTrue(report["partial"])
        self.assertEqual(errors, report["skipped_actions"])
        self.assertNotIn("private value", json.dumps(report))
        self.assertNotIn("secret", json.dumps(report))

    def test_malformed_tool_source_is_reported_without_stopping_other_rows(self):
        bad = {**self.row(), "f_id": "broken", "f_name": "坏工具", "f_action_source": "[1]"}
        changes, errors = plan_rows([self.row(), bad])
        self.assertEqual(["notify"], [item["id"] for item in changes])
        self.assertEqual("坏工具", errors[0]["name"])
        self.assertEqual({}, errors[0]["tool_source"])
        self.assertIn("action_source", errors[0]["reason"])

    def test_partial_batches_have_distinct_snapshots_and_safe_default_rollback(self):
        first, _ = plan_rows([self.row()])
        second_row = {**self.row(), "f_id": "other", "f_name": "Other"}
        second, _ = plan_rows([second_row])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(output=root / "rollback.json", output_explicit=False)
            first_path = apply_snapshot_path(args, first, [{"id": "broken"}])
            second_path = apply_snapshot_path(args, second, [{"id": "broken"}])
            self.assertNotEqual(first_path, second_path)
            write_migration_snapshot(first_path, first)
            write_migration_snapshot(second_path, second)
            paths = rollback_snapshot_paths(root)
            self.assertEqual({first_path, second_path}, set(paths))
            all_changes = load_rollback_snapshots(paths)
            current = [{**item["before"], **item["after"]} for item in all_changes]
            selected, already = select_rollback_changes(all_changes, current)
            self.assertEqual(2, len(selected))
            self.assertEqual([], already)
            current[0] = all_changes[0]["before"]
            selected, already = select_rollback_changes(all_changes, current)
            self.assertEqual(1, len(selected))
            self.assertEqual([all_changes[0]["id"]], already)
            current[1]["f_parameters"] = "edited"
            with self.assertRaisesRegex(MigrationError, "changed after migration"):
                select_rollback_changes(all_changes, current)
            write_migration_snapshot(root / "rollback.json", first)
            with self.assertRaisesRegex(MigrationError, "multiple rollback snapshots"):
                load_rollback_snapshots(rollback_snapshot_paths(root))

    def test_cli_partial_apply_writes_valid_actions_and_reports_skipped_tools(self):
        good = self.row()
        bad = {**self.row(), "f_id": "broken", "f_name": "发送通知"}
        bad["f_action_source"] = json.dumps({"type": "mcp", "mcp_id": "server-1", "tool_name": "send"})
        bad["f_parameters"] = json.dumps([{"name": "message", "value_from": "input"}])

        class ReadOnlyDatabase:
            def cursor(self): return self
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, sql, values=None): self.sql = sql
            def fetchone(self): return {"Field": "f_operation_config"}
            def fetchall(self): return [] if "t_action_schedule" in self.sql else [good, bad]
            def close(self): pass

        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {"OPENBKN_MIGRATION_WORKDIR": directory}, clear=True,
        ), mock.patch.dict(sys.modules, {"pymysql": mock.Mock()}), mock.patch(
            "migrate_action_definitions.connect_database", return_value=(ReadOnlyDatabase(), None),
        ), mock.patch("migrate_action_definitions.stop_database_port_forward"), mock.patch(
            "migrate_action_definitions.apply_plan"
        ) as applied, mock.patch("sys.stdout", new_callable=io.StringIO):
            report = Path(directory) / "report.json"
            arguments = ["migrate_action_definitions.py", "--host", "db", "--port", "3306",
                         "--user", "test", "--password", "test", "--database", "test", "--report", str(report)]
            with mock.patch.object(sys, "argv", arguments + ["--apply"]):
                self.assertEqual(2, main())
            applied.assert_not_called()
            with mock.patch.object(sys, "argv", arguments + ["--apply", "--allow-partial"]):
                self.assertEqual(2, main())
            self.assertEqual(["notify"], [change["id"] for change in applied.call_args.args[1]])
            result = json.loads(report.read_text())
            self.assertEqual(1, result["applied_count"])
            self.assertEqual("发送通知", result["skipped_actions"][0]["name"])
            self.assertEqual("send", result["skipped_actions"][0]["tool_source"]["tool_name"])
            self.assertEqual(1, len(list(Path(directory).rglob("rollback-*.json"))))

    def test_plan_preserves_rollback_values_and_updates_bkn(self):
        row = self.row()
        changes, errors = plan_rows([row])
        self.assertEqual([], errors)
        self.assertEqual(row, changes[0]["before"])
        self.assertIn("### Operation Config", changes[0]["after"]["f_bkn_raw_content"])
        migrated_row = {**row, **changes[0]["after"]}
        self.assertEqual(([], []), plan_rows([migrated_row]))

    def test_legacy_create_condition_is_removed_from_data_and_raw_bkn(self):
        row = self.row()
        row["f_action_type"] = "add"
        row["f_condition"] = json.dumps({
            "object_type_id": "orders", "field": "id", "operation": "!=", "value": "1"
        })
        row["f_bkn_raw_content"] += (
            "### Trigger Condition\n\n```yaml\nfield: id\noperation: '!='\nvalue: '1'\n```\n\n"
            "### Action Source\n\n```yaml\ntype: mcp\n```\n"
        )
        changes, errors = plan_rows([row])
        self.assertEqual([], errors)
        after = changes[0]["after"]
        self.assertEqual("null", after["f_condition"])
        self.assertNotIn("### Trigger Condition", after["f_bkn_raw_content"])
        self.assertIn("### Action Source", after["f_bkn_raw_content"])
        self.assertEqual(row["f_bkn_raw_content"], changes[0]["before"]["f_bkn_raw_content"])
        self.assertEqual(
            [{"kn_id": "kn", "branch": "main", "id": "notify", "name": "Notify"}],
            migration_report(changes, [], applied=False)["removed_create_conditions"],
        )

    def test_legacy_create_condition_refuses_ambiguous_raw_bkn(self):
        row = self.row()
        row["f_action_type"] = "add"
        row["f_condition"] = json.dumps({"field": "id", "operation": "!=", "value": "1"})
        changes, errors = plan_rows([row])
        self.assertEqual([], changes)
        self.assertIn("raw BKN section", errors[0]["reason"])

    def test_legacy_create_removes_empty_raw_condition_section(self):
        row = self.row()
        row["f_action_type"] = "add"
        row["f_bkn_raw_content"] += "### Trigger Condition\n\n\n### Action Source\n"
        changes, errors = plan_rows([row])
        self.assertEqual([], errors)
        self.assertNotIn("### Trigger Condition", changes[0]["after"]["f_bkn_raw_content"])
        self.assertIn("### Action Source", changes[0]["after"]["f_bkn_raw_content"])
        self.assertEqual([], migration_report(changes, [], applied=False)["removed_create_conditions"])

    def test_empty_rerun_does_not_erase_original_rollback_snapshot(self):
        changes, _ = plan_rows([self.row()])
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "rollback.json"
            write_migration_snapshot(snapshot, changes)
            original = snapshot.read_bytes()
            write_migration_snapshot(snapshot, changes)
            write_migration_snapshot(snapshot, [])
            self.assertEqual(original, snapshot.read_bytes())
            changed = copy.deepcopy(changes)
            changed[0]["before"]["f_update_time"] += 1
            with self.assertRaises(MigrationError):
                write_migration_snapshot(snapshot, changed)
            self.assertEqual(original, snapshot.read_bytes())
            self.assertEqual(0o600, snapshot.stat().st_mode & 0o777)

    def test_artifact_paths_reject_input_snapshot_and_hardlink_collisions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output, report = root / "source.json", root / "output.json", root / "report.json"
            source.write_text("original data")
            validate_artifact_paths(output, report, [source])
            with self.assertRaises(MigrationError):
                validate_artifact_paths(output, output, [source])
            with self.assertRaises(MigrationError):
                validate_artifact_paths(source, report, [source])
            report.hardlink_to(source)
            with self.assertRaises(MigrationError):
                validate_artifact_paths(output, report, [source])
            self.assertEqual("original data", source.read_text())

    def test_apply_and_rollback_are_parameterized_and_optimistic(self):
        changes, _ = plan_rows([self.row()])
        forward = Connection()
        apply_plan(forward, changes)
        self.assertTrue(forward.committed)
        sql, values = forward.commands[0]
        self.assertIn("f_update_time = %s", sql)
        self.assertIn("f_operation_config <=> %s", sql)
        self.assertNotIn("notify", sql)
        self.assertIn("notify", values)
        backward = Connection()
        apply_plan(backward, changes, rollback=True)
        self.assertTrue(backward.committed)
        self.assertEqual(None, backward.commands[0][1][2])

    def test_concurrent_edit_rolls_back_the_entire_plan(self):
        row = self.row()
        other = {**row, "f_id": "other"}
        changes, _ = plan_rows([row, other])
        connection = Connection((1, 0))
        with self.assertRaises(MigrationError):
            apply_plan(connection, changes)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)

    def test_snapshots_are_private(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            private_json(path, {"changes": []})
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            self.assertEqual({"changes": []}, json.loads(path.read_text()))


if __name__ == "__main__":
    unittest.main()
