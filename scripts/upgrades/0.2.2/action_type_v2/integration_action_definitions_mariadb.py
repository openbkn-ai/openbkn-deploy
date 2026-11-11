# Copyright openbkn.ai

"""Exercise the real action migration CLI against an explicitly selected test DB.

Run with BKN_MIGRATION_TEST_DB_HOST/PORT/USER/PASSWORD. Each test creates and
removes its own randomly named action_migration_it_* database. The regular unit
test discovery does not collect this integration entry point. Requires PyMySQL.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
import uuid

from migrate_action_definitions import MigrationError, apply_plan, plan_rows

try:
    import pymysql
except ImportError:
    pymysql = None


DDL_ROOT = Path(os.environ.get("BKN_FOUNDRY_ROOT", ".")) / "migrations/bkn-backend/mariadb/0.2.2"
CLI = Path(__file__).with_name("migrate_action_definitions.py")


@unittest.skipUnless(os.environ.get("BKN_MIGRATION_TEST_DB_HOST") and os.environ.get("BKN_FOUNDRY_ROOT") and pymysql, "requires an explicit MariaDB test connection, BKN_FOUNDRY_ROOT and PyMySQL")
class ActionMigrationMariaDBTests(unittest.TestCase):
    def setUp(self):
        self.database = "action_migration_it_" + uuid.uuid4().hex
        self.connection_options = {
            "host": os.environ["BKN_MIGRATION_TEST_DB_HOST"],
            "port": int(os.environ.get("BKN_MIGRATION_TEST_DB_PORT", "3306")),
            "user": os.environ.get("BKN_MIGRATION_TEST_DB_USER", "root"),
            "password": os.environ.get("BKN_MIGRATION_TEST_DB_PASSWORD", ""),
            "charset": "utf8mb4",
            "autocommit": True,
            "cursorclass": pymysql.cursors.DictCursor,
        }
        self.admin = pymysql.connect(**self.connection_options)
        self.addCleanup(self.admin.close)
        with self.admin.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version")
            version = cursor.fetchone()["version"]
            self.assertIn("MariaDB", version)
            release = re.match(r"(\d+)\.(\d+)", version)
            self.assertIsNotNone(release)
            self.assertGreaterEqual(tuple(map(int, release.groups())), (11, 4))
            cursor.execute(f"CREATE DATABASE `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        self.addCleanup(self.drop_database)
        self.connection = pymysql.connect(database=self.database, **self.connection_options)
        self.addCleanup(self.connection.close)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.artifacts = Path(self.directory.name)
        self.create_tables_before_upgrade()

    def drop_database(self):
        with self.admin.cursor() as cursor:
            cursor.execute(f"DROP DATABASE `{self.database}`")

    def create_tables_before_upgrade(self):
        initialization = (DDL_ROOT / "init.sql").read_text(encoding="utf-8")
        with self.connection.cursor() as cursor:
            for name in ("t_action_type", "t_action_schedule"):
                statement = re.search(rf"CREATE TABLE IF NOT EXISTS {name} \(.*?;", initialization, re.S).group()
                if name == "t_action_type":
                    statement = re.sub(r"^.*f_operation_config.*\n", "", statement, flags=re.M)
                cursor.execute(statement)

    def upgrade_schema(self):
        sql = (DDL_ROOT / "02-action-operation.sql").read_text(encoding="utf-8")
        with self.connection.cursor() as cursor:
            cursor.execute(sql)

    def seed_action(self, identifier="notify", *, network="kn", bound=False, implicit_mcp=False):
        parameters = [] if implicit_mcp else [
            {"name": "enabled", "type": "boolean", "value_from": "const", "value": "false"},
            {"name": "reference", "type": "integer", "value_from": "const", "value": "9223372036854775807"},
        ]
        row = {
            "f_id": identifier, "f_name": identifier, "f_kn_id": network, "f_branch": "main",
            "f_action_type": "modify", "f_action_intent": "", "f_object_type_id": "orders" if bound else "",
            "f_comment": "Migration integration fixture", "f_bkn_raw_content": "---\ntype: action_type\n---\n## ActionType: Notify\n",
            "f_action_source": json.dumps({"type": "mcp", "mcp_id": "m1", "tool_name": "notify"}),
            "f_parameters": json.dumps(parameters), "f_update_time": 123,
        }
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO t_action_type (" + ",".join(row) + ") VALUES (" + ",".join(["%s"] * len(row)) + ")", list(row.values()))

    def rows(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM t_action_type ORDER BY f_kn_id, f_id")
            return cursor.fetchall()

    def run_cli(self, *arguments, expected=0):
        environment = dict(os.environ)
        environment.update({
            "BKN_DB_HOST": self.connection_options["host"],
            "BKN_DB_PORT": str(self.connection_options["port"]),
            "BKN_DB_USER": self.connection_options["user"],
            "BKN_DB_PASSWORD": self.connection_options["password"],
            "BKN_DB_NAME": self.database,
        })
        command = [sys.executable, str(CLI), "--output", str(self.artifacts / "snapshot.json"), "--report", str(self.artifacts / "report.json"), *arguments]
        result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(expected, result.returncode, result.stderr)
        return json.loads((self.artifacts / "report.json").read_text())

    def test_schema_cli_apply_rerun_and_rollback_restore_original_rows(self):
        self.seed_action("object", bound=True)
        self.seed_action("tool")
        self.upgrade_schema()
        self.upgrade_schema()
        original = self.rows()
        dry_run = self.run_cli()
        self.assertFalse(dry_run["applied"])
        self.assertEqual(original, self.rows())
        snapshot = (self.artifacts / "snapshot.json").read_bytes()
        applied = self.run_cli("--apply")
        self.assertTrue(applied["applied"])
        migrated = self.rows()
        self.assertEqual(["object", "tool"], [json.loads(row["f_operation_config"])["type"] for row in migrated])
        for row in migrated:
            self.assertEqual("modify", row["f_action_intent"])
            values = [item["value"] for item in json.loads(row["f_parameters"])]
            self.assertIs(values[0], False)
            self.assertEqual(9223372036854775807, values[1])
            self.assertIn("### Operation Config", row["f_bkn_raw_content"])
        rerun = self.run_cli("--apply")
        self.assertEqual([], rerun["planned"])
        self.assertEqual(snapshot, (self.artifacts / "snapshot.json").read_bytes())
        self.run_cli("--rollback", str(self.artifacts / "snapshot.json"), "--output", str(self.artifacts / "rollback-result.json"), "--apply")
        self.assertEqual(original, self.rows())

    def test_conflicts_rollback_all_changes_in_both_directions(self):
        self.seed_action("first")
        self.seed_action("second")
        self.upgrade_schema()
        plan, errors = plan_rows(self.rows())
        self.assertFalse(errors)
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE t_action_type SET f_update_time = 124 WHERE f_id = 'second'")
        concurrent = self.rows()
        with self.assertRaises(MigrationError):
            apply_plan(self.connection, plan)
        self.assertEqual(concurrent, self.rows())
        plan, errors = plan_rows(self.rows())
        self.assertFalse(errors)
        apply_plan(self.connection, plan)
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE t_action_type SET f_parameters = '[]' WHERE f_id = 'second'")
        edited = self.rows()
        with self.assertRaises(MigrationError):
            apply_plan(self.connection, plan, rollback=True)
        self.assertEqual(edited, self.rows())

    def test_scoped_conversion_resolves_implicit_mcp_inputs_from_schema(self):
        self.seed_action("tool", implicit_mcp=True)
        self.seed_action("outside", network="other")
        self.upgrade_schema()
        refused = self.run_cli("--kn-id", "kn", "--apply", expected=2)
        self.assertTrue(refused["errors"])
        self.assertTrue(all(row["f_operation_config"] is None for row in self.rows()))
        schemas = self.artifacts / "schemas.json"
        schemas.write_text(json.dumps([{"kn_id": "kn", "branch": "main", "action_type_id": "tool", "input_schema": {"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}}]))
        self.run_cli("--kn-id", "kn", "--tool-schemas", str(schemas), "--output", str(self.artifacts / "resolved-snapshot.json"), "--apply")
        first, outside = self.rows()
        self.assertEqual([{"name": "message", "type": "string", "value_from": "input"}], json.loads(first["f_parameters"]))
        self.assertIsNone(outside["f_operation_config"])

    def test_implicit_scheduled_target_prevents_conversion(self):
        self.seed_action("scheduled", bound=True)
        self.upgrade_schema()
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO t_action_schedule (f_id, f_name, f_kn_id, f_branch, f_action_type_id, f_cron_expression, f_instance_identities) VALUES ('s1','s1','kn','main','scheduled','* * * * *','[]')")
        report = self.run_cli("--apply", expected=2)
        self.assertIn("scheduled", report["errors"][0]["reason"])
        self.assertIsNone(self.rows()[0]["f_operation_config"])

    def test_legacy_parameter_types_are_recovered_before_apply(self):
        self.seed_action("tool")
        parameters = [
            {"name": "body.count", "value_from": "const", "value": "0"},
            {"name": "enabled", "value_from": "input"},
        ]
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE t_action_type SET f_parameters = %s", (json.dumps(parameters),))
        self.upgrade_schema()
        refused = self.run_cli("--apply", expected=2)
        self.assertTrue(refused["errors"])
        schemas = self.artifacts / "schemas.json"
        schemas.write_text(json.dumps([{
            "kn_id": "kn", "branch": "main", "action_type_id": "tool",
            "input_schema": {"type": "object", "properties": {
                "body": {"type": "object", "properties": {"count": {"type": "integer"}}},
                "enabled": {"type": "boolean"},
            }},
        }]))
        self.run_cli("--tool-schemas", str(schemas), "--output", str(self.artifacts / "resolved-snapshot.json"), "--apply")
        migrated = json.loads(self.rows()[0]["f_parameters"])
        self.assertEqual("integer", migrated[0]["type"])
        self.assertIs(type(migrated[0]["value"]), int)
        self.assertEqual(0, migrated[0]["value"])
        self.assertEqual("boolean", migrated[1]["type"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
