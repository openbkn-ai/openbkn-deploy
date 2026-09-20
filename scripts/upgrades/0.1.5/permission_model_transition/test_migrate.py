# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import migrate


class StepRegistryTest(unittest.TestCase):
    def test_bundled_authorization_migrator_is_executable(self):
        executable = migrate.SCRIPT_DIRECTORY / "authz_migrate" / "authz-migrate"

        if not executable.exists():
            self.skipTest("source checkout intentionally excludes the release executable")
        self.assertTrue(executable.is_file())
        self.assertTrue(os.access(executable, os.X_OK))

    def test_registers_data_steps_before_authorization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "authz-migrate"
            executable.write_text("binary", encoding="utf-8")
            executable.chmod(0o700)
            args = argparse.Namespace(
                command="dry-run",
                authz_migrator=str(executable),
            )

            steps = migrate.build_steps(args, root / "reports")

        self.assertEqual(
            ["bkn-data", "vega-data", "authorization"],
            [step.name for step in steps],
        )
        self.assertIn("bkn_data.py", steps[0].command[1])
        self.assertTrue(steps[1].command[1].endswith("vega/vega_data.py"))
        self.assertEqual("dry-run", steps[2].command[2])


class OrchestrationTest(unittest.TestCase):
    def test_apply_requires_the_stop_replica_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(
                command="apply",
                state_file=str(root / "missing.tsv"),
                namespace="openbkn",
                expected_context="",
                report_dir=str(root / "reports"),
            )

            with patch.object(migrate, "installed_target_version", return_value="0.1.5"), self.assertRaisesRegex(
                migrate.OrchestrationError, "run the unified stop command first"
            ):
                migrate.run_migration(args)

    def test_runs_registered_steps_in_order_and_writes_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.tsv"
            state.write_text("replica snapshot", encoding="utf-8")
            report_dir = root / "reports"
            steps = [
                migrate.Step("bkn-data", ("bkn",), report_dir / "01.json"),
                migrate.Step("vega-data", ("vega",), report_dir / "02.json"),
                migrate.Step("authorization", ("safe",), report_dir / "03.json"),
            ]
            args = argparse.Namespace(
                command="apply",
                state_file=str(state),
                namespace="openbkn",
                expected_context="",
                report_dir=str(report_dir),
            )
            calls = []

            def complete(step):
                calls.append(step.name)
                step.report_path.write_text("{}", encoding="utf-8")

            with patch.object(migrate, "installed_target_version", return_value="0.1.5"), patch.object(migrate, "require_stopped_workloads") as stopped, patch.object(
                migrate, "build_steps", return_value=steps
            ), patch.object(migrate, "run_step", side_effect=complete):
                self.assertEqual(0, migrate.run_migration(args))

            stopped.assert_called_once_with(args)
            self.assertEqual(["bkn-data", "vega-data", "authorization"], calls)
            summary = json.loads((report_dir / "summary.json").read_text())
            self.assertTrue(all(step["completed"] for step in summary["steps"]))

    def test_failure_stops_before_the_next_step_and_records_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_dir = root / "reports"
            steps = [
                migrate.Step("bkn-data", ("bkn",), report_dir / "01.json"),
                migrate.Step("vega-data", ("vega",), report_dir / "02.json"),
                migrate.Step("authorization", ("safe",), report_dir / "03.json"),
            ]
            args = argparse.Namespace(
                command="dry-run",
                state_file=str(root / "unused.tsv"),
                namespace="openbkn",
                expected_context="",
                report_dir=str(report_dir),
            )

            with patch.object(migrate, "installed_target_version", return_value="0.1.5"), patch.object(migrate, "build_steps", return_value=steps), patch.object(
                migrate,
                "run_step",
                side_effect=migrate.OrchestrationError("bkn failed"),
            ) as run_step:
                with self.assertRaisesRegex(migrate.OrchestrationError, "bkn failed"):
                    migrate.run_migration(args)

            run_step.assert_called_once_with(steps[0])
            summary = json.loads((report_dir / "summary.json").read_text())
            self.assertEqual("bkn failed", summary["error"])
            self.assertFalse(summary["steps"][1]["completed"])

    def test_reads_and_validates_the_installed_bkn_safe_chart_version(self):
        inventory = '[{"name":"bkn-safe","chart":"bkn-safe-0.1.5"}]'
        completed = type("Completed", (), {"returncode": 0, "stdout": inventory, "stderr": ""})()
        with patch.object(migrate.subprocess, "run", return_value=completed):
            self.assertEqual("0.1.5", migrate.installed_target_version("openbkn"))

        completed.stdout = '[{"name":"bkn-safe","chart":"bkn-safe-0.1.4"}]'
        with patch.object(migrate.subprocess, "run", return_value=completed), self.assertRaisesRegex(
            migrate.OrchestrationError, "install bkn-safe 0.1.5"
        ):
            migrate.installed_target_version("openbkn")

    def test_upgrade_runs_stop_dry_run_apply_and_start_without_user_inputs(self):
        args = argparse.Namespace(
            namespace="openbkn", expected_context="", authz_migrator="/tmp/authz-migrate"
        )
        calls = []

        def record_migration(command):
            calls.append(command.command)
            return 0

        def record_control(command):
            calls.append(command.command)
            return 0

        with tempfile.TemporaryDirectory() as directory, patch.object(
            migrate, "automatic_run_directory", return_value=Path(directory)
        ), patch.object(
            migrate, "installed_target_version", return_value="0.1.5"
        ), patch.object(migrate, "run_migration", side_effect=record_migration), patch.object(
            migrate, "control_services", side_effect=record_control
        ):
            self.assertEqual(0, migrate.run_upgrade(args))

        self.assertEqual(["stop", "dry-run", "apply", "start"], calls)


if __name__ == "__main__":
    unittest.main()
