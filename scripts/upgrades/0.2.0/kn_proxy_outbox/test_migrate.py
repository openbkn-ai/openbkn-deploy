# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import argparse
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import migrate


class MigrationEntryTest(unittest.TestCase):
    def test_control_commands_delegate_to_versioned_script(self):
        args = argparse.Namespace(
            command="stop",
            namespace="openbkn",
            expected_context="test-context",
            state_file="/tmp/state.tsv",
            timeout_seconds=30,
        )
        result = MagicMock(returncode=0)
        with patch.object(migrate.subprocess, "run", return_value=result) as run:
            self.assertEqual(0, migrate.control_services(args))
        command = run.call_args.args[0]
        self.assertTrue(command[0].endswith("service_control.sh"))
        self.assertIn("test-context", command)

    def test_verify_stopped_checks_desired_and_observed_replicas(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kubectl = root / "kubectl"
            kubectl.write_text(
                """#!/usr/bin/env bash
if [[ $1 == config && $2 == current-context ]]; then
  echo test-context
elif [[ \" $* \" == *\" scale deployment \"* ]]; then
  if [[ -n ${FAKE_SCALE_FAILURE:-} ]]; then
    echo \"simulated scale failure\" >&2
    exit 1
  fi
elif [[ \" $* \" == *\" -o jsonpath=\"* ]]; then
  echo \"${FAKE_DEPLOYMENT_STATE}\"
fi
""",
                encoding="utf-8",
            )
            kubectl.chmod(0o755)
            state_file = root / "state.tsv"
            state_file.write_text(
                "openbkn-kn-proxy-outbox-workload-v1\topenbkn\ttest-context\tbkn-backend\t2\n",
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment["PATH"] = f"{root}:{environment['PATH']}"
            command = [
                str(Path(migrate.__file__).resolve().parent / "service_control.sh"),
                "verify-stopped",
                "--state-file",
                str(state_file),
                "--timeout-seconds",
                "1",
            ]
            environment["FAKE_DEPLOYMENT_STATE"] = "0 0 0"
            stopped = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            environment["FAKE_DEPLOYMENT_STATE"] = "1 0 0"
            scaling_up = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            stop_command = [*command]
            stop_command[1] = "stop"
            environment["FAKE_DEPLOYMENT_STATE"] = "0 0 0"
            original_state = state_file.read_text(encoding="utf-8")
            retried_stop = subprocess.run(
                stop_command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            state_after_retry = state_file.read_text(encoding="utf-8")
            environment["FAKE_SCALE_FAILURE"] = "1"
            failed_stop = subprocess.run(
                stop_command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
        self.assertEqual(0, stopped.returncode, stopped.stderr)
        self.assertNotEqual(0, scaling_up.returncode)
        self.assertIn("desired=1", scaling_up.stderr)
        self.assertEqual(0, retried_stop.returncode, retried_stop.stderr)
        self.assertEqual(original_state, state_after_retry)
        self.assertNotEqual(0, failed_stop.returncode)
        self.assertIn("simulated scale failure", failed_stop.stderr)

    def test_apply_accepts_no_command_options(self):
        args = migrate.build_parser().parse_args(["apply"])
        self.assertEqual("apply", args.command)
        self.assertFalse(hasattr(args, "report"))
        self.assertFalse(hasattr(args, "confirm_bkn_stopped"))

    def test_apply_report_path_is_generated_in_user_migration_directory(self):
        now = datetime(2026, 9, 29, 10, 30, 0, 123456, tzinfo=timezone.utc)
        report = Path(migrate.default_report_path("apply", now))
        self.assertEqual(
            Path.home()
            / ".openbkn-ai"
            / "migrations"
            / "0.2.0"
            / "kn_proxy_outbox",
            report.parent,
        )
        self.assertEqual(
            "proxy-outbox-apply-20260929T103000123456Z.json", report.name
        )

    def test_stopped_check_uses_standard_deployment_state(self):
        with patch.object(migrate, "control_services", return_value=0) as control:
            migrate.verify_bkn_stopped()
        args = control.call_args.args[0]
        self.assertEqual("verify-stopped", args.command)
        self.assertEqual("openbkn", args.namespace)
        self.assertEqual(migrate.DEFAULT_STATE_FILE, args.state_file)

    def test_stop_and_start_use_the_recorded_standard_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = str(Path(directory) / "state.tsv")
            with patch.object(migrate, "DEFAULT_STATE_FILE", state_file):
                with patch.object(migrate, "control_services", return_value=0) as control:
                    migrate.stop_bkn()
                    migrate.start_bkn()
        self.assertEqual("stop", control.call_args_list[0].args[0].command)
        self.assertEqual("start", control.call_args_list[1].args[0].command)
        self.assertEqual(state_file, control.call_args_list[0].args[0].state_file)
        self.assertEqual(state_file, control.call_args_list[1].args[0].state_file)

    def test_stop_retries_with_an_existing_state_file(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.tsv"
            state_file.write_text("recorded state", encoding="utf-8")
            with patch.object(migrate, "DEFAULT_STATE_FILE", str(state_file)):
                with patch.object(migrate, "control_services", return_value=0) as control:
                    migrate.stop_bkn()
        self.assertEqual(1, control.call_count)
        self.assertEqual("stop", control.call_args.args[0].command)
        self.assertEqual(str(state_file), control.call_args.args[0].state_file)

    def test_stop_failure_reports_an_actionable_error(self):
        with patch.object(migrate, "control_services", return_value=1):
            with self.assertRaisesRegex(
                migrate.kn_proxy_outbox.MigrationError,
                "fix the reported Kubernetes error and rerun apply",
            ):
                migrate.stop_bkn()

    def test_apply_main_uses_generated_report_path(self):
        def run(command):
            return {"command": command.command}

        with patch("sys.argv", ["migrate.py", "apply"]):
            with patch.object(migrate, "run", side_effect=run):
                with patch.object(
                    migrate,
                    "default_report_path",
                    return_value="/tmp/proxy-outbox-apply-generated.json",
                ):
                    with patch.object(migrate, "prepare_report_destination"):
                        with patch.object(migrate, "stop_bkn") as stop:
                            with patch.object(migrate, "verify_bkn_stopped") as stopped:
                                with patch.object(migrate, "start_bkn") as start:
                                    with patch.object(
                                        migrate, "write_report"
                                    ) as write_report:
                                        self.assertEqual(0, migrate.main())
        stop.assert_called_once_with()
        stopped.assert_called_once_with()
        start.assert_called_once_with()
        write_report.assert_called_once_with(
            "/tmp/proxy-outbox-apply-generated.json",
            {
                "command": "apply",
                "preflight": {"command": "dry-run"},
            },
        )

    def test_apply_leaves_bkn_stopped_when_stopped_verification_fails(self):
        commands = []

        def run(command):
            commands.append(command.command)
            return {"command": command.command}

        with patch("sys.argv", ["migrate.py", "apply"]):
            with patch.object(migrate, "prepare_report_destination"):
                with patch.object(migrate, "run", side_effect=run):
                    with patch.object(migrate, "stop_bkn"):
                        with patch.object(
                            migrate,
                            "verify_bkn_stopped",
                            side_effect=migrate.kn_proxy_outbox.MigrationError(
                                "not stopped"
                            ),
                        ):
                            with patch.object(migrate, "start_bkn") as start:
                                self.assertEqual(1, migrate.main())
        self.assertEqual(["dry-run"], commands)
        start.assert_not_called()

    def test_apply_report_failure_does_not_misreport_committed_migration(self):
        def run(command):
            return {"command": command.command}

        with patch("sys.argv", ["migrate.py", "apply"]):
            with patch.object(migrate, "default_report_path", return_value="report.json"):
                with patch.object(migrate, "prepare_report_destination"):
                    with patch.object(migrate, "stop_bkn"):
                        with patch.object(migrate, "verify_bkn_stopped"):
                            with patch.object(migrate, "run", side_effect=run):
                                with patch.object(
                                    migrate,
                                    "write_report",
                                    side_effect=OSError("disk full"),
                                ):
                                    with patch.object(migrate, "start_bkn") as start:
                                        with patch("builtins.print") as output:
                                            self.assertEqual(0, migrate.main())
        self.assertIn("migration completed", output.call_args.args[0])
        start.assert_called_once_with()

    def test_apply_runs_the_full_workflow_in_order(self):
        events = []

        def run(command):
            events.append(f"run:{command.command}")
            return {"command": command.command}

        with patch("sys.argv", ["migrate.py", "apply"]):
            with patch.object(migrate, "default_report_path", return_value="report.json"):
                with patch.object(migrate, "prepare_report_destination"):
                    with patch.object(
                        migrate, "stop_bkn", side_effect=lambda: events.append("stop")
                    ):
                        with patch.object(
                            migrate,
                            "verify_bkn_stopped",
                            side_effect=lambda: events.append("verify-stopped"),
                        ):
                            with patch.object(migrate, "run", side_effect=run):
                                with patch.object(
                                    migrate,
                                    "write_report",
                                    side_effect=lambda *_: events.append("write-report"),
                                ):
                                    with patch.object(
                                        migrate,
                                        "start_bkn",
                                        side_effect=lambda: events.append("start"),
                                    ):
                                        self.assertEqual(0, migrate.main())
        self.assertEqual(
            [
                "run:dry-run",
                "stop",
                "verify-stopped",
                "run:apply",
                "write-report",
                "start",
            ],
            events,
        )

    def test_apply_runs_initialize_and_verify_before_commit(self):
        args = argparse.Namespace(command="apply")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        events = []
        connection.commit.side_effect = lambda: events.append("commit")
        with patch.object(migrate.kn_proxy_outbox.DBConfig, "from_environment"):
            with patch.object(
                migrate.kn_proxy_outbox, "connect", return_value=connection
            ):
                with patch.object(
                    migrate.kn_proxy_outbox,
                    "precheck",
                    side_effect=lambda *_: events.append("precheck") or {},
                ):
                    with patch.object(
                        migrate.kn_proxy_outbox,
                        "initialize",
                        side_effect=lambda *_: events.append("initialize") or {},
                    ):
                        with patch.object(
                            migrate.kn_proxy_outbox,
                            "verify",
                            side_effect=lambda *_: events.append("verify") or {},
                        ):
                            migrate.run(args)
        self.assertEqual(["precheck", "initialize", "verify", "commit"], events)
        connection.rollback.assert_not_called()
        connection.close.assert_called_once_with()

    def test_report_destination_is_checked_before_database_work(self):
        with tempfile.TemporaryDirectory() as directory:
            parent_file = Path(directory) / "not-a-directory"
            parent_file.write_text("file", encoding="utf-8")
            report = parent_file / "report.json"
            with self.assertRaisesRegex(Exception, "not writable"):
                migrate.prepare_report_destination(str(report))

    def test_report_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_text("existing", encoding="utf-8")
            with self.assertRaisesRegex(Exception, "refusing to overwrite"):
                migrate.write_report(str(report), {"ok": True})


if __name__ == "__main__":
    unittest.main()
