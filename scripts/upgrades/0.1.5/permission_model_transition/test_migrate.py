# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import argparse
import base64
import contextlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import migrate


class StepRegistryTest(unittest.TestCase):
    def test_bundled_authorization_migrators_match_their_platform(self):
        # Executable magic and CPU type fields: ELF e_machine, Mach-O cputype.
        expected_headers = {
            "linux-amd64": (b"\x7fELF", slice(18, 20), 0x3E),
            "linux-arm64": (b"\x7fELF", slice(18, 20), 0xB7),
            "darwin-arm64": (b"\xcf\xfa\xed\xfe", slice(4, 8), 0x0100000C),
        }
        executables = {
            platform: migrate.SCRIPT_DIRECTORY / "authz_migrate" / f"authz-migrate-{platform}"
            for platform in expected_headers
        }

        if not any(executable.exists() for executable in executables.values()):
            self.skipTest("source checkout intentionally excludes the release executable")
        for platform, executable in executables.items():
            magic, cpu_field, cpu_type = expected_headers[platform]
            with self.subTest(platform=platform):
                self.assertTrue(executable.is_file())
                self.assertTrue(os.access(executable, os.X_OK))
                header = executable.read_bytes()[:20]
                self.assertEqual(magic, header[:4])
                self.assertEqual(cpu_type, int.from_bytes(header[cpu_field], "little"))

    def test_selects_the_bundled_authorization_migrator_for_the_host(self):
        directory = migrate.SCRIPT_DIRECTORY / "authz_migrate"
        cases = {
            ("Linux", "x86_64"): "authz-migrate-linux-amd64",
            ("Linux", "aarch64"): "authz-migrate-linux-arm64",
            ("Linux", "arm64"): "authz-migrate-linux-arm64",
            ("Darwin", "arm64"): "authz-migrate-darwin-arm64",
        }

        for (system, machine), name in cases.items():
            with self.subTest(system=system, machine=machine):
                self.assertEqual(
                    directory / name, migrate.bundled_authz_migrator(system, machine)
                )

    def test_x86_64_python_on_apple_silicon_selects_the_arm64_migrator(self):
        directory = migrate.SCRIPT_DIRECTORY / "authz_migrate"
        cases = {"1\n": "authz-migrate-darwin-arm64", "": "authz-migrate-darwin-amd64"}

        for sysctl_output, name in cases.items():
            completed = MagicMock(returncode=0, stdout=sysctl_output, stderr="")
            with self.subTest(sysctl_output=sysctl_output), patch.object(
                migrate.platform, "machine", return_value="x86_64"
            ), patch.object(migrate.subprocess, "run", return_value=completed) as run:
                self.assertEqual(directory / name, migrate.bundled_authz_migrator("Darwin"))
                self.assertEqual(["/usr/sbin/sysctl", "-n", "hw.optional.arm64"], run.call_args.args[0])

        with patch.object(migrate.platform, "machine", return_value="x86_64"), patch.object(
            migrate.subprocess, "run"
        ) as run:
            self.assertEqual(
                directory / "authz-migrate-linux-amd64", migrate.bundled_authz_migrator("Linux")
            )
            run.assert_not_called()

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
    def test_json_decode_failure_does_not_expose_sensitive_stdout(self):
        result = MagicMock(returncode=0, stdout="database-password", stderr="")
        with patch.object(migrate.subprocess, "run", return_value=result):
            with self.assertRaises(migrate.OrchestrationError) as raised:
                migrate.run_json_command(
                    ["kubectl", "get", "secret", "bkn-safe-secrets"],
                    "read the Safe database secret",
                )

        self.assertNotIn("database-password", str(raised.exception))

    def test_discovers_the_unique_bkn_safe_namespace(self):
        inventory = [
            {
                "name": "bkn-safe",
                "namespace": "customer-openbkn",
                "chart": "bkn-safe-0.1.5",
            }
        ]
        with patch.object(migrate, "run_json_command", return_value=inventory):
            self.assertEqual(
                "customer-openbkn", migrate.discover_target_namespace("")
            )

    def test_discovers_database_settings_from_installed_cluster_resources(self):
        bkn_values = {
            "depServices": {
                "rds": {
                    "host": "bkn-db",
                    "port": 3306,
                    "user": "bkn-user",
                    "password": "bkn-password",
                    "database": "openbkn",
                }
            }
        }
        vega_values = {
            "depServices": {
                "rds": {
                    "host": "vega-db",
                    "port": 3307,
                    "user": "vega-user",
                    "password": "vega-password",
                    "database": "vega",
                }
            }
        }
        safe_config = {
            "data": {
                "SAFE_DB_TYPE": "MySQL",
                "SAFE_DB_HOST": "safe-db",
                "SAFE_DB_PORT": "3308",
                "SAFE_DB_USER": "safe-user",
                "SAFE_DB_NAME": "safe",
            }
        }
        safe_secret = {
            "data": {
                "SAFE_DB_PASSWORD": base64.b64encode(b"safe-password").decode()
            }
        }

        with patch.object(
            migrate,
            "helm_release_values",
            side_effect=[bkn_values, vega_values],
        ), patch.object(
            migrate,
            "kubernetes_resource",
            side_effect=[safe_config, safe_secret],
        ):
            environment = migrate.discover_database_environment("openbkn")

        self.assertEqual("bkn-db", environment["BKN_DB_HOST"])
        self.assertEqual("vega", environment["VEGA_DB_NAME"])
        self.assertEqual("safe-db", environment["SAFE_DB_HOST"])
        self.assertEqual("safe-password", environment["SAFE_DB_PASSWORD"])

    def test_installs_discovered_database_settings_over_stale_shell_values(self):
        with patch.dict(
            os.environ,
            {"SAFE_CONFIG": "/stale/config.yaml", "BKN_DB_HOST": "stale"},
            clear=True,
        ):
            migrate.install_database_environment(
                {"BKN_DB_HOST": "installed", "SAFE_DB_PASSWORD": "secret"}
            )
            self.assertNotIn("SAFE_CONFIG", os.environ)
            self.assertEqual("installed", os.environ["BKN_DB_HOST"])
            self.assertEqual("secret", os.environ["SAFE_DB_PASSWORD"])

    def test_reuses_one_tunnel_for_shared_cluster_database_service(self):
        environment = {}
        for prefix in ("BKN_DB", "VEGA_DB", "SAFE_DB"):
            environment[f"{prefix}_HOST"] = "mariadb.resource.svc.cluster.local"
            environment[f"{prefix}_PORT"] = "3306"
        process = MagicMock()
        with patch.object(
            migrate, "available_local_port", return_value=43306
        ), patch.object(
            migrate.subprocess, "Popen", return_value=process
        ) as popen, patch.object(
            migrate, "wait_for_port_forward"
        ), patch.object(
            migrate, "stop_port_forward"
        ) as stop:
            with migrate.database_access_environment(environment) as effective:
                for prefix in ("BKN_DB", "VEGA_DB", "SAFE_DB"):
                    self.assertEqual("127.0.0.1", effective[f"{prefix}_HOST"])
                    self.assertEqual("43306", effective[f"{prefix}_PORT"])

        popen.assert_called_once()
        stop.assert_called_once_with(process)

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

    def test_upgrade_discovers_configuration_and_runs_once_without_user_inputs(self):
        args = argparse.Namespace(
            namespace="", expected_context="", authz_migrator="/tmp/authz-migrate"
        )
        calls = []

        def record_migration(command):
            calls.append(command.command)
            return 0

        def record_control(command):
            calls.append(command.command)
            return 0

        database_environment = {"BKN_DB_HOST": "database"}
        with tempfile.TemporaryDirectory() as directory, patch.object(
            migrate, "automatic_run_directory", return_value=Path(directory)
        ), patch.object(
            migrate, "discover_target_namespace", return_value="openbkn"
        ), patch.object(
            migrate, "installed_target_version", return_value="0.1.5"
        ), patch.object(
            migrate,
            "discover_database_environment",
            return_value=database_environment,
        ), patch.object(
            migrate,
            "database_access_environment",
            return_value=contextlib.nullcontext(database_environment),
        ), patch.object(
            migrate, "install_database_environment"
        ) as install_environment, patch.object(
            migrate, "run_migration", side_effect=record_migration
        ), patch.object(
            migrate, "control_services", side_effect=record_control
        ):
            self.assertEqual(0, migrate.run_upgrade(args))

        install_environment.assert_called_once_with(database_environment)
        self.assertEqual("openbkn", args.namespace)
        self.assertEqual(["dry-run", "stop", "apply", "start"], calls)


if __name__ == "__main__":
    unittest.main()
