#!/usr/bin/env python3
"""Offline mirror regressions under the installer's errexit settings."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
HARNESS = r'''
set -euo pipefail
source "$MIRROR_TEST_SOURCE"
log_info() { printf 'INFO: %s\n' "$*"; }
log_warn() { printf 'WARN: %s\n' "$*"; }
log_error() { printf 'ERROR: %s\n' "$*"; }
curl() {
    local url="${!#}"
    printf '%s\n' "$url" >> "$MIRROR_TEST_CALLS"
    case "$url" in
        https://first.example/*)
            printf '%s' "$MIRROR_TEST_FIRST_CODE"
            return "$MIRROR_TEST_FIRST_EXIT"
            ;;
        https://second.example/*)
            printf '%s' "$MIRROR_TEST_SECOND_CODE"
            return "$MIRROR_TEST_SECOND_EXIT"
            ;;
        *) return 99 ;;
    esac
}
_openbkn_resolve_latest_manifest() { :; }
_openbkn_require_version_manifest() { :; }
_openbkn_apply_default_set_values() { :; }
ensure_platform_prerequisites() {
    echo 'platform prerequisites reached'
    # Stop here so this test can never perform a real deployment.
    return 1
}
diagnose_cluster_context() { :; }
OFFLINE_MODE=false
CORE_DOCKERHUB_MIRROR="$MIRROR_TEST_MODE"
_install_openbkn_impl
'''


class DockerhubMirrorTest(unittest.TestCase):
    def run_install(self, first_exit=0, first_code="200", second_exit=0,
                    second_code="200", mode="auto"):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        certs = root / "certs.d"
        config = root / "config.toml"
        config.write_text(f'config_path = "{certs}"\n')
        source = (ROOT / "scripts/services/openbkn.sh").read_text()
        # Sandbox only the root check and config location; retain the actual
        # probe, hosts.toml writer and installer control flow on non-root CI too.
        source = source.replace('${EUID:-$(id -u)}', '0')
        source = source.replace('/etc/containerd/config.toml', str(config))
        fixture = root / "openbkn.sh"
        fixture.write_text(source)
        calls = root / "calls"
        env = {
            **os.environ,
            "MIRROR_TEST_SOURCE": str(fixture),
            "MIRROR_TEST_CALLS": str(calls),
            "MIRROR_TEST_FIRST_EXIT": str(first_exit),
            "MIRROR_TEST_FIRST_CODE": first_code,
            "MIRROR_TEST_SECOND_EXIT": str(second_exit),
            "MIRROR_TEST_SECOND_CODE": second_code,
            "MIRROR_TEST_MODE": mode,
            "OPENBKN_DOCKERHUB_MIRROR_CANDIDATES": "first.example second.example",
        }
        result = subprocess.run(["bash", "-c", HARNESS], env=env,
                                capture_output=True, text=True, timeout=10)
        self.assertIn("platform prerequisites reached", result.stdout,
                      result.stdout + result.stderr)
        self.assertEqual(result.returncode, 1)  # Intentional prerequisite stop.
        hosts = certs / "docker.io/hosts.toml"
        return result.stdout, calls.read_text().splitlines() if calls.exists() else [], hosts

    def test_transport_failure_continues_to_working_mirror(self):
        for exit_code in (28, 7):
            with self.subTest(curl_exit=exit_code):
                output, calls, hosts = self.run_install(exit_code, "000")
                self.assertEqual(len(calls), 2)
                self.assertIn(f"probe first.example failed (curl exit {exit_code}", output)
                self.assertIn("selected second.example", output)
                self.assertIn('[host."https://second.example"]', hosts.read_text())

    def test_failed_transfer_with_http_200_is_not_selected(self):
        output, calls, hosts = self.run_install(28, "200")
        self.assertEqual(len(calls), 2)
        self.assertIn("selected second.example", output)
        self.assertIn('[host."https://second.example"]', hosts.read_text())

    def test_http_rejection_continues_and_first_success_stops_probing(self):
        for status, expected, count in (("403", "second.example", 2),
                                        ("200", "first.example", 1)):
            with self.subTest(http_status=status):
                output, calls, hosts = self.run_install(first_code=status)
                self.assertEqual(len(calls), count)
                self.assertIn(f"selected {expected}", output)
                self.assertIn(f'[host."https://{expected}"]', hosts.read_text())

    def test_all_unavailable_warns_and_continues_without_writing_mirror(self):
        for second_exit, second_code in ((7, "000"), (0, "503")):
            with self.subTest(second_exit=second_exit):
                output, calls, hosts = self.run_install(28, "000", second_exit, second_code)
                self.assertEqual(len(calls), 2)
                self.assertIn("WARN: dockerhub-mirror=auto: no candidate mirror reachable", output)
                self.assertFalse(hosts.exists())

    def test_explicit_host_and_off_do_not_probe(self):
        for mode in ("first.example", "off"):
            with self.subTest(mode=mode):
                _, calls, hosts = self.run_install(mode=mode)
                self.assertEqual(calls, [])
                self.assertEqual(hosts.exists(), mode != "off")


if __name__ == "__main__":
    unittest.main()
