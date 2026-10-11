"""Offline Helm staging, actual merge, and cleanup safety regressions."""
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import audit_helm_render as helper

FIXTURE = Path(__file__).resolve().parents[1] / "testdata" / "audit-install"


class RenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="openbkn-audit.")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        (self.directory / ".audit-owner").touch()

    def test_owned_cleanup_only(self):
        other = self.directory / "unowned"
        other.mkdir()
        with self.assertRaises(ValueError):
            helper.cleanup(other)
        self.assertTrue(other.exists())
        helper.cleanup(self.directory)
        self.assertFalse(self.directory.exists())

    def test_archive_rejects_traversal_and_links(self):
        for index, (name, kind) in enumerate((("../escape", tarfile.REGTYPE), ("chart/link", tarfile.SYMTYPE))):
            archive = self.directory / f"bad-{index}.tgz"
            with tarfile.open(archive, "w:gz") as package:
                member = tarfile.TarInfo(name)
                member.type = kind
                member.linkname = "../../outside"
                package.addfile(member)
            with self.assertRaises(ValueError):
                helper.extract(archive, self.directory / f"out-{index}")
        self.assertFalse((self.directory.parent / "escape").exists())

    def test_archive_extracts_one_root(self):
        archive = self.directory / "good.tgz"
        with tarfile.open(archive, "w:gz") as package:
            package.add(FIXTURE, arcname="agent-observability")
        root = helper.extract(archive, self.directory / "out")
        self.assertTrue((root / "Chart.yaml").is_file())

    def test_probe_removed_on_failure_and_error_sanitized(self):
        target = self.directory / "chart"
        shutil.copytree(FIXTURE, target)
        with patch.object(helper, "run_helm", side_effect=ValueError("test failure")):
            with self.assertRaises(ValueError):
                helper.render(self.directory, "actual", ["upgrade", "--install", "agent-observability", str(target)])
        self.assertFalse((target / "templates" / "openbkn-audit-values-probe.yaml").exists())
        result = type("Result", (), {"returncode": 1, "stdout": "private", "stderr": "private"})()
        with patch.object(helper.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(ValueError, "without logging credentials") as error:
                helper.run_helm(["template"])
        self.assertNotIn("private", str(error.exception))

    @unittest.skipUnless(shutil.which("helm"), "real Helm is required")
    def test_real_helm_merge_and_explicit_input(self):
        args = ["upgrade", "--install", "agent-observability", str(FIXTURE), "--namespace", "openbkn"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            helper.stage(self.directory, args)
        args[3] = output.getvalue().strip()
        values = self.directory / "values.json"
        values.write_text(json.dumps({"resources": {"requests": {"cpu": "50m"}}}), encoding="utf-8")
        args += ["-f", str(values), "--set", "resources.requests.cpu=0m", "--set-string", "image.tag=123"]
        with patch.dict(os.environ, {"KUBECONFIG": str(self.directory / "no-cluster")}), contextlib.redirect_stdout(io.StringIO()) as output:
            helper.render(self.directory, "actual", args.copy())
        actual = json.loads(output.getvalue())
        self.assertEqual(actual["config"]["resources"]["requests"], {"cpu": "0m", "memory": "128Mi"})
        self.assertEqual(actual["config"]["image"]["tag"], "123")
        self.assertEqual(actual["chart"]["metadata"]["version"], "0.1.4")
        with contextlib.redirect_stdout(io.StringIO()) as output:
            helper.render(self.directory, "explicit", args.copy())
        self.assertEqual(json.loads(output.getvalue())["config"]["resources"]["requests"], {"cpu": "0m"})
        for target in (FIXTURE, Path(args[3])):
            self.assertFalse((target / "templates" / "openbkn-audit-values-probe.yaml").exists())

    @unittest.skipUnless(os.environ.get("AUDIT_PRODUCT_CHART"), "set AUDIT_PRODUCT_CHART for the optional real Chart contract check")
    def test_real_product_chart_contract(self):
        from audit_consumer import NON_AUDIT_KEYS, defaults
        args = ["upgrade", "--install", "agent-observability", os.environ["AUDIT_PRODUCT_CHART"], "--namespace", "openbkn"]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            helper.stage(self.directory, args)
        args[3] = output.getvalue().strip()
        values = defaults("broker:9092", "PLAIN", "existing-client")
        values["core"].update(store="mariadb", mariadb={"existingSecret": "core-db"})
        values_path = self.directory / "values.json"
        values_path.write_text(json.dumps(values), encoding="utf-8")
        args += ["-f", str(values_path)]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            helper.render(self.directory, "actual", args.copy())
        effective = json.loads(output.getvalue())["config"]
        self.assertTrue(effective["kafkaConsumers"]["audit"]["enabled"])
        self.assertTrue(effective["auditPublisher"]["enabled"])
        self.assertEqual(effective["auditPublisher"]["environment"], "development")
        self.assertFalse(set(effective) - set(NON_AUDIT_KEYS) - {"auditPublisher"})
        manifests = helper.run_helm(["template", *args[2:]])
        self.assertRegex(manifests, r'BKN_TRACE_AUDIT_KAFKA_ENABLED\s+value: "true"')
        self.assertRegex(manifests, r'BKN_AUDIT_KAFKA_ENABLED\s+value: "true"')
        self.assertRegex(manifests, r'BKN_AUDIT_ENVIRONMENT\s+value: "development"')
        self.assertTrue("openbkn-audit-values-probe" not in manifests)


if __name__ == "__main__":
    unittest.main()
