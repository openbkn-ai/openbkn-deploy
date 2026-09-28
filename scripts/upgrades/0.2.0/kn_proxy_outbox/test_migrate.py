# Copyright openbkn.ai
#
# Licensed under the OpenBKN License. See LICENSE-OPENBKN.txt in the project root.

import argparse
import tempfile
import unittest
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

    def test_apply_requires_explicit_stopped_confirmation(self):
        args = argparse.Namespace(
            command="apply", report="unused.json", confirm_bkn_stopped=False
        )
        with self.assertRaisesRegex(Exception, "confirm-bkn-stopped"):
            migrate.run(args)

    def test_report_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            report.write_text("existing", encoding="utf-8")
            with self.assertRaisesRegex(Exception, "refusing to overwrite"):
                migrate.write_report(str(report), {"ok": True})


if __name__ == "__main__":
    unittest.main()
